# Clavis

Clavis is a Raft-backed distributed lock service that provides globally monotonic fencing tokens.

It is built for systems where "only one worker should do this" is not enough on its own. The harder problem is what happens *after* a pause, partition, or failover: how does a downstream system know whether a write is coming from the current owner or a stale one? Clavis answers that with fencing tokens.

## The Problem

In a distributed system, a worker acquires a lock, then writes to a database or external system. But the worker can pause (GC, scheduling, network), and during that pause another worker can acquire the same lock. When the first worker resumes, it still thinks it holds the lock and writes stale data.

Plain mutual exclusion does not prevent this. The downstream system has no way to distinguish a current writer from a stale one.

## How Clavis Solves It

Every time a lock is acquired, Clavis increments a cluster-wide counter and returns the new value as a **fencing token**. The token is globally monotonic: it never decreases and is never reused.

A downstream system (database, queue, external API) can store the highest token it has accepted and reject any write carrying a lower token. That makes stale writes impossible, regardless of timing.

```
Worker A acquires lock    -> token = 5
Worker A pauses
Worker B acquires lock    -> token = 6, writes to DB with token 6
Worker A resumes, writes  -> DB sees token 5 < 6, rejects the write
```

## Use Cases

- A controller or scheduler that should have exactly one active instance
- A migration runner that must not double-apply work
- A workflow executor that touches external systems
- A job worker writing to a database where stale writes would corrupt state

## Design

Clavis is a CP system. It prioritizes consistent lock state over availability during partitions or leader loss. All mutating operations go through the Raft leader, commit to a quorum, and apply to a deterministic state machine before becoming visible.

The system is structured as a narrow stack:

```
gRPC API
  Service Layer
    Cluster / Raft
      Deterministic FSM
        BoltDB Persistence
```

Leases and locks are separate concepts. A **lease** is a time-bounded session kept alive by heartbeats. A **lock** is ownership of a named resource, tied to a lease. One lease can hold many locks, and if a lease expires, all its locks are released. This separation means a single heartbeat loop keeps all of a client's locks alive.

## Guarantees

**Linearizable lock and lease mutations.** All writes go through Raft and commit to a quorum before taking effect. There is a single replicated order for every state transition.

**Globally monotonic fencing tokens.** Every successful lock acquisition increments a cluster-wide counter. Tokens are never reused or decremented, even across leader elections.

**Lock ownership requires a valid lease.** A lock can only be acquired with a lease that exists, is not expired, and belongs to the requesting owner. When a lease expires, its locks are released.

**Idempotent re-acquisition.** If the same lease re-acquires a lock it already holds, the same fencing token is returned. No new token is minted.

**Renewal-safe expiry.** The leader tracks in-flight lease renewals in the Raft pipeline. The expiry loop will not propose a stale expiry while a timely renewal is still uncommitted.

**Fail-closed client behavior.** If the Go client loses confidence in its lease health, it invalidates the session and refuses future lock operations rather than proceeding with uncertain ownership.

## Limitations

**No fairness.** There is no waiter queue. If multiple clients race for the same lock, one may win repeatedly while others are starved.

**No availability during quorum loss.** Writes fail or stall until a new leader is elected. This is expected for a CP system.

**Expiry is delayed after failover.** A newly elected leader will not expire any lease until it has been leader for at least that lease's TTL. This gives clients time to reconnect, but a crashed client's locks are held for up to one extra TTL after a failover.

## Getting Started

### Build

```bash
go build -o clavis ./cmd/clavis
```

### Bootstrap a Cluster

Start the first node:

```bash
./clavis --bootstrap \
  --node-id node1 \
  --raft-addr 127.0.0.1:7000 \
  --grpc-addr 127.0.0.1:9000 \
  --data-dir ./data/node1
```

Join additional nodes:

```bash
./clavis \
  --node-id node2 \
  --raft-addr 127.0.0.1:7001 \
  --grpc-addr 127.0.0.1:9001 \
  --data-dir ./data/node2 \
  --join 127.0.0.1:9000

./clavis \
  --node-id node3 \
  --raft-addr 127.0.0.1:7002 \
  --grpc-addr 127.0.0.1:9002 \
  --data-dir ./data/node3 \
  --join 127.0.0.1:9000
```

A 3-node cluster is the recommended deployment for production use.

### Remove a Node

```bash
./clavis --remove-node node3 --cluster-addr 127.0.0.1:9000
```

### Run the Examples

The two programs in [`examples/`](examples/) each start their own in-process 3-node cluster, so there is nothing to set up first:

```bash
go run examples/fencing.go    # a stalled worker's late write is rejected by its stale fencing token (~6s)
go run examples/failover.go   # controller replicas ride out a Raft leader crash, then a replica crash (~25s)
```

## Go SDK

The `pkg/client` package provides a Go client with leader discovery, connection pooling, and automatic lease heartbeating.

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/Mfon-19/clavis/pkg/client"
)

func main() {
    ctx := context.Background()

    // Create a client with seed addresses and an owner identity
    c, err := client.NewClientWithSeeds(
        []string{"127.0.0.1:9000", "127.0.0.1:9001", "127.0.0.1:9002"},
        "my-service",
    )
    if err != nil {
        panic(err)
    }

    // Start a session: creates a lease and begins heartbeating
    if err := c.Start(ctx, 15*time.Second); err != nil {
        panic(err)
    }
    defer c.Stop()

    // Acquire a named lock
    lock, err := c.Acquire(ctx, "critical-section")
    if err != nil {
        panic(err)
    }

    // Use the fencing token in downstream writes
    token := lock.Token()
    fmt.Printf("acquired lock with fencing token: %d\n", token)

    // Release when done
    if err := c.Release(ctx, "critical-section"); err != nil {
        panic(err)
    }
}
```

### Client Behavior

The client discovers the current leader by probing seed addresses and following structured redirect hints from follower nodes. gRPC connections are pooled and reused.

Heartbeats go out every TTL/3. A successful renewal proves the lease is alive for one TTL after it was sent, so when renewals fail (usually during a Raft election) the client keeps retrying until a quarter TTL before that proof runs out. Only then does it give up: the session is invalidated, lock operations fail with `ErrLeaseUnavailable`, and `Client.Done()` is closed. Work guarded by a lock should watch `Done()` and stop when it fires, because the correct response to uncertain lease ownership is to stop acting on it.

Pick a TTL well above your cluster's election time plus one heartbeat interval. With the default Raft timeouts an election takes about two seconds, so very short TTLs fail closed on every leader failover.

## gRPC API

Clavis exposes two gRPC services defined in [`api/proto/lock.proto`](api/proto/lock.proto).

### LockService (Data Plane)

| RPC | Purpose |
|-----|---------|
| `CreateLease` | Create a time-bounded session for a given owner |
| `Heartbeat` | Bidirectional stream that keeps a lease alive |
| `AcquireLock` | Acquire a named lock, returns a fencing token |
| `ReleaseLock` | Release a lock held by a given lease |
| `GetStatus` | Local read for discovery and diagnostics |

### AdminService (Control Plane)

| RPC | Purpose |
|-----|---------|
| `JoinNode` | Add a new Raft voter to the cluster |
| `RemoveNode` | Remove a node from the cluster |
| `GetStatus` | Cluster state inspection for admin tools |

Mutating RPCs must be served by the current Raft leader. When a follower receives a mutating request, it returns a structured `LeaderHint` error detail so clients can redirect without parsing error messages.

## Testing

### Unit and Integration Tests

```bash
make test
```

This includes FSM invariant tests, snapshot/restore tests, and Porcupine linearizability checks that run concurrent clients against a 3-node in-process cluster and verify the history is consistent with a sequential specification.

### Benchmarks

`cmd/clavis-bench` measures the system end to end through the Go SDK and reports percentiles:

| Scenario | Measures |
|---|---|
| `latency` | Acquire and release latency for one uncontended client |
| `throughput` | Lock cycles per second with 1, 8, and 64 clients on distinct locks |
| `handoff` | How quickly a contended lock passes between waiting clients, and how unevenly |
| `sessions` | Lock latency as idle heartbeating sessions add Raft write load |
| `failover` | Time until clients succeed again after the Raft leader crashes, and sessions lost |
| `handover` | Time for a waiter to get a lock after its holder crashes without releasing |

```bash
make bench                                      # every scenario, about 2.5 minutes
go run ./cmd/clavis-bench latency failover      # a subset
go run ./cmd/clavis-bench --seeds host1:9000,host2:9000,host3:9000   # a real cluster
```

By default each scenario gets a fresh in-process 3-node cluster, so every node shares one machine and one disk. Commit latency is dominated by disk syncs (on macOS, bbolt's `F_FULLFSYNC`), so run against a real cluster with `--seeds` for numbers that mean anything in production. Run `go run ./cmd/clavis-bench -h` for all flags.

#### Results

In-process 3-node cluster, Apple M4 laptop, default flags:

| Scenario | Result |
|---|---|
| latency | acquire p50 **20ms**, p99 29ms |
| throughput | **19 / 50 / 337** cycles/s at 1 / 8 / 64 clients |
| handoff | p50 45ms, max 715ms; wins per client ranged 8–44 |
| sessions | lock p99 **68ms → 1.15s** from 0 to 2,500 idle sessions; none lost |
| failover | clients recover **1.7–3.5s** after a leader crash; 0/8 sessions lost |
| handover | waiter gets the lock **2.4–3.6s** after a crash (3s TTL) |

Latency is mostly disk syncs: macOS `F_FULLFSYNC` makes each commit about 10ms per node.

### Jepsen Tests

Clavis includes a [Jepsen](https://jepsen.io) test suite that runs a fenced register workload under network partitions, node crashes, and clock skew. The checker verifies that fencing token invariants hold even under fault injection.

```bash
make jepsen-build   # Cross-compile for Linux
make jepsen-test    # Run the Jepsen test suite
```