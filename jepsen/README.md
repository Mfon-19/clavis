# Clavis Jepsen

This folder contains the Jepsen harness for Clavis. Each operation is one
client, built on the Go SDK in [`holder/`](holder/), that opens a heartbeating
session, acquires a lock (sometimes waiting in line for it), holds it, and then
makes a fenced write to a downstream register. Some holds outlast the lease
TTL, so they depend on renewals; some clients stall long enough for their lease
to expire before they write.

The checker fails the run if, under partitions, restarts, pauses, and clock
skew:

- two clients held the same lock at overlapping times, by their own
  confirmation that their session was alive,
- a client that had just confirmed it held a lock was fenced out,
- a fencing token was issued twice, or
- an operation that started after another finished got a lower token.

## Prerequisites

Install these tools on the controller machine:

- Go
- Leiningen (`lein`)
- Multipass (for local disposable Ubuntu VMs)

You also need a Linux `arm64` binary because the local Multipass VMs on Apple
Silicon run Ubuntu `aarch64`.

## Quick Start

1. Build the Linux binary:

```bash
make jepsen-build
```

2. Run the Jepsen unit tests for the harness itself:

```bash
make jepsen-test
```

3. Create a dedicated SSH key for the Jepsen VMs:

```bash
ssh-keygen -t ed25519 -N '' -f ~/.ssh/clavis_jepsen_key
```

4. Launch three Ubuntu VMs sequentially:

```bash
multipass launch 24.04 --name clavis-j1 --cpus 2 --memory 2G --disk 10G
multipass launch 24.04 --name clavis-j2 --cpus 2 --memory 2G --disk 10G
multipass launch 24.04 --name clavis-j3 --cpus 2 --memory 2G --disk 10G
```

5. If Multipass leaves a node in `Unknown` right after `launch`, recover it once:

```bash
multipass stop clavis-j1 && multipass start clavis-j1
multipass stop clavis-j2 && multipass start clavis-j2
multipass stop clavis-j3 && multipass start clavis-j3
```

6. Get the VM IPs:

```bash
multipass info clavis-j1
multipass info clavis-j2
multipass info clavis-j3
```

7. Install your public key into the default `ubuntu` user on each VM:

```bash
PUBKEY="$(cat ~/.ssh/clavis_jepsen_key.pub)"
for vm in clavis-j1 clavis-j2 clavis-j3; do
  multipass exec "$vm" -- bash -lc "mkdir -p ~/.ssh && echo '$PUBKEY' >> ~/.ssh/authorized_keys && chmod 700 ~/.ssh && chmod 600 ~/.ssh/authorized_keys"
done
```

8. Verify SSH and passwordless sudo on each node:

```bash
ssh -o StrictHostKeyChecking=no -i ~/.ssh/clavis_jepsen_key ubuntu@<ip> hostname
ssh -i ~/.ssh/clavis_jepsen_key ubuntu@<ip> 'sudo -n true && echo ok'
```

9. Run the real Jepsen workload:

```bash
make jepsen-run \
  JEPSEN_NODES=<ip1>,<ip2>,<ip3> \
  JEPSEN_SSH_KEY=$HOME/.ssh/clavis_jepsen_key \
  JEPSEN_USER=ubuntu \
  JEPSEN_TIME_LIMIT=120
```

## Make Targets

- `make jepsen-build`
  Builds `./clavis` for Linux `arm64`.
- `make jepsen-holder`
  Builds the Jepsen client for the controller machine. `make jepsen-run` does
  this for you.
- `make jepsen-test`
  Runs the Clojure unit tests for the Jepsen harness.
- `make jepsen-help`
  Prints the Jepsen CLI help for the harness.
- `make jepsen-run JEPSEN_NODES=...`
  Runs the full Jepsen fault-injection workload.

## Notes

- The harness uses the `ubuntu` SSH user and runs privileged remote commands
  through `sudo`.
- The default workload enables partitions, restarts, pauses, and clock skew.
- Results are written under `jepsen/store/`, and the clients' SDK logs go to
  `jepsen/store/holder.log`.
- Clavis measures lease time with Go's monotonic clock, so clock skew is
  expected to have no effect. The skew nemesis guards against that changing.
- A successful run ends with a checker result like:

```clojure
{:valid? true, :accepted-count 102, :stale-rejection-count 3, :held-count 98,
 :lost-count 0, :resource-count 3, :minimum-successful-ops 1, :violations []}
```

## Cleanup

When you are done:

```bash
multipass delete clavis-j1 clavis-j2 clavis-j3
multipass purge
```
