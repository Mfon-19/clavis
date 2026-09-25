//go:build ignore

// Failover runs three replicas of a controller against a three-node Clavis
// cluster, then breaks things.
//
// Only the replica holding the lock does work: it writes a heartbeat row to a
// shared database, tagged with its fencing token. The other two wait. Midway
// through, the demo crashes the Clavis node that is the Raft leader, and later
// it crashes the active replica itself. Watch for:
//
//   - the active replica keeping its lock and token across the Raft election,
//   - a standby taking over with a higher token once the crashed replica's
//     lease expires, and
//   - the database never accepting a write from two replicas out of order.
//
// Run it with:
//
//	go run examples/failover.go
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/Mfon-19/clavis/internal/app"
	"github.com/Mfon-19/clavis/pkg/client"
)

const (
	lockName = "controllers/billing"
	// The TTL must comfortably exceed a Raft election (about two seconds here)
	// plus one heartbeat interval, or clients fail closed during failover.
	leaseTTL = 8 * time.Second
	tick     = 500 * time.Millisecond
)

var start = time.Now()

func main() {
	// The SDK logs heartbeat retries through the standard logger. The demo
	// narrates what matters itself, so keep the output readable.
	log.SetOutput(io.Discard)

	dataDir, err := os.MkdirTemp("", "clavis-failover-*")
	check(err)
	defer os.RemoveAll(dataDir)

	cluster, err := app.StartLocalCluster(3, dataDir, io.Discard)
	check(err)
	defer cluster.Close()
	leader, err := cluster.WaitForLeader(10 * time.Second)
	check(err)
	say("cluster", "3 nodes up, node-%d is the Raft leader", leader)

	db := &controllerTable{}
	replicas := make([]*replica, 3)
	for i := range replicas {
		replicas[i] = startReplica(fmt.Sprintf("replica-%d", i+1), cluster.Addrs(), db)
	}

	// Let one replica win the lock and do some work.
	time.Sleep(3 * time.Second)

	// Crash the Clavis node that is the Raft leader. The survivors elect a new
	// leader, clients find it, and the active replica's lease carries over:
	// the new leader waits one full TTL before expiring any lease.
	say("chaos", "crashing Clavis node-%d, the current Raft leader", leader)
	check(cluster.StopNode(leader))
	electionStart := time.Now()
	newLeader, err := cluster.WaitForLeader(10 * time.Second)
	check(err)
	say("cluster", "node-%d elected leader after %.1fs", newLeader, time.Since(electionStart).Seconds())

	time.Sleep(3 * time.Second)
	if writer, token, at := db.lastWrite(); time.Since(at) < 2*tick {
		say("database", "still receiving writes from %s with token %d", writer, token)
	} else {
		say("database", "no writes since %s stepped down", writer)
	}

	time.Sleep(time.Second)

	// Crash the active replica without releasing the lock, as a killed process
	// would. Its lease expires one TTL later and a standby takes over.
	active, _, _ := db.lastWrite()
	for _, r := range replicas {
		if r.name == active {
			say("chaos", "crashing %s, the active replica, without releasing its lock", r.name)
			r.crash()
		}
	}

	time.Sleep(leaseTTL + 4*time.Second)

	for _, r := range replicas {
		r.shutdown()
	}
	db.summarize()
}

// replica is one instance of the controller. Every replica runs the same
// loop; the lock decides which one is active.
type replica struct {
	name   string
	cancel context.CancelFunc
	done   chan struct{}

	mu      sync.Mutex
	crashed bool
}

func startReplica(name string, seeds []string, db *controllerTable) *replica {
	ctx, cancel := context.WithCancel(context.Background())
	r := &replica{name: name, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		r.run(ctx, seeds, db)
	}()
	return r
}

// run keeps the replica in the election. Each pass opens a Clavis session,
// waits for the lock, and does work until the lock is lost or the replica is
// told to stop.
func (r *replica) run(ctx context.Context, seeds []string, db *controllerTable) {
	for ctx.Err() == nil {
		c, err := client.NewClientWithSeeds(seeds, r.name)
		check(err)

		if err := c.Start(ctx, leaseTTL); err != nil {
			_ = c.Stop()
			time.Sleep(tick)
			continue
		}

		lock, err := c.WaitAcquire(ctx, lockName)
		if err != nil {
			_ = c.Stop()
			continue
		}

		say(r.name, "became active with fencing token %d", lock.Token())
		r.lead(ctx, c, lock, db)

		if !r.isCrashed() {
			// A clean exit releases the lock so a standby can take over
			// immediately instead of waiting for the lease to expire.
			_ = lock.Release(context.Background())
		}
		_ = c.Stop()
	}
}

// lead does the controller's work while this replica holds the lock. It
// stops as soon as the client reports that its session has ended, and it
// treats a rejected write as proof that someone newer holds the lock.
func (r *replica) lead(ctx context.Context, c *client.Client, lock *client.Lock, db *controllerTable) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.Done():
			say(r.name, "lost its Clavis session; stepping down")
			return
		case <-ticker.C:
			if err := db.write(r.name, lock.Token()); err != nil {
				say(r.name, "%v; stepping down", err)
				return
			}
		}
	}
}

// crash kills the replica without releasing its lock or ending its lease.
func (r *replica) crash() {
	r.mu.Lock()
	r.crashed = true
	r.mu.Unlock()
	r.cancel()
	<-r.done
}

func (r *replica) isCrashed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.crashed
}

func (r *replica) shutdown() {
	r.cancel()
	<-r.done
}

// controllerTable stands in for the database the controller writes to. Like
// a row guarded by a conditional UPDATE, it rejects any write whose fencing
// token is older than the newest it has accepted.
type controllerTable struct {
	mu       sync.Mutex
	token    uint64
	writer   string
	at       time.Time
	accepted []write
	rejected int
}

type write struct {
	writer string
	token  uint64
}

var errStaleToken = errors.New("database rejected write with a stale fencing token")

func (t *controllerTable) write(writer string, token uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if token < t.token {
		t.rejected++
		return errStaleToken
	}
	t.token, t.writer, t.at = token, writer, time.Now()
	t.accepted = append(t.accepted, write{writer: writer, token: token})
	return nil
}

func (t *controllerTable) lastWrite() (string, uint64, time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.writer, t.token, t.at
}

// summarize prints each leadership term the database saw, in order. Every
// term has exactly one writer and a strictly higher token than the last.
func (t *controllerTable) summarize() {
	t.mu.Lock()
	defer t.mu.Unlock()

	fmt.Println()
	fmt.Println("database history:")
	for i := 0; i < len(t.accepted); {
		j := i
		for j < len(t.accepted) && t.accepted[j].token == t.accepted[i].token {
			j++
		}
		fmt.Printf("  token %d: %2d writes from %s\n", t.accepted[i].token, j-i, t.accepted[i].writer)
		i = j
	}
	fmt.Printf("  stale writes rejected: %d\n", t.rejected)
}

func say(who, format string, args ...any) {
	elapsed := time.Since(start).Seconds()
	fmt.Printf("[%4.1fs] %-9s %s\n", elapsed, who, fmt.Sprintf(format, args...))
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
