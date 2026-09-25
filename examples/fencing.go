//go:build ignore

// Fencing shows the problem Clavis exists to solve, end to end, in one process.
//
// Two workers update the same account balance. Worker A takes the lock and then
// stalls: a GC pause, a VM migration, a stuck syscall. Its lease expires, so
// Clavis hands the lock to worker B. When A wakes up it still believes it holds
// the lock and tries to finish its write. Mutual exclusion alone cannot stop
// that write. The fencing token does.
//
// Run it with:
//
//	go run examples/fencing.go
package main

import (
	"context"
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
	lockName = "accounts/42"
	leaseTTL = 2 * time.Second
)

var start = time.Now()

func main() {
	ctx := context.Background()

	dataDir, err := os.MkdirTemp("", "clavis-fencing-*")
	check(err)
	defer os.RemoveAll(dataDir)

	cluster, err := app.StartLocalCluster(3, dataDir, io.Discard)
	check(err)
	defer cluster.Close()
	say("cluster", "3 nodes up, leader elected")

	db := &accountRow{balance: 0}

	// Worker A takes the lock and makes its first write.
	a := startWorker(ctx, cluster.Addrs(), "worker-a")
	lockA, err := a.Acquire(ctx, lockName)
	check(err)
	say("worker-a", "acquired %q, fencing token %d", lockName, lockA.Token())
	db.write("worker-a", lockA.Token(), 100)

	// Worker A stalls. A real pause freezes the whole process, so its
	// heartbeats stop. That is all Clavis can observe, so stopping A's client
	// is a faithful simulation. A keeps its lock handle and still believes it
	// owns the lock.
	check(a.Stop())
	say("worker-a", "stalls mid-task; heartbeats stop, but it still holds token %d", lockA.Token())

	// Worker B waits for the lock. Clavis will not hand it over until A's lease
	// has expired, one TTL after A's last heartbeat.
	b := startWorker(ctx, cluster.Addrs(), "worker-b")
	defer b.Stop()
	say("worker-b", "waiting for %q...", lockName)
	lockB, err := b.WaitAcquire(ctx, lockName)
	check(err)
	say("worker-b", "acquired %q after A's lease expired, fencing token %d", lockName, lockB.Token())
	db.write("worker-b", lockB.Token(), 250)

	// Worker A wakes up and finishes the write it started before the pause.
	say("worker-a", "wakes up and finishes its write, still using token %d", lockA.Token())
	db.write("worker-a", lockA.Token(), 90)

	fmt.Println()
	fmt.Printf("final balance: %d, written by %s with token %d\n", db.balance, db.writer, db.token)
	fmt.Println("without the token check, worker-a's stale write would have replaced worker-b's.")
}

// accountRow stands in for a database row guarded by a fencing token. In
// Postgres the same check is a conditional update:
//
//	UPDATE accounts SET balance = $1, fence = $2 WHERE id = 42 AND fence <= $2
//
// The row remembers the highest token it has accepted and rejects any write
// that carries an older one.
type accountRow struct {
	mu      sync.Mutex
	balance int
	token   uint64
	writer  string
}

func (r *accountRow) write(writer string, token uint64, balance int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if token < r.token {
		say("database", "REJECTED %s: balance=%d with token %d (newest token seen is %d)", writer, balance, token, r.token)
		return
	}
	r.balance, r.token, r.writer = balance, token, writer
	say("database", "accepted %s: balance=%d with token %d", writer, balance, token)
}

func startWorker(ctx context.Context, seeds []string, name string) *client.Client {
	c, err := client.NewClientWithSeeds(seeds, name)
	check(err)
	check(c.Start(ctx, leaseTTL))
	return c
}

func say(who, format string, args ...any) {
	elapsed := time.Since(start).Seconds()
	fmt.Printf("[%4.1fs] %-9s %s\n", elapsed, who, fmt.Sprintf(format, args...))
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
