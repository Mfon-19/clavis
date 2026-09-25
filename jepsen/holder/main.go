// Command holder is the Jepsen harness's Clavis client. Each run is one
// operation: it opens a session with the Go SDK, which heartbeats in the
// background, acquires one lock, and then takes commands on stdin so the
// harness can hold the lock, check it, and release it.
//
// It writes one JSON object per line to stdout:
//
//	{"event":"acquired","token":42}
//	{"event":"busy"}                    lock held by another lease, or the wait ran out
//	{"event":"error","phase":"...","error":"..."}
//
// After "acquired" it reads commands, one per line:
//
//	check    replies {"event":"valid"} while the session is alive, else {"event":"lost"}
//	release  releases the lock, replies {"event":"released"} or an error, and exits
//
// It exits when stdin closes. The SDK's own logs go to stderr.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Mfon-19/clavis/pkg/client"
)

const rpcTimeout = 10 * time.Second

func main() {
	seeds := flag.String("seeds", "", "comma-separated gRPC addresses of the cluster")
	owner := flag.String("owner", "", "owner ID for the lease")
	lock := flag.String("lock", "", "lock name")
	ttl := flag.Duration("ttl", 6*time.Second, "lease TTL")
	wait := flag.Duration("wait", 0, "wait up to this long for a busy lock; 0 fails at once")
	probe := flag.Bool("probe", false, "only check that a session can be opened, then exit")
	flag.Parse()

	c, err := client.NewClientWithSeeds(strings.Split(*seeds, ","), *owner)
	if err != nil {
		fail("setup", err)
	}
	defer c.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	err = c.Start(ctx, *ttl)
	cancel()
	if err != nil {
		fail("lease", err)
	}
	if *probe {
		emit(map[string]any{"event": "ready"})
		return
	}

	var l *client.Lock
	if *wait > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), *wait)
		l, err = c.WaitAcquire(ctx, *lock)
	} else {
		ctx, cancel = context.WithTimeout(context.Background(), rpcTimeout)
		l, err = c.Acquire(ctx, *lock)
	}
	cancel()
	switch {
	case errors.Is(err, client.ErrLockHeld), *wait > 0 && errors.Is(err, context.DeadlineExceeded):
		emit(map[string]any{"event": "busy"})
		return
	case err != nil:
		fail("acquire", err)
	}
	emit(map[string]any{"event": "acquired", "token": l.Token()})

	in := bufio.NewScanner(os.Stdin)
	for in.Scan() {
		switch in.Text() {
		case "check":
			select {
			case <-c.Done():
				emit(map[string]any{"event": "lost"})
			default:
				emit(map[string]any{"event": "valid"})
			}
		case "release":
			ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
			err := l.Release(ctx)
			cancel()
			if err != nil {
				fail("release", err)
			}
			emit(map[string]any{"event": "released"})
			return
		}
	}
}

func emit(v map[string]any) {
	b, _ := json.Marshal(v)
	fmt.Println(string(b))
}

func fail(phase string, err error) {
	emit(map[string]any{"event": "error", "phase": phase, "error": err.Error()})
	os.Exit(1)
}
