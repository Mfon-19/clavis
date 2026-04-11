// Command controller-leader shows the classic active/passive controller shape:
// every replica runs the same loop, but only the process holding the Clavis
// lock executes the control loop at any moment.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Mfon-19/clavis/pkg/client"
)

const controllerLock = "controllers:payments"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	c, err := client.NewClientWithSeeds(seedAddrs(), ownerID())
	if err != nil {
		log.Fatal(err)
	}
	defer c.Stop()

	if err := c.Start(ctx, 15*time.Second); err != nil {
		log.Fatal(err)
	}

	for {
		// WaitAcquire turns the lock into active/passive leadership. Only one
		// controller enters the inner loop; the others sleep and retry.
		lock, err := c.WaitAcquire(ctx, controllerLock)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Fatal(err)
		}

		if err := runController(ctx, lock); err != nil && ctx.Err() == nil {
			log.Printf("controller loop exited: %v", err)
		}

		if err := lock.Release(context.Background()); err != nil {
			log.Printf("release leadership: %v", err)
		}

		if ctx.Err() != nil {
			return
		}
	}
}

func runController(ctx context.Context, lock *client.Lock) error {
	log.Printf("became active controller with fencing token %d", lock.Token())

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Replace this with the real reconciliation or scheduling logic. If
			// the controller writes to another system, carry the fencing token so
			// a stale leader can be rejected after failover.
			log.Printf("reconciling payments with fencing token %d", lock.Token())
		}
	}
}

func seedAddrs() []string {
	value := os.Getenv("CLAVIS_SEEDS")
	if value == "" {
		return []string{"127.0.0.1:9000"}
	}
	return strings.Split(value, ",")
}

func ownerID() string {
	if value := os.Getenv("CLAVIS_OWNER_ID"); value != "" {
		return value
	}
	host, _ := os.Hostname()
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}
