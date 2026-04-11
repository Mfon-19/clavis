// Command tenant-reconciler shows how Clavis can serialize work per tenant
// without forcing the whole service to run as a singleton. Different tenants
// can be processed independently, but one tenant's reconciliation is protected
// by a stable lock name.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Mfon-19/clavis/pkg/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	ctx := context.Background()

	c, err := client.NewClientWithSeeds(seedAddrs(), ownerID())
	if err != nil {
		log.Fatal(err)
	}
	defer c.Stop()

	if err := c.Start(ctx, 15*time.Second); err != nil {
		log.Fatal(err)
	}

	tenantIDs := []string{"acme", "globex", "initech"}
	for _, tenantID := range tenantIDs {
		if err := reconcileTenant(ctx, c, tenantID); err != nil {
			log.Printf("tenant %s: %v", tenantID, err)
		}
	}
}

func reconcileTenant(ctx context.Context, c *client.Client, tenantID string) error {
	lockName := fmt.Sprintf("tenant:%s:reconcile", tenantID)

	lock, err := c.Acquire(ctx, lockName)
	if err != nil {
		// Busy is not exceptional in this pattern. Another worker may already be
		// responsible for this tenant, so we skip it and move on.
		if status.Code(err) == codes.FailedPrecondition {
			return fmt.Errorf("another worker is already reconciling this tenant")
		}
		return err
	}
	defer func() {
		if err := lock.Release(ctx); err != nil {
			log.Printf("release tenant lock: %v", err)
		}
	}()

	// Replace this with the real tenant-specific reconciliation logic. The
	// important design choice is the lock name: it serializes one tenant without
	// serializing all tenants.
	log.Printf("reconciling tenant %s with fencing token %d", tenantID, lock.Token())
	time.Sleep(100 * time.Millisecond)
	return nil
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
