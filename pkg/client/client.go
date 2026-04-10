// Package client provides the public Go SDK for the clavis distributed lock
// service. It handles leader discovery, automatic failover across cluster
// nodes, lease lifecycle with heartbeat keepalive, and fencing token exposure
//
// Usage:
//
// c, _ := client.NewClientWithSeeds([]string{"node1:9000", "node2:9000"}, "my-service")
// c.Start(ctx, 15*time.Second) // create lease + start heartbeating
// lock, _ := c.Acquire(ctx, "my-lock")
// defer lock.Release(ctx)
// // use lock.Token() as fencing token for downstream writes
package client

import "errors"

// ErrLeaseUnavailable indicates the heartbeat loop has failed persistently
// and the lease health is unknown. All subsequent Acquire/Release calls will
// fail until the client is restarted
var ErrLeaseUnavailable = errors.New("lease health is unknown. reconnect required")

// Client is the primary entry point for interacting with a clavis cluster.
// Create one via [NewClient] or [NewClientWithSeeds], call [Client.Start] to
// establish a lease, the use [Client.Acquire] and [Client.Release] for locks
type Client struct {
	ownerID  string
	resolver *resolver
	session  *leaseSession
}

// NewClientWithSeeds creates a client with multiple seed addresses.
// The client will try each address and follow leader redirects automatically
func NewClientWithSeeds(addrs []string, ownerID string) (*Client, error) {
	
}
