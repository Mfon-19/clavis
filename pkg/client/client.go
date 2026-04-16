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

import (
	"context"
	"errors"
	"fmt"
	pb "github.com/Mfon-19/clavis/api/v1"
	"time"
)

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
	seeds := normalizeSeedAddrs(addrs)
	if len(seeds) == 0 {
		return nil, fmt.Errorf("at least one server address is required")
	}
	if ownerID == "" {
		return nil, fmt.Errorf("ownerID is required")
	}

	return &Client{
		ownerID:  ownerID,
		resolver: newResolver(seeds),
		session:  newLeaseSession(),
	}, nil
}

// Start creates a lease with the given TTL, opens a heartbeat stream to the
// leader, and starts a background goroutine that sends heartbeats at TTL/3
// intervals to keep the lease alive
func (c *Client) Start(ctx context.Context, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("ttl must be greater than 0")
	}

	resp, err := callWithFailover(c, ctx, func(client pb.LockServiceClient) (*pb.CreateLeaseResponse, error) {
		return client.CreateLease(ctx, &pb.CreateLeaseRequest{
			OwnerId:    c.ownerID,
			TtlSeconds: int64(ttl.Seconds()),
		})
	})
	if err != nil {
		return fmt.Errorf("create lease: %w", err)
	}

	c.session.startLease(resp.LeaseId, ttl)

	currentAddr := c.resolver.current()
	if currentAddr == "" {
		currentAddr, err = c.resolver.discoverLeader(ctx)
		if err != nil {
			return err
		}
	}

	sessionCtx := c.session.context()
	if err = c.openHeartbeatStream(sessionCtx, currentAddr); err != nil {
		return err
	}

	go c.heartbeatLoop(sessionCtx)
	return nil
}

// Acquire acquires a named distributed lock and returns a [Lock] handle.
// The lock is bound to the client's active lease; it will be auto-released
// if the lease expires. Use Lock.Token() for fencing.
// Use [Client.WaitAcquire] if you want to wait for a busy lock instead of
// receiving an immediate failed-precondition error.
func (c *Client) Acquire(ctx context.Context, lockName string) (*Lock, error) {
	leaseID, err := c.session.activeLeaseID()
	if err != nil {
		return nil, err
	}

	resp, err := callWithFailover(c, ctx, func(client pb.LockServiceClient) (*pb.AcquireLockResponse, error) {
		return client.AcquireLock(ctx, &pb.AcquireLockRequest{
			LockName: lockName,
			OwnerId:  c.ownerID,
			LeaseId:  leaseID,
		})
	})
	if err != nil {
		return nil, fmt.Errorf("acquire lock: %w", err)
	}

	return &Lock{
		client:       c,
		name:         lockName,
		fencingToken: resp.FencingToken,
	}, nil
}

// Release releases a held lock by name.
func (c *Client) Release(ctx context.Context, lockName string) error {
	leaseID, err := c.session.activeLeaseID()
	if err != nil {
		return err
	}

	_, err = callWithFailover(c, ctx, func(client pb.LockServiceClient) (*pb.ReleaseLockResponse, error) {
		return client.ReleaseLock(ctx, &pb.ReleaseLockRequest{
			LockName: lockName,
			LeaseId:  leaseID,
		})
	})
	if err != nil {
		return fmt.Errorf("release lock: %w", err)
	}

	return nil
}

// Status queries the cluster for health, leader info, and FSM stats.
func (c *Client) Status(ctx context.Context) (*pb.GetStatusResponse, error) {
	resp, err := callWithFailover(c, ctx, func(client pb.LockServiceClient) (*pb.GetStatusResponse, error) {
		return client.GetStatus(ctx, &pb.GetStatusRequest{})
	})
	if err != nil {
		return nil, err
	}

	c.resolver.rememberStatus(resp)
	return resp, nil
}

// Stop tears down the heartbeat stream and closes all gRPC connections.
func (c *Client) Stop() error {
	sessionErr := c.session.stop()
	resolverErr := c.resolver.close()
	if sessionErr != nil {
		return sessionErr
	}
	return resolverErr
}
