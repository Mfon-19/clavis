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
	"time"

	pb "github.com/Mfon-19/clavis/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrLeaseUnavailable indicates the heartbeat loop has failed persistently
// and the lease health is unknown. All subsequent Acquire/Release calls will
// fail until the client is restarted
var ErrLeaseUnavailable = errors.New("lease health is unknown. reconnect required")

// ErrLockHeld indicates the lock is currently held by another lease. It is
// an expected outcome under contention, not a failure of the client.
var ErrLockHeld = errors.New("lock is held by another lease")

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
	if ttl < time.Second {
		return fmt.Errorf("ttl must be at least 1 second")
	}

	ttlSeconds := int64(ttl / time.Second)
	// The lease is created no earlier than this, so it is alive until at least
	// createSentAt+TTL. The heartbeat loop builds on that proof.
	createSentAt := time.Now()
	resp, err := callWithFailover(c, ctx, func(attemptCtx context.Context, client pb.LockServiceClient) (*pb.CreateLeaseResponse, error) {
		return client.CreateLease(attemptCtx, &pb.CreateLeaseRequest{
			OwnerId:    c.ownerID,
			TtlSeconds: ttlSeconds,
		})
	})
	if err != nil {
		return fmt.Errorf("create lease: %w", err)
	}

	if resp.TtlSeconds <= 0 {
		return fmt.Errorf("create lease: server returned invalid TTL %d", resp.TtlSeconds)
	}

	leaseTTL := time.Duration(resp.TtlSeconds) * time.Second
	c.session.startLease(resp.LeaseId, leaseTTL)

	currentAddr := c.resolver.current()
	if currentAddr == "" {
		currentAddr, err = c.resolver.discoverLeader(ctx)
		if err != nil {
			return err
		}
	}

	sessionCtx := c.session.context()
	if err = c.openHeartbeatStream(sessionCtx, currentAddr); err != nil {
		c.session.invalidate(fmt.Errorf("%w: %v", ErrLeaseUnavailable, err))
		return err
	}

	go c.heartbeatLoop(sessionCtx, createSentAt)
	return nil
}

// Acquire acquires a named distributed lock and returns a [Lock] handle.
// The lock is bound to the client's active lease; it will be auto-released
// if the lease expires. Use Lock.Token() for fencing.
// If another lease holds the lock, the error wraps [ErrLockHeld]. Use
// [Client.WaitAcquire] to wait for a busy lock instead.
func (c *Client) Acquire(ctx context.Context, lockName string) (*Lock, error) {
	return c.acquire(ctx, lockName, false)
}

// acquire asks the leader for the lock. With wait set, the leader queues the
// request behind earlier waiters and answers once the lock is handed over or
// its wait window ends.
func (c *Client) acquire(ctx context.Context, lockName string, wait bool) (*Lock, error) {
	leaseID, err := c.session.activeLeaseID()
	if err != nil {
		return nil, err
	}

	resp, err := callWithFailover(c, ctx, func(attemptCtx context.Context, client pb.LockServiceClient) (*pb.AcquireLockResponse, error) {
		return client.AcquireLock(attemptCtx, &pb.AcquireLockRequest{
			LockName: lockName,
			OwnerId:  c.ownerID,
			LeaseId:  leaseID,
			Wait:     wait,
		})
	})
	if status.Code(err) == codes.AlreadyExists {
		return nil, fmt.Errorf("acquire lock %q: %w", lockName, ErrLockHeld)
	}
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

	_, err = callWithFailover(c, ctx, func(attemptCtx context.Context, client pb.LockServiceClient) (*pb.ReleaseLockResponse, error) {
		return client.ReleaseLock(attemptCtx, &pb.ReleaseLockRequest{
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
	resp, err := callWithFailover(c, ctx, func(attemptCtx context.Context, client pb.LockServiceClient) (*pb.GetStatusResponse, error) {
		return client.GetStatus(attemptCtx, &pb.GetStatusRequest{})
	})
	if err != nil {
		return nil, err
	}

	c.resolver.rememberStatus(resp)
	return resp, nil
}

// Done returns a channel that is closed when the client's session ends,
// either because Stop was called or because the lease could no longer be
// confirmed alive. Once Done is closed, every lock acquired through this
// client must be treated as lost: stop the work it protects.
func (c *Client) Done() <-chan struct{} {
	return c.session.context().Done()
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
