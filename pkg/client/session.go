package client

import (
	"context"
	"fmt"
	pb "github.com/Mfon-19/clavis/api/v1"
	"log"
	"sync"
	"time"
)

// leaseSession manages the lifecycle of a single lease and its heartbeat stream
//
// The session transitions through:
//
//	no lease -> active lease -> invalidated.
//
// Invalidated is a fail-closed state. Once heartbeat health is unknown, the
// client refuses further lock operations because a paused or partitioned
// worker must now assume it still owns a lease
type leaseSession struct {
	mu sync.Mutex

	leaseID  uint64
	leaseTTL time.Duration
	leaseErr error

	heartbeat       pb.LockService_HeartbeatClient
	heartbeatCancel context.CancelFunc

	runCtx    context.Context
	runCancel context.CancelFunc
	stopOnce  sync.Once
}

// newLeaseSession creates an idle session with no active lease.
func newLeaseSession() *leaseSession {
	runCtx, runCancel := context.WithCancel(context.Background())
	return &leaseSession{
		runCtx:    runCtx,
		runCancel: runCancel,
	}
}

// startLease records a newly created lease, clearing any prior error state.
func (s *leaseSession) startLease(leaseID uint64, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.leaseID = leaseID
	s.leaseTTL = ttl
	s.leaseErr = nil
}

// ttl returns the lease's TTL, used to derive the heartbeat interval.
func (s *leaseSession) ttl() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.leaseTTL
}

// activeLeaseID returns the current lease ID, or an error if the session
// has been invalidated or no lease has been created yet.
func (s *leaseSession) activeLeaseID() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.leaseErr != nil {
		return 0, s.leaseErr
	}
	if s.leaseID == 0 {
		return 0, fmt.Errorf("client has no active lease; call Start first")
	}

	return s.leaseID, nil
}

// heartbeatState returns the lease ID and current heartbeat stream atomically.
// A nil stream indicates the connection needs to be re-established.
func (s *leaseSession) heartbeatState() (uint64, pb.LockService_HeartbeatClient) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.leaseID, s.heartbeat
}

// swapHeartbeat atomically replaces the heartbeat stream and its cancellation
// function, returning the old cancellation function for use outside the lock.
func (s *leaseSession) swapHeartbeat(
	stream pb.LockService_HeartbeatClient,
	cancel context.CancelFunc,
) context.CancelFunc {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldCancel := s.heartbeatCancel
	s.heartbeat = stream
	s.heartbeatCancel = cancel
	return oldCancel
}

// closeHeartbeat cancels stream if it is still the session's current stream.
// Cancellation is what guarantees a blocked Send or Recv is interrupted.
func (s *leaseSession) closeHeartbeat(stream pb.LockService_HeartbeatClient) {
	s.mu.Lock()
	if s.heartbeat != stream {
		s.mu.Unlock()
		return
	}
	cancel := s.heartbeatCancel
	s.heartbeat = nil
	s.heartbeatCancel = nil
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
}

// invalidate marks the session as unhealthy, recording the cause and tearing
// down the heartbeat stream. All subsequent Acquire/Release calls will fail
// with ErrLeaseUnavailable until a new client is created. It also ends the
// session context, which is what [Client.Done] reports.
func (s *leaseSession) invalidate(err error) {
	s.mu.Lock()
	if s.leaseErr == nil {
		s.leaseErr = err
	}
	cancel := s.heartbeatCancel
	s.heartbeat = nil
	s.heartbeatCancel = nil
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	s.stopOnce.Do(s.runCancel)
}

// context returns the session-owned context that controls the background
// heartbeat lifetime. Start's caller context is only a startup deadline.
func (s *leaseSession) context() context.Context {
	return s.runCtx
}

// stop signals the heartbeat goroutine to exit and closes the stream
// connection. Safe to call multiple times.
func (s *leaseSession) stop() error {
	s.stopOnce.Do(s.runCancel)

	s.mu.Lock()
	cancel := s.heartbeatCancel
	s.heartbeat = nil
	s.heartbeatCancel = nil
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	return nil
}

// openHeartbeatStream dials the given address, opens a bidirectional heartbeat
// stream, and atomically swaps it into the session, closing any prior stream.
func (c *Client) openHeartbeatStream(ctx context.Context, addr string) error {
	conn, err := c.resolver.connFor(addr)
	if err != nil {
		return err
	}

	client := pb.NewLockServiceClient(conn)
	streamCtx, streamCancel := context.WithCancel(ctx)
	stream, err := client.Heartbeat(streamCtx)
	if err != nil {
		streamCancel()
		return fmt.Errorf("heartbeat stream: %w", err)
	}

	oldCancel := c.session.swapHeartbeat(stream, streamCancel)
	c.resolver.rememberAddr(addr)

	if oldCancel != nil {
		oldCancel()
	}

	return nil
}

// reconnectHeartbeat discovers the current leader and opens a fresh heartbeat
// stream. Called when the existing stream breaks due to leader failover or
// network partition.
func (c *Client) reconnectHeartbeat(ctx context.Context) error {
	leaderAddr, err := c.resolver.discoverLeader(ctx)
	if err != nil {
		return err
	}

	if err := c.openHeartbeatStream(c.session.context(), leaderAddr); err != nil {
		return err
	}

	log.Printf("[INFO] Re-established heartbeat stream with leader %s", leaderAddr)
	return nil
}

// heartbeatExchange sends one renewal and waits for its acknowledgement. The
// stream RPC itself has a session-long context, so enforce a per-exchange
// deadline here and cancel the stream if Send or Recv stalls.
func (c *Client) heartbeatExchange(
	ctx context.Context,
	leaseID uint64,
	stream pb.LockService_HeartbeatClient,
	timeout time.Duration,
) error {
	result := make(chan error, 1)
	go func() {
		if err := stream.Send(&pb.HeartbeatRequest{LeaseId: leaseID}); err != nil {
			result <- err
			return
		}

		resp, err := stream.Recv()
		if err == nil && resp.GetLeaseId() != leaseID {
			err = fmt.Errorf("heartbeat response lease mismatch: got %d, want %d", resp.GetLeaseId(), leaseID)
		}
		result <- err
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case err := <-result:
		if err != nil {
			c.session.closeHeartbeat(stream)
		}
		return err
	case <-timer.C:
		c.session.closeHeartbeat(stream)
		return fmt.Errorf("heartbeat acknowledgement timed out after %s", timeout)
	case <-ctx.Done():
		c.session.closeHeartbeat(stream)
		return ctx.Err()
	}
}

func (c *Client) heartbeatAttempt(ctx context.Context, timeout time.Duration) error {
	leaseID, stream := c.session.heartbeatState()
	if stream == nil {
		if err := c.reconnectHeartbeat(ctx); err != nil {
			return err
		}
		leaseID, stream = c.session.heartbeatState()
	}
	if stream == nil {
		return fmt.Errorf("heartbeat stream unavailable")
	}

	return c.heartbeatExchange(ctx, leaseID, stream, timeout)
}

func (c *Client) heartbeatAttemptWithin(ctx context.Context, timeout time.Duration) error {
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return c.heartbeatAttempt(attemptCtx, timeout)
}

// heartbeatLoop renews the lease every TTL/3 and fails closed only when the
// lease can no longer be proven alive.
//
// The server keeps a lease alive for at least one TTL after it receives a
// renewal, so a renewal sent at time T that succeeds proves the lease is alive
// until T+TTL. When renewals start failing (typically a Raft leader election,
// during which surviving nodes may still point at the dead leader), the loop
// keeps retrying with backoff. It gives up a quarter TTL before the last proof
// runs out, leaving the application time to stop the work its locks protect.
//
// confirmedAt is the send time of the last successful renewal; for a fresh
// session it is the time CreateLease was sent.
func (c *Client) heartbeatLoop(ctx context.Context, confirmedAt time.Time) {
	ttl := c.session.ttl()
	interval := ttl / 3
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		sentAt := time.Now()
		err := c.heartbeatAttemptWithin(ctx, interval)
		if err == nil {
			confirmedAt = sentAt
			continue
		}
		if ctx.Err() != nil {
			return
		}
		log.Printf("[WARNING] Heartbeat failed, retrying: %v", err)

		giveUpAt := confirmedAt.Add(ttl - ttl/4)
		backoff := 50 * time.Millisecond
		for err != nil {
			remaining := time.Until(giveUpAt)
			if remaining <= 0 {
				leaseID, _ := c.session.heartbeatState()
				log.Printf("[CRITICAL] Lease %d could not be renewed in time: %v", leaseID, err)
				c.session.invalidate(fmt.Errorf("%w: %v", ErrLeaseUnavailable, err))
				return
			}

			timer := time.NewTimer(min(backoff, remaining))
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			backoff = min(2*backoff, 500*time.Millisecond)

			sentAt = time.Now()
			err = c.heartbeatAttemptWithin(ctx, min(interval, max(time.Until(giveUpAt), time.Millisecond)))
			if ctx.Err() != nil {
				return
			}
		}
		confirmedAt = sentAt
		log.Printf("[INFO] Heartbeat recovered")
	}
}
