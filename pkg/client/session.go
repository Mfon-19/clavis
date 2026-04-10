package client

import (
	"fmt"
	pb "github.com/Mfon-19/clavis/api/v1"
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

	heartbeat pb.LockService_HeartbeatClient

	stopCh   chan struct{}
	stopOnce sync.Once
}

// newLeaseSession creates an idle session with no active lease
func newLeaseSession() *leaseSession {
	return &leaseSession{
		stopCh: make(chan struct{}),
	}
}

// startLease records a newly created lease, clearing any prior error state
func (s *leaseSession) startLease(leaseID uint64, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.leaseID = leaseID
	s.leaseTTL = ttl
	s.leaseErr = nil
}

// ttl returns the lease's TTL, used to derive the heartbeat interval
func (s *leaseSession) ttl() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.leaseTTL
}

// activeLeaseID returns the current lease ID, or an error if the session
// has been invalidated or no lease has been created yet
func (s *leaseSession) activeLeaseID() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.leaseErr != nil {
		return 0, s.leaseErr
	}
	if s.leaseID == 0 {
		return 0, fmt.Errorf("client has no active lease. call Start first")
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

// swapHeartbeat atomically replaces the heartbeat connection and stream,
// returning the old ones so the caller can close them outside the lock.
func (s *leaseSession) swapHeartbeat(stream pb.LockService_HeartbeatClient) pb.LockService_HeartbeatClient {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldStream := s.heartbeat
	s.heartbeat = stream
	return oldStream
}

// invalidate marks the session as unhealthy, recording the cause and tearing
// down the heartbeat stream. All subsequent Acquire/Release calls will fail
// with ErrLeaseUnavailable until a new client is created.
func (s *leaseSession) invalidate(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.leaseErr == nil {
		s.leaseErr = err
	}
	if s.heartbeat != nil {
		_ = s.heartbeat.CloseSend()
		s.heartbeat = nil
	}
}

// done returns a channel that is closed when Stop is called, signaling the
// heartbeat goroutine to exit.
func (s *leaseSession) done() <-chan struct{} {
	return s.stopCh
}

// stop signals the heartbeat goroutine to exit and closes the stream
// connection. Safe to call multiple times.
func (s *leaseSession) stop() error {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})

	s.mu.Lock()
	stream := s.heartbeat
	s.heartbeat = nil
	s.mu.Unlock()

	if stream != nil {
		_ = stream.CloseSend()
	}

	return nil
}
