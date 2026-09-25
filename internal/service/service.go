package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Mfon-19/clavis/internal/cluster"
	"github.com/Mfon-19/clavis/internal/domain"
	"github.com/Mfon-19/clavis/internal/raftlog"
	"github.com/Mfon-19/clavis/internal/state"
)

// Service orchestrates business operations between the transport layer and
// the Raft cluster. Every mutating operation enforces leader-only execution
// via ensureLeader, returning a [NotLeaderError] with the leader's gRPC
// address for client-side redirect
type Service struct {
	node *cluster.Node
}

func NewService(node *cluster.Node) *Service {
	return &Service{node: node}
}

// StatusResult aggregates this node's view of the cluster for GetStatus.
type StatusResult struct {
	NodeID            string
	IsLeader          bool
	LeaderGRPCAddress string
	State             string
	GRPCAddress       string
	Members           []domain.ClusterMember
	Stats             state.Stats
}

// as converts the untyped FSM response from a Raft apply into the concrete
// response type the command is known to produce.
func as[T any](result any, err error) (T, error) {
	var zero T
	if err != nil {
		return zero, err
	}
	typed, ok := result.(T)
	if !ok {
		return zero, fmt.Errorf("unexpected FSM response type %T", result)
	}
	return typed, nil
}

const maxLeaseTTLSeconds int64 = (1<<63 - 1) / int64(time.Second)

func leaseTTLDuration(ttlSeconds int64, now time.Time) (time.Duration, error) {
	switch {
	case ttlSeconds <= 0:
		return 0, &InvalidArgumentError{Message: "ttl_seconds must be greater than 0"}
	case ttlSeconds > maxLeaseTTLSeconds:
		return 0, &InvalidArgumentError{Message: "ttl_seconds exceeds the maximum supported duration"}
	}

	ttl := time.Duration(ttlSeconds) * time.Second
	maxExpiryNanos := int64(1<<63-1) - now.UnixNano()
	if maxExpiryNanos <= 0 || ttl > time.Duration(maxExpiryNanos) {
		return 0, &InvalidArgumentError{Message: "ttl_seconds exceeds the maximum supported expiry"}
	}
	return ttl, nil
}

// ensureLeader returns nil if this node is the Raft leader, or a
// NotLeaderError containing the leader's gRPC address for client redirect
func (s *Service) ensureLeader() error {
	if s.node.IsLeader() {
		return nil
	}

	return &NotLeaderError{
		LeaderGRPCAddress: s.node.GetLeaderGRPCAddress(),
	}
}

// CreateLease creates a new time-bounded lease for the given owner. The service
// attaches the proposal timestamp before submitting the command to Raft so the
// FSM does not need to read local wall-clock time during replay
func (s *Service) CreateLease(ownerID string, ttlSeconds int64) (state.CreateLeaseResponse, error) {
	if err := s.ensureLeader(); err != nil {
		return state.CreateLeaseResponse{}, err
	}
	if ownerID == "" {
		return state.CreateLeaseResponse{}, &InvalidArgumentError{Message: "owner_id required"}
	}

	now := s.node.Now()
	ttl, err := leaseTTLDuration(ttlSeconds, now)
	if err != nil {
		return state.CreateLeaseResponse{}, err
	}

	return as[state.CreateLeaseResponse](s.node.Apply(raftlog.NewCreateLeaseCmd(ownerID, ttl, now)))
}

// RenewLease extends a lease by its TTL and returns that TTL. Renewals are
// tracked in memory on the leader rather than written to the Raft log.
func (s *Service) RenewLease(leaseID uint64) (time.Duration, error) {
	if err := s.ensureLeader(); err != nil {
		return 0, err
	}

	return s.node.RenewLease(leaseID)
}

const (
	// maxLockWait bounds one waiting acquire. It stays below the SDK's
	// per-attempt timeout so a wait ends with an answer, not a client timeout.
	maxLockWait = 4 * time.Second
	// leaderRecheck is how often a waiter confirms this node still leads, so
	// waiters are redirected promptly after a failover.
	leaderRecheck = 250 * time.Millisecond
)

// AcquireLock acquires a named distributed lock bound to the given lease and
// returns its monotonic fencing token.
//
// If another lease holds the lock and wait is false, it fails at once with
// ErrLockAlreadyHeld. If wait is true, the caller joins the lock's queue and
// is handed the lock, in arrival order, as soon as it is freed. A wait ends
// with ErrLockAlreadyHeld after maxLockWait or when ctx is done.
func (s *Service) AcquireLock(ctx context.Context, lockName, ownerID string, leaseID uint64, wait bool) (state.AcquireLockResponse, error) {
	if err := s.ensureLeader(); err != nil {
		return state.AcquireLockResponse{}, err
	}
	if ownerID == "" || lockName == "" {
		return state.AcquireLockResponse{}, &InvalidArgumentError{Message: "owner_id, lock_name and lease_id are required"}
	}

	if !wait {
		// Callers that are not queued may not take a contended lock ahead of
		// those that are. The current holder may still re-acquire.
		if s.node.LockHasWaiters(lockName) && !s.heldBy(lockName, leaseID) {
			return state.AcquireLockResponse{}, domain.ErrLockAlreadyHeld
		}
		return s.acquire(lockName, ownerID, leaseID)
	}

	waiter := s.node.JoinLockQueue(lockName)
	defer waiter.Leave()

	timeout := time.NewTimer(maxLockWait)
	defer timeout.Stop()
	recheck := time.NewTicker(leaderRecheck)
	defer recheck.Stop()

	for {
		// Only the head of the queue tries, and only when the lock looks free,
		// so waiting never spends Raft commits on attempts that must fail.
		if waiter.IsHead() && !s.heldByOther(lockName, leaseID) {
			resp, err := s.acquire(lockName, ownerID, leaseID)
			if !errors.Is(err, domain.ErrLockAlreadyHeld) {
				return resp, err
			}
		}

		select {
		case <-waiter.Ready():
		case <-recheck.C:
			if err := s.ensureLeader(); err != nil {
				return state.AcquireLockResponse{}, err
			}
		case <-timeout.C:
			return state.AcquireLockResponse{}, domain.ErrLockAlreadyHeld
		case <-ctx.Done():
			return state.AcquireLockResponse{}, domain.ErrLockAlreadyHeld
		}
	}
}

func (s *Service) acquire(lockName, ownerID string, leaseID uint64) (state.AcquireLockResponse, error) {
	if err := s.node.CheckLeaseAlive(leaseID); err != nil {
		return state.AcquireLockResponse{}, err
	}
	return as[state.AcquireLockResponse](s.node.Apply(raftlog.NewAcquireLockCmd(lockName, ownerID, leaseID)))
}

func (s *Service) heldBy(lockName string, leaseID uint64) bool {
	lock, held := s.node.GetFSM().GetLock(lockName)
	return held && lock.LeaseID == leaseID
}

func (s *Service) heldByOther(lockName string, leaseID uint64) bool {
	lock, held := s.node.GetFSM().GetLock(lockName)
	return held && lock.LeaseID != leaseID
}

// ReleaseLock releases a held lock. Only the lease that acquired it may release it.
func (s *Service) ReleaseLock(lockName string, leaseID uint64) error {
	if err := s.ensureLeader(); err != nil {
		return err
	}
	if lockName == "" {
		return &InvalidArgumentError{Message: "lock_name required"}
	}

	_, err := s.node.Apply(raftlog.NewReleaseLockCmd(lockName, leaseID))
	return err
}

// Status returns cluster health, leader info, shared endpoint metadata, and
// FSM stats. This is the only read path that bypasses Raft consensus.
func (s *Service) Status() *StatusResult {
	stats := s.node.Stats()
	self := s.node.SelfMember()

	return &StatusResult{
		NodeID:            s.node.GetNodeID().String(),
		IsLeader:          s.node.IsLeader(),
		LeaderGRPCAddress: s.node.GetLeaderGRPCAddress(),
		State:             s.node.GetState().String(),
		GRPCAddress:       self.GRPCAddress,
		Members:           s.node.Members(),
		Stats:             stats,
	}
}

// JoinNode adds a new node to the Raft cluster as a voter and returns the
// leader's client-facing gRPC address.
func (s *Service) JoinNode(member domain.ClusterMember) (string, error) {
	if err := s.ensureLeader(); err != nil {
		return "", err
	}

	if err := s.node.AddClusterMember(member); err != nil {
		return "", err
	}

	return s.node.GetLeaderGRPCAddress(), nil
}

// RemoveNode removes a node from the Raft cluster.
func (s *Service) RemoveNode(nodeID string) error {
	if err := s.ensureLeader(); err != nil {
		return err
	}
	if nodeID == "" {
		return &InvalidArgumentError{Message: "node_id required"}
	}

	return s.node.RemoveClusterMember(nodeID)
}
