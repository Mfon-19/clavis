package state

import (
	"fmt"
	"github.com/Mfon-19/clavis/internal/domain"
	"github.com/Mfon-19/clavis/internal/raftlog"
	"sync"

	tm "time"
)

// FSM is the core in-memory state machine for the distributed lock service.
// It holds all mutable application state and enforces the following invariants:
//   - Fencing tokens are globally monotonic (never reused, never decremented)
//   - Locks require a valid, non-expired lease owned by the caller
//   - Lease expiry cascades to all associated locks
//   - Re-acquiring a lock you already hold is idempotent (same token returned)
//
// All mutations go through [FSM.Apply] under a write lock, making the FSM
// safe for concurrent use by the Raft apply goroutine and read-only queries
type FSM struct {
	mu sync.RWMutex

	locks     map[string]*domain.Lock  // lock name -> Lock
	leases    map[uint64]*domain.Lease // lease ID -> Lease
	endpoints map[string]string        // node ID -> client-facing gRPC address

	fencingCounter uint64 // global monotonic fencing token counter
	nextLeaseID    uint64 // next leaseID to assign
}

// NewFSM creates an empty state machine with lease IDs starting from 1
func NewFSM() *FSM {
	return &FSM{
		locks:          make(map[string]*domain.Lock),
		leases:         make(map[uint64]*domain.Lease),
		endpoints:      make(map[string]string),
		fencingCounter: 0,
		nextLeaseID:    1,
	}
}

// Apply dispatches a command to the appropriate handler and returns a typed
// response (e.g. [CreateLeaseResponse], [AcquireLockResponse]) or an error.
// All business invariants are enforced here.
func (f *FSM) Apply(cmd *raftlog.CommandWrapper) (any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.GetType() {
	case raftlog.CommandType_COMMAND_TYPE_CREATE_LEASE:
		return f.applyCreateLease(cmd.GetCreateLease())
	case raftlog.CommandType_COMMAND_TYPE_RENEW_LEASE:
		return f.applyRenewLease(cmd.GetRenewLease())
	case raftlog.CommandType_COMMAND_TYPE_ACQUIRE_LOCK:
		return f.applyAcquireLock(cmd.GetAcquireLock())
	case raftlog.CommandType_COMMAND_TYPE_RELEASE_LOCK:
		return f.applyReleaseLock(cmd.GetReleaseLock())
	case raftlog.CommandType_COMMAND_TYPE_EXPIRE_LEASE:
		return f.applyExpireLease(cmd.GetExpireLease())
	case raftlog.CommandType_COMMAND_TYPE_UPSERT_ENDPOINT:
		return f.applyUpsertEndpoint(cmd.GetUpsertEndpoint())
	case raftlog.CommandType_COMMAND_TYPE_REMOVE_ENDPOINT:
		return f.applyRemoveEndpoint(cmd.GetRemoveEndpoint())
	default:
		return nil, fmt.Errorf("%w: %s", domain.ErrUnknownCommand, cmd.GetType().String())
	}
}

// returned when a lease is created
type CreateLeaseResponse struct {
	LeaseID   uint64
	ExpiresAt tm.Time
}

func (f *FSM) applyCreateLease(cmd *raftlog.CreateLeaseCommand) (any, error) {
	ttl := tm.Duration(cmd.GetTtlNanos())
	if ttl <= 0 {
		return nil, domain.ErrInvalidLeaseTTL
	}

	createdAt, err := commandTime(cmd.GetCreateAtUnixNano())
	if err != nil {
		return nil, err
	}

	leaseID := f.nextLeaseID
	f.nextLeaseID++

	// Expiry is computed from the timestamp carried in the Raft log entry, not
	// from each node's local clock at apply time. That keeps replay deterministic
	// across followers and restarts.
	expiresAt := createdAt.Add(ttl)

	lease := &domain.Lease{
		LeaseID:           leaseID,
		OwnerID:           cmd.GetOwnerId(),
		ExpiresAtUnixNano: expiresAt.UnixNano(),
		TTL:               ttl,
	}

	f.leases[leaseID] = lease

	return CreateLeaseResponse{
		LeaseID:   leaseID,
		ExpiresAt: expiresAt,
	}, nil
}

// returned when a lease is renewed
type RenewLeaseResponse struct {
	ExpiresAt tm.Time
	TTL       tm.Duration
}

func (f *FSM) applyRenewLease(cmd *raftlog.RenewLeaseCommand) (any, error) {
	renewedAt, err := commandTime(cmd.GetRenewedAtUnixNano())
	if err != nil {
		return nil, err
	}

	lease, exists := f.leases[cmd.GetLeaseId()]
	if !exists {
		return nil, domain.ErrLeaseNotFound
	}

	// If the lease was already expired at the renewal proposal time, this renewal
	// is rejected. The cluster layer handles pending renewals so a timely renewal
	// is not incorrectly beaten by the expiry loop
	if lease.IsExpired(renewedAt) {
		return nil, domain.ErrLeaseExpired
	}

	lease.ExpiresAtUnixNano = renewedAt.Add(lease.TTL).UnixNano()

	return RenewLeaseResponse{
		ExpiresAt: lease.ExpiresAt(),
		TTL:       lease.TTL,
	}, nil
}

// AcquireLockResponse is returned when a lock is successfully acquired.
// FencingToken is globally monotonic and should be passed to downstream
// systems to prevent stale writes from expired lock holders
type AcquireLockResponse struct {
	FencingToken uint64
	LeaseTTL     tm.Duration
}

func (f *FSM) applyAcquireLock(cmd *raftlog.AcquireLockCommand) (any, error) {
	acquredAt, err := commandTime(cmd.GetAcquiredAtUnixNano())
	if err != nil {
		return nil, err
	}

	lease, exists := f.leases[cmd.GetLeaseId()]
	if !exists {
		return nil, domain.ErrLeaseNotFound
	}

	if lease.IsExpired(acquredAt) {
		return nil, domain.ErrLeaseExpired
	}

	if lease.OwnerID != cmd.GetOwnerId() {
		return nil, domain.ErrNotLockOwner
	}

	if existingLock, held := f.locks[cmd.GetLockName()]; held {
		// If held by the same lease, allow re-acquisition (idempotent)
		if existingLock.LeaseID == cmd.GetLeaseId() {
			return AcquireLockResponse{
				FencingToken: existingLock.FencingToken,
			}, nil
		}
		// Held by a different lease, cannot acquire
		return nil, domain.ErrLockAlreadyHeld
	}

	// This is the core stale-writer defense. Every new acquisition increments a
	// cluster-wide fencing counter. Downstream systems should reject writes with
	// tokens lower than the last token they accepted
	f.fencingCounter++
	fencingToken := f.fencingCounter

	lock := &domain.Lock{
		Name:         cmd.GetLockName(),
		OwnerID:      cmd.GetOwnerId(),
		FencingToken: fencingToken,
		LeaseID:      cmd.GetLeaseId(),
	}

	f.locks[cmd.GetLockName()] = lock

	return AcquireLockResponse{
		FencingToken: fencingToken,
		LeaseTTL:     lease.TTL,
	}, nil
}

type ReleaseLockResponse struct {
	Released bool
}

func (f *FSM) applyReleaseLock(cmd *raftlog.ReleaseLockCommand) (any, error) {
	lock, held := f.locks[cmd.GetLockName()]
	if !held {
		return nil, domain.ErrLockNotFound
	}

	if lock.LeaseID != cmd.GetLeaseId() {
		return nil, domain.ErrNotLockOwner
	}

	delete(f.locks, cmd.GetLockName())

	return ReleaseLockResponse{
		Released: true,
	}, nil
}

type ExpireLeaseResponse struct {
	LocksReleased int
}

func (f *FSM) applyExpireLease(cmd *raftlog.ExpireLeaseCommand) (any, error) {
	expiredAt, err := commandTime(cmd.GetExpiredAtUnixNano())
	if err != nil {
		return nil, err
	}

	lease, exists := f.leases[cmd.GetLeaseId()]
	if !exists {
		return ExpireLeaseResponse{}, nil
	}

	if !lease.IsExpired(expiredAt) {
		// A renewal may have committed before this expiry command. In that case,
		// the expiry command is stale and must not delete the renewed lease.
		return ExpireLeaseResponse{}, nil
	}

	// Lease expiry cascades to lock release so crashed clients do not hold locks
	// forever. This is safe because downstream writes are protected by fencing
	// tokens even if a paused client resumes later.
	locksReleased := 0
	for lockName, lock := range f.locks {
		if lock.LeaseID == lease.LeaseID {
			delete(f.locks, lockName)
			locksReleased++
		}
	}

	delete(f.leases, lease.LeaseID)

	return ExpireLeaseResponse{
		LocksReleased: locksReleased,
	}, nil
}

func (f *FSM) applyUpsertEndpoint(cmd *raftlog.UpsertEndpointCommand) (any, error) {
	if cmd.GetNodeId() == "" || cmd.GetGrpcAddress() == "" {
		return nil, domain.ErrInvalidClusterNode
	}

	f.endpoints[cmd.GetNodeId()] = cmd.GetGrpcAddress()
	return nil, nil
}

func (f *FSM) applyRemoveEndpoint(cmd *raftlog.RemoveEndpointCommand) (any, error) {
	if cmd.GetNodeId() == "" {
		return nil, domain.ErrInvalidClusterNode
	}

	delete(f.endpoints, cmd.GetNodeId())
	return nil, nil
}

func (f *FSM) GetLock(lockName string) (*domain.Lock, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	lock, exists := f.locks[lockName]
	return lock, exists
}

func (f *FSM) LockState(lockName string) (bool, uint64) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	lock, exists := f.locks[lockName]
	if !exists {
		return false, 0
	}
	return true, lock.FencingToken
}

func (f *FSM) GetLease(leaseID uint64) (*domain.Lease, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	lease, exists := f.leases[leaseID]
	return lease, exists
}

func (f *FSM) GetEndpoint(nodeID string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	addr, exists := f.endpoints[nodeID]
	return addr, exists
}

// Stats holds a snapshot of FSM counters for observability
type Stats struct {
	Locks          int
	Leases         int
	FencingCounter uint64
}

func (f *FSM) Stats() Stats {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return Stats{
		Locks:          len(f.locks),
		Leases:         len(f.leases),
		FencingCounter: f.fencingCounter,
	}
}

// GetExpiredLeases returns the IDs of all leases whose expiry is at or before now.
// Called by the leader's background expiry loop.
func (f *FSM) GetExpiredLeases(now tm.Time) []uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()

	var expired []uint64
	for leaseID, lease := range f.leases {
		if lease.IsExpired(now) {
			expired = append(expired, leaseID)
		}
	}

	return expired
}

func commandTime(unixNano int64) (tm.Time, error) {
	if unixNano <= 0 {
		return tm.Time{}, fmt.Errorf("invalid command timestamp")
	}
	return tm.Unix(0, unixNano).UTC(), nil
}
