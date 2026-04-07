package state

import (
	"github.com/Mfon-19/clavis/internal/domain"
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

	locks   map[string]*domain.Lock  // lock name -> Lock
	leases  map[uint64]*domain.Lease // lease ID -> Lease
	members map[string]*domain.ClusterMember

	fencingCounter uint64 // global monotonic fencing token counter
	nextLeaseID    uint64 // next leaseID to assign
}

// NewFSM creates an empty state machine with lease IDs starting from 1
func NewFSM() *FSM {
	return &FSM{
		locks:          make(map[string]*domain.Lock),
		leases:         make(map[uint64]*domain.Lease),
		members:        make(map[string]*domain.ClusterMember),
		fencingCounter: 0,
		nextLeaseID:    1,
	}
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

func (f *FSM) GetMember(nodeID string) (*domain.ClusterMember, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	member, exists := f.members[nodeID]
	if !exists {
		return nil, false
	}

	memberCopy := *member
	return &memberCopy, true
}

// Members returns a snapshot of all registered cluster members.
func (f *FSM) Members() []domain.ClusterMember {
	f.mu.RLock()
	defer f.mu.RUnlock()

	members := make([]domain.ClusterMember, 0, len(f.members))
	for _, member := range f.members {
		members = append(members, *member)
	}

	return members
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
