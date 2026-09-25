package cluster

import (
	"fmt"
	"sync"
	"time"

	"github.com/Mfon-19/clavis/internal/domain"
	"github.com/Mfon-19/clavis/internal/raftlog"
)

// leaseTracker is the leader's in-memory view of lease liveness.
//
// Renewals are not written to the Raft log. The leader records when it last
// renewed each lease, and only the leader decides a lease is dead, which it
// then makes durable with an expiry command. A lease's deadline is one TTL
// after the latest of:
//
//   - its creation, from the replicated lease record,
//   - its last renewal on this leader, and
//   - the moment this node became leader.
//
// The last term is the failover rule. Renewal times die with the old leader,
// so a new leader treats every lease as renewed when it took over. Any renewal
// the old leader acknowledged happened before that, because the old leader
// verified its leadership with a quorum before acknowledging.
type leaseTracker struct {
	mu          sync.Mutex
	term        uint64
	leaderSince time.Time
	renewedAt   map[uint64]time.Time
	expiring    map[uint64]bool
}

// syncTerm resets the tracker when this node starts a new leadership term.
// The caller must hold t.mu.
func (t *leaseTracker) syncTerm(term uint64, now time.Time) {
	if term == t.term {
		return
	}
	t.term = term
	t.leaderSince = now
	t.renewedAt = make(map[uint64]time.Time)
	t.expiring = make(map[uint64]bool)
}

// deadline returns when lease expires unless renewed again. The caller must
// hold t.mu.
func (t *leaseTracker) deadline(lease domain.Lease) time.Time {
	deadline := lease.ExpiresAt()
	for _, from := range []time.Time{t.leaderSince, t.renewedAt[lease.LeaseID]} {
		if candidate := from.Add(lease.TTL); !from.IsZero() && candidate.After(deadline) {
			deadline = candidate
		}
	}
	return deadline
}

// renew records a renewal at now, or reports that the lease is already dead.
// Checking and recording under one lock is what keeps a renewal from racing
// the expiry loop: once a lease is claimed for expiry, it cannot be renewed.
func (t *leaseTracker) renew(term uint64, now time.Time, lease domain.Lease) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.syncTerm(term, now)
	if t.expiring[lease.LeaseID] || !now.Before(t.deadline(lease)) {
		return domain.ErrLeaseExpired
	}
	t.renewedAt[lease.LeaseID] = now
	return nil
}

// claimExpired returns the leases past their deadline at now and marks them
// as expiring, so later renewals are rejected.
func (t *leaseTracker) claimExpired(term uint64, now time.Time, leases []domain.Lease) []uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.syncTerm(term, now)
	var expired []uint64
	for _, lease := range leases {
		if !t.expiring[lease.LeaseID] && !now.Before(t.deadline(lease)) {
			t.expiring[lease.LeaseID] = true
			expired = append(expired, lease.LeaseID)
		}
	}
	return expired
}

// alive reports whether a lease is still within its deadline and not being
// expired.
func (t *leaseTracker) alive(term uint64, now time.Time, lease domain.Lease) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.syncTerm(term, now)
	return !t.expiring[lease.LeaseID] && now.Before(t.deadline(lease))
}

// finishExpiry forgets a lease once its expiry committed. If the expiry
// failed, the lease is unclaimed so the next pass can retry it.
func (t *leaseTracker) finishExpiry(leaseID uint64, committed bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.expiring, leaseID)
	if committed {
		delete(t.renewedAt, leaseID)
	}
}

// RenewLease extends a lease by its TTL without writing to the Raft log. It
// returns the lease's TTL once the renewal is safe to acknowledge.
func (n *Node) RenewLease(leaseID uint64) (time.Duration, error) {
	term := n.raft.CurrentTerm()

	// A new leader may still have an expiry from the previous term in its log,
	// committed or not. Wait until every earlier entry has been applied, so
	// the lease lookup below cannot acknowledge a lease that is about to be
	// deleted.
	if err := n.awaitTermApplied(term); err != nil {
		return 0, err
	}

	lease, exists := n.fsm.GetLease(leaseID)
	if !exists {
		return 0, domain.ErrLeaseNotFound
	}
	if err := n.leases.renew(term, n.Now(), *lease); err != nil {
		return 0, err
	}

	// Only acknowledge once a quorum confirms this node is still leader. That
	// orders the renewal before any future leader's term, whose grace period
	// then covers it. This is a network round trip, not a disk write.
	if err := n.raft.VerifyLeader().Error(); err != nil {
		return 0, fmt.Errorf("verify leadership: %w", err)
	}
	return lease.TTL, nil
}

// CheckLeaseAlive rejects a lease that exists in replicated state but that the
// leader already considers dead. It is a courtesy check before acquiring a
// lock: if the lease expires right after, the expiry releases the lock too.
func (n *Node) CheckLeaseAlive(leaseID uint64) error {
	lease, exists := n.fsm.GetLease(leaseID)
	if !exists {
		return domain.ErrLeaseNotFound
	}
	if !n.leases.alive(n.raft.CurrentTerm(), n.Now(), *lease) {
		return domain.ErrLeaseExpired
	}
	return nil
}

// awaitTermApplied blocks until the FSM has applied every entry from before
// the given leadership term. It issues one Raft barrier per term.
func (n *Node) awaitTermApplied(term uint64) error {
	n.barrierMu.Lock()
	defer n.barrierMu.Unlock()

	if n.barrierTerm == term {
		return nil
	}
	if err := n.raft.Barrier(5 * time.Second).Error(); err != nil {
		return fmt.Errorf("apply earlier entries: %w", err)
	}
	n.barrierTerm = term
	return nil
}

// leaseExpiryLoop runs on every node but acts only on the leader. Every tick
// it expires the leases whose deadline has passed, through Raft so every node
// deletes the lease and releases its locks at the same log position.
func (n *Node) leaseExpiryLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			if !n.IsLeader() {
				continue
			}

			term := n.raft.CurrentTerm()
			for _, leaseID := range n.leases.claimExpired(term, n.Now(), n.fsm.Leases()) {
				_, err := n.Apply(raftlog.NewExpireLeaseCmd(leaseID))
				n.leases.finishExpiry(leaseID, err == nil)
			}
		}
	}
}
