package cluster

import (
	"time"

	"github.com/Mfon-19/clavis/internal/raftlog"
)

// recordPendingRenewal tracks in-flight renewal that has been submitted to
// Raft but not yet committed. This prevents the lease expiry loop from racing
// ahead and expiring a lease that is about to be renewed
func (n *Node) recordPendingRenewal(leaseID uint64, proposedAt time.Time) {
	n.pendingRenewalsMu.Lock()
	defer n.pendingRenewalsMu.Unlock()

	if _, exists := n.pendingRenewals[leaseID]; !exists {
		n.pendingRenewals[leaseID] = make(map[int64]int)
	}
	n.pendingRenewals[leaseID][proposedAt.UnixNano()]++
}

// clearPendingRenewal removes a tracked renewal after it has been committed (or failed).
// Uses reference counting so duplicate timestamps are handled
func (n *Node) clearPendingRenewal(leaseID uint64, proposedAt time.Time) {
	n.pendingRenewalsMu.Lock()
	defer n.pendingRenewalsMu.Unlock()

	leaseRenewals, exists := n.pendingRenewals[leaseID]
	if !exists {
		return
	}

	timestamp := proposedAt.UnixNano()
	count := leaseRenewals[timestamp]
	if count <= 1 {
		delete(leaseRenewals, timestamp)
	} else {
		leaseRenewals[timestamp] = count - 1
	}

	if len(leaseRenewals) == 0 {
		delete(n.pendingRenewals, leaseID)
	}
}

// hasPendingRenewalBefore reports whether a renewal proposed before deadline
// is still in the Raft pipeline. Such a renewal will extend the lease when it
// commits, so the lease must not be expired until it has.
func (n *Node) hasPendingRenewalBefore(leaseID uint64, deadline time.Time) bool {
	n.pendingRenewalsMu.Lock()
	defer n.pendingRenewalsMu.Unlock()

	for unixNano := range n.pendingRenewals[leaseID] {
		if unixNano < deadline.UnixNano() {
			return true
		}
	}
	return false
}

// shouldExpireLease returns true if the lease is expired at now, no timely
// renewal is still in flight, and this node has been leader for at least one
// full TTL.
//
// The leadership grace period exists because pending renewals are tracked only
// on the leader that proposed them, and clients need time to find a new leader
// after failover. Without it, a new leader would immediately expire every lease
// whose holder was mid-reconnect.
func (n *Node) shouldExpireLease(leaseID uint64, now, leaderSince time.Time) bool {
	lease, exists := n.fsm.GetLease(leaseID)
	if !exists || !lease.IsExpired(now) {
		return false
	}
	if now.Before(leaderSince.Add(lease.TTL)) {
		return false
	}
	return !n.hasPendingRenewalBefore(leaseID, lease.ExpiresAt())
}

// ApplyRenewLease renews a lease through Raft consensus while tracking the
// renewal as pending to prevent the expiry loop from racing ahead
func (n *Node) ApplyRenewLease(leaseID uint64, proposedAt time.Time) (any, error) {
	proposedAt = proposedAt.UTC()
	n.recordPendingRenewal(leaseID, proposedAt)
	defer n.clearPendingRenewal(leaseID, proposedAt)

	return n.Apply(raftlog.NewRenewLeaseCmd(leaseID, proposedAt))
}

func (n *Node) leaseExpiryLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// leaderTerm and leaderSince identify the current leadership stint. Polling
	// IsLeader alone would miss a lose-and-regain between ticks; the Raft term
	// always changes across such a transition.
	var leaderTerm uint64
	var leaderSince time.Time

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			if !n.IsLeader() {
				leaderTerm = 0
				continue
			}

			now := n.Now()
			if term := n.raft.CurrentTerm(); term != leaderTerm {
				leaderTerm = term
				leaderSince = now
			}

			for _, leaseID := range n.fsm.GetExpiredLeases(now) {
				if !n.shouldExpireLease(leaseID, now, leaderSince) {
					continue
				}
				// Expiry still goes through Raft. Followers must see the same
				// lease deletion and lock release in the same log order
				_, _ = n.Apply(raftlog.NewExpireLeaseCmd(leaseID, now))
			}
		}
	}
}
