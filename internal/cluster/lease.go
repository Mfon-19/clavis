package cluster

import (
	"github.com/Mfon-19/clavis/internal/raftlog"
	"sort"
	"time"
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

// leaseExpiryHorizon projects the furthest possible expiry time for a lease,
// accounting for any pending renewals that have been submitted to Raft but not
// yet applied. This is the key correctness mechanism that prevents the expiry
// loop from expiring a lease whose renewal is still in the Raft pipeline
func (n *Node) leaseExpiryHorizon(leaseID uint64) (time.Time, bool) {
	lease, exists := n.fsm.GetLease(leaseID)
	if !exists {
		return time.Time{}, false
	}

	n.pendingRenewalsMu.Lock()
	leaseRenewals := n.pendingRenewals[leaseID]
	pendingTimes := make([]int64, 0, len(leaseRenewals))
	for unixNano, count := range leaseRenewals {
		for i := 0; i < count; i++ {
			pendingTimes = append(pendingTimes, unixNano)
		}
	}
	n.pendingRenewalsMu.Unlock()

	if len(pendingTimes) == 0 {
		return lease.ExpiresAt(), true
	}

	sort.Slice(pendingTimes, func(i, j int) bool {
		return pendingTimes[i] < pendingTimes[j]
	})

	expiry := lease.ExpiresAt()
	for _, unixNano := range pendingTimes {
		proposedAt := time.Unix(0, unixNano).UTC()
		if proposedAt.After(expiry) {
			continue
		}
		expiry = proposedAt.Add(lease.TTL)
	}

	return expiry, true
}

// shouldExpireLease returns true if the lease is expired even after accounting
// for all pending renewals in the Raft pipeline
func (n *Node) shouldExpireLease(leaseID uint64, now time.Time) bool {
	expiry, exists := n.leaseExpiryHorizon(leaseID)
	if !exists {
		return false
	}

	return !now.Before(expiry)
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

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			if !n.IsLeader() {
				continue
			}

			now := time.Now().UTC()
			expired := n.fsm.GetExpiredLeases(now)
			for _, leaseID := range expired {
				// A lease can look expired in the committed FSM while a timely
				// renewal is still waiting for Raft commit. Re-check with the
				// pending renewal horizon before proposing an expiry command
				if !n.shouldExpireLease(leaseID, now) {
					continue
				}
				// Expiry still goes through Raft. Followers must see the same
				// lease deletion and lock release in the same log order
				_, _ = n.Apply(raftlog.NewExpireLeaseCmd(leaseID, now))
			}
		}
	}
}
