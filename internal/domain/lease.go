package domain

import "time"

// Lease is a time-bounded ownership grant that authorizes a client to hold locks.
// When a lease expires (because the client crashed or stopped sending heartbeats),
// all locks bound to it are automatically released
//
// The replicated lease records only its initial deadline. Renewals are tracked
// in memory on the leader, which alone decides when a lease has expired; a
// lease stays alive in replicated state until an expiry command commits.
type Lease struct {
	LeaseID           uint64
	OwnerID           string
	ExpiresAtUnixNano int64 // creation time plus TTL
	TTL               time.Duration
}

// ExpiresAt returns the lease's initial deadline: its creation time plus TTL.
func (l *Lease) ExpiresAt() time.Time {
	return time.Unix(0, l.ExpiresAtUnixNano).UTC()
}
