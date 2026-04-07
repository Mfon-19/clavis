package domain

import "time"

// Lease is a time-bounded ownership grant that authorizes a client to hold locks.
// When a lease expires (because the client crashed or stopped sending heartbeats),
// all locks bound to it are automatically released
type Lease struct {
	LeaseID           uint64
	OwnerID           string
	ExpiresAtUnixNano int64
	TTL               time.Duration
}

// ExpiresAt returns the wall clock time at which this lease expires
func (l *Lease) ExpiresAt() time.Time {
	return time.Unix(0, l.ExpiresAtUnixNano).UTC()
}

// IsExpired reports whether the lease has expired at the given wall-clock instant
func (l *Lease) IsExpired(at time.Time) bool {
	return !at.Before(l.ExpiresAt())
}
