package domain

// Lock is a distributed mutex bound to a [Lease]. Each lock carries a globally
// monotonic fencing token that prevents split-brain scenarios, i.e., downstream
// systems can reject writes from stale lock holders by comparing tokens.
//
// The fencing token is incremented on every new lock acquisition across the entire
// cluster (not per-lock), so tokens are globally ordered
type Lock struct {
	Name         string // Unique lock identifier
	OwnerID      string // Client that holds the lock
	FencingToken uint64 // Monotonic token assigned at acquisition time
	LeaseID      uint64 // Lease that authorizes this lock. expires when the Lease does
}
