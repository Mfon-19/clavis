package domain

import "errors"

// Sentinel errors used across the FSM, service, and transport layers
var (
	// Lease errors
	ErrLeaseNotFound   = errors.New("lease not found")
	ErrLeaseExpired    = errors.New("lease has expired")
	ErrInvalidLeaseTTL = errors.New("invalid lease TTL")

	// ErrInvalidTimestamp indicates a command carried a malformed wall-clock timestamp
	ErrInvalidTimestamp = errors.New("invalid command timestamp")

	// Lock errors
	ErrLockNotFound    = errors.New("lock not found")
	ErrLockAlreadyHeld = errors.New("lock is already held by another client")
	ErrNotLockOwner    = errors.New("caller is not the lock owner")
	ErrInvalidLeaseID  = errors.New("invalid lease ID")

	// ErrStaleToken means a fencing token is older than the current holder's token.
	ErrStaleToken = errors.New("fencing token is stale")

	// Cluster membership errors
	ErrNodeNotFound       = errors.New("cluster node not found")
	ErrInvalidClusterNode = errors.New("invalid cluster node metadata")

	// ErrUnknownCommand is returned when the FSM receives an unrecognized command type.
	ErrUnknownCommand = errors.New("unknown command type")
)
