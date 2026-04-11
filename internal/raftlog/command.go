// Package raftlog defines the protobuf commands serialized into Raft log entries.
// The generated CommandWrapper oneof is the internal command type. This file only
// provides typed constructors so call sites stay readable
package raftlog

import (
	"time"
)

// NewCreateLeaseCmd builds the Raft log entry that creates a lease. The caller
// supplies now so the FSM does not depend on local apply-time clocks.
func NewCreateLeaseCmd(ownerID string, ttl time.Duration, now time.Time) *CommandWrapper {
	return &CommandWrapper{
		Type: CommandType_COMMAND_TYPE_CREATE_LEASE,
		Payload: &CommandWrapper_CreateLease{
			CreateLease: &CreateLeaseCommand{
				OwnerId:          ownerID,
				TtlNanos:         ttl.Nanoseconds(),
				CreateAtUnixNano: now.UTC().UnixNano(),
			},
		},
	}
}

// NewRenewLeaseCmd builds the Raft log entry that renews a lease.
func NewRenewLeaseCmd(leaseID uint64, now time.Time) *CommandWrapper {
	return &CommandWrapper{
		Type: CommandType_COMMAND_TYPE_RENEW_LEASE,
		Payload: &CommandWrapper_RenewLease{
			RenewLease: &RenewLeaseCommand{
				LeaseId:           leaseID,
				RenewedAtUnixNano: now.UTC().UnixNano(),
			},
		},
	}
}

// NewAcquireLockCmd builds the Raft log entry that attempts to acquire a lock
// and, if successful, advances the global fencing counter.
func NewAcquireLockCmd(lockName, ownerID string, leaseID uint64, now time.Time) *CommandWrapper {
	return &CommandWrapper{
		Type: CommandType_COMMAND_TYPE_ACQUIRE_LOCK,
		Payload: &CommandWrapper_AcquireLock{
			AcquireLock: &AcquireLockCommand{
				LockName:           lockName,
				OwnerId:            ownerID,
				LeaseId:            leaseID,
				AcquiredAtUnixNano: now.UTC().UnixNano(),
			},
		},
	}
}

// NewReleaseLockCmd builds the Raft log entry that releases a lock.
func NewReleaseLockCmd(lockName string, leaseID uint64) *CommandWrapper {
	return &CommandWrapper{
		Type: CommandType_COMMAND_TYPE_RELEASE_LOCK,
		Payload: &CommandWrapper_ReleaseLock{
			ReleaseLock: &ReleaseLockCommand{
				LockName: lockName,
				LeaseId:  leaseID,
			},
		},
	}
}

// NewExpireLeaseCmd builds the Raft log entry that expires a lease and releases
// any locks associated with it.
func NewExpireLeaseCmd(leaseID uint64, now time.Time) *CommandWrapper {
	return &CommandWrapper{
		Type: CommandType_COMMAND_TYPE_EXPIRE_LEASE,
		Payload: &CommandWrapper_ExpireLease{
			ExpireLease: &ExpireLeaseCommand{
				LeaseId:           leaseID,
				ExpiredAtUnixNano: now.UTC().UnixNano(),
			},
		},
	}
}
