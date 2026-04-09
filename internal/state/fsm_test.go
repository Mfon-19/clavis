package state

import (
	"testing"
	"time"

	"github.com/Mfon-19/clavis/internal/domain"
	"github.com/Mfon-19/clavis/internal/raftlog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fixedTestTime(offset time.Duration) time.Time {
	return time.Unix(1_700_000_000, 0).UTC().Add(offset)
}

func mustCreateLease(t testing.TB, fsm *FSM, owner string, ttl time.Duration, createdAt time.Time) CreateLeaseResponse {
	t.Helper()

	result, err := fsm.Apply(raftlog.NewCreateLeaseCmd(owner, ttl, createdAt))
	require.NoError(t, err)

	resp, ok := result.(CreateLeaseResponse)
	require.True(t, ok, "expected CreateLeaseResponse")
	return resp
}

func mustAcquireLock(t testing.TB, fsm *FSM, lockName, owner string, leaseID uint64, acquiredAt time.Time) AcquireLockResponse {
	t.Helper()

	result, err := fsm.Apply(raftlog.NewAcquireLockCmd(lockName, owner, leaseID, acquiredAt))
	require.NoError(t, err)

	resp, ok := result.(AcquireLockResponse)
	require.True(t, ok, "expected AcquireLockResponse")
	return resp
}

// TestLeaseLifecycle verifies that creating and renewing a lease uses the
// command timestamps carried in the log and preserves exact lease state.
func TestLeaseLifecycle(t *testing.T) {
	fsm := NewFSM()
	createdAt := fixedTestTime(0)

	createResp := mustCreateLease(t, fsm, "client-1", 10*time.Second, createdAt)
	assert.Equal(t, uint64(1), createResp.LeaseID)
	assert.Equal(t, createdAt.Add(10*time.Second), createResp.ExpiresAt)

	lease, ok := fsm.GetLease(createResp.LeaseID)
	require.True(t, ok)
	assert.Equal(t, "client-1", lease.OwnerID)
	assert.Equal(t, 10*time.Second, lease.TTL)
	assert.Equal(t, createResp.ExpiresAt.UnixNano(), lease.ExpiresAtUnixNano)

	renewedAt := createdAt.Add(3 * time.Second)
	result, err := fsm.Apply(raftlog.NewRenewLeaseCmd(createResp.LeaseID, renewedAt))
	require.NoError(t, err)

	renewResp, ok := result.(RenewLeaseResponse)
	require.True(t, ok, "expected RenewLeaseResponse")
	assert.Equal(t, renewedAt.Add(10*time.Second), renewResp.ExpiresAt)
	assert.Equal(t, 10*time.Second, renewResp.TTL)

	lease, ok = fsm.GetLease(createResp.LeaseID)
	require.True(t, ok)
	assert.Equal(t, renewedAt.Add(10*time.Second).UnixNano(), lease.ExpiresAtUnixNano)
	assert.Equal(t, stateStats(0, 1, 0), fsm.Stats())
}

// TestAcquireLockSemantics verifies the core lock invariants: first acquisition
// increments fencing, re-acquisition by the same lease is idempotent, and a
// different lease cannot take the lock while it is held.
func TestAcquireLock(t *testing.T) {
	fsm := NewFSM()
	createdAt := fixedTestTime(0)

	lease1 := mustCreateLease(t, fsm, "client-1", 10*time.Second, createdAt).LeaseID
	lease2 := mustCreateLease(t, fsm, "client-2", 10*time.Second, createdAt.Add(time.Second)).LeaseID

	first := mustAcquireLock(t, fsm, "my-lock", "client-1", lease1, createdAt.Add(2*time.Second))
	assert.Equal(t, uint64(1), first.FencingToken)
	assert.Equal(t, 10*time.Second, first.LeaseTTL)

	second := mustAcquireLock(t, fsm, "my-lock", "client-1", lease1, createdAt.Add(3*time.Second))
	assert.Equal(t, first.FencingToken, second.FencingToken, "same lease should re-acquire idempotently")
	assert.Equal(t, stateStats(1, 2, 1), fsm.Stats(), "re-acquire should not advance fencing counter")

	lock, ok := fsm.GetLock("my-lock")
	require.True(t, ok)
	assert.Equal(t, lease1, lock.LeaseID)
	assert.Equal(t, "client-1", lock.OwnerID)
	assert.Equal(t, uint64(1), lock.FencingToken)

	_, err := fsm.Apply(raftlog.NewAcquireLockCmd("my-lock", "client-2", lease2, createdAt.Add(4*time.Second)))
	assert.ErrorIs(t, err, domain.ErrLockAlreadyHeld)

	other := mustAcquireLock(t, fsm, "other-lock", "client-1", lease1, createdAt.Add(5*time.Second))
	assert.Equal(t, uint64(2), other.FencingToken)
	assert.Equal(t, stateStats(2, 2, 2), fsm.Stats())
}

// TestLeaseOwnershipAndExpiryChecks verifies that lock operations reject
// expired leases, missing leases, and callers whose owner ID does not match
// the lease that authorizes the lock.
func TestLeaseChecks(t *testing.T) {
	fsm := NewFSM()
	createdAt := fixedTestTime(0)

	leaseResp := mustCreateLease(t, fsm, "client-1", 2*time.Second, createdAt)

	_, err := fsm.Apply(raftlog.NewAcquireLockCmd("missing-lease-lock", "client-1", 999, createdAt))
	assert.ErrorIs(t, err, domain.ErrLeaseNotFound)

	_, err = fsm.Apply(raftlog.NewAcquireLockCmd("wrong-owner-lock", "client-2", leaseResp.LeaseID, createdAt.Add(time.Second)))
	assert.ErrorIs(t, err, domain.ErrNotLockOwner)

	_, err = fsm.Apply(raftlog.NewAcquireLockCmd("expired-lock", "client-1", leaseResp.LeaseID, createdAt.Add(3*time.Second)))
	assert.ErrorIs(t, err, domain.ErrLeaseExpired)

	_, err = fsm.Apply(raftlog.NewRenewLeaseCmd(leaseResp.LeaseID, createdAt.Add(3*time.Second)))
	assert.ErrorIs(t, err, domain.ErrLeaseExpired)
}

// TestReleaseAndExpirySemantics verifies that only the owning lease can release
// a lock, and that a stale expiry command becomes a no-op if the lease was
// renewed before the expiry command's timestamp.
func TestReleaseAndExpiry(t *testing.T) {
	fsm := NewFSM()
	createdAt := fixedTestTime(0)

	lease1 := mustCreateLease(t, fsm, "client-1", 5*time.Second, createdAt).LeaseID
	lease2 := mustCreateLease(t, fsm, "client-2", 5*time.Second, createdAt.Add(time.Second)).LeaseID

	lockResp := mustAcquireLock(t, fsm, "my-lock", "client-1", lease1, createdAt.Add(2*time.Second))
	assert.Equal(t, uint64(1), lockResp.FencingToken)

	_, err := fsm.Apply(raftlog.NewReleaseLockCmd("my-lock", lease2))
	assert.ErrorIs(t, err, domain.ErrNotLockOwner)

	result, err := fsm.Apply(raftlog.NewReleaseLockCmd("my-lock", lease1))
	require.NoError(t, err)
	releaseResp, ok := result.(ReleaseLockResponse)
	require.True(t, ok)
	assert.True(t, releaseResp.Released)
	assert.Equal(t, stateStats(0, 2, 1), fsm.Stats())

	mustAcquireLock(t, fsm, "my-lock", "client-1", lease1, createdAt.Add(3*time.Second))

	result, err = fsm.Apply(raftlog.NewRenewLeaseCmd(lease1, createdAt.Add(4*time.Second)))
	require.NoError(t, err)
	renewResp := result.(RenewLeaseResponse)
	assert.Equal(t, createdAt.Add(9*time.Second), renewResp.ExpiresAt)

	result, err = fsm.Apply(raftlog.NewExpireLeaseCmd(lease1, createdAt.Add(5*time.Second)))
	require.NoError(t, err)
	expireResp := result.(ExpireLeaseResponse)
	assert.Equal(t, 0, expireResp.LocksReleased, "stale expiry must not delete a renewed lease")

	lock, ok := fsm.GetLock("my-lock")
	require.True(t, ok)
	assert.Equal(t, lease1, lock.LeaseID)

	result, err = fsm.Apply(raftlog.NewExpireLeaseCmd(lease1, createdAt.Add(9*time.Second)))
	require.NoError(t, err)
	expireResp = result.(ExpireLeaseResponse)
	assert.Equal(t, 1, expireResp.LocksReleased)

	_, ok = fsm.GetLease(lease1)
	assert.False(t, ok)
	_, ok = fsm.GetLock("my-lock")
	assert.False(t, ok)
	assert.Equal(t, stateStats(0, 1, 2), fsm.Stats())
}

// TestMembershipRegistration verifies that membership commands maintain an
// exact replicated view of cluster member metadata and reject incomplete input.
func TestMembership(t *testing.T) {
	fsm := NewFSM()

	member := domain.ClusterMember{
		NodeID:      "node-1",
		RaftAddress: "127.0.0.1:7000",
		GRPCAddress: "127.0.0.1:9000",
	}

	result, err := fsm.Apply(raftlog.NewRegisterNodeCmd(member))
	require.NoError(t, err)
	assert.True(t, result.(RegisterNodeResponse).Registered)

	stored, exists := fsm.GetMember(member.NodeID)
	require.True(t, exists)
	assert.Equal(t, &member, stored)
	assert.Equal(t, []domain.ClusterMember{member}, fsm.Members())

	result, err = fsm.Apply(raftlog.NewDeregisterNodeCmd(member.NodeID))
	require.NoError(t, err)
	assert.True(t, result.(DeregisterNodeResponse).Removed)
	assert.Empty(t, fsm.Members())

	_, err = fsm.Apply(raftlog.NewRegisterNodeCmd(domain.ClusterMember{
		NodeID:      "node-2",
		RaftAddress: "127.0.0.1:7001",
	}))
	assert.ErrorIs(t, err, domain.ErrInvalidClusterNode)
}

// TestRejectsCommandsWithoutTimestamps verifies that the FSM rejects malformed
// log entries whose timestamp fields are missing, preventing nondeterministic
// replay behavior on restore or log replication.
func TestRejectsMissingTimestamps(t *testing.T) {
	fsm := NewFSM()

	tests := []struct {
		name string
		cmd  *raftlog.CommandWrapper
	}{
		{
			name: "create lease",
			cmd: &raftlog.CommandWrapper{
				Type: raftlog.CommandType_COMMAND_TYPE_CREATE_LEASE,
				Payload: &raftlog.CommandWrapper_CreateLease{CreateLease: &raftlog.CreateLeaseCommand{
					OwnerId:  "client-1",
					TtlNanos: (20 * time.Millisecond).Nanoseconds(),
				}},
			},
		},
		{
			name: "renew lease",
			cmd: &raftlog.CommandWrapper{
				Type: raftlog.CommandType_COMMAND_TYPE_RENEW_LEASE,
				Payload: &raftlog.CommandWrapper_RenewLease{RenewLease: &raftlog.RenewLeaseCommand{
					LeaseId: 1,
				}},
			},
		},
		{
			name: "acquire lock",
			cmd: &raftlog.CommandWrapper{
				Type: raftlog.CommandType_COMMAND_TYPE_ACQUIRE_LOCK,
				Payload: &raftlog.CommandWrapper_AcquireLock{AcquireLock: &raftlog.AcquireLockCommand{
					LockName: "my-lock",
					OwnerId:  "client-1",
					LeaseId:  1,
				}},
			},
		},
		{
			name: "expire lease",
			cmd: &raftlog.CommandWrapper{
				Type: raftlog.CommandType_COMMAND_TYPE_EXPIRE_LEASE,
				Payload: &raftlog.CommandWrapper_ExpireLease{ExpireLease: &raftlog.ExpireLeaseCommand{
					LeaseId: 1,
				}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := fsm.Apply(tt.cmd)
			require.EqualError(t, err, "invalid command timestamp")
		})
	}
}

func stateStats(locks, leases int, fencing uint64) Stats {
	return Stats{
		Locks:          locks,
		Leases:         leases,
		FencingCounter: fencing,
	}
}
