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

func mustAcquireLock(t testing.TB, fsm *FSM, lockName, owner string, leaseID uint64) AcquireLockResponse {
	t.Helper()

	result, err := fsm.Apply(raftlog.NewAcquireLockCmd(lockName, owner, leaseID))
	require.NoError(t, err)

	resp, ok := result.(AcquireLockResponse)
	require.True(t, ok, "expected AcquireLockResponse")
	return resp
}

// TestAcquireLockSemantics verifies the core lock invariants: first acquisition
// increments fencing, re-acquisition by the same lease is idempotent, and a
// different lease cannot take the lock while it is held.
func TestAcquireLock(t *testing.T) {
	fsm := NewFSM()
	createdAt := fixedTestTime(0)

	lease1 := mustCreateLease(t, fsm, "client-1", 10*time.Second, createdAt).LeaseID
	lease2 := mustCreateLease(t, fsm, "client-2", 10*time.Second, createdAt.Add(time.Second)).LeaseID

	first := mustAcquireLock(t, fsm, "my-lock", "client-1", lease1)
	assert.Equal(t, uint64(1), first.FencingToken)
	assert.Equal(t, 10*time.Second, first.LeaseTTL)

	second := mustAcquireLock(t, fsm, "my-lock", "client-1", lease1)
	assert.Equal(t, first.FencingToken, second.FencingToken, "same lease should re-acquire idempotently")
	assert.Equal(t, 10*time.Second, second.LeaseTTL, "same lease should receive the original TTL")
	assert.Equal(t, stateStats(1, 2, 1), fsm.Stats(), "re-acquire should not advance fencing counter")

	lock, ok := fsm.GetLock("my-lock")
	require.True(t, ok)
	assert.Equal(t, lease1, lock.LeaseID)
	assert.Equal(t, "client-1", lock.OwnerID)
	assert.Equal(t, uint64(1), lock.FencingToken)

	_, err := fsm.Apply(raftlog.NewAcquireLockCmd("my-lock", "client-2", lease2))
	assert.ErrorIs(t, err, domain.ErrLockAlreadyHeld)

	other := mustAcquireLock(t, fsm, "other-lock", "client-1", lease1)
	assert.Equal(t, uint64(2), other.FencingToken)
	assert.Equal(t, stateStats(2, 2, 2), fsm.Stats())
}

// TestLeaseChecks verifies that acquiring a lock requires an existing lease
// owned by the caller. Lease liveness is decided by the leader, not the FSM.
func TestLeaseChecks(t *testing.T) {
	fsm := NewFSM()
	leaseID := mustCreateLease(t, fsm, "client-1", 2*time.Second, fixedTestTime(0)).LeaseID

	_, err := fsm.Apply(raftlog.NewAcquireLockCmd("missing-lease-lock", "client-1", 999))
	assert.ErrorIs(t, err, domain.ErrLeaseNotFound)

	_, err = fsm.Apply(raftlog.NewAcquireLockCmd("wrong-owner-lock", "client-2", leaseID))
	assert.ErrorIs(t, err, domain.ErrNotLockOwner)
}

// TestReleaseAndExpiry verifies that only the owning lease can release a lock,
// that expiring a lease releases its locks, and that renewal entries written
// by older versions are ignored on replay.
func TestReleaseAndExpiry(t *testing.T) {
	fsm := NewFSM()
	createdAt := fixedTestTime(0)

	lease1 := mustCreateLease(t, fsm, "client-1", 5*time.Second, createdAt).LeaseID
	lease2 := mustCreateLease(t, fsm, "client-2", 5*time.Second, createdAt.Add(time.Second)).LeaseID

	lockResp := mustAcquireLock(t, fsm, "my-lock", "client-1", lease1)
	assert.Equal(t, uint64(1), lockResp.FencingToken)

	_, err := fsm.Apply(raftlog.NewReleaseLockCmd("my-lock", lease2))
	assert.ErrorIs(t, err, domain.ErrNotLockOwner)

	_, err = fsm.Apply(raftlog.NewReleaseLockCmd("my-lock", lease1))
	require.NoError(t, err)
	assert.Equal(t, stateStats(0, 2, 1), fsm.Stats())

	mustAcquireLock(t, fsm, "my-lock", "client-1", lease1)

	legacyRenewal := &raftlog.CommandWrapper{
		Type:    raftlog.CommandType_COMMAND_TYPE_RENEW_LEASE,
		Payload: &raftlog.CommandWrapper_RenewLease{RenewLease: &raftlog.RenewLeaseCommand{LeaseId: lease1}},
	}
	_, err = fsm.Apply(legacyRenewal)
	require.NoError(t, err)

	result, err := fsm.Apply(raftlog.NewExpireLeaseCmd(lease1))
	require.NoError(t, err)
	assert.Equal(t, 1, result.(ExpireLeaseResponse).LocksReleased)

	_, ok := fsm.GetLease(lease1)
	assert.False(t, ok)
	_, ok = fsm.GetLock("my-lock")
	assert.False(t, ok)
	assert.Equal(t, stateStats(0, 1, 2), fsm.Stats())
}

func stateStats(locks, leases int, fencing uint64) Stats {
	return Stats{
		Locks:          locks,
		Leases:         leases,
		FencingCounter: fencing,
	}
}
