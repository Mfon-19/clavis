package state

import (
	"fmt"
	"github.com/Mfon-19/clavis/internal/domain"
	"github.com/Mfon-19/clavis/internal/raftlog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func fixedTestTime(offset time.Duration) time.Time {
	return time.Unix(1_700_000_000, 0).UTC().Add(offset)
}

// TestCreateLease tests lease creation
func TestCreateLease(t *testing.T) {
	fsm := NewFSM()
	now := fixedTestTime(0)

	cmd := raftlog.NewCreateLeaseCmd("client-1", 10*time.Second, now)

	result, err := fsm.Apply(cmd)
	require.NoError(t, err)

	resp, ok := result.(CreateLeaseResponse)
	require.Truef(t, ok, "expected CreateLeaseResponse")
	assert.NotZero(t, resp.LeaseID)

	// Verify lease was stored
	lease, exists := fsm.GetLease(resp.LeaseID)
	require.True(t, exists, "lease should exist")
	assert.Equal(t, "client-1", lease.OwnerID)
	assert.Equal(t, 10*time.Second, lease.TTL)
	assert.Equal(t, now.Add(10*time.Second), lease.ExpiresAt())
}

// TestRenewLease tests lease renewal
func TestRenewLease(t *testing.T) {
	fsm := NewFSM()
	createdAt := fixedTestTime(0)

	// Create Lease
	createdResp, _ := fsm.Apply(raftlog.NewCreateLeaseCmd("client-1", 5*time.Second, createdAt))
	leaseID := createdResp.(CreateLeaseResponse).LeaseID

	// Renew Lease
	renewedAt := createdAt.Add(1 * time.Second)
	result, err := fsm.Apply(raftlog.NewRenewLeaseCmd(leaseID, renewedAt))

	require.NoError(t, err)

	resp, ok := result.(RenewLeaseResponse)
	require.True(t, ok, "expected RenewLeaseResponse")
	assert.Equal(t, renewedAt.Add(5*time.Second), resp.ExpiresAt)
}

// TestAcquireLock tests lock acquisition with fencing token
func TestAcquireLock(t *testing.T) {
	fsm := NewFSM()
	createdAt := fixedTestTime(0)

	// Create lease first
	createResp, _ := fsm.Apply(raftlog.NewCreateLeaseCmd("client-1", 10*time.Second, createdAt))
	leaseID := createResp.(CreateLeaseResponse).LeaseID

	// Acquire lock
	result, err := fsm.Apply(raftlog.NewAcquireLockCmd("my-lock", "client-1", leaseID, createdAt.Add(1*time.Second)))

	require.NoError(t, err)

	resp, ok := result.(AcquireLockResponse)
	require.True(t, ok, "expected AcquireLockResponse")
	assert.NotZero(t, resp.FencingToken)

	// Verify lock was stored
	lock, exists := fsm.GetLock("my-lock")
	require.True(t, exists, "lock should exist")
	assert.Equal(t, "client-1", lock.OwnerID)
	assert.Equal(t, resp.FencingToken, lock.FencingToken)
}

// TestFencingTokenMonotonicity tests that fencing tokens strictly increase
func TestFencingTokenMonotonicity(t *testing.T) {
	fsm := NewFSM()
	createdAt := fixedTestTime(0)

	// Create lease
	createResp, _ := fsm.Apply(raftlog.NewCreateLeaseCmd("client-1", 10*time.Second, createdAt))
	leaseID := createResp.(CreateLeaseResponse).LeaseID

	// Acquire multiple locks and verify tokens increase
	tokens := make([]uint64, 10)

	for i := 0; i < 10; i++ {
		lockName := fmt.Sprintf("lock-%d", i)

		result, err := fsm.Apply(raftlog.NewAcquireLockCmd(lockName, "client-1", leaseID, createdAt.Add(time.Duration(i+1)*time.Millisecond)))

		require.NoError(t, err)
		resp := result.(AcquireLockResponse)
		tokens[i] = resp.FencingToken
	}

	// Verify strictly increasing
	for i := 1; i < len(tokens); i++ {
		assert.Greater(t, tokens[i], tokens[i-1], "tokens must be strictly increasing")
	}

	// Final token should be 10
	assert.Equal(t, uint64(10), tokens[9])
}

// TestLockAlreadyHeld tests that a lock can't be acquired if held by another
func TestLockAlreadyHeld(t *testing.T) {
	fsm := NewFSM()
	createdAt := fixedTestTime(0)

	// Create two leases
	lease1Resp, _ := fsm.Apply(raftlog.NewCreateLeaseCmd("client-1", 10*time.Second, createdAt))
	lease1 := lease1Resp.(CreateLeaseResponse).LeaseID

	lease2Resp, _ := fsm.Apply(raftlog.NewCreateLeaseCmd("client-2", 10*time.Second, createdAt.Add(1*time.Second)))
	lease2 := lease2Resp.(CreateLeaseResponse).LeaseID

	// Client 1 acquires lock
	_, err := fsm.Apply(raftlog.NewAcquireLockCmd("my-lock", "client-1", lease1, createdAt.Add(2*time.Second)))
	require.NoError(t, err, "client 1 should acquire")

	// Client 2 tries to acquire same lock (should fail)
	_, err = fsm.Apply(raftlog.NewAcquireLockCmd("my-lock", "client-2", lease2, createdAt.Add(3*time.Second)))

	assert.ErrorIs(t, err, domain.ErrLockAlreadyHeld)
}

// TestReleaseLock tests lock release
func TestReleaseLock(t *testing.T) {
	fsm := NewFSM()
	createdAt := fixedTestTime(0)

	// Create lease and acquire lock
	createResp, _ := fsm.Apply(raftlog.NewCreateLeaseCmd("client-1", 10*time.Second, createdAt))
	leaseID := createResp.(CreateLeaseResponse).LeaseID

	fsm.Apply(raftlog.NewAcquireLockCmd("my-lock", "client-1", leaseID, createdAt.Add(1*time.Second)))

	// Release lock
	result, err := fsm.Apply(raftlog.NewReleaseLockCmd("my-lock", leaseID))

	require.NoError(t, err)

	resp, ok := result.(ReleaseLockResponse)
	require.True(t, ok, "expected ReleaseLockResponse")
	assert.True(t, resp.Released)

	// Verify lock is gone
	_, exists := fsm.GetLock("my-lock")
	assert.False(t, exists, "lock should be deleted")
}

// TestExpireLeaseReleasesLocks tests that expiring a lease releases its locks
func TestExpireLeaseReleasesLocks(t *testing.T) {
	fsm := NewFSM()
	createdAt := fixedTestTime(0)

	// Create lease
	createResp, _ := fsm.Apply(raftlog.NewCreateLeaseCmd("client-1", 10*time.Second, createdAt))
	leaseID := createResp.(CreateLeaseResponse).LeaseID

	// Acquire 3 locks
	for i := 0; i < 3; i++ {
		lockName := fmt.Sprintf("lock-%d", i)
		fsm.Apply(raftlog.NewAcquireLockCmd(lockName, "client-1", leaseID, createdAt.Add(time.Duration(i+1)*time.Second)))
	}

	// Verify 3 locks exist
	stats := fsm.Stats()
	assert.Equal(t, 3, stats.Locks)

	// Expire the lease
	result, err := fsm.Apply(raftlog.NewExpireLeaseCmd(leaseID, createdAt.Add(11*time.Second)))

	require.NoError(t, err)

	resp := result.(ExpireLeaseResponse)
	assert.Equal(t, 3, resp.LocksReleased)

	// Verify all locks are gone
	stats = fsm.Stats()
	assert.Equal(t, 0, stats.Locks)

	// Verify lease is gone
	_, exists := fsm.GetLease(leaseID)
	assert.False(t, exists, "lease should be deleted")
}

// TestAcquireWithInvalidLease tests that you can't acquire with invalid lease
func TestAcquireWithInvalidLease(t *testing.T) {
	fsm := NewFSM()

	// Try to acquire with non-existent lease
	_, err := fsm.Apply(raftlog.NewAcquireLockCmd("my-lock", "client-1", 999, fixedTestTime(0)))

	assert.ErrorIs(t, err, domain.ErrLeaseNotFound)
}

func TestNodeRegistration(t *testing.T) {
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
	assert.Equal(t, member.GRPCAddress, stored.GRPCAddress)

	result, err = fsm.Apply(raftlog.NewDeregisterNodeCmd(member.NodeID))
	require.NoError(t, err)
	assert.True(t, result.(DeregisterNodeResponse).Removed)

	_, exists = fsm.GetMember(member.NodeID)
	assert.False(t, exists)
}

func TestReplayRejectsCommandsWithoutTimestamps(t *testing.T) {
	fsm := NewFSM()

	_, err := fsm.Apply(&raftlog.CommandWrapper{
		Type: raftlog.CommandType_COMMAND_TYPE_CREATE_LEASE,
		Payload: &raftlog.CommandWrapper_CreateLease{CreateLease: &raftlog.CreateLeaseCommand{
			OwnerId:  "client-1",
			TtlNanos: (20 * time.Millisecond).Nanoseconds(),
		}},
	})
	require.EqualError(t, err, "invalid command timestamp")
}
