package state

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/Mfon-19/clavis/internal/domain"
	"github.com/Mfon-19/clavis/internal/raftlog"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func applyRaftLog(t testing.TB, raftFSM *RaftFSM, cmd *raftlog.CommandWrapper) any {
	t.Helper()

	data, err := proto.Marshal(cmd)
	require.NoError(t, err)

	return raftFSM.Apply(&raft.Log{
		Index: 1,
		Term:  1,
		Type:  raft.LogCommand,
		Data:  data,
	})
}

func snapshotBytes(t testing.TB, raftFSM *RaftFSM) []byte {
	t.Helper()

	snapshot, err := raftFSM.Snapshot()
	require.NoError(t, err)

	var buf bytes.Buffer
	sink := &mockSnapshotSink{buffer: &buf}
	require.NoError(t, snapshot.Persist(sink))

	return buf.Bytes()
}

// TestRaftFSMApply verifies that the Raft adapter deserializes a committed log
// entry, applies it through the pure FSM, and returns the typed FSM response.
func TestRaftFSMApply(t *testing.T) {
	raftFSM := NewRaftFSM()
	createdAt := fixedTestTime(0)

	result := applyRaftLog(t, raftFSM, raftlog.NewCreateLeaseCmd("client-1", 10*time.Second, createdAt))

	resp, ok := result.(CreateLeaseResponse)
	require.True(t, ok, "expected CreateLeaseResponse")
	assert.Equal(t, uint64(1), resp.LeaseID)
	assert.Equal(t, createdAt.Add(10*time.Second), resp.ExpiresAt)

	lease, exists := raftFSM.fsm.GetLease(resp.LeaseID)
	require.True(t, exists)
	assert.Equal(t, "client-1", lease.OwnerID)
	assert.Equal(t, createdAt.Add(10*time.Second).UnixNano(), lease.ExpiresAtUnixNano)
}

// TestRaftFSMSnapshot verifies that snapshots include the full state required
// to continue operation after restore: leases, locks, members, fencing, and the
// next lease ID allocator value.
func TestRaftFSMSnapshot(t *testing.T) {
	raftFSM := NewRaftFSM()
	createdAt := fixedTestTime(0)

	lease1 := mustCreateLease(t, raftFSM.fsm, "client-1", 10*time.Second, createdAt).LeaseID
	lease2 := mustCreateLease(t, raftFSM.fsm, "client-2", 10*time.Second, createdAt.Add(time.Second)).LeaseID
	lock := mustAcquireLock(t, raftFSM.fsm, "snap-lock", "client-1", lease1, createdAt.Add(2*time.Second))

	member := domain.ClusterMember{
		NodeID:      "node-1",
		RaftAddress: "127.0.0.1:7000",
		GRPCAddress: "127.0.0.1:9000",
	}
	_, err := raftFSM.fsm.Apply(raftlog.NewRegisterNodeCmd(member))
	require.NoError(t, err)

	snapshot, err := raftFSM.Snapshot()
	require.NoError(t, err)

	fsmSnap := snapshot.(*fsmSnapshot)
	require.Len(t, fsmSnap.Leases, 2)
	require.Len(t, fsmSnap.Locks, 1)
	require.Len(t, fsmSnap.Members, 1)

	assert.Equal(t, uint64(1), fsmSnap.FencingCounter)
	assert.Equal(t, uint64(3), fsmSnap.NextLeaseID)
	assert.Equal(t, lease2, fsmSnap.Leases[lease2].LeaseID)
	assert.Equal(t, lock.FencingToken, fsmSnap.Locks["snap-lock"].FencingToken)
	assert.Equal(t, member.GRPCAddress, fsmSnap.Members[member.NodeID].GRPCAddress)
}

// TestRaftFSMRestore verifies that restoring a snapshot recreates the exact FSM
// state and preserves allocator continuity for future leases and fencing tokens.
func TestRaftFSMRestore(t *testing.T) {
	original := NewRaftFSM()
	createdAt := fixedTestTime(0)

	firstLease := mustCreateLease(t, original.fsm, "client-1", 10*time.Second, createdAt).LeaseID
	firstLock := mustAcquireLock(t, original.fsm, "alpha", "client-1", firstLease, createdAt.Add(time.Second))

	_, err := original.fsm.Apply(raftlog.NewRegisterNodeCmd(domain.ClusterMember{
		NodeID:      "node-1",
		RaftAddress: "127.0.0.1:7000",
		GRPCAddress: "127.0.0.1:9000",
	}))
	require.NoError(t, err)

	restored := NewRaftFSM()
	err = restored.Restore(io.NopCloser(bytes.NewReader(snapshotBytes(t, original))))
	require.NoError(t, err)

	lease, exists := restored.fsm.GetLease(firstLease)
	require.True(t, exists)
	assert.Equal(t, "client-1", lease.OwnerID)

	lock, exists := restored.fsm.GetLock("alpha")
	require.True(t, exists)
	assert.Equal(t, firstLock.FencingToken, lock.FencingToken)

	member, exists := restored.fsm.GetMember("node-1")
	require.True(t, exists)
	assert.Equal(t, "127.0.0.1:9000", member.GRPCAddress)

	assert.Equal(t, stateStats(1, 1, 1), restored.fsm.Stats())

	secondLease := mustCreateLease(t, restored.fsm, "client-2", 10*time.Second, createdAt.Add(2*time.Second))
	assert.Equal(t, firstLease+1, secondLease.LeaseID)

	secondLock := mustAcquireLock(t, restored.fsm, "beta", "client-2", secondLease.LeaseID, createdAt.Add(3*time.Second))
	assert.Equal(t, uint64(2), secondLock.FencingToken)
	assert.Equal(t, stateStats(2, 2, 2), restored.fsm.Stats())
}

// TestRaftFSMRestoreRejectsInvalidSnapshot verifies that restore fails fast on
// malformed snapshot payloads instead of silently installing broken state.
func TestRaftFSMRejectsInvalidSnapshot(t *testing.T) {
	badSnapshot := fsmSnapshot{
		Locks: make(map[string]*domain.Lock),
		Leases: map[uint64]*domain.Lease{
			1: {
				LeaseID:           1,
				OwnerID:           "client-1",
				ExpiresAtUnixNano: 0,
				TTL:               10 * time.Second,
			},
		},
		Members:        make(map[string]*domain.ClusterMember),
		FencingCounter: 0,
		NextLeaseID:    2,
	}

	data, err := json.Marshal(badSnapshot)
	require.NoError(t, err)

	raftFSM := NewRaftFSM()
	err = raftFSM.Restore(io.NopCloser(bytes.NewReader(data)))
	require.EqualError(t, err, "snapshot lease 1 has invalid expiry")
}

// mockSnapshotSink implements raft.SnapshotSink for testing.
type mockSnapshotSink struct {
	buffer *bytes.Buffer
}

func (m *mockSnapshotSink) Write(p []byte) (n int, err error) {
	return m.buffer.Write(p)
}

func (m *mockSnapshotSink) Close() error {
	return nil
}

func (m *mockSnapshotSink) ID() string {
	return "mock-snapshot"
}

func (m *mockSnapshotSink) Cancel() error {
	return nil
}
