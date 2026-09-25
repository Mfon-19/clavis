package state

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/Mfon-19/clavis/internal/raftlog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func snapshotBytes(t testing.TB, raftFSM *RaftFSM) []byte {
	t.Helper()

	snapshot, err := raftFSM.Snapshot()
	require.NoError(t, err)

	var buf bytes.Buffer
	sink := &mockSnapshotSink{buffer: &buf}
	require.NoError(t, snapshot.Persist(sink))

	return buf.Bytes()
}

// TestRaftFSMRestore verifies that restoring a snapshot recreates the exact FSM
// state and preserves allocator continuity for future leases and fencing tokens.
func TestRaftFSMRestore(t *testing.T) {
	original := NewRaftFSM()
	createdAt := fixedTestTime(0)

	firstLease := mustCreateLease(t, original.fsm, "client-1", 10*time.Second, createdAt).LeaseID
	firstLock := mustAcquireLock(t, original.fsm, "alpha", "client-1", firstLease, createdAt.Add(time.Second))
	_, err := original.fsm.Apply(raftlog.NewUpsertEndpointCmd("node-1", "127.0.0.1:9000"))
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

	assert.Equal(t, stateStats(1, 1, 1), restored.fsm.Stats())

	endpoint, exists := restored.fsm.GetEndpoint("node-1")
	require.True(t, exists)
	assert.Equal(t, "127.0.0.1:9000", endpoint)

	secondLease := mustCreateLease(t, restored.fsm, "client-2", 10*time.Second, createdAt.Add(2*time.Second))
	assert.Equal(t, firstLease+1, secondLease.LeaseID)

	secondLock := mustAcquireLock(t, restored.fsm, "beta", "client-2", secondLease.LeaseID, createdAt.Add(3*time.Second))
	assert.Equal(t, uint64(2), secondLock.FencingToken)
	assert.Equal(t, stateStats(2, 2, 2), restored.fsm.Stats())
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
