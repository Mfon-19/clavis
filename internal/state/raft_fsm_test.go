package state

import (
	"bytes"
	"github.com/Mfon-19/clavis/internal/raftlog"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"io"
	"testing"
	"time"
)

func TestRaftFSMApply(t *testing.T) {
	raftFSM := NewRaftFSM()

	// Create a command
	cmd := raftlog.NewCreateLeaseCmd("client-1", 10*time.Second, time.Now().UTC())

	// Serialize to bytes for Raft
	data, err := proto.Marshal(cmd)
	require.NoError(t, err)

	// Create a Raft log entry
	logEntry := &raft.Log{
		Index: 1,
		Term:  1,
		Type:  raft.LogCommand,
		Data:  data,
	}

	// Apply through Raft FSM
	result := raftFSM.Apply(logEntry)

	// Verify result
	resp, ok := result.(CreateLeaseResponse)
	require.True(t, ok, "expected CreateLeaseResponse")
	assert.NotZero(t, "client-1", resp.LeaseID)

	// Verify state was updated
	lease, exists := raftFSM.fsm.GetLease(resp.LeaseID)
	require.True(t, exists)
	assert.Equal(t, "client-1", lease.OwnerID)
}

func TestRaftFSMSnapshot(t *testing.T) {
	raftFSM := NewRaftFSM()

	// Create some state
	raftFSM.fsm.Apply(raftlog.NewCreateLeaseCmd("client-1", 10*time.Second, time.Now().UTC()))

	raftFSM.fsm.Apply(raftlog.NewCreateLeaseCmd("client-2", 10*time.Second, time.Now().UTC()))

	// Create snapshot
	snapshot, err := raftFSM.Snapshot()
	require.NoError(t, err)

	// Verify snapshot contains state
	fsmSnap := snapshot.(*fsmSnapshot)
	assert.Equal(t, 2, len(fsmSnap.Leases))
	assert.Equal(t, uint64(3), fsmSnap.NextLeaseID) // Next would be 3
}

// TestRaftFSMRestore tests restoring from snapshot
func TestRaftFSMRestore(t *testing.T) {
	// Create original FSM with state
	original := NewRaftFSM()

	result1, _ := original.fsm.Apply(raftlog.NewCreateLeaseCmd("client-1", 10*time.Second, time.Now().UTC()))
	leaseID := result1.(CreateLeaseResponse).LeaseID

	original.fsm.Apply(raftlog.NewAcquireLockCmd("my-lock", "client-1", leaseID, time.Now().UTC()))

	// Create snapshot
	snapshot, err := original.Snapshot()
	require.NoError(t, err)

	// Persist snapshot to buffer
	var buf bytes.Buffer
	mockSink := &mockSnapshotSink{buffer: &buf}
	err = snapshot.Persist(mockSink)
	require.NoError(t, err)

	// Create new FSM and restore from snapshot
	newFSM := NewRaftFSM()
	err = newFSM.Restore(io.NopCloser(&buf))
	require.NoError(t, err)

	// Verify new FSM has same state
	lease, exists := newFSM.fsm.GetLease(leaseID)
	require.True(t, exists)
	assert.Equal(t, "client-1", lease.OwnerID)

	lock, exists := newFSM.fsm.GetLock("my-lock")
	require.True(t, exists)
	assert.Equal(t, "client-1", lock.OwnerID)
}

// mockSnapshotSink implements raft.SnapshotSink for testing
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
