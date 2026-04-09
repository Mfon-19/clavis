package cluster

import (
	"fmt"
	"github.com/Mfon-19/clavis/internal/raftlog"
	"github.com/Mfon-19/clavis/internal/state"
	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func freeBindAddr(t testing.TB) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate free port: %v", err)
	}
	defer listener.Close()

	return listener.Addr().String()
}

// TestSingleNodeSmoke tests basic Raft functionality with a single node
func TestSingleNodeSmoke(t *testing.T) {
	tmpDir := t.TempDir()

	// Create config for single node
	cfg := &Config{
		NodeID:    uuid.New(),
		BindAddr:  freeBindAddr(t),
		DataDir:   tmpDir,
		Bootstrap: true, // First node in cluster
	}

	// Create node
	node, err := NewNode(cfg)
	require.NoError(t, err, "failed to create node")
	defer node.Shutdown()

	// Wait for leader election
	err = node.WaitForLeader(5 * time.Second)
	require.NoError(t, err, "no leader elected")
	assert.True(t, node.IsLeader(), "single node should be leader")

	// Create a lease
	createResult, err := node.Apply(raftlog.NewCreateLeaseCmd("client-1", 10*time.Second, time.Now().UTC()))
	require.NoError(t, err, "failed to create lease")

	// Verify response type
	createResp, ok := createResult.(state.CreateLeaseResponse)
	require.True(t, ok, "expected CreateLeaseResponse")
	assert.NotZero(t, createResp.LeaseID, "lease ID should not be zero")

	// Acquire a lock with the created lease
	acquireResult, err := node.Apply(raftlog.NewAcquireLockCmd("my-lock", "client-1", createResp.LeaseID, time.Now().UTC()))

	require.NoError(t, err, "failed to acquire lock")

	// Verify lock response
	acquireResp, ok := acquireResult.(state.AcquireLockResponse)
	require.True(t, ok, "expected AcquireLockResponse")
	assert.Equal(t, uint64(1), acquireResp.FencingToken, "first lock should have token 1")

	// Check FSM stats
	stats := node.Stats()
	assert.Equal(t, 1, stats.Leases, "should have 1 lease")
	assert.Equal(t, 1, stats.Locks, "should have 1 lock")

	// Release the lock
	releaseResult, err := node.Apply(raftlog.NewReleaseLockCmd("my-lock", createResp.LeaseID))
	require.NoError(t, err, "failed to release lock")

	// Verify release response
	releaseResp, ok := releaseResult.(state.ReleaseLockResponse)
	require.True(t, ok, "expected ReleaseLockResponse")
	assert.True(t, releaseResp.Released, "lock release should be successful")

	stats = node.Stats()
	assert.Equal(t, 1, stats.Leases, "should still have 1 lease")
	assert.Equal(t, 0, stats.Locks, "should have 0 locks after release")
}

func TestStatePersistence(t *testing.T) {
	tmpDir := t.TempDir()
	nodeID := uuid.New()

	cfg := &Config{
		NodeID:    nodeID,
		BindAddr:  freeBindAddr(t),
		DataDir:   tmpDir,
		Bootstrap: true,
	}

	node1, err := NewNode(cfg)
	require.NoError(t, err, "failed to create node1")

	err = node1.WaitForLeader(5 * time.Second)
	require.NoError(t, err)

	createResult, err := node1.Apply(raftlog.NewCreateLeaseCmd("client-1", time.Minute, time.Now().UTC()))
	require.NoError(t, err)

	createResp, ok := createResult.(state.CreateLeaseResponse)
	require.True(t, ok)
	leaseId := createResp.LeaseID

	acquireResult, err := node1.Apply(raftlog.NewAcquireLockCmd("my-lock", "client-1", leaseId, time.Now().UTC()))
	require.NoError(t, err)

	acquireResp, ok := acquireResult.(state.AcquireLockResponse)
	require.True(t, ok)
	assert.Equal(t, uint64(1), acquireResp.FencingToken)

	originalToken := acquireResp.FencingToken

	// Stats before shutdown
	statsBefore := node1.Stats()
	assert.Equal(t, 1, statsBefore.Leases)
	assert.Equal(t, 1, statsBefore.Locks)

	// Shutdown node
	err = node1.Shutdown()
	require.NoError(t, err, "failed to shutdown node1")

	// Modify config to not bootstrap
	cfg.Bootstrap = false

	// Restart node
	node2, err := NewNode(cfg)
	require.NoError(t, err, "failed to recreate node")
	defer node2.Shutdown()

	err = node2.WaitForLeader(5 * time.Second)
	require.NoError(t, err)

	// Verify state after restart
	var statsAfter state.Stats
	require.Eventually(t, func() bool {
		statsAfter = node2.Stats()
		return statsAfter.Leases == statsBefore.Leases && statsAfter.Locks == statsBefore.Locks
	}, 5*time.Second, 100*time.Millisecond, "state should be replayed after restart")
	assert.Equal(t, statsBefore.Leases, statsAfter.Leases, "leases count should persist after restart")
	assert.Equal(t, statsBefore.Locks, statsAfter.Locks, "locks count should persist after restart")

	// Try to acquire the same lock again, should get next fencing token
	acquireResult2, err := node2.Apply(raftlog.NewAcquireLockCmd("another-lock", "client-1", leaseId, time.Now().UTC()))
	require.NoError(t, err)

	acquireResp2, ok := acquireResult2.(state.AcquireLockResponse)
	require.True(t, ok)
	assert.Equal(t, originalToken+1, acquireResp2.FencingToken, "fencing token should increment after restart")
}

func TestMultiNodeCluster(t *testing.T) {
	// 3 node cluster
	nodes := make([]*Node, 3)
	cfgs := make([]*Config, 3)

	for i := 0; i < 3; i++ {
		cfgs[i] = &Config{
			NodeID:    uuid.New(),
			BindAddr:  freeBindAddr(t),
			DataDir:   filepath.Join(t.TempDir(), fmt.Sprintf("node%d", i)),
			Bootstrap: i == 0,
		}
	}

	var err error
	nodes[0], err = NewNode(cfgs[0])
	require.NoError(t, err, "failed to create node 0")
	defer nodes[0].Shutdown()

	err = nodes[0].WaitForLeader(5 * time.Second)
	require.NoError(t, err, "no leader elected in cluster")
	require.True(t, nodes[0].IsLeader(), "node 0 should be leader")

	for i := 1; i < 3; i++ {
		nodes[i], err = NewNode(cfgs[i])
		require.NoError(t, err, fmt.Sprintf("failed to create node %d", i))
		defer nodes[i].Shutdown()

		future := nodes[0].raft.AddVoter(
			raft.ServerID(cfgs[i].NodeID.String()),
			raft.ServerAddress(cfgs[i].BindAddr),
			0, 0,
		)
		require.NoError(t, future.Error(), fmt.Sprintf("failed to add node %d as voter", i))

	}

	var leader *Node
	require.Eventually(t, func() bool {
		leader = nil
		leaderCnt := 0
		for _, node := range nodes {
			if node.IsLeader() {
				leader = node
				leaderCnt++
			}
		}
		return leaderCnt == 1
	}, 5*time.Second, 100*time.Millisecond, "there should be exactly one leader")
	require.NotNil(t, leader, "leader node should not be nil")

	// Apply command via leader
	createResult, err := leader.Apply(raftlog.NewCreateLeaseCmd("client-1", 10*time.Second, time.Now().UTC()))
	require.NoError(t, err, "failed to create lease via leader")

	createResp, ok := createResult.(state.CreateLeaseResponse)
	require.True(t, ok, "expected CreateLeaseResponse from leader")
	assert.NotZero(t, createResp.LeaseID, "lease ID should not be zero from leader")

	acquireResult, err := leader.Apply(raftlog.NewAcquireLockCmd("cluster-lock", "client-1", createResp.LeaseID, time.Now().UTC()))
	require.NoError(t, err, "failed to acquire lock via leader")

	acquireResp, ok := acquireResult.(state.AcquireLockResponse)
	require.True(t, ok, "expected AcquireLockResponse from leader")
	assert.Equal(t, uint64(1), acquireResp.FencingToken, "first lock should have token 1 from leader")

	// All nodes should have the lease and lock
	for i, node := range nodes {
		i, node := i, node
		require.Eventually(t, func() bool {
			stats := node.Stats()
			return stats.Leases >= 1 && stats.Locks >= 1
		}, 5*time.Second, 100*time.Millisecond, fmt.Sprintf("node %d should replicate lease and lock", i))
	}
}
