package cluster

import (
	"fmt"
	"net"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/Mfon-19/clavis/internal/domain"
	"github.com/Mfon-19/clavis/internal/raftlog"
	"github.com/Mfon-19/clavis/internal/state"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Some helpers

func freeBindAddr(t testing.TB) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate free port: %v", err)
	}
	defer listener.Close()

	return listener.Addr().String()
}

func newTestConfig(t testing.TB, dataDir string, bootstrap bool) *Config {
	t.Helper()

	bindAddr := freeBindAddr(t)
	return &Config{
		NodeID:            uuid.New(),
		BindAddr:          bindAddr,
		RaftAdvertiseAddr: bindAddr,
		GRPCAdvertiseAddr: bindAddr,
		DataDir:           dataDir,
		Bootstrap:         bootstrap,
	}
}

func expectedMembers(cfgs []*Config) []domain.ClusterMember {
	members := make([]domain.ClusterMember, 0, len(cfgs))
	for _, cfg := range cfgs {
		members = append(members, domain.ClusterMember{
			NodeID:      cfg.NodeID.String(),
			RaftAddress: cfg.RaftAdvertiseAddr,
			GRPCAddress: cfg.GRPCAdvertiseAddr,
		})
	}

	sort.Slice(members, func(i, j int) bool {
		return members[i].NodeID < members[j].NodeID
	})
	return members
}

func startTestCluster(t *testing.T, size int) ([]*Node, []*Config) {
	t.Helper()

	nodes := make([]*Node, size)
	cfgs := make([]*Config, size)

	for i := 0; i < size; i++ {
		cfgs[i] = newTestConfig(t, filepath.Join(t.TempDir(), fmt.Sprintf("node-%d", i)), i == 0)
		node, err := NewNode(cfgs[i])
		require.NoError(t, err, "failed to create node %d", i)
		nodes[i] = node
		t.Cleanup(func() {
			_ = node.Shutdown()
		})
	}

	require.NoError(t, nodes[0].WaitForLeader(5*time.Second), "bootstrap leader was not elected")
	require.True(t, nodes[0].IsLeader(), "bootstrap node should lead before followers join")

	for i := 1; i < size; i++ {
		err := nodes[0].AddClusterMember(nodes[i].SelfMember())
		require.NoError(t, err, "failed to add node %d to cluster", i)
	}

	waitForSingleLeader(t, nodes, 5*time.Second)
	waitForMemberViews(t, nodes, expectedMembers(cfgs), 5*time.Second)

	return nodes, cfgs
}

func waitForSingleLeader(t testing.TB, nodes []*Node, timeout time.Duration) *Node {
	t.Helper()

	var leader *Node
	require.Eventually(t, func() bool {
		leader = nil
		leaders := 0
		for _, node := range nodes {
			if node != nil && node.IsLeader() {
				leader = node
				leaders++
			}
		}
		return leaders == 1
	}, timeout, 100*time.Millisecond, "expected exactly one leader")

	require.NotNil(t, leader, "leader should not be nil")
	return leader
}

func memberViewMatches(node *Node, want []domain.ClusterMember) bool {
	current := node.Members()
	if len(current) != len(want) {
		return false
	}

	for i := range want {
		if current[i].NodeID != want[i].NodeID ||
			current[i].RaftAddress != want[i].RaftAddress ||
			current[i].GRPCAddress != want[i].GRPCAddress {
			return false
		}
	}

	return true
}

func waitForMemberViews(t testing.TB, nodes []*Node, want []domain.ClusterMember, timeout time.Duration) {
	t.Helper()

	require.Eventually(t, func() bool {
		for _, node := range nodes {
			if node == nil {
				continue
			}
			if !memberViewMatches(node, want) {
				return false
			}
		}
		return true
	}, timeout, 100*time.Millisecond, "cluster membership view did not converge")
}

func waitForExactLeaseAndLockState(
	t testing.TB,
	nodes []*Node,
	members []domain.ClusterMember,
	leaseID uint64,
	owner string,
	ttl time.Duration,
	expiresAt time.Time,
	lockName string,
	lockToken uint64,
	timeout time.Duration,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		for _, node := range nodes {
			if node == nil {
				continue
			}

			if !memberViewMatches(node, members) {
				return false
			}

			stats := node.Stats()
			if stats.Leases != 1 || stats.Locks != 1 || stats.FencingCounter != lockToken {
				return false
			}

			lease, ok := node.GetFSM().GetLease(leaseID)
			if !ok {
				return false
			}
			if lease.OwnerID != owner || lease.TTL != ttl || lease.ExpiresAtUnixNano != expiresAt.UnixNano() {
				return false
			}

			lock, ok := node.GetFSM().GetLock(lockName)
			if !ok {
				return false
			}
			if lock.OwnerID != owner || lock.LeaseID != leaseID || lock.FencingToken != lockToken {
				return false
			}
		}
		return true
	}, timeout, 100*time.Millisecond, "cluster state did not converge")
}

func TestRestartPersistence(t *testing.T) {
	cfg := newTestConfig(t, t.TempDir(), true)

	node, err := NewNode(cfg)
	require.NoError(t, err)

	require.NoError(t, node.WaitForLeader(5*time.Second))

	firstLeaseResult, err := node.Apply(raftlog.NewCreateLeaseCmd("client-1", time.Minute, time.Now().UTC()))
	require.NoError(t, err)
	firstLease := firstLeaseResult.(state.CreateLeaseResponse)

	firstAcquireResult, err := node.Apply(raftlog.NewAcquireLockCmd("persisted-lock", "client-1", firstLease.LeaseID, time.Now().UTC()))
	require.NoError(t, err)
	firstAcquire := firstAcquireResult.(state.AcquireLockResponse)
	assert.Equal(t, uint64(1), firstAcquire.FencingToken)

	require.NoError(t, node.Shutdown())

	cfg.Bootstrap = false
	restarted, err := NewNode(cfg)
	require.NoError(t, err)
	defer restarted.Shutdown()

	require.NoError(t, restarted.WaitForLeader(5*time.Second))

	require.Eventually(t, func() bool {
		stats := restarted.Stats()
		if stats != (state.Stats{Locks: 1, Leases: 1, FencingCounter: 1}) {
			return false
		}

		lock, ok := restarted.GetFSM().GetLock("persisted-lock")
		if !ok {
			return false
		}
		if lock.LeaseID != firstLease.LeaseID || lock.FencingToken != 1 || lock.OwnerID != "client-1" {
			return false
		}

		lease, ok := restarted.GetFSM().GetLease(firstLease.LeaseID)
		if !ok {
			return false
		}
		return lease.OwnerID == "client-1"
	}, 5*time.Second, 100*time.Millisecond, "restarted node did not replay committed state")

	secondLeaseResult, err := restarted.Apply(raftlog.NewCreateLeaseCmd("client-2", time.Minute, time.Now().UTC()))
	require.NoError(t, err)
	secondLease := secondLeaseResult.(state.CreateLeaseResponse)
	assert.Equal(t, firstLease.LeaseID+1, secondLease.LeaseID, "lease ID allocator should continue after restart")

	secondAcquireResult, err := restarted.Apply(raftlog.NewAcquireLockCmd("second-lock", "client-2", secondLease.LeaseID, time.Now().UTC()))
	require.NoError(t, err)
	secondAcquire := secondAcquireResult.(state.AcquireLockResponse)
	assert.Equal(t, uint64(2), secondAcquire.FencingToken, "fencing counter should continue after restart")
}

// TestLeaderFailover verifies that after the
// current leader is shut down, a surviving node becomes leader, preserves the
// replicated lock state, and can continue serving new writes safely.
func TestLeaderFailover(t *testing.T) {
	nodes, cfgs := startTestCluster(t, 3)
	originalLeader := waitForSingleLeader(t, nodes, 5*time.Second)
	members := expectedMembers(cfgs)

	ttl := 30 * time.Second
	createResult, err := originalLeader.Apply(raftlog.NewCreateLeaseCmd("client-1", ttl, time.Now().UTC()))
	require.NoError(t, err)
	createResp := createResult.(state.CreateLeaseResponse)

	acquireResult, err := originalLeader.Apply(raftlog.NewAcquireLockCmd("alpha", "client-1", createResp.LeaseID, time.Now().UTC()))
	require.NoError(t, err)
	acquireResp := acquireResult.(state.AcquireLockResponse)
	assert.Equal(t, uint64(1), acquireResp.FencingToken)

	waitForExactLeaseAndLockState(
		t,
		nodes,
		members,
		createResp.LeaseID,
		"client-1",
		ttl,
		createResp.ExpiresAt,
		"alpha",
		1,
		5*time.Second,
	)

	remaining := make([]*Node, 0, len(nodes)-1)
	for _, node := range nodes {
		if node == originalLeader {
			require.NoError(t, node.Shutdown())
			continue
		}
		remaining = append(remaining, node)
	}

	newLeader := waitForSingleLeader(t, remaining, 10*time.Second)
	require.NotEqual(t, originalLeader.GetNodeID(), newLeader.GetNodeID(), "leadership should move to a surviving node")
	newLeaderAddr := newLeader.SelfMember().GRPCAddress

	require.Eventually(t, func() bool {
		for _, node := range remaining {
			if node.GetLeaderGRPCAddress() != newLeaderAddr {
				return false
			}
		}
		return true
	}, 5*time.Second, 100*time.Millisecond, "remaining nodes did not learn the new leader gRPC address")

	require.Eventually(t, func() bool {
		lock, ok := newLeader.GetFSM().GetLock("alpha")
		if !ok {
			return false
		}
		return lock.LeaseID == createResp.LeaseID && lock.FencingToken == 1
	}, 5*time.Second, 100*time.Millisecond, "new leader did not preserve replicated lock state")

	secondAcquireResult, err := newLeader.Apply(raftlog.NewAcquireLockCmd("beta", "client-1", createResp.LeaseID, time.Now().UTC()))
	require.NoError(t, err)
	secondAcquire := secondAcquireResult.(state.AcquireLockResponse)
	assert.Equal(t, uint64(2), secondAcquire.FencingToken, "new leader should continue fencing sequence")

	_, err = newLeader.Apply(raftlog.NewReleaseLockCmd("alpha", createResp.LeaseID))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		for _, node := range remaining {
			stats := node.Stats()
			if stats != (state.Stats{Locks: 1, Leases: 1, FencingCounter: 2}) {
				return false
			}

			if _, ok := node.GetFSM().GetLock("alpha"); ok {
				return false
			}

			lock, ok := node.GetFSM().GetLock("beta")
			if !ok {
				return false
			}
			if lock.LeaseID != createResp.LeaseID || lock.FencingToken != 2 || lock.OwnerID != "client-1" {
				return false
			}
		}
		return true
	}, 5*time.Second, 100*time.Millisecond, "remaining cluster did not converge after failover")
}

// TestLeaseExpiryGuards verifies the two conditions that hold back lease
// expiry on the leader: a renewal proposed before the lease's deadline that is
// still in the Raft pipeline, and the post-election grace period.
func TestLeaseExpiryGuards(t *testing.T) {
	stateMachine := state.NewFSM()
	createdAt := time.Unix(1_700_000_000, 0).UTC()

	createResult, err := stateMachine.Apply(raftlog.NewCreateLeaseCmd("client-1", 5*time.Second, createdAt))
	require.NoError(t, err)
	leaseID := createResult.(state.CreateLeaseResponse).LeaseID

	node := &Node{
		fsm:             stateMachine,
		pendingRenewals: make(map[uint64]map[int64]int),
	}
	leaderSince := createdAt
	now := createdAt.Add(12 * time.Second)

	timely := createdAt.Add(4 * time.Second)
	node.recordPendingRenewal(leaseID, timely)
	node.recordPendingRenewal(leaseID, createdAt.Add(8*time.Second))
	assert.False(t, node.shouldExpireLease(leaseID, now, leaderSince), "timely renewal still in flight")

	node.clearPendingRenewal(leaseID, timely)
	assert.True(t, node.shouldExpireLease(leaseID, now, leaderSince), "renewal proposed after expiry cannot save the lease")

	newLeaderSince := createdAt.Add(10 * time.Second)
	assert.False(t, node.shouldExpireLease(leaseID, now, newLeaderSince), "new leader must wait one TTL")
	assert.True(t, node.shouldExpireLease(leaseID, newLeaderSince.Add(5*time.Second), newLeaderSince))
}
