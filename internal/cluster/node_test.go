package cluster

import (
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"google.golang.org/protobuf/proto"

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

	firstAcquireResult, err := node.Apply(raftlog.NewAcquireLockCmd("persisted-lock", "client-1", firstLease.LeaseID))
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

	secondAcquireResult, err := restarted.Apply(raftlog.NewAcquireLockCmd("second-lock", "client-2", secondLease.LeaseID))
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

	acquireResult, err := originalLeader.Apply(raftlog.NewAcquireLockCmd("alpha", "client-1", createResp.LeaseID))
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

	secondAcquireResult, err := newLeader.Apply(raftlog.NewAcquireLockCmd("beta", "client-1", createResp.LeaseID))
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

// TestLeaseTracker verifies how the leader decides lease liveness: renewals
// extend the deadline, a lease claimed for expiry cannot be renewed, and a new
// leadership term restarts every lease's clock.
func TestLeaseTracker(t *testing.T) {
	createdAt := time.Unix(1_700_000_000, 0).UTC()
	lease := domain.Lease{
		LeaseID:           1,
		OwnerID:           "client-1",
		ExpiresAtUnixNano: createdAt.Add(5 * time.Second).UnixNano(),
		TTL:               5 * time.Second,
	}
	leases := []domain.Lease{lease}
	at := func(d time.Duration) time.Time { return createdAt.Add(d) }

	var tracker leaseTracker
	require.NoError(t, tracker.renew(1, at(4*time.Second), lease))
	assert.Empty(t, tracker.claimExpired(1, at(8*time.Second), leases), "renewal at 4s lasts until 9s")
	assert.Equal(t, []uint64{1}, tracker.claimExpired(1, at(9*time.Second), leases))
	assert.ErrorIs(t, tracker.renew(1, at(9*time.Second), lease), domain.ErrLeaseExpired, "claimed lease cannot be renewed")

	tracker.finishExpiry(1, false)
	assert.Equal(t, []uint64{1}, tracker.claimExpired(1, at(9*time.Second), leases), "failed expiry is retried")

	// A new term forgets renewals and claims, and restarts every deadline.
	assert.Empty(t, tracker.claimExpired(2, at(10*time.Second), leases))
	assert.Empty(t, tracker.claimExpired(2, at(14*time.Second), leases), "new leader waits one TTL")
	assert.Equal(t, []uint64{1}, tracker.claimExpired(2, at(15*time.Second), leases))
}

// TestRenewalsBypassRaftLog verifies that renewing a lease keeps it alive past
// its TTL without appending to the Raft log, and that the leader expires it
// once renewals stop.
func TestRenewalsBypassRaftLog(t *testing.T) {
	nodes, _ := startTestCluster(t, 3)
	leader := waitForSingleLeader(t, nodes, 5*time.Second)

	const ttl = time.Second
	result, err := leader.Apply(raftlog.NewCreateLeaseCmd("client-1", ttl, leader.Now()))
	require.NoError(t, err)
	leaseID := result.(state.CreateLeaseResponse).LeaseID

	// The first renewal in a term issues one barrier entry; after that,
	// renewals must not touch the log.
	_, err = leader.RenewLease(leaseID)
	require.NoError(t, err)
	logIndex := leader.raft.LastIndex()

	for deadline := time.Now().Add(3 * ttl); time.Now().Before(deadline); {
		renewedTTL, err := leader.RenewLease(leaseID)
		require.NoError(t, err)
		assert.Equal(t, ttl, renewedTTL)
		time.Sleep(ttl / 4)
	}
	_, exists := leader.GetFSM().GetLease(leaseID)
	require.True(t, exists, "renewed lease must outlive its TTL")
	assert.Equal(t, logIndex, leader.raft.LastIndex(), "renewals must not append to the Raft log")

	require.Eventually(t, func() bool {
		_, exists := leader.GetFSM().GetLease(leaseID)
		return !exists
	}, 3*ttl, 50*time.Millisecond, "lease was not expired after renewals stopped")

	_, err = leader.RenewLease(leaseID)
	assert.ErrorIs(t, err, domain.ErrLeaseNotFound)
}

// TestMigratesBoltDataDir verifies that a data directory written by an older
// version, with a BoltDB log store, is migrated to the WAL on startup without
// losing committed state. Losing it would restart the fencing counter.
func TestMigratesBoltDataDir(t *testing.T) {
	cfg := newTestConfig(t, t.TempDir(), false)

	// Write state the way older versions did: a single-node cluster on BoltDB.
	bolt, err := raftboltdb.New(raftboltdb.Options{Path: filepath.Join(cfg.DataDir, legacyBoltName)})
	require.NoError(t, err)
	snapshots, err := raft.NewFileSnapshotStore(filepath.Join(cfg.DataDir, snapshotsDirName), 3, io.Discard)
	require.NoError(t, err)
	transport, err := raft.NewTCPTransport(cfg.BindAddr, nil, 3, time.Second, io.Discard)
	require.NoError(t, err)

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID.String())
	raftCfg.LogOutput = io.Discard
	legacy, err := raft.NewRaft(raftCfg, state.NewRaftFSM(), bolt, bolt, snapshots, transport)
	require.NoError(t, err)
	require.NoError(t, legacy.BootstrapCluster(raft.Configuration{Servers: []raft.Server{
		{ID: raftCfg.LocalID, Address: raft.ServerAddress(cfg.BindAddr)},
	}}).Error())
	require.Eventually(t, func() bool { return legacy.State() == raft.Leader }, 5*time.Second, 20*time.Millisecond)

	applyLegacy := func(cmd *raftlog.CommandWrapper) any {
		data, err := proto.Marshal(cmd)
		require.NoError(t, err)
		future := legacy.Apply(data, time.Second)
		require.NoError(t, future.Error())
		return future.Response()
	}
	lease := applyLegacy(raftlog.NewCreateLeaseCmd("client-1", time.Minute, time.Now())).(state.CreateLeaseResponse)
	applyLegacy(raftlog.NewAcquireLockCmd("migrated-lock", "client-1", lease.LeaseID))

	require.NoError(t, legacy.Shutdown().Error())
	require.NoError(t, transport.Close())
	require.NoError(t, bolt.Close())

	node, err := NewNode(cfg)
	require.NoError(t, err)
	defer node.Shutdown()
	require.NoError(t, node.WaitForLeader(5*time.Second))

	require.Eventually(t, func() bool {
		return node.Stats() == state.Stats{Locks: 1, Leases: 1, FencingCounter: 1}
	}, 5*time.Second, 50*time.Millisecond, "migrated node did not replay committed state")

	result, err := node.Apply(raftlog.NewAcquireLockCmd("second-lock", "client-1", lease.LeaseID))
	require.NoError(t, err)
	assert.Equal(t, uint64(2), result.(state.AcquireLockResponse).FencingToken, "fencing counter must survive migration")

	assert.NoFileExists(t, filepath.Join(cfg.DataDir, legacyBoltName))
	assert.FileExists(t, filepath.Join(cfg.DataDir, migratedBoltName))
}
