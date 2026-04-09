package cluster

import (
	"fmt"
	"github.com/Mfon-19/clavis/internal/raftlog"
	"github.com/Mfon-19/clavis/internal/state"
	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Node wraps a Raft instance with the clavis FSM and provides a clean API for applying
// commands, querying state, and managing cluster membership. It runs two background
// goroutines:
//   - leaseExpiryLoop (100ms): leader-only, expires dead leases and cascade-releases locks
//   - membershipSyncLoop (500ms): leader-only, ensures this node's metadata is registered
type Node struct {
	raft         *raft.Raft
	fsm          *state.FSM
	raftFSM      *state.RaftFSM
	transport    *raft.NetworkTransport
	storage      *Storage
	cfg          *Config
	stopCh       chan struct{}
	shutdownOnce sync.Once

	pendingRenewalsMu       sync.Mutex
	pendingRenewals         map[uint64]map[int64]int
	selfRegistrationBlocked atomic.Bool
}

// Config holds the settings for creating a new Raft node
type Config struct {
	NodeID            uuid.UUID // Unique Raft node identifier
	BindAddr          string    // Address to bind Raft TCP transport
	RaftAdvertiseAddr string    // Routable Raft address advertised to peers
	DataDir           string    // Directory for BoltDB and snapshot storage
	Bootstrap         bool      // True to bootstrap a new single-node cluster
	GRPCAdvertiseAddr string    // gRPC address stored in FSM for client redirects
}

// NewNode creates a Raft node with BoltDB storage, TCP transport, and the
// clavis FSM.
//
// This function is intentionally the only place that assembles the Raft stack:
// persistent log storage, stable storage, snapshots, TCP peer transport, and
// the FSM adapter. Once the Raft instance exists, this function also starts the
// leader-only background loops for lease expiry and membership metadata sync
//
// Bootstrap should be true only for the first node in a new cluster. If the
// data directory already contains Raft state, bootstrap is skipped so restarts
// do not accidentally form a new cluster.
func NewNode(cfg *Config) (*Node, error) {
	if cfg.RaftAdvertiseAddr == "" {
		cfg.RaftAdvertiseAddr = cfg.BindAddr
	}
	if cfg.GRPCAdvertiseAddr == "" {
		cfg.GRPCAdvertiseAddr = cfg.BindAddr
	}

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}

	raftFSM := state.NewRaftFSM()
	stateMachine := raftFSM.GetFSM()
	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID.String())
	raftCfg.HeartbeatTimeout = 1000 * time.Millisecond
	raftCfg.ElectionTimeout = 1000 * time.Millisecond
	raftCfg.CommitTimeout = 50 * time.Millisecond
	raftCfg.SnapshotThreshold = 8192

	// BoltDB stores both the Raft log and stable Raft metadata. Snapshots are
	// file-backed so lagging/restarting nodes can catch up without replaying
	// the entire log.
	raftStorage, err := NewStorage(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create stores: %w", err)
	}

	// HashiCorp Raft binds to BindAddr locally but advertises a routable peer
	// address to the Raft cluster. These are separate because deployments often
	// bind to :7000 or 0.0.0.0:7000 inside a VM/container.
	advertiseAddr, err := net.ResolveTCPAddr("tcp", cfg.RaftAdvertiseAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve raft advertise addr: %w", err)
	}

	transport, err := raft.NewTCPTransport(cfg.BindAddr, advertiseAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	r, err := raft.NewRaft(raftCfg, raftFSM, raftStorage.LogStore, raftStorage.StableStore, raftStorage.SnapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}

	if cfg.Bootstrap {
		// Only bootstrap brand-new storage. Existing storage already has a
		// cluster configuration and must be allowed to rejoin that cluster.
		hasState, err := raft.HasExistingState(raftStorage.LogStore, raftStorage.StableStore, raftStorage.SnapshotStore)
		if err != nil {
			return nil, fmt.Errorf("failed to check existing state: %w", err)
		}

		if !hasState {
			configuration := raft.Configuration{
				Servers: []raft.Server{
					{
						ID:      raftCfg.LocalID,
						Address: raft.ServerAddress(cfg.RaftAdvertiseAddr),
					},
				},
			}

			r.BootstrapCluster(configuration)
		}
	}

	node := &Node{
		raft:      r,
		fsm:       stateMachine,
		raftFSM:   raftFSM,
		transport: transport,
		storage:   raftStorage,
		cfg:       cfg,
		stopCh:    make(chan struct{}),

		pendingRenewals: make(map[uint64]map[int64]int),
	}

	go node.leaseExpiryLoop()
	go node.membershipSyncLoop()

	return node, nil
}

func (n *Node) Apply(cmd *raftlog.CommandWrapper) (any, error) {

}

func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}
