package cluster

import (
	"github.com/Mfon-19/clavis/internal/state"
	"github.com/hashicorp/raft"
	"sync"
	"sync/atomic"
)

// Node wraps a Raft instance with the clavis FSM and provides a clean API for applying
// commands, querying state, and managing cluster membership. It runs two background
// goroutines:
// 		- leaseExpiryLoop (100ms): leader-only, expires dead leases and cascade-releases locks
// 		- membershipSyncLoop (500ms): leader-only, ensures this node's metadata is registered
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
}
