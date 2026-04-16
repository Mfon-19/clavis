package cluster

import (
	"fmt"
	"sort"
	"time"

	"github.com/Mfon-19/clavis/internal/domain"
	"github.com/Mfon-19/clavis/internal/raftlog"
	"github.com/hashicorp/raft"
)

// SelfMember returns this node's identity and client-facing endpoint metadata.
// Raft membership is still authoritative for who is in the cluster; this
// struct only carries the routable addresses associated with this node ID.
func (n *Node) SelfMember() domain.ClusterMember {
	return domain.ClusterMember{
		NodeID:      n.cfg.NodeID.String(),
		RaftAddress: n.cfg.RaftAdvertiseAddr,
		GRPCAddress: n.cfg.GRPCAdvertiseAddr,
	}
}

// endpointMetadataLoop keeps the leader's own gRPC endpoint present in the
// replicated endpoint map. This covers the initial bootstrap node and repairs
// self metadata after restarts or snapshot restores without making endpoint
// metadata a second source of truth for membership.
func (n *Node) endpointMetadataLoop() {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !n.IsLeader() {
				continue
			}

			self := n.SelfMember()
			if current, ok := n.fsm.GetEndpoint(self.NodeID); ok && current == self.GRPCAddress {
				continue
			}

			_, _ = n.Apply(raftlog.NewUpsertEndpointCmd(self.NodeID, self.GRPCAddress))
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) upsertEndpointMetadata(member domain.ClusterMember) error {
	if _, err := n.Apply(raftlog.NewUpsertEndpointCmd(member.NodeID, member.GRPCAddress)); err != nil {
		return fmt.Errorf("replicate endpoint metadata: %w", err)
	}
	return nil
}

func (n *Node) removeEndpointMetadata(nodeID string) error {
	if _, err := n.Apply(raftlog.NewRemoveEndpointCmd(nodeID)); err != nil {
		return fmt.Errorf("remove endpoint metadata: %w", err)
	}
	return nil
}

// GetLeaderGRPCAddress returns the client-facing gRPC address of the current
// Raft leader using replicated endpoint metadata keyed by leader node ID.
func (n *Node) GetLeaderGRPCAddress() string {
	leaderID := n.GetLeaderID()
	if leaderID == "" {
		return ""
	}

	if leaderID == n.cfg.NodeID.String() {
		return n.cfg.GRPCAdvertiseAddr
	}

	addr, _ := n.fsm.GetEndpoint(leaderID)
	return addr
}

// Members returns the active Raft configuration joined with replicated
// client-facing endpoint metadata. Raft remains the source of truth for
// membership; the FSM only stores gRPC addresses keyed by node ID.
func (n *Node) Members() []domain.ClusterMember {
	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return []domain.ClusterMember{n.SelfMember()}
	}

	servers := configFuture.Configuration().Servers
	members := make([]domain.ClusterMember, 0, len(servers))
	for _, server := range servers {
		member := domain.ClusterMember{
			NodeID:      string(server.ID),
			RaftAddress: string(server.Address),
		}

		if server.ID == raft.ServerID(n.cfg.NodeID.String()) {
			member.GRPCAddress = n.cfg.GRPCAdvertiseAddr
		} else if grpcAddr, ok := n.fsm.GetEndpoint(member.NodeID); ok {
			member.GRPCAddress = grpcAddr
		}

		members = append(members, member)
	}

	sort.Slice(members, func(i, j int) bool {
		return members[i].NodeID < members[j].NodeID
	})
	return members
}

// AddClusterMember adds a node to the Raft cluster as a voter and replicates
// its client-facing gRPC endpoint metadata so all followers can discover the
// leader after failover without relying on local seed lists.
func (n *Node) AddClusterMember(member domain.ClusterMember) error {
	if !n.IsLeader() {
		return fmt.Errorf("cannot add cluster member: not leader")
	}

	if member.NodeID == "" || member.RaftAddress == "" || member.GRPCAddress == "" {
		return fmt.Errorf("cluster member metadata is incomplete")
	}

	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("failed to read cluster config: %w", err)
	}

	for _, server := range configFuture.Configuration().Servers {
		if string(server.ID) == member.NodeID {
			if string(server.Address) == member.RaftAddress && server.Suffrage == raft.Voter {
				return n.upsertEndpointMetadata(member)
			}
			break
		}

		if string(server.Address) == member.RaftAddress && string(server.ID) != member.NodeID {
			return fmt.Errorf("raft address %s is already in use by node %s; remove it before adding node %s", member.RaftAddress, server.ID, member.NodeID)
		}
	}

	addFuture := n.raft.AddVoter(raft.ServerID(member.NodeID), raft.ServerAddress(member.RaftAddress), 0, 0)
	if err := addFuture.Error(); err != nil {
		return fmt.Errorf("failed to add voter: %w", err)
	}

	return n.upsertEndpointMetadata(member)
}

// RemoveClusterMember removes a node from the Raft cluster and deletes any
// replicated endpoint metadata for it. Stale endpoint entries are harmless
// because Members() always derives the active set from Raft config first.
func (n *Node) RemoveClusterMember(nodeID string) error {
	if !n.IsLeader() {
		return fmt.Errorf("cannot remove cluster member: not leader")
	}

	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("failed to read cluster config: %w", err)
	}

	found := false
	for _, server := range configFuture.Configuration().Servers {
		if string(server.ID) == nodeID {
			found = true
			break
		}
	}
	if !found {
		return domain.ErrNodeNotFound
	}

	removeFuture := n.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
	if err := removeFuture.Error(); err != nil {
		return fmt.Errorf("failed to remove server: %w", err)
	}

	return n.removeEndpointMetadata(nodeID)
}
