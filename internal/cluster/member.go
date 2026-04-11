package cluster

import (
	"fmt"
	"sort"

	"github.com/Mfon-19/clavis/internal/domain"
	"github.com/hashicorp/raft"
)

// SelfMember returns this node's local endpoint metadata. Raft membership is
// still authoritative for who is in the cluster; this struct only describes
// how clients and admin tools should reach this node over gRPC.
func (n *Node) SelfMember() domain.ClusterMember {
	return domain.ClusterMember{
		NodeID:      n.cfg.NodeID.String(),
		RaftAddress: n.cfg.RaftAdvertiseAddr,
		GRPCAddress: n.cfg.GRPCAdvertiseAddr,
	}
}

func (n *Node) rememberMember(member domain.ClusterMember) {
	n.endpointRegistryMu.Lock()
	defer n.endpointRegistryMu.Unlock()

	if n.endpointRegistry == nil {
		n.endpointRegistry = make(map[string]domain.ClusterMember)
	}
	n.endpointRegistry[member.NodeID] = member
}

func (n *Node) forgetMember(nodeID string) {
	n.endpointRegistryMu.Lock()
	defer n.endpointRegistryMu.Unlock()

	delete(n.endpointRegistry, nodeID)
}

func (n *Node) knownMember(nodeID string) (domain.ClusterMember, bool) {
	n.endpointRegistryMu.RLock()
	defer n.endpointRegistryMu.RUnlock()

	member, ok := n.endpointRegistry[nodeID]
	return member, ok
}

func (n *Node) localMembers() []domain.ClusterMember {
	n.endpointRegistryMu.RLock()
	defer n.endpointRegistryMu.RUnlock()

	members := make([]domain.ClusterMember, 0, len(n.endpointRegistry))
	for _, member := range n.endpointRegistry {
		members = append(members, member)
	}
	sort.Slice(members, func(i, j int) bool {
		return members[i].NodeID < members[j].NodeID
	})
	return members
}

// GetLeaderGRPCAddress returns the leader's gRPC address if this node knows it.
// Because endpoint metadata is local, followers may return an empty string and
// rely on clients probing their configured seed addresses instead.
func (n *Node) GetLeaderGRPCAddress() string {
	leaderID := n.GetLeaderID()
	if leaderID == "" {
		return ""
	}

	if leaderID == n.cfg.NodeID.String() {
		return n.cfg.GRPCAdvertiseAddr
	}

	if member, ok := n.knownMember(leaderID); ok {
		return member.GRPCAddress
	}

	return ""
}

// Members returns the current Raft configuration joined with whatever local
// endpoint metadata this node knows. Raft remains the source of truth for
// membership; unknown gRPC addresses are left empty instead of fabricating a
// second replicated membership system.
func (n *Node) Members() []domain.ClusterMember {
	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return n.localMembers()
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
		} else if known, ok := n.knownMember(member.NodeID); ok {
			member.GRPCAddress = known.GRPCAddress
		}
		members = append(members, member)
	}

	sort.Slice(members, func(i, j int) bool {
		return members[i].NodeID < members[j].NodeID
	})
	return members
}

// AddClusterMember adds a node to the Raft cluster as a voter and remembers
// its gRPC endpoint locally on the leader. Raft config changes are the only
// source of truth for actual cluster membership.
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
				n.rememberMember(member)
				return nil
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

	n.rememberMember(member)
	return nil
}

// RemoveClusterMember removes a node from the Raft cluster and drops any local
// endpoint metadata this node was caching for it.
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

	n.forgetMember(nodeID)
	return nil
}
