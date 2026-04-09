package cluster

import (
	"fmt"
	"github.com/Mfon-19/clavis/internal/domain"
	"github.com/Mfon-19/clavis/internal/raftlog"
	"github.com/hashicorp/raft"
	"sort"
	"time"
)

// SelfMember returns this node's replicated membership metadata. This metadata
// is separate from the Raft configuration: Raft only needs peer addresses, but
// clients also need gRPC addresses for leader redirects and discovery.
func (n *Node) SelfMember() domain.ClusterMember {
	return domain.ClusterMember{
		NodeID:      n.cfg.NodeID.String(),
		RaftAddress: n.cfg.RaftAdvertiseAddr,
		GRPCAddress: n.cfg.GRPCAdvertiseAddr,
	}
}

// GetLeaderGRPCAddress returns the gRPC address of the current leader by
// looking up the leader's node ID in the FSM's membership table.
func (n *Node) GetLeaderGRPCAddress() string {
	leaderID := n.GetLeaderID()
	if leaderID == "" {
		return ""
	}

	if leaderID == n.cfg.NodeID.String() {
		return n.cfg.GRPCAdvertiseAddr
	}

	if member, ok := n.fsm.GetMember(leaderID); ok {
		return member.GRPCAddress
	}

	return ""
}

func (n *Node) Members() []domain.ClusterMember {
	members := n.fsm.Members()
	sort.Slice(members, func(i, j int) bool {
		return members[i].NodeID < members[j].NodeID
	})
	return members
}

func (n *Node) memberMatchesSelf(member *domain.ClusterMember) bool {
	self := n.SelfMember()
	return member.NodeID == self.NodeID &&
		member.RaftAddress == self.RaftAddress &&
		member.GRPCAddress == self.GRPCAddress
}

func (n *Node) ensureSelfMember() error {
	self := n.SelfMember()
	if member, ok := n.fsm.GetMember(self.NodeID); ok && n.memberMatchesSelf(member) {
		return nil
	}

	_, err := n.Apply(raftlog.NewRegisterNodeCmd(self))
	return err
}

// AddClusterMember adds a node to the Raft cluster as a voter and then
// registers its client-facing metadata in the FSM. It must run on the leader
// because both Raft config changes and replicated metadata writes are
// leader-only operations.
//
// The order matters: the new voter is added first. This avoids evicting an
// existing member during failed replacements or bad advertise-address joins.
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
				_, err := n.Apply(raftlog.NewRegisterNodeCmd(member))
				return err
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

	_, err := n.Apply(raftlog.NewRegisterNodeCmd(member))
	return err
}

// RemoveClusterMember removes a node from the Raft cluster and replicated
// member metadata.
//
// Metadata is deregistered before RemoveServer so self-removing leaders do not
// lose leadership before the metadata update commits. If RemoveServer fails,
// the old metadata is restored. During self-removal, membershipSyncLoop is
// blocked so the leader does not re-register itself while stepping down.
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

	existingMember, hadMember := n.fsm.GetMember(nodeID)
	selfRemoval := nodeID == n.cfg.NodeID.String()
	if selfRemoval {
		n.selfRegistrationBlocked.Store(true)
	}

	if hadMember {
		if _, err := n.Apply(raftlog.NewDeregisterNodeCmd(nodeID)); err != nil {
			if selfRemoval {
				n.selfRegistrationBlocked.Store(false)
			}
			return err
		}
	}

	removeFuture := n.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
	if err := removeFuture.Error(); err != nil {
		if selfRemoval {
			n.selfRegistrationBlocked.Store(false)
		}
		if hadMember {
			_, _ = n.Apply(raftlog.NewRegisterNodeCmd(*existingMember))
		}
		return fmt.Errorf("failed to remove server: %w", err)
	}

	return nil
}

// membershipSyncLoop ensures the leader's own metadata is registered in the
// FSM. This handles the case where a node restarts with a new address or
// bootstraps for the first time.
func (n *Node) membershipSyncLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			if !n.IsLeader() {
				continue
			}
			if n.selfRegistrationBlocked.Load() {
				continue
			}

			_ = n.ensureSelfMember()
		}
	}
}
