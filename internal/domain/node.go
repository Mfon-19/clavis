package domain

// ClusterMember describes a node's identity and network addresses. Raft
// configuration remains the source of truth for cluster membership; this struct
// only carries endpoint metadata used for client and admin RPCs.
type ClusterMember struct {
	NodeID      string
	RaftAddress string
	GRPCAddress string
}
