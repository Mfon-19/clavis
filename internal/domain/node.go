package domain

// ClusterMember describes a node's identity and network addresses.
// Members are stored in the FSM and replicated via Raft so every node
// knows the full cluster topology for leader redirection and client discovery.
type ClusterMember struct {
	NodeID      string `json:"node_id"`
	RaftAddress string `json:"raft_address"`
	GRPCAddress string `json:"grpc_address"`
}
