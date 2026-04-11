package service

import (
	"github.com/Mfon-19/clavis/internal/cluster"
	"github.com/Mfon-19/clavis/internal/domain"
	"github.com/Mfon-19/clavis/internal/raftlog"
	"github.com/Mfon-19/clavis/internal/state"
	"time"
)

// Service orchestrates business operations between the transport layer and
// the Raft cluster. Every mutating operation enforces leader-only execution
// via ensureLeader, returning a [NotLeaderError] with the leader's gRPC
// address for client-side redirect
type Service struct {
	node *cluster.Node
}

func NewService(node *cluster.Node) *Service {
	return &Service{node: node}
}

type CreateLeaseResult struct {
	LeaseID    uint64
	TTLSeconds int64
}

type RenewLeaseResult struct {
	TTLSeconds int64
}

type AcquireLockResult struct {
	FencingToken    uint64
	LeaseTTLSeconds int64
}

type ReleaseLockResult struct {
	Released bool
}

type LockStateResult struct {
	Held         bool
	FencingToken uint64
}

type StatusResult struct {
	NodeID            string
	IsLeader          bool
	LeaderAddress     string
	LeaderGRPCAddress string
	ClusterSize       int32
	State             string
	GRPCAddress       string
	Members           []domain.ClusterMember
	Stats             state.Stats
}

type JoinNodeResult struct {
	Joined            bool
	LeaderGRPCAddress string
}

type RemoveNodeResult struct {
	Removed bool
}

// ensureLeader returns nil if this node is the Raft leader, or a
// NotLeaderError containing the leader's gRPC address for client redirect
func (s *Service) ensureLeader() error {
	if s.node.IsLeader() {
		return nil
	}

	return &NotLeaderError{
		LeaderGRPCAddress: s.node.GetLeaderGRPCAddress(),
	}
}

// CreateLease creates a new time-bounded lease for the given owner. The service
// attaches the proposal timestamp before submitting the command to Raft so the
// FSM does not need to read local wall-clock time during replay
func (s *Service) CreateLease(ownerID string, ttlSeconds int64) (*CreateLeaseResult, error) {
	if err := s.ensureLeader(); err != nil {
		return nil, err
	}
	if ownerID == "" {
		return nil, &InvalidArgumentError{Message: "owner_id required"}
	}
	if ttlSeconds <= 0 {
		return nil, &InvalidArgumentError{Message: "ttl_seconds must be greater than 0"}
	}

	resp, err := s.node.Apply(raftlog.NewCreateLeaseCmd(ownerID, time.Duration(ttlSeconds)*time.Second, time.Now().UTC()))
	if err != nil {
		return nil, err
	}

	lease := resp.(state.CreateLeaseResponse)
	return &CreateLeaseResult{
		LeaseID:    lease.LeaseID,
		TTLSeconds: ttlSeconds,
	}, nil
}

// RenewLease extends a lease's expiry by its original TTL. It uses
// Node.ApplyRenewLease instead of Node.Apply directly so the leader can track
// the renewal as pending while Raft replication is in flight
func (s *Service) RenewLease(leaseID uint64) (*RenewLeaseResult, error) {
	if err := s.ensureLeader(); err != nil {
		return nil, err
	}

	result, err := s.node.ApplyRenewLease(leaseID, time.Now().UTC())
	if err != nil {
		return nil, err
	}

	resp := result.(state.RenewLeaseResponse)
	return &RenewLeaseResult{
		TTLSeconds: int64(resp.TTL.Seconds()),
	}, nil
}

// Heartbeat renews a lease from the bidirectional streaming Heartbeat RPC. It
// intentionally shares the same Raft-backed path as unary RenewLease
func (s *Service) Heartbeat(leaseID uint64) (*RenewLeaseResult, error) {
	return s.RenewLease(leaseID)
}

// AcquireLock acquires a named distributed lock bound to the given lease.
// Returns a monotonic fencing token for split-brain protection
func (s *Service) AcquireLock(lockName, ownerID string, leaseID uint64) (*AcquireLockResult, error) {
	if err := s.ensureLeader(); err != nil {
		return nil, err
	}
	if ownerID == "" || lockName == "" {
		return nil, &InvalidArgumentError{Message: "owner_id, lock_name and lease_id are required"}
	}

	result, err := s.node.Apply(raftlog.NewAcquireLockCmd(lockName, ownerID, leaseID, time.Now().UTC()))
	if err != nil {
		return nil, err
	}

	resp := result.(state.AcquireLockResponse)
	return &AcquireLockResult{
		FencingToken:    resp.FencingToken,
		LeaseTTLSeconds: int64(resp.LeaseTTL.Seconds()),
	}, nil
}

// ReleaseLock releases a held lock. Only the lease that acquired it may release it.
func (s *Service) ReleaseLock(lockName string, leaseID uint64) (*ReleaseLockResult, error) {
	if err := s.ensureLeader(); err != nil {
		return nil, err
	}
	if lockName == "" {
		return nil, &InvalidArgumentError{Message: "lock_name required"}
	}

	result, err := s.node.Apply(raftlog.NewReleaseLockCmd(lockName, leaseID))
	if err != nil {
		return nil, err
	}

	resp := result.(state.ReleaseLockResponse)
	return &ReleaseLockResult{Released: resp.Released}, nil
}

func (s *Service) LockState(lockName string) (*LockStateResult, error) {
	if err := s.ensureLeader(); err != nil {
		return nil, err
	}
	if lockName == "" {
		return nil, &InvalidArgumentError{Message: "lock_name required"}
	}

	held, token := s.node.LockState(lockName)
	return &LockStateResult{
		Held:         held,
		FencingToken: token,
	}, nil
}

// Status returns cluster health, leader info, local endpoint metadata, and FSM
// stats. This is the only read path that bypasses Raft consensus.
func (s *Service) Status() *StatusResult {
	stats := s.node.Stats()
	self := s.node.SelfMember()

	return &StatusResult{
		NodeID:            s.node.GetNodeID().String(),
		IsLeader:          s.node.IsLeader(),
		LeaderAddress:     s.node.GetLeader(),
		LeaderGRPCAddress: s.node.GetLeaderGRPCAddress(),
		ClusterSize:       int32(s.node.GetClusterSize()),
		State:             s.node.GetState().String(),
		GRPCAddress:       self.GRPCAddress,
		Members:           s.node.Members(),
		Stats:             stats,
	}
}

// JoinNode adds a new node to the Raft cluster as a voter.
func (s *Service) JoinNode(member domain.ClusterMember) (*JoinNodeResult, error) {
	if err := s.ensureLeader(); err != nil {
		return nil, err
	}

	if err := s.node.AddClusterMember(member); err != nil {
		return nil, err
	}

	return &JoinNodeResult{
		Joined:            true,
		LeaderGRPCAddress: s.node.GetLeaderGRPCAddress(),
	}, nil
}

// RemoveNode removes a node from the Raft cluster.
func (s *Service) RemoveNode(nodeID string) (*RemoveNodeResult, error) {
	if err := s.ensureLeader(); err != nil {
		return nil, err
	}
	if nodeID == "" {
		return nil, &InvalidArgumentError{Message: "node_id required"}
	}

	if err := s.node.RemoveClusterMember(nodeID); err != nil {
		return nil, err
	}

	return &RemoveNodeResult{Removed: true}, nil
}
