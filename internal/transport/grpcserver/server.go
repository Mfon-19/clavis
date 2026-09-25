// Package grpcserver implements the LockService gRPC server. It is a pure
// adapter layer. Each method maps a protobuf request to a service call and
// translates the response back. Error mapping is centralized in toGRPCError
package grpcserver

import (
	"context"
	"errors"
	"io"
	"time"

	pb "github.com/Mfon-19/clavis/api/v1"
	"github.com/Mfon-19/clavis/internal/domain"
	"github.com/Mfon-19/clavis/internal/service"
	"github.com/Mfon-19/clavis/internal/transport/leaderhint"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	pb.UnimplementedLockServiceServer
	pb.UnimplementedAdminServiceServer
	service *service.Service
}

func NewServer(svc *service.Service) *Server {
	return &Server{service: svc}
}

// toProtoMembers is the only place the transport layer knows how to translate
// node endpoint metadata into public protobuf messages.
func toProtoMembers(members []domain.ClusterMember) []*pb.ClusterMember {
	protoMembers := make([]*pb.ClusterMember, 0, len(members))
	for _, member := range members {
		memberCopy := member
		protoMembers = append(protoMembers, &pb.ClusterMember{
			NodeId:      memberCopy.NodeID,
			RaftAddress: memberCopy.RaftAddress,
			GrpcAddress: memberCopy.GRPCAddress,
		})
	}

	return protoMembers
}

func (s *Server) CreateLease(ctx context.Context, req *pb.CreateLeaseRequest) (*pb.CreateLeaseResponse, error) {
	resp, err := s.service.CreateLease(req.OwnerId, req.TtlSeconds)
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &pb.CreateLeaseResponse{
		LeaseId:    resp.LeaseID,
		TtlSeconds: int64(resp.TTL / time.Second),
	}, nil
}

func (s *Server) AcquireLock(ctx context.Context, req *pb.AcquireLockRequest) (*pb.AcquireLockResponse, error) {
	resp, err := s.service.AcquireLock(ctx, req.LockName, req.OwnerId, req.LeaseId, req.Wait)
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &pb.AcquireLockResponse{
		FencingToken:    resp.FencingToken,
		LeaseTtlSeconds: int64(resp.LeaseTTL / time.Second),
	}, nil
}

func (s *Server) ReleaseLock(ctx context.Context, req *pb.ReleaseLockRequest) (*pb.ReleaseLockResponse, error) {
	if err := s.service.ReleaseLock(req.LockName, req.LeaseId); err != nil {
		return nil, toGRPCError(err)
	}

	return &pb.ReleaseLockResponse{Released: true}, nil
}

// Heartbeat implements a bidirectional streaming RPC for lease keepalive.
// The client sends HeartbeatRequest on a tick interval and receives
// HeartbeatResponse with the renewed TTL.
func (s *Server) Heartbeat(stream pb.LockService_HeartbeatServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		ttl, err := s.service.RenewLease(req.LeaseId)
		if err != nil {
			return toGRPCError(err)
		}

		if err := stream.Send(&pb.HeartbeatResponse{
			LeaseId:    req.LeaseId,
			TtlSeconds: int64(ttl / time.Second),
		}); err != nil {
			return err
		}
	}
}

func (s *Server) GetStatus(ctx context.Context, req *pb.GetStatusRequest) (*pb.GetStatusResponse, error) {
	// Status is intentionally a local read. It is used for discovery and
	// diagnostics. It does not mutate replicated state.
	resp := s.service.Status()

	return &pb.GetStatusResponse{
		NodeId:            resp.NodeID,
		IsLeader:          resp.IsLeader,
		LeaderGrpcAddress: resp.LeaderGRPCAddress,
		State:             resp.State,
		GrpcAddress:       resp.GRPCAddress,
		Members:           toProtoMembers(resp.Members),
		Stats: &pb.Stats{
			Leases:         int32(resp.Stats.Leases),
			Locks:          int32(resp.Stats.Locks),
			FencingCounter: resp.Stats.FencingCounter,
		},
	}, nil
}

func (s *Server) JoinNode(ctx context.Context, req *pb.JoinNodeRequest) (*pb.JoinNodeResponse, error) {
	leaderAddr, err := s.service.JoinNode(domain.ClusterMember{
		NodeID:      req.NodeId,
		RaftAddress: req.RaftAddress,
		GRPCAddress: req.GrpcAddress,
	})
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &pb.JoinNodeResponse{
		Joined:            true,
		LeaderGrpcAddress: leaderAddr,
	}, nil
}

func (s *Server) RemoveNode(ctx context.Context, req *pb.RemoveNodeRequest) (*pb.RemoveNodeResponse, error) {
	if err := s.service.RemoveNode(req.NodeId); err != nil {
		return nil, toGRPCError(err)
	}

	return &pb.RemoveNodeResponse{Removed: true}, nil
}

// toGRPCError maps domain and service errors to gRPC status codes.
// This is the centralized error translation boundary between the application
// layer and the gRPC transport.
func toGRPCError(err error) error {
	if err == nil {
		return nil
	}

	var invalidArgErr *service.InvalidArgumentError
	if errors.As(err, &invalidArgErr) {
		return status.Error(codes.InvalidArgument, invalidArgErr.Error())
	}

	var notLeaderErr *service.NotLeaderError
	if errors.As(err, &notLeaderErr) {
		return leaderhint.Error(notLeaderErr.LeaderGRPCAddress)
	}

	if errors.Is(err, raft.ErrNotLeader) ||
		errors.Is(err, raft.ErrLeadershipLost) ||
		errors.Is(err, raft.ErrLeadershipTransferInProgress) ||
		errors.Is(err, raft.ErrRaftShutdown) {
		// A leadership transition can race with the service layer's initial
		// IsLeader check. Surface the resulting Raft error as retryable so the
		// SDK performs discovery instead of treating it as an internal failure.
		return status.Error(codes.Unavailable, err.Error())
	}

	switch {
	case errors.Is(err, domain.ErrLockAlreadyHeld):
		// Distinct from other precondition failures so clients can tell "someone
		// else holds this lock" apart from "your lease is gone" by code alone.
		return status.Error(codes.AlreadyExists, err.Error())

	case errors.Is(err, domain.ErrLeaseNotFound),
		errors.Is(err, domain.ErrLockNotFound),
		errors.Is(err, domain.ErrNodeNotFound):
		return status.Error(codes.NotFound, err.Error())

	case errors.Is(err, domain.ErrLeaseExpired):
		return status.Error(codes.FailedPrecondition, err.Error())

	case errors.Is(err, domain.ErrInvalidLeaseTTL),
		errors.Is(err, domain.ErrInvalidTimestamp),
		errors.Is(err, domain.ErrInvalidClusterNode):
		return status.Error(codes.InvalidArgument, err.Error())

	case errors.Is(err, domain.ErrNotLockOwner):
		return status.Error(codes.PermissionDenied, err.Error())

	default:
		return status.Error(codes.Internal, err.Error())
	}
}
