// Package grpcserver implements the LockService gRPC server. It is a pure
// adapter layer. Each method maps a protobuf request to a service call and
// translates the response back. Error mapping in centralized in errors.go
package grpcserver

import (
	"context"
	"errors"
	pb "github.com/Mfon-19/clavis/api/v1"
	"github.com/Mfon-19/clavis/internal/domain"
	"github.com/Mfon-19/clavis/internal/service"
	"github.com/Mfon-19/clavis/internal/transport/leaderhint"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
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
		TtlSeconds: resp.TTLSeconds,
	}, nil
}

func (s *Server) RenewLease(ctx context.Context, req *pb.RenewLeaseRequest) (*pb.RenewLeaseResponse, error) {
	resp, err := s.service.RenewLease(req.LeaseId)
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &pb.RenewLeaseResponse{
		TtlSeconds: resp.TTLSeconds,
	}, nil
}

func (s *Server) AcquireLock(ctx context.Context, req *pb.AcquireLockRequest) (*pb.AcquireLockResponse, error) {
	resp, err := s.service.AcquireLock(req.LockName, req.OwnerId, req.LeaseId)
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &pb.AcquireLockResponse{
		FencingToken:    resp.FencingToken,
		LeaseTtlSeconds: resp.LeaseTTLSeconds,
	}, nil
}

func (s *Server) ReleaseLock(ctx context.Context, req *pb.ReleaseLockRequest) (*pb.ReleaseLockResponse, error) {
	resp, err := s.service.ReleaseLock(req.LockName, req.LeaseId)
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &pb.ReleaseLockResponse{
		Released: resp.Released,
	}, nil
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

		resp, err := s.service.Heartbeat(req.LeaseId)
		if err != nil {
			return toGRPCError(err)
		}

		if err := stream.Send(&pb.HeartbeatResponse{
			LeaseId:    req.LeaseId,
			TtlSeconds: resp.TTLSeconds,
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
		LeaderAddress:     resp.LeaderAddress,
		LeaderGrpcAddress: resp.LeaderGRPCAddress,
		ClusterSize:       resp.ClusterSize,
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
	resp, err := s.service.JoinNode(domain.ClusterMember{
		NodeID:      req.NodeId,
		RaftAddress: req.RaftAddress,
		GRPCAddress: req.GrpcAddress,
	})
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &pb.JoinNodeResponse{
		Joined:            resp.Joined,
		LeaderGrpcAddress: resp.LeaderGRPCAddress,
	}, nil
}

func (s *Server) RemoveNode(ctx context.Context, req *pb.RemoveNodeRequest) (*pb.RemoveNodeResponse, error) {
	resp, err := s.service.RemoveNode(req.NodeId)
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &pb.RemoveNodeResponse{Removed: resp.Removed}, nil
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

	switch err {
	case domain.ErrLeaseNotFound, domain.ErrLockNotFound, domain.ErrNodeNotFound:
		return status.Error(codes.NotFound, err.Error())

	case domain.ErrLeaseExpired, domain.ErrLockAlreadyHeld, domain.ErrStaleToken:
		return status.Error(codes.FailedPrecondition, err.Error())

	case domain.ErrInvalidLeaseTTL, domain.ErrInvalidTimestamp, domain.ErrInvalidClusterNode:
		return status.Error(codes.InvalidArgument, err.Error())

	case domain.ErrNotLockOwner:
		return status.Error(codes.PermissionDenied, err.Error())

	default:
		return status.Error(codes.Internal, err.Error())
	}
}
