package client

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/Mfon-19/clavis/api/v1"
	"github.com/Mfon-19/clavis/internal/transport/leaderhint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func startLockTestServer(t testing.TB, server pb.LockServiceServer) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	pb.RegisterLockServiceServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(listener)
	}()

	return listener.Addr().String(), func() {
		grpcServer.Stop()
		_ = listener.Close()
	}
}

type redirectServer struct {
	pb.UnimplementedLockServiceServer
	leaderAddr string
	calls      atomic.Int64
}

func (s *redirectServer) CreateLease(
	context.Context,
	*pb.CreateLeaseRequest,
) (*pb.CreateLeaseResponse, error) {
	s.calls.Add(1)
	return nil, leaderhint.Error(s.leaderAddr)
}

type successfulLeaseServer struct {
	pb.UnimplementedLockServiceServer
	calls atomic.Int64
}

func (s *successfulLeaseServer) CreateLease(
	context.Context,
	*pb.CreateLeaseRequest,
) (*pb.CreateLeaseResponse, error) {
	s.calls.Add(1)
	return &pb.CreateLeaseResponse{LeaseId: 42, TtlSeconds: 3}, nil
}

func TestCallWithFailoverFollowsLeaderHintImmediately(t *testing.T) {
	leader := &successfulLeaseServer{}
	leaderAddr, stopLeader := startLockTestServer(t, leader)
	defer stopLeader()

	follower := &redirectServer{leaderAddr: leaderAddr}
	followerAddr, stopFollower := startLockTestServer(t, follower)
	defer stopFollower()

	sdk, err := NewClientWithSeeds([]string{followerAddr}, "redirect")
	require.NoError(t, err)
	defer sdk.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := callWithFailover(sdk, ctx, func(attemptCtx context.Context, client pb.LockServiceClient) (*pb.CreateLeaseResponse, error) {
		return client.CreateLease(attemptCtx, &pb.CreateLeaseRequest{OwnerId: "redirect", TtlSeconds: 3})
	})

	require.NoError(t, err)
	assert.Equal(t, uint64(42), resp.LeaseId)
	assert.Equal(t, int64(1), follower.calls.Load())
	assert.Equal(t, int64(1), leader.calls.Load())
}
