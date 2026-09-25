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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// electionServer answers "not leader" without a hint, as every node does
// during a Raft election, until the election ends.
type electionServer struct {
	pb.UnimplementedLockServiceServer
	electedAt time.Time
	calls     atomic.Int64
}

func (s *electionServer) CreateLease(
	context.Context,
	*pb.CreateLeaseRequest,
) (*pb.CreateLeaseResponse, error) {
	s.calls.Add(1)
	if time.Now().Before(s.electedAt) {
		return nil, status.Error(codes.Unavailable, "not leader")
	}
	return &pb.CreateLeaseResponse{LeaseId: 7, TtlSeconds: 3}, nil
}

func TestCallWithFailoverWaitsOutAnElection(t *testing.T) {
	server := &electionServer{electedAt: time.Now().Add(300 * time.Millisecond)}
	addr, stop := startLockTestServer(t, server)
	defer stop()

	sdk, err := NewClientWithSeeds([]string{addr}, "election")
	require.NoError(t, err)
	defer sdk.Stop()

	createLease := func(attemptCtx context.Context, client pb.LockServiceClient) (*pb.CreateLeaseResponse, error) {
		return client.CreateLease(attemptCtx, &pb.CreateLeaseRequest{OwnerId: "election", TtlSeconds: 3})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := callWithFailover(sdk, ctx, createLease)
	require.NoError(t, err, "the call should outlast a short election")
	assert.Equal(t, uint64(7), resp.LeaseId)
	assert.Less(t, server.calls.Load(), int64(20), "retries should back off")

	server.electedAt = time.Now().Add(time.Hour)
	ctx, cancel = context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = callWithFailover(sdk, ctx, createLease)
	assert.Equal(t, codes.Unavailable, status.Code(err), "a call that runs out of time reports why")
	assert.GreaterOrEqual(t, time.Since(started), 200*time.Millisecond, "it keeps trying until the context ends")
}

// stalledServer accepts TCP connections but never speaks gRPC, like a paused
// process.
func startStalledServer(t testing.TB) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var conns []net.Conn
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conns = append(conns, conn)
		}
	}()
	return listener.Addr().String(), func() { _ = listener.Close() }
}

func TestCallWithFailoverSkipsAnUnresponsiveNode(t *testing.T) {
	defer func(d time.Duration) { attemptTimeout = d }(attemptTimeout)
	attemptTimeout = 200 * time.Millisecond

	stalledAddr, stopStalled := startStalledServer(t)
	defer stopStalled()
	healthy := &successfulLeaseServer{}
	healthyAddr, stopHealthy := startLockTestServer(t, healthy)
	defer stopHealthy()

	sdk, err := NewClientWithSeeds([]string{stalledAddr, healthyAddr}, "stalled")
	require.NoError(t, err)
	defer sdk.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := callWithFailover(sdk, ctx, func(attemptCtx context.Context, client pb.LockServiceClient) (*pb.CreateLeaseResponse, error) {
		return client.CreateLease(attemptCtx, &pb.CreateLeaseRequest{OwnerId: "stalled", TtlSeconds: 3})
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(42), resp.LeaseId)
}
