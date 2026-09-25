package client

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/Mfon-19/clavis/api/v1"
	"github.com/stretchr/testify/require"
)

type stalledHeartbeatServer struct {
	pb.UnimplementedLockServiceServer
	addr string
}

func (s *stalledHeartbeatServer) CreateLease(
	context.Context,
	*pb.CreateLeaseRequest,
) (*pb.CreateLeaseResponse, error) {
	return &pb.CreateLeaseResponse{LeaseId: 1, TtlSeconds: 1}, nil
}

func (s *stalledHeartbeatServer) GetStatus(
	context.Context,
	*pb.GetStatusRequest,
) (*pb.GetStatusResponse, error) {
	return &pb.GetStatusResponse{
		IsLeader:          true,
		GrpcAddress:       s.addr,
		LeaderGrpcAddress: s.addr,
	}, nil
}

func (s *stalledHeartbeatServer) Heartbeat(stream pb.LockService_HeartbeatServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func TestHeartbeatTimeoutInvalidatesSession(t *testing.T) {
	fake := &stalledHeartbeatServer{}
	addr, stop := startLockTestServer(t, fake)
	defer stop()
	fake.addr = addr

	sdk, err := NewClientWithSeeds([]string{addr}, "heartbeat-timeout")
	require.NoError(t, err)
	require.NoError(t, sdk.Start(context.Background(), time.Second))
	defer sdk.Stop()

	require.Eventually(t, func() bool {
		_, activeErr := sdk.session.activeLeaseID()
		return errors.Is(activeErr, ErrLeaseUnavailable)
	}, 2*time.Second, 25*time.Millisecond)
}
