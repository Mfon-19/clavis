package app

import (
	"context"
	"fmt"
	pb "github.com/Mfon-19/clavis/api/v1"
	"github.com/Mfon-19/clavis/internal/transport/leaderhint"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"time"
)

// RemoveClusterNode connects to the cluster and removes the specified node.
// Automatically follows leader redirects via callLeader.
func RemoveClusterNode(ctx context.Context, clusterAddr, nodeID string) error {
	_, err := callLeader(ctx, clusterAddr, func(attemptCtx context.Context, client pb.AdminServiceClient) (*pb.RemoveNodeResponse, error) {
		return client.RemoveNode(attemptCtx, &pb.RemoveNodeRequest{NodeId: nodeID})
	})
	if err != nil {
		return fmt.Errorf("remove node: %w", err)
	}
	return nil
}

// JoinCluster connects to the cluster and adds this node as a voter.
// Automatically follows leader redirects via callLeader.
func JoinCluster(ctx context.Context, clusterAddr string, member *pb.JoinNodeRequest) error {
	_, err := callLeader(ctx, clusterAddr, func(attemptCtx context.Context, client pb.AdminServiceClient) (*pb.JoinNodeResponse, error) {
		return client.JoinNode(attemptCtx, member)
	})
	if err != nil {
		return fmt.Errorf("join cluster: %w", err)
	}
	return nil
}

// callLeader is the admin CLI's small leader-chasing loop. It intentionally
// opens short-lived connections because join/remove are rare operator actions,
// unlike the SDK data path which keeps a pooled connection cache
func callLeader[T any](
	ctx context.Context,
	addr string,
	op func(context.Context, pb.AdminServiceClient) (T, error),
) (T, error) {
	var zero T
	var lastErr error
	target := addr

	for attempt := 0; attempt < 10; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, err
		}

		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			lastErr = fmt.Errorf("connect to cluster: %w", err)
			if err := waitForRetry(ctx, 300*time.Millisecond); err != nil {
				return zero, err
			}
			continue
		}

		client := pb.NewAdminServiceClient(conn)
		attemptCtx, cancel := context.WithTimeout(ctx, 6*time.Second)
		resp, err := op(attemptCtx, client)
		cancel()
		_ = conn.Close()
		if err == nil {
			return resp, nil
		}

		lastErr = err
		if leaderAddr := leaderhint.FromError(err); leaderAddr != "" {
			target = leaderAddr
			continue
		}

		if err := waitForRetry(ctx, 300*time.Millisecond); err != nil {
			return zero, err
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no reachable leader")
	}

	return zero, lastErr
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
