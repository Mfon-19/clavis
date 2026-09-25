package client_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/Mfon-19/clavis/api/v1"
	"github.com/Mfon-19/clavis/internal/app"
	clientpkg "github.com/Mfon-19/clavis/pkg/client"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// These are system benchmarks. Each benchmark runs against a real
// in-process 3-node cluster with gRPC transport and Raft
// replication enabled.
//
// Run with:
//
//	go test -run '^$' -bench '^BenchmarkSystem' ./pkg/client/ -count=1

type benchmarkCluster struct {
	runtimes  []*app.Runtime
	grpcAddrs []string
}

func newBenchmarkCluster(tb testing.TB, nodes int) *benchmarkCluster {
	tb.Helper()

	cluster := &benchmarkCluster{
		runtimes:  make([]*app.Runtime, 0, nodes),
		grpcAddrs: make([]string, 0, nodes),
	}

	rootDir := tb.TempDir()

	for i := 0; i < nodes; i++ {
		raftAddr := freeLocalAddr(tb)
		grpcAddr := freeLocalAddr(tb)

		runtime, err := app.NewRuntime(app.Config{
			NodeID:            uuid.New(),
			RaftAddr:          raftAddr,
			RaftAdvertiseAddr: raftAddr,
			GRPCAddr:          grpcAddr,
			GRPCAdvertiseAddr: grpcAddr,
			DataDir:           filepath.Join(rootDir, fmt.Sprintf("node-%d", i)),
			Bootstrap:         i == 0,
			RaftLogOutput:     io.Discard,
		})
		if err != nil {
			cluster.Close()
			tb.Fatalf("create runtime %d: %v", i, err)
		}

		_ = runtime.Start(context.Background())
		cluster.runtimes = append(cluster.runtimes, runtime)
		cluster.grpcAddrs = append(cluster.grpcAddrs, grpcAddr)

		if err := cluster.waitForNodeReady(grpcAddr, 10*time.Second); err != nil {
			cluster.Close()
			tb.Fatalf("wait for node %d grpc ready: %v", i, err)
		}

		if i == 0 {
			if _, err := cluster.waitForLeader(10 * time.Second); err != nil {
				cluster.Close()
				tb.Fatalf("wait for bootstrap leader: %v", err)
			}
			continue
		}

		member := runtime.Member()
		if err := app.JoinCluster(context.Background(), cluster.grpcAddrs[0], &pb.JoinNodeRequest{
			NodeId:      member.NodeID,
			RaftAddress: member.RaftAddress,
			GrpcAddress: member.GRPCAddress,
		}); err != nil {
			cluster.Close()
			tb.Fatalf("join node %d: %v", i, err)
		}
	}

	if err := cluster.waitForClusterSize(nodes, 10*time.Second); err != nil {
		cluster.Close()
		tb.Fatalf("wait for cluster size %d: %v", nodes, err)
	}

	return cluster
}

func (c *benchmarkCluster) Close() {
	for i := len(c.runtimes) - 1; i >= 0; i-- {
		_ = c.runtimes[i].Stop(context.Background())
	}
}

func (c *benchmarkCluster) leaderRuntime(tb testing.TB) *app.Runtime {
	tb.Helper()

	leaderAddr, err := c.waitForLeader(10 * time.Second)
	if err != nil {
		tb.Fatalf("wait for leader: %v", err)
	}

	for i, runtime := range c.runtimes {
		if runtime.Member().GRPCAddress == leaderAddr {
			return c.runtimes[i]
		}
	}

	tb.Fatalf("no runtime found for leader address %s", leaderAddr)
	return nil
}

func (c *benchmarkCluster) waitForNodeReady(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_, err := c.status(ctx, addr)
		cancel()
		if err == nil {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("node %s did not become ready", addr)
}

func (c *benchmarkCluster) waitForLeader(timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, addr := range c.grpcAddrs {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			resp, err := c.status(ctx, addr)
			cancel()
			if err != nil {
				continue
			}
			if resp.GetIsLeader() && resp.GetGrpcAddress() != "" {
				return resp.GetGrpcAddress(), nil
			}
			if resp.GetLeaderGrpcAddress() != "" {
				return resp.GetLeaderGrpcAddress(), nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return "", fmt.Errorf("no leader elected within %s", timeout)
}

func (c *benchmarkCluster) waitForClusterSize(size int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, addr := range c.grpcAddrs {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			resp, err := c.status(ctx, addr)
			cancel()
			if err != nil {
				continue
			}
			if len(resp.GetMembers()) == size {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("cluster did not reach size %d within %s", size, timeout)
}

func (c *benchmarkCluster) status(ctx context.Context, addr string) (*pb.GetStatusResponse, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pb.NewAdminServiceClient(conn)
	return client.GetStatus(ctx, &pb.GetStatusRequest{})
}

func freeLocalAddr(tb testing.TB) string {
	tb.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("allocate free address: %v", err)
	}
	defer listener.Close()

	return listener.Addr().String()
}

func newBenchmarkClient(tb testing.TB, seeds []string, owner string) *clientpkg.Client {
	tb.Helper()

	client, err := clientpkg.NewClientWithSeeds(seeds, owner)
	if err != nil {
		tb.Fatalf("new client: %v", err)
	}
	return client
}

func acquireUntilSuccess(ctx context.Context, client *clientpkg.Client, lockName string) (*clientpkg.Lock, error) {
	for {
		lock, err := client.Acquire(ctx, lockName)
		if err == nil {
			return lock, nil
		}
		if !errors.Is(err, clientpkg.ErrLockHeld) {
			return nil, err
		}

		timer := time.NewTimer(250 * time.Microsecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func acquireReleaseEventually(ctx context.Context, client *clientpkg.Client, lockName string) error {
	for {
		lock, err := client.Acquire(ctx, lockName)
		if err == nil {
			return lock.Release(ctx)
		}
		if errors.Is(err, clientpkg.ErrLeaseUnavailable) {
			return err
		}

		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func BenchmarkSystemUncontended(b *testing.B) {
	cluster := newBenchmarkCluster(b, 3)
	defer cluster.Close()

	client := newBenchmarkClient(b, cluster.grpcAddrs, "bench-system-uncontended")
	defer client.Stop()

	ctx := context.Background()
	if err := client.Start(ctx, 30*time.Second); err != nil {
		b.Fatalf("start client: %v", err)
	}

	lockName := "bench:system:uncontended"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lock, err := client.Acquire(ctx, lockName)
		if err != nil {
			b.Fatalf("acquire: %v", err)
		}
		if err := lock.Release(ctx); err != nil {
			b.Fatalf("release: %v", err)
		}
	}
}

func BenchmarkSystemContention(b *testing.B) {
	const clientCount = 4

	cluster := newBenchmarkCluster(b, 3)
	defer cluster.Close()

	ctx := context.Background()
	clients := make([]*clientpkg.Client, 0, clientCount)
	for i := 0; i < clientCount; i++ {
		client := newBenchmarkClient(b, cluster.grpcAddrs, fmt.Sprintf("bench-system-contention-%d", i))
		if err := client.Start(ctx, 30*time.Second); err != nil {
			b.Fatalf("start client %d: %v", i, err)
		}
		defer client.Stop()
		clients = append(clients, client)
	}

	lockName := "bench:system:contention"
	var next atomic.Uint64
	errCh := make(chan error, clientCount)
	var wg sync.WaitGroup

	b.ReportAllocs()
	b.ResetTimer()

	for _, client := range clients {
		wg.Add(1)
		go func(c *clientpkg.Client) {
			defer wg.Done()
			for {
				op := next.Add(1)
				if op > uint64(b.N) {
					return
				}

				lock, err := acquireUntilSuccess(ctx, c, lockName)
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}

				time.Sleep(250 * time.Microsecond)

				if err := lock.Release(ctx); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
			}
		}(client)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			b.Fatalf("contention benchmark failed: %v", err)
		}
	}
}

func BenchmarkSystemFailover(b *testing.B) {
	b.ReportAllocs()
	var totalRecovery time.Duration

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		cluster := newBenchmarkCluster(b, 3)
		client := newBenchmarkClient(b, cluster.grpcAddrs, fmt.Sprintf("bench-system-failover-%d", i))
		ctx := context.Background()

		if err := client.Start(ctx, 45*time.Second); err != nil {
			cluster.Close()
			b.Fatalf("start client: %v", err)
		}

		warmupLock, err := client.Acquire(ctx, "bench:system:failover:warmup")
		if err != nil {
			_ = client.Stop()
			cluster.Close()
			b.Fatalf("warmup acquire: %v", err)
		}
		if err := warmupLock.Release(ctx); err != nil {
			_ = client.Stop()
			cluster.Close()
			b.Fatalf("warmup release: %v", err)
		}

		leader := cluster.leaderRuntime(b)

		b.StartTimer()
		start := time.Now()

		if err := leader.StopNow(context.Background()); err != nil {
			b.Fatalf("stop leader: %v", err)
		}

		opCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = acquireReleaseEventually(opCtx, client, "bench:system:failover")
		cancel()
		if err != nil {
			b.Fatalf("recover after failover: %v", err)
		}

		recovery := time.Since(start)
		totalRecovery += recovery
		b.StopTimer()

		if err := client.Stop(); err != nil {
			b.Fatalf("stop client: %v", err)
		}
		cluster.Close()
	}

	b.ReportMetric(float64(totalRecovery.Microseconds())/float64(b.N), "failover_recovery_us/op")
}
