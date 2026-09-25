package client_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mfon-19/clavis/internal/app"
	clientpkg "github.com/Mfon-19/clavis/pkg/client"
)

// These are system benchmarks. Each benchmark runs against a real
// in-process 3-node cluster with gRPC transport and Raft
// replication enabled.
//
// Run with:
//
//	go test -run '^$' -bench '^BenchmarkSystem' ./pkg/client/ -count=1

func newBenchmarkCluster(tb testing.TB, nodes int) *app.LocalCluster {
	tb.Helper()

	cluster, err := app.StartLocalCluster(nodes, tb.TempDir(), io.Discard)
	if err != nil {
		tb.Fatalf("start %d-node cluster: %v", nodes, err)
	}
	return cluster
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

	client := newBenchmarkClient(b, cluster.Addrs(), "bench-system-uncontended")
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
		client := newBenchmarkClient(b, cluster.Addrs(), fmt.Sprintf("bench-system-contention-%d", i))
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
		client := newBenchmarkClient(b, cluster.Addrs(), fmt.Sprintf("bench-system-failover-%d", i))
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

		leader, err := cluster.WaitForLeader(10 * time.Second)
		if err != nil {
			b.Fatalf("wait for leader: %v", err)
		}

		b.StartTimer()
		start := time.Now()

		if err := cluster.StopNode(leader); err != nil {
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
