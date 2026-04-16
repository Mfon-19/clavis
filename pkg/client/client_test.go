package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestStartContextIsStartupOnly verifies that Start may be called with a short
// startup deadline without tying the lease heartbeat lifetime to that context.
func TestStartContextIsStartupOnly(t *testing.T) {
	cluster := newBenchmarkCluster(t, 3)
	defer cluster.Close()

	client := newBenchmarkClient(t, cluster.grpcAddrs, "start-context-startup-only")
	defer func() {
		require.NoError(t, client.Stop())
	}()

	startCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	require.NoError(t, client.Start(startCtx, 3*time.Second))

	<-startCtx.Done()

	// Wait past the original lease TTL. Without background heartbeats, the lease
	// would be expired by the time Acquire runs below.
	time.Sleep(4 * time.Second)

	acquireCtx, acquireCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer acquireCancel()

	lock, err := client.Acquire(acquireCtx, "start-context-lock")
	require.NoError(t, err)
	require.NoError(t, lock.Release(acquireCtx))
}
