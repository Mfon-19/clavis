package service

import (
	"context"
	"testing"
	"time"

	"github.com/Mfon-19/clavis/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWaitingAcquireIsFIFO verifies that waiters get a freed lock in arrival
// order and within about one commit, that callers who are not queued cannot
// take the lock ahead of them, and that a waiter who gives up leaves the queue.
func TestWaitingAcquireIsFIFO(t *testing.T) {
	nodes, leader := newPorcupineCluster(t, 3)
	defer shutDownNodes(t, nodes)
	svc := NewService(leader)
	ctx := context.Background()
	const lock = "fifo-lock"

	leases := make(map[string]uint64)
	for _, owner := range []string{"holder", "first", "second", "barger", "quitter"} {
		resp, err := svc.CreateLease(owner, 60)
		require.NoError(t, err)
		leases[owner] = resp.LeaseID
	}

	_, err := svc.AcquireLock(ctx, lock, "holder", leases["holder"], false)
	require.NoError(t, err)

	acquired := make(chan string, 2)
	wait := func(owner string) {
		go func() {
			if _, err := svc.AcquireLock(ctx, lock, owner, leases[owner], true); err == nil {
				acquired <- owner
			}
		}()
	}
	wait("first")
	require.Eventually(t, func() bool { return leader.LockHasWaiters(lock) }, time.Second, 5*time.Millisecond)
	time.Sleep(50 * time.Millisecond) // let "first" settle ahead of "second"
	wait("second")
	time.Sleep(50 * time.Millisecond)

	_, err = svc.AcquireLock(ctx, lock, "barger", leases["barger"], false)
	assert.ErrorIs(t, err, domain.ErrLockAlreadyHeld, "a caller that is not queued must not jump the queue")

	quitCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_, err = svc.AcquireLock(quitCtx, lock, "quitter", leases["quitter"], true)
	assert.ErrorIs(t, err, domain.ErrLockAlreadyHeld, "a waiter that gives up reports the lock as held")

	for _, next := range []string{"first", "second"} {
		prev := map[string]string{"first": "holder", "second": "first"}[next]
		released := time.Now()
		require.NoError(t, svc.ReleaseLock(lock, leases[prev]))

		select {
		case got := <-acquired:
			assert.Equal(t, next, got, "waiters must be served in arrival order")
			assert.Less(t, time.Since(released), 250*time.Millisecond, "handoff should take about one commit")
		case <-time.After(2 * time.Second):
			t.Fatalf("%s was not handed the lock after %s released it", next, prev)
		}
	}
	assert.False(t, leader.LockHasWaiters(lock), "every waiter has left the queue")
}
