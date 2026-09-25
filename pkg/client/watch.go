package client

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// WaitAcquire blocks until it holds lockName or ctx is done. Waiters are
// queued on the leader and handed the lock in arrival order as soon as it is
// freed, so no waiter is starved and there is no polling delay.
func (c *Client) WaitAcquire(ctx context.Context, lockName string) (*Lock, error) {
	if lockName == "" {
		return nil, fmt.Errorf("lockName is required")
	}

	backoff := 50 * time.Millisecond
	for {
		asked := time.Now()
		lock, err := c.acquire(ctx, lockName, true)
		if err == nil {
			return lock, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !errors.Is(err, ErrLockHeld) {
			return nil, err
		}

		// The leader waits several seconds before reporting a held lock, so
		// normally we ask again at once. An immediate answer means the wait was
		// cut short, for example by a leader change, so back off briefly.
		if time.Since(asked) < 100*time.Millisecond {
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			backoff = min(2*backoff, time.Second)
		} else {
			backoff = 50 * time.Millisecond
		}
	}
}
