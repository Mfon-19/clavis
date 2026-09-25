package client

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// WaitAcquire waits until lockName appears available, then attempts to acquire
// it. It uses bounded exponential backoff instead of a server-side watch stream
// to keep the public API and server state machine small.
func (c *Client) WaitAcquire(ctx context.Context, lockName string) (*Lock, error) {
	if lockName == "" {
		return nil, fmt.Errorf("lockName is required")
	}

	backoff := 50 * time.Millisecond
	for {
		lock, err := c.Acquire(ctx, lockName)
		if err == nil {
			return lock, nil
		}
		if !errors.Is(err, ErrLockHeld) {
			return nil, err
		}

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}

		if backoff < time.Second {
			backoff *= 2
		}
	}
}
