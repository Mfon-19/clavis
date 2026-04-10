package client

import "context"

// Lock is a handle to an acquired distributed lock. Use [Lock.Token] to get
// the fencing token for downstream write protection, and [Lock.Release] to
// explicitly release the lock
type Lock struct {
	client       *Client
	name         string
	fencingToken uint64
}

// Token returns the globally monotonic fencing token assigned at acquisition
func (l *Lock) Token() uint64 {
	return l.fencingToken
}

// Release releases this lock. Only the lease that acquired it may release it
func (l *Lock) Release(ctx context.Context) error {
	return l.client.Release(ctx, l.name)
}
