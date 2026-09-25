package cluster

import (
	"slices"
	"sync"
)

// lockQueues holds, for each lock, the callers waiting for it in arrival
// order. Like lease renewals it lives only in the leader's memory: after a
// failover, waiters are redirected and queue again on the new leader.
//
// The queue only decides who may try next. Acquisition itself still goes
// through Raft, so a queue bug can cost fairness but never exclusivity.
type lockQueues struct {
	mu     sync.Mutex
	queues map[string][]*LockWaiter
}

// LockWaiter is one caller's place in a lock's queue.
type LockWaiter struct {
	queues   *lockQueues
	lockName string
	ready    chan struct{}
}

func newLockQueues() *lockQueues {
	return &lockQueues{queues: make(map[string][]*LockWaiter)}
}

// JoinLockQueue adds the caller to the back of lockName's queue. The caller
// must call Leave when it stops waiting, whether or not it got the lock.
func (n *Node) JoinLockQueue(lockName string) *LockWaiter {
	q := n.lockQueues
	w := &LockWaiter{queues: q, lockName: lockName, ready: make(chan struct{}, 1)}

	q.mu.Lock()
	defer q.mu.Unlock()
	q.queues[lockName] = append(q.queues[lockName], w)
	return w
}

// LockHasWaiters reports whether anyone is queued for lockName. Callers that
// are not queued should not take a contended lock ahead of those that are.
func (n *Node) LockHasWaiters(lockName string) bool {
	q := n.lockQueues
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.queues[lockName]) > 0
}

// Ready is signalled when the waiter should check whether it can take the
// lock: the lock was freed, or the waiter moved to the front of the queue.
func (w *LockWaiter) Ready() <-chan struct{} {
	return w.ready
}

// IsHead reports whether this waiter is next in line.
func (w *LockWaiter) IsHead() bool {
	w.queues.mu.Lock()
	defer w.queues.mu.Unlock()
	queue := w.queues.queues[w.lockName]
	return len(queue) > 0 && queue[0] == w
}

// Leave removes the waiter from its queue. If it was next in line, the new
// head is woken so it can check the lock.
func (w *LockWaiter) Leave() {
	q := w.queues
	q.mu.Lock()
	defer q.mu.Unlock()

	queue := q.queues[w.lockName]
	i := slices.Index(queue, w)
	if i < 0 {
		return
	}
	queue = slices.Delete(queue, i, i+1)
	if len(queue) == 0 {
		delete(q.queues, w.lockName)
		return
	}
	q.queues[w.lockName] = queue
	if i == 0 {
		queue[0].wake()
	}
}

// wakeHead signals the first waiter for lockName. It is the FSM's lock-freed
// hook, so it runs inside Raft's apply path and must not block.
func (q *lockQueues) wakeHead(lockName string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if queue := q.queues[lockName]; len(queue) > 0 {
		queue[0].wake()
	}
}

func (w *LockWaiter) wake() {
	select {
	case w.ready <- struct{}{}:
	default: // already signalled
	}
}
