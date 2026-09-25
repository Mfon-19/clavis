package client

import (
	"context"
	"fmt"
	pb "github.com/Mfon-19/clavis/api/v1"
	"github.com/Mfon-19/clavis/internal/transport/leaderhint"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"strings"
	"sync"
	"time"
)

// resolver maintains a pool of gRPC connections to cluster nodes and tracks the
// last known leader address.
//
// The resolver has three jobs:
//   - normalize and remember seed/member addresses
//   - reuse long-lived HTTP/2 gRPC connections
//   - chase structured leader redirects when a follower rejects a write
type resolver struct {
	mu          sync.Mutex
	seedAddrs   []string
	currentAddr string
	conns       map[string]*grpc.ClientConn
}

// newResolver creates a resolver seeded with the given addresses. The first
// seed is used as the initial current address
func newResolver(seeds []string) *resolver {
	current := ""
	if len(seeds) > 0 {
		current = seeds[0]
	}

	return &resolver{
		seedAddrs:   append([]string(nil), seeds...),
		currentAddr: current,
		conns:       make(map[string]*grpc.ClientConn),
	}
}

// normalizeSeedAddrs splits comma-separated addresses, trims whitespace,
// and deduplicates the result
func normalizeSeedAddrs(addrs []string) []string {
	seen := make(map[string]struct{})
	normalized := make([]string, 0, len(addrs))

	for _, addr := range addrs {
		for _, part := range strings.Split(addr, ",") {
			trimmed := strings.TrimSpace(part)
			if trimmed == "" {
				continue
			}
			if _, exists := seen[trimmed]; exists {
				continue
			}
			seen[trimmed] = struct{}{}
			normalized = append(normalized, trimmed)
		}
	}

	return normalized
}

// dialConn opens a raw gRPC connection to a cluster node
func dialConn(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// connFor returns a pooled connection for the given address, creating one if
// needed. Concurrent callers for the same address are safe; the loser's
// connection is closed and the winner's is reused
func (r *resolver) connFor(addr string) (*grpc.ClientConn, error) {
	r.mu.Lock()
	if conn := r.conns[addr]; conn != nil {
		r.mu.Unlock()
		return conn, nil
	}
	r.mu.Unlock()

	conn, err := dialConn(addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if existing := r.conns[addr]; existing != nil {
		_ = conn.Close()
		return existing, nil
	}

	if r.conns == nil {
		r.conns = make(map[string]*grpc.ClientConn)
	}
	r.conns[addr] = conn
	return conn, nil
}

// close drains a connection pool, closing all cached gRPC connections
func (r *resolver) close() error {
	r.mu.Lock()
	conns := r.conns
	r.conns = make(map[string]*grpc.ClientConn)
	r.mu.Unlock()

	var firstErr error
	for _, conn := range conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// current returns the last known leader address, or empty if unknown
func (r *resolver) current() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.currentAddr
}

// candidateAddrs returns deduplicated addresses to try, with the current
// (likely leader) address first, followed by all seed addresses
func (r *resolver) candidateAddrs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	seen := make(map[string]struct{})
	addrs := make([]string, 0, len(r.seedAddrs)+1)

	if r.currentAddr != "" {
		addrs = append(addrs, r.currentAddr)
		seen[r.currentAddr] = struct{}{}
	}

	for _, addr := range r.seedAddrs {
		if _, exists := seen[addr]; exists {
			continue
		}
		addrs = append(addrs, addr)
		seen[addr] = struct{}{}
	}

	return addrs
}

// rememberAddr updates the current address and adds it to the seed list if
// not already present, so future discovery rounds include it.
func (r *resolver) rememberAddr(addr string) {
	if addr == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.currentAddr = addr
	r.rememberSeedLocked(addr)
}

func (r *resolver) rememberSeed(addr string) {
	if addr == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.rememberSeedLocked(addr)
}

func (r *resolver) rememberSeedLocked(addr string) {
	for _, existing := range r.seedAddrs {
		if existing == addr {
			return
		}
	}
	r.seedAddrs = append(r.seedAddrs, addr)
}

// rememberStatus extracts all known addresses from a status response (this
// node, leader, and cluster members) and adds them to the seed list.
func (r *resolver) rememberStatus(resp *pb.GetStatusResponse) {
	if resp == nil {
		return
	}

	r.rememberSeed(resp.GrpcAddress)
	for _, member := range resp.Members {
		r.rememberSeed(member.GetGrpcAddress())
	}

	switch {
	case resp.LeaderGrpcAddress != "":
		r.rememberAddr(resp.LeaderGrpcAddress)
	case resp.IsLeader && resp.GrpcAddress != "":
		r.rememberAddr(resp.GrpcAddress)
	}
}

// discoverLeader probes all candidate addresses via GetStatus RPCs,
// following leader redirects, and returns the first confirmed
// leader address. Returns an error if no leader can be found
func (r *resolver) discoverLeader(ctx context.Context) (string, error) {
	var lastErr error

	for _, addr := range r.candidateAddrs() {
		statusCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		conn, err := r.connFor(addr)
		if err != nil {
			cancel()
			lastErr = err
			continue
		}
		client := pb.NewLockServiceClient(conn)

		resp, err := client.GetStatus(statusCtx, &pb.GetStatusRequest{})
		cancel()
		if err != nil {
			lastErr = err
			if leaderAddr := leaderhint.FromError(err); leaderAddr != "" {
				r.rememberAddr(leaderAddr)
				return leaderAddr, nil
			}
			continue
		}

		r.rememberStatus(resp)
		switch {
		case resp.GetLeaderGrpcAddress() != "":
			return resp.GetLeaderGrpcAddress(), nil
		case resp.GetIsLeader() && resp.GetGrpcAddress() != "":
			return resp.GetGrpcAddress(), nil
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no reachable cluster nodes")
	}

	return "", fmt.Errorf("discover leader: %w", lastErr)
}

// attemptTimeout bounds one call to one node, so an unresponsive node, such
// as a paused process that still accepts connections, does not use up the
// caller's time. It exceeds the server's five-second Raft apply timeout.
var attemptTimeout = 6 * time.Second

// failoverBudget bounds how long callWithFailover keeps retrying when the
// caller's context has no deadline. It covers several Raft elections.
const failoverBudget = 10 * time.Second

// callWithFailover executes on SDK RPC against the cluster with automatic
// leader chasing. This function is generic so Acquire, Release, CreateLease,
// and Status all share the same retry behaviour.
//
// While no node can serve the call, typically during a Raft election when
// every node answers "not leader" without a hint, it backs off and tries
// again until ctx is done, or for failoverBudget if ctx has no deadline.
func callWithFailover[T any](
	c *Client,
	ctx context.Context,
	op func(context.Context, pb.LockServiceClient) (T, error),
) (T, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, failoverBudget)
		defer cancel()
	}

	backoff := 50 * time.Millisecond
	for {
		resp, err, retry := tryCandidates(c, ctx, op)
		if !retry {
			return resp, err
		}

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return resp, err
		case <-timer.C:
		}
		backoff = min(2*backoff, 500*time.Millisecond)
	}
}

// tryCandidates makes one pass over the known nodes, current leader first,
// following leader redirects. retry reports whether every node was
// unavailable, so a later pass may succeed.
func tryCandidates[T any](
	c *Client,
	ctx context.Context,
	op func(context.Context, pb.LockServiceClient) (T, error),
) (resp T, err error, retry bool) {
	var zero T
	var lastErr error
	queue := c.resolver.candidateAddrs()
	addressAttempts := make(map[string]int)

	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			if lastErr == nil {
				lastErr = err
			}
			return zero, lastErr, false
		}

		addr := queue[0]
		queue = queue[1:]
		if addr == "" || addressAttempts[addr] >= 2 {
			continue
		}
		addressAttempts[addr]++

		conn, err := c.resolver.connFor(addr)
		if err != nil {
			lastErr = err
			continue
		}
		client := pb.NewLockServiceClient(conn)

		attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
		resp, err := op(attemptCtx, client)
		unresponsive := attemptCtx.Err() != nil && ctx.Err() == nil
		cancel()
		if err == nil {
			c.resolver.rememberAddr(addr)
			return resp, nil, false
		}

		lastErr = err
		if leaderAddr := leaderhint.FromError(err); leaderAddr != "" {
			c.resolver.rememberAddr(leaderAddr)
			// Process the structured redirect on the very next iteration.
			queue = append([]string{leaderAddr}, queue...)
			continue
		}

		// Every Clavis call is safe to repeat on another node: acquire and
		// release are idempotent, and a duplicate lease just expires.
		if status.Code(err) != codes.Unavailable && !unresponsive {
			return zero, err, false
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("all cluster addresses failed")
	}
	return zero, lastErr, true
}
