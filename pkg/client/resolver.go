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

// callWithFailover executes on SDK RPC against the cluster with automatic
// leader chasing. This function is generic so Acquire, Release, CreateLease,
// and Status all share the same retry behaviour
func callWithFailover[T any](
	c *Client,
	ctx context.Context,
	op func(context.Context, pb.LockServiceClient) (T, error),
) (T, error) {
	var zero T
	var lastErr error
	queue := append([]string(nil), c.resolver.candidateAddrs()...)
	maxOperationAttempts := len(queue) + 3
	const maxDiscoveryRounds = 2

	addressAttempts := make(map[string]int)
	operationAttempts := 0
	discoveryRounds := 0

	for operationAttempts < maxOperationAttempts {
		if err := ctx.Err(); err != nil {
			return zero, err
		}

		if len(queue) == 0 {
			if discoveryRounds >= maxDiscoveryRounds {
				break
			}
			discoveryRounds++
			leaderAddr, err := c.resolver.discoverLeader(ctx)
			if err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return zero, ctxErr
				}
				lastErr = err
				continue
			}
			queue = append(queue, leaderAddr)
		}

		addr := queue[0]
		queue = queue[1:]
		if addr == "" {
			continue
		}
		if addressAttempts[addr] >= 2 {
			continue
		}
		addressAttempts[addr]++
		operationAttempts++

		conn, err := c.resolver.connFor(addr)
		if err != nil {
			lastErr = err
			continue
		}
		client := pb.NewLockServiceClient(conn)

		// A blackholed endpoint must not consume the caller's entire lifetime
		// and prevent later candidates from being tried. Six seconds exceeds
		// the server's five-second Raft apply timeout.
		attemptCtx, cancel := context.WithTimeout(ctx, 6*time.Second)
		resp, err := op(attemptCtx, client)
		cancel()
		if err == nil {
			c.resolver.rememberAddr(addr)
			return resp, nil
		}

		lastErr = err
		if leaderAddr := leaderhint.FromError(err); leaderAddr != "" {
			c.resolver.rememberAddr(leaderAddr)
			// Process the structured redirect on the very next iteration.
			queue = append([]string{leaderAddr}, queue...)
			continue
		}

		if status.Code(err) != codes.Unavailable {
			return zero, err
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("all cluster addresses failed")
	}

	return zero, lastErr
}
