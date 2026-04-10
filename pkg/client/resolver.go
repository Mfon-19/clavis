package client

import (
	"google.golang.org/grpc"
	"sync"
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
