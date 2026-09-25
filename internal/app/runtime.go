// Package app is the composition root that wires together the cluster node,
// service layer, and gRPC server into a single [Runtime]
package app

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"

	pb "github.com/Mfon-19/clavis/api/v1"
	"github.com/Mfon-19/clavis/internal/cluster"
	"github.com/Mfon-19/clavis/internal/domain"
	"github.com/Mfon-19/clavis/internal/service"
	"github.com/Mfon-19/clavis/internal/transport/grpcserver"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

// Config holds the top-level runtime configuration after the CLI validation
// and advertise=address derivation. Runtime code expects all advertise
// addresses to already be routable; wildcard listen addresses should be
// rejected earlier
type Config struct {
	NodeID            uuid.UUID
	RaftAddr          string
	RaftAdvertiseAddr string
	GRPCAddr          string
	GRPCAdvertiseAddr string
	DataDir           string
	Bootstrap         bool
	RaftLogOutput     io.Writer
}

// Runtime is the composition root. It owns the Raft node and gRPC server,
// and manages their lifecycle
type Runtime struct {
	node         *cluster.Node
	grpcServer   *grpc.Server
	grpcListener net.Listener
	stopOnce     sync.Once
	stopErr      error
	isStopped    atomic.Bool
}

// NewRuntime creates a Runtime by wiring the dependency chain in one direction:
//
//	Raft node -> service layer -> gRPC server
//
// Keeping construction here prevents lower layers from importing transport or
// CLI packages, which keeps tests cheap and avoids circular dependencies
func NewRuntime(cfg Config) (*Runtime, error) {
	node, err := cluster.NewNode(&cluster.Config{
		NodeID:            cfg.NodeID,
		BindAddr:          cfg.RaftAddr,
		RaftAdvertiseAddr: cfg.RaftAdvertiseAddr,
		DataDir:           cfg.DataDir,
		Bootstrap:         cfg.Bootstrap,
		GRPCAdvertiseAddr: cfg.GRPCAdvertiseAddr,
		LogOutput:         cfg.RaftLogOutput,
	})
	if err != nil {
		return nil, fmt.Errorf("create raft node: %w", err)
	}

	listener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		_ = node.Shutdown()
		return nil, fmt.Errorf("listen on %s: %w", cfg.GRPCAddr, err)
	}

	appService := service.NewService(node)
	grpcSrv := grpc.NewServer()
	grpcHandler := grpcserver.NewServer(appService)
	pb.RegisterLockServiceServer(grpcSrv, grpcHandler)
	pb.RegisterAdminServiceServer(grpcSrv, grpcHandler)

	return &Runtime{
		node:         node,
		grpcServer:   grpcSrv,
		grpcListener: listener,
	}, nil
}

// Start launches the gRPC server as a background goroutine and returns a
// channel for fatal serve errors. The Raft node starts its own background
// loops during cluster.NewNode construction
func (r *Runtime) Start(_ context.Context) <-chan error {
	errCh := make(chan error, 1)

	go func() {
		if err := r.grpcServer.Serve(r.grpcListener); err != nil {
			errCh <- fmt.Errorf("gRPC server failed: %w", err)
		}
	}()

	return errCh
}

// Stop gracefully shuts down in order: gRPC -> Raft. Idempotent
func (r *Runtime) Stop(ctx context.Context) error {
	r.stopOnce.Do(func() {
		r.isStopped.Store(true)
		gracefulDone := make(chan struct{})
		go func() {
			r.grpcServer.GracefulStop()
			close(gracefulDone)
		}()

		select {
		case <-gracefulDone:
		case <-ctx.Done():
			// Long-lived heartbeat streams can otherwise keep GracefulStop
			// blocked forever. Force-close them when the caller's deadline
			// expires, then continue shutting down Raft and storage.
			r.grpcServer.Stop()
			<-gracefulDone
			r.stopErr = ctx.Err()
		}

		if err := r.node.Shutdown(); err != nil && r.stopErr == nil {
			r.stopErr = err
		}
	})
	return r.stopErr
}

// StopNow force-stops the gRPC server before shutting down Raft. This is
// useful for tests and benchmarks that need crash-like behavior instead of a
// graceful drain of long-lived streams such as Heartbeat.
func (r *Runtime) StopNow(ctx context.Context) error {
	r.stopOnce.Do(func() {
		r.isStopped.Store(true)
		r.grpcServer.Stop()
		r.stopErr = r.node.Shutdown()
	})
	return r.stopErr
}

func (r *Runtime) stopped() bool {
	return r.isStopped.Load()
}

func (r *Runtime) Member() domain.ClusterMember {
	return r.node.SelfMember()
}

func isWildcardHost(host string) bool {
	switch host {
	case "", "0.0.0.0", "::":
		return true
	default:
		return false
	}
}

func validateAdvertiseAddr(addr, kind string) (string, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("parse %s address: %w", kind, err)
	}
	if isWildcardHost(host) {
		return "", fmt.Errorf("%s address must be explicitly routable, got %q", kind, addr)
	}
	return addr, nil
}

// DeriveAdvertiseAddr computes a routable gRPC advertise address. Listen
// addresses such as ":9000" and "0.0.0.0:9000" are valid for binding, but they
// are not valid for other machines to dial. This function derives or validates
// the address clients and other nodes should actually use.
func DeriveAdvertiseAddr(raftAddr, listenAddr, explicit string) (string, error) {
	if explicit != "" {
		return validateAdvertiseAddr(explicit, "advertise")
	}

	raftHost, _, err := net.SplitHostPort(raftAddr)
	if err != nil {
		return "", fmt.Errorf("parse raft address: %w", err)
	}

	listenHost, listenPort, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return "", fmt.Errorf("parse listen address: %w", err)
	}

	switch listenHost {
	case "", "0.0.0.0", "::":
		if isWildcardHost(raftHost) {
			return "", fmt.Errorf("cannot derive advertise address from wildcard hosts; set an explicit advertise address")
		}
		listenHost = raftHost
	}

	return validateAdvertiseAddr(net.JoinHostPort(listenHost, listenPort), "advertise")
}

// DeriveRaftAdvertiseAddr computes a routable Raft advertise address. If the
// bind address is a wildcard, it falls back to the host from the gRPC
// advertise address.
func DeriveRaftAdvertiseAddr(bindAddr, explicit, fallbackHostAddr string) (string, error) {
	if explicit != "" {
		return validateAdvertiseAddr(explicit, "raft advertise")
	}

	bindHost, bindPort, err := net.SplitHostPort(bindAddr)
	if err != nil {
		return "", fmt.Errorf("parse raft bind address: %w", err)
	}

	if isWildcardHost(bindHost) {
		if fallbackHostAddr == "" {
			return "", fmt.Errorf("raft bind address %q is not routable; set --raft-advertise-addr or --grpc-advertise-addr", bindAddr)
		}

		fallbackHost, _, err := net.SplitHostPort(fallbackHostAddr)
		if err != nil {
			return "", fmt.Errorf("parse raft advertise fallback address: %w", err)
		}
		if isWildcardHost(fallbackHost) {
			return "", fmt.Errorf("raft advertise fallback address %q is not routable; set an explicit advertise address", fallbackHostAddr)
		}
		bindHost = fallbackHost
	}

	return validateAdvertiseAddr(net.JoinHostPort(bindHost, bindPort), "raft advertise")
}
