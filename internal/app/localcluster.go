package app

import (
	"context"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"time"

	pb "github.com/Mfon-19/clavis/api/v1"
	"github.com/google/uuid"
)

// LocalCluster is a multi-node Clavis cluster running inside one process on
// loopback ports. It exists for examples and system benchmarks, which need a
// real Raft + gRPC cluster without launching separate binaries.
type LocalCluster struct {
	runtimes []*Runtime
	addrs    []string
}

// StartLocalCluster boots size nodes under dataDir, joins them into one
// cluster, and waits until every node agrees on a leader. Raft logs go to
// logOutput; pass io.Discard to silence them.
func StartLocalCluster(size int, dataDir string, logOutput io.Writer) (*LocalCluster, error) {
	c := &LocalCluster{}

	for i := 0; i < size; i++ {
		raftAddr, err := freeLoopbackAddr()
		if err != nil {
			c.Close()
			return nil, err
		}
		grpcAddr, err := freeLoopbackAddr()
		if err != nil {
			c.Close()
			return nil, err
		}

		runtime, err := NewRuntime(Config{
			NodeID:            uuid.New(),
			RaftAddr:          raftAddr,
			RaftAdvertiseAddr: raftAddr,
			GRPCAddr:          grpcAddr,
			GRPCAdvertiseAddr: grpcAddr,
			DataDir:           filepath.Join(dataDir, fmt.Sprintf("node-%d", i)),
			Bootstrap:         i == 0,
			RaftLogOutput:     logOutput,
		})
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("start node %d: %w", i, err)
		}
		_ = runtime.Start(context.Background())
		c.runtimes = append(c.runtimes, runtime)
		c.addrs = append(c.addrs, grpcAddr)

		if i == 0 {
			if _, err := c.WaitForLeader(10 * time.Second); err != nil {
				c.Close()
				return nil, err
			}
			continue
		}

		member := runtime.Member()
		if err := JoinCluster(context.Background(), c.addrs[0], &pb.JoinNodeRequest{
			NodeId:      member.NodeID,
			RaftAddress: member.RaftAddress,
			GrpcAddress: member.GRPCAddress,
		}); err != nil {
			c.Close()
			return nil, fmt.Errorf("join node %d: %w", i, err)
		}
	}

	if err := c.waitForMembers(size, 10*time.Second); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

// Addrs returns every node's client-facing gRPC address, including stopped
// nodes, so it can be passed to a client as its seed list.
func (c *LocalCluster) Addrs() []string {
	return append([]string(nil), c.addrs...)
}

// WaitForLeader returns the index of the node that is Raft leader once every
// running node knows that leader's gRPC address.
func (c *LocalCluster) WaitForLeader(timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if leader, ok := c.agreedLeader(); ok {
			return leader, nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return -1, fmt.Errorf("no leader elected within %s", timeout)
}

// StopNode crashes one node without a graceful drain, as a failing machine
// would. The node stays in the Raft configuration, so the rest of the cluster
// must elect around it.
func (c *LocalCluster) StopNode(i int) error {
	return c.runtimes[i].StopNow(context.Background())
}

// Close stops every node that is still running.
func (c *LocalCluster) Close() {
	for i := len(c.runtimes) - 1; i >= 0; i-- {
		_ = c.runtimes[i].StopNow(context.Background())
	}
}

func (c *LocalCluster) agreedLeader() (int, bool) {
	leader := -1
	for i, r := range c.runtimes {
		if r.stopped() {
			continue
		}
		if r.node.IsLeader() {
			leader = i
		}
	}
	if leader < 0 {
		return -1, false
	}

	want := c.addrs[leader]
	for _, r := range c.runtimes {
		if !r.stopped() && r.node.GetLeaderGRPCAddress() != want {
			return -1, false
		}
	}
	return leader, true
}

func (c *LocalCluster) waitForMembers(size int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if leader, ok := c.agreedLeader(); ok && len(c.runtimes[leader].node.Members()) == size {
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("cluster did not reach %d members within %s", size, timeout)
}

func freeLoopbackAddr() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("allocate loopback port: %w", err)
	}
	defer listener.Close()
	return listener.Addr().String(), nil
}
