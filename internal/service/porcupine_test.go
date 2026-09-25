// This file contains linearizability tests using the Porcupine checker. Each
// test records a concurrent history of acquire/release operations and verifies
// that the observed results are consistent with some sequential ordering of the
// lock state machine
package service

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Mfon-19/clavis/internal/cluster"
	"github.com/Mfon-19/clavis/internal/domain"
	"github.com/Mfon-19/clavis/internal/state"
	"github.com/anishathalye/porcupine"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// lockOp is the input to the linearizability model. An acquire or release
// from a specific client
type lockOp struct {
	Kind     string
	ClientID int
}

// lockResult is the output observed for each operation. Success with a fencing
// token, busy (lock held by another client), or released
type lockResult struct {
	Kind  string
	Token uint64
}

// lockState is the sequential specification. Who holds the lock and the
// current fencing token. HeldBy is -1 when the lock is free
type lockState struct {
	Held   bool
	HeldBy int
	Token  uint64
}

// historyRecorder collects completed operations with monotonic timestamps
type historyRecorder struct {
	mu    sync.Mutex
	start time.Time
	ops   []porcupine.Operation
}

func newHistoryRecorder() historyRecorder {
	return historyRecorder{start: time.Now()}
}

func (r *historyRecorder) sinceStart() int64 {
	return time.Since(r.start).Nanoseconds()
}

func (r *historyRecorder) record(clientID int, input lockOp, callAt int64, output lockResult, returnAt int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ops = append(r.ops, porcupine.Operation{
		ClientId: clientID,
		Input:    input,
		Call:     callAt,
		Output:   output,
		Return:   returnAt,
	})
}

func (r *historyRecorder) snapshot() []porcupine.Operation {
	r.mu.Lock()
	defer r.mu.Unlock()

	ops := make([]porcupine.Operation, len(r.ops))
	copy(ops, r.ops)
	return ops
}

// TestLinearizabilityWithFailover runs concurrent acquire/release cycles and kills
// the leader mid-test to verify that lock operations remain linearizable
// across a leader election.
func TestLinearizabilityWithFailover(t *testing.T) {
	nodes, leader := newPorcupineCluster(t, 3)
	defer shutDownNodes(t, nodes)

	const (
		clients = 3
		phases  = 8
		lock    = "porcupine:failover-lock"
	)

	svc := NewService(leader)
	owners := make([]string, clients)
	leases := make([]uint64, clients)
	for i := 0; i < clients; i++ {
		owners[i] = fmt.Sprintf("client-%d", i)
		// Longer ttl to account for leader election
		resp, err := svc.CreateLease(owners[i], 120)
		require.NoError(t, err)
		leases[i] = resp.LeaseID
	}

	recorder := newHistoryRecorder()
	failoverStarted := make(chan struct{})
	failoverErrCh := make(chan error, 1)

	go func(oldLeader *cluster.Node) {
		<-failoverStarted
		time.Sleep(5 * time.Millisecond)

		if err := oldLeader.Shutdown(); err != nil {
			failoverErrCh <- fmt.Errorf("shutdown leader: %w", err)
			return
		}

		if _, err := waitForLeaderService(nodes, 10*time.Second); err != nil {
			failoverErrCh <- fmt.Errorf("wait for new leader: %w", err)
			return
		}

		failoverErrCh <- nil
	}(leader)

	for phase := 0; phase < phases; phase++ {
		start := make(chan struct{})
		var wg sync.WaitGroup
		errCh := make(chan error, clients)

		for i := 0; i < clients; i++ {
			wg.Add(1)
			go func(clientID int) {
				defer wg.Done()
				<-start

				input := lockOp{Kind: "acquire", ClientID: clientID}
				callAt := recorder.sinceStart()
				resp, busy, err := acquireWithLeaderRetry(nodes, lock, owners[clientID], leases[clientID], 10*time.Second)
				returnAt := recorder.sinceStart()
				if err != nil {
					errCh <- fmt.Errorf("acquire client %d phase %d: %w", clientID, phase, err)
					return
				}
				if busy {
					recorder.record(clientID, input, callAt, lockResult{Kind: "busy"}, returnAt)
					return
				}

				recorder.record(clientID, input, callAt, lockResult{
					Kind:  "ok",
					Token: resp.FencingToken,
				}, returnAt)

				time.Sleep(15 * time.Millisecond)

				releaseInput := lockOp{Kind: "release", ClientID: clientID}
				releaseCallAt := recorder.sinceStart()
				err = releaseWithLeaderRetry(nodes, lock, leases[clientID], 10*time.Second)
				releaseReturnAt := recorder.sinceStart()
				if err != nil {
					errCh <- fmt.Errorf("release client %d phase %d: %w", clientID, phase, err)
					return
				}

				recorder.record(clientID, releaseInput, releaseCallAt, lockResult{Kind: "released"}, releaseReturnAt)
			}(i)
		}

		close(start)
		if phase == 1 {
			close(failoverStarted)
		}

		wg.Wait()
		close(errCh)
		for err := range errCh {
			require.NoError(t, err)
		}
	}

	require.NoError(t, <-failoverErrCh)
	assertLinearizable(t, recorder.snapshot())
}

// lockLinearizabilityModel defines the sequential specification of a single lock.
// The Step function returns true only if the observed output is consistent with
// the current state, enforcing mutual exclusion and monotonically increasing
// fencing tokens
func lockLinearizabilityModel() porcupine.Model {
	return porcupine.Model{
		Init: func() interface{} {
			return lockState{HeldBy: -1}
		},
		Step: func(state interface{}, input interface{}, output interface{}) (bool, interface{}) {
			s := state.(lockState)
			in := input.(lockOp)
			out := output.(lockResult)

			switch in.Kind {
			case "acquire":
				switch out.Kind {
				case "busy":
					// Tried to acquire the lock, but failed. Surely it is held
					// by someone else.
					return s.Held && s.HeldBy != in.ClientID, s
				case "ok":
					if !s.Held {
						// Lock was free, this acquire should have advanced the
						// token by exactly one
						if out.Token != s.Token+1 {
							return false, s
						}
						// Successful acquisition
						return true, lockState{
							Held:   true,
							HeldBy: in.ClientID,
							Token:  out.Token,
						}
					}
					// Is held by whoever requested it, and the output Token
					// doesn't change (idempotency)
					if s.HeldBy == in.ClientID && out.Token == s.Token {
						return true, s
					}
					return false, s
				default:
					return false, s
				}

			case "release":
				// Tried to release, output should reflect that
				if out.Kind != "released" {
					return false, s
				}
				// The lock was never held, or not by the client calling release
				if !s.Held || s.HeldBy != in.ClientID {
					return false, s
				}
				return true, lockState{
					Held:   false,
					HeldBy: -1,
					Token:  s.Token,
				}
			default:
				return false, s
			}
		},
		DescribeOperation: func(input interface{}, output interface{}) string {
			in := input.(lockOp)
			out := output.(lockResult)
			switch in.Kind {
			case "acquire":
				if out.Kind == "ok" {
					return fmt.Sprintf("Acquire(c=%d) -> ok(token=%d)", in.ClientID, out.Token)
				}
				return fmt.Sprintf("Acquire(c=%d) -> %s", in.ClientID, out.Kind)
			case "release":
				return fmt.Sprintf("Release(c=%d) -> %s", in.ClientID, out.Kind)
			default:
				return fmt.Sprintf("%s(c=%d)", in.Kind, in.ClientID)
			}
		},
		DescribeState: func(state interface{}) string {
			s := state.(lockState)
			if !s.Held {
				return fmt.Sprintf("{free token=%d}", s.Token)
			}
			return fmt.Sprintf("{heldBy=%d token=%d}", s.HeldBy, s.Token)
		},
	}
}

func assertLinearizable(t *testing.T, history []porcupine.Operation) {
	t.Helper()

	result, info := porcupine.CheckOperationsVerbose(lockLinearizabilityModel(), history, 0)
	visualizationPath := filepath.Join("tmp", t.Name()+".html")
	require.NoError(t, os.MkdirAll(filepath.Dir(visualizationPath), 0o755))
	require.NoError(t, porcupine.VisualizePath(lockLinearizabilityModel(), info, visualizationPath))
	if result == porcupine.Ok {
		return
	}

	switch result {
	case porcupine.Illegal:
		t.Fatalf("history is not linearizable. visualization written to %s", visualizationPath)
	case porcupine.Unknown:
		t.Fatalf("linearizability check timed out. visualization written to %s", visualizationPath)
	default:
		t.Fatalf("unexpected linearizability result %q. visualization written to %s", result, visualizationPath)
	}
}

// newPorcupineCluster bootstraps an in-process Raft cluster of the given size
// and waits for a stable leader. Returns all nodes and the leader
func newPorcupineCluster(t *testing.T, size int) ([]*cluster.Node, *cluster.Node) {
	t.Helper()
	require.GreaterOrEqual(t, size, 1)

	leaderBindAddr := freeBindAddr(t)
	leaderCfg := &cluster.Config{
		NodeID:            uuid.New(),
		BindAddr:          leaderBindAddr,
		DataDir:           t.TempDir(),
		Bootstrap:         true,
		RaftAdvertiseAddr: leaderBindAddr,
		GRPCAdvertiseAddr: freeBindAddr(t),
	}

	leader, err := cluster.NewNode(leaderCfg)
	require.NoError(t, err)

	nodes := []*cluster.Node{leader}
	require.NoError(t, leader.WaitForLeader(5*time.Second))

	for i := 1; i < size; i++ {
		bindAddr := freeBindAddr(t)
		cfg := &cluster.Config{
			NodeID:            uuid.New(),
			BindAddr:          bindAddr,
			DataDir:           t.TempDir(),
			Bootstrap:         false,
			RaftAdvertiseAddr: bindAddr,
			GRPCAdvertiseAddr: freeBindAddr(t),
		}

		node, err := cluster.NewNode(cfg)
		require.NoError(t, err)
		nodes = append(nodes, node)

		require.NoError(t, leader.AddClusterMember(node.SelfMember()))
	}

	require.Eventually(t, func() bool {
		leaders := 0
		for _, node := range nodes {
			if node.IsLeader() {
				leader = node
				leaders++
			}
		}
		return leaders == 1
	}, 5*time.Second, 100*time.Millisecond)

	return nodes, leader
}

func shutDownNodes(t *testing.T, nodes []*cluster.Node) {
	t.Helper()
	for _, node := range nodes {
		require.NoError(t, node.Shutdown())
	}
}

// waitForLeaderService polls the node list until exactly one leader exists
// and returns a Service bound to it
func waitForLeaderService(nodes []*cluster.Node, timeout time.Duration) (*Service, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leaders := 0
		var service *Service
		for _, node := range nodes {
			if node.IsLeader() {
				leaders++
				service = NewService(node)
			}
		}
		if leaders == 1 {
			return service, nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return nil, fmt.Errorf("no stable leader within %s", timeout)
}

// acquireWithLeaderRetry retries AcquireLock across leader elections until it succeeds,
// gets a "busy" response, or times out
func acquireWithLeaderRetry(nodes []*cluster.Node, lockName, owner string, leaseID uint64, timeout time.Duration) (state.AcquireLockResponse, bool, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		service, err := waitForLeaderService(nodes, 250*time.Millisecond)
		if err != nil {
			continue
		}

		resp, err := service.AcquireLock(context.Background(), lockName, owner, leaseID, false)
		switch {
		case err == nil:
			return resp, false, nil
		case errors.Is(err, domain.ErrLockAlreadyHeld):
			return state.AcquireLockResponse{}, true, nil
		default:
			time.Sleep(25 * time.Millisecond)
		}
	}

	return state.AcquireLockResponse{}, false, fmt.Errorf("acquire time out waiting for stable leader")
}

// releaseWithLeaderRetry retries ReleaseLock across leader elections  until it
// succeeds or times out
func releaseWithLeaderRetry(nodes []*cluster.Node, lockName string, leaseID uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		service, err := waitForLeaderService(nodes, 250*time.Millisecond)
		if err != nil {
			continue
		}

		if err := service.ReleaseLock(lockName, leaseID); err == nil {
			return nil
		}

		time.Sleep(25 * time.Millisecond)
	}

	return fmt.Errorf("release timed out waiting for stable leader")
}

// freeBindAddr allocates an ephemeral TCP port and returns its address.
func freeBindAddr(t testing.TB) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate free port: %v", err)
	}
	defer listener.Close()

	return listener.Addr().String()
}
