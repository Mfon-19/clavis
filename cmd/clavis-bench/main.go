// Command clavis-bench measures Clavis end to end through the Go SDK:
//
//	latency     acquire and release latency for a single uncontended client
//	throughput  lock cycles per second with many clients on distinct locks
//	handoff     time for a contended lock to pass between waiting clients
//	sessions    lock latency as idle heartbeating sessions add Raft load
//	failover    unavailability and lost sessions when the Raft leader crashes
//	handover    time for a waiter to get a lock after its holder crashes
//
// By default each scenario runs against a fresh in-process cluster, which is
// convenient but puts every node on one machine and one disk. Pass --seeds to
// measure a real cluster instead. The failover scenario crashes nodes, so it
// only runs in-process.
//
// Usage:
//
//	go run ./cmd/clavis-bench                    # every scenario
//	go run ./cmd/clavis-bench latency failover   # a subset
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Mfon-19/clavis/internal/app"
	"github.com/Mfon-19/clavis/pkg/client"
)

type config struct {
	seeds    []string
	nodes    int
	duration time.Duration
	ttl      time.Duration
	rounds   int
	clients  []int
	sessions []int
}

type scenario struct {
	name          string
	inProcessOnly bool
	run           func(context.Context, config) error
}

var scenarios = []scenario{
	{name: "latency", run: runLatency},
	{name: "throughput", run: runThroughput},
	{name: "handoff", run: runHandoff},
	{name: "sessions", run: runSessions},
	{name: "failover", inProcessOnly: true, run: runFailover},
	{name: "handover", run: runHandover},
}

func main() {
	var (
		seeds    = flag.String("seeds", "", "comma-separated gRPC addresses of a running cluster (default: in-process cluster)")
		nodes    = flag.Int("nodes", 3, "in-process cluster size")
		duration = flag.Duration("duration", 5*time.Second, "measurement time for latency, throughput, handoff, and each sessions level")
		ttl      = flag.Duration("ttl", 10*time.Second, "lease TTL for benchmark clients")
		rounds   = flag.Int("rounds", 3, "repetitions for failover and handover")
		clients  = flag.String("clients", "1,8,64", "client counts for throughput")
		sessions = flag.String("sessions", "0,250,1000,2500", "idle session counts for sessions")
	)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: clavis-bench [flags] [scenario ...]\n\nscenarios:")
		for _, s := range scenarios {
			fmt.Fprintf(os.Stderr, " %s", s.name)
		}
		fmt.Fprintf(os.Stderr, "\n\nflags:\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	cfg := config{
		nodes:    *nodes,
		duration: *duration,
		ttl:      *ttl,
		rounds:   *rounds,
		clients:  parseInts(*clients),
		sessions: parseInts(*sessions),
	}
	if *seeds != "" {
		cfg.seeds = strings.Split(*seeds, ",")
	}

	selected := scenarios
	if flag.NArg() > 0 {
		selected = nil
		for _, name := range flag.Args() {
			s, ok := findScenario(name)
			if !ok {
				fmt.Fprintf(os.Stderr, "unknown scenario %q\n\n", name)
				flag.Usage()
				os.Exit(2)
			}
			selected = append(selected, s)
		}
	}

	// The SDK reports heartbeat retries through the standard logger. The
	// scenarios measure those effects directly, so keep the output to tables.
	log.SetOutput(io.Discard)

	target := fmt.Sprintf("in-process %d-node cluster", cfg.nodes)
	if cfg.seeds != nil {
		target = "cluster at " + strings.Join(cfg.seeds, ",")
	}
	fmt.Printf("clavis-bench: %s, lease TTL %s\n", target, cfg.ttl)

	ctx := context.Background()
	for _, s := range selected {
		fmt.Printf("\n== %s\n", s.name)
		if s.inProcessOnly && cfg.seeds != nil {
			fmt.Println("skipped: needs to crash nodes, so it only runs in-process")
			continue
		}
		if err := s.run(ctx, cfg); err != nil {
			fmt.Fprintf(os.Stderr, "%s failed: %v\n", s.name, err)
			os.Exit(1)
		}
	}
}

// target is the cluster a scenario runs against.
type target struct {
	seeds   []string
	cluster *app.LocalCluster // nil for an external cluster
	dataDir string
}

// newTarget returns the external cluster from --seeds, or boots a fresh
// in-process cluster so scenarios never see each other's leases and locks.
func newTarget(cfg config) (*target, error) {
	if cfg.seeds != nil {
		return &target{seeds: cfg.seeds}, nil
	}

	dataDir, err := os.MkdirTemp("", "clavis-bench-*")
	if err != nil {
		return nil, err
	}
	cluster, err := app.StartLocalCluster(cfg.nodes, dataDir, io.Discard)
	if err != nil {
		os.RemoveAll(dataDir)
		return nil, err
	}
	return &target{seeds: cluster.Addrs(), cluster: cluster, dataDir: dataDir}, nil
}

func (t *target) close() {
	if t.cluster != nil {
		t.cluster.Close()
		os.RemoveAll(t.dataDir)
	}
}

// startClient opens a session with the given TTL.
func startClient(ctx context.Context, seeds []string, owner string, ttl time.Duration) (*client.Client, error) {
	c, err := client.NewClientWithSeeds(seeds, owner)
	if err != nil {
		return nil, err
	}
	if err := c.Start(ctx, ttl); err != nil {
		_ = c.Stop()
		return nil, fmt.Errorf("start session %s: %w", owner, err)
	}
	return c, nil
}

// cycle acquires and immediately releases one lock, returning how long each
// half took.
func cycle(ctx context.Context, c *client.Client, lockName string) (acquire, release time.Duration, err error) {
	start := time.Now()
	lock, err := c.Acquire(ctx, lockName)
	if err != nil {
		return 0, 0, err
	}
	acquired := time.Now()
	if err := lock.Release(ctx); err != nil {
		return 0, 0, err
	}
	return acquired.Sub(start), time.Since(acquired), nil
}

func findScenario(name string) (scenario, bool) {
	for _, s := range scenarios {
		if s.name == name {
			return s, true
		}
	}
	return scenario{}, false
}

func parseInts(s string) []int {
	var out []int
	for _, part := range strings.Split(s, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n < 0 {
			fmt.Fprintf(os.Stderr, "invalid count %q\n", part)
			os.Exit(2)
		}
		out = append(out, n)
	}
	return out
}
