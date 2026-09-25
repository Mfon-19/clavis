package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/Mfon-19/clavis/pkg/client"
)

// runLatency measures one client acquiring and releasing an uncontended lock
// back to back. Each operation is a single Raft commit, so this is the floor
// every other scenario builds on.
func runLatency(ctx context.Context, cfg config) error {
	t, err := newTarget(cfg)
	if err != nil {
		return err
	}
	defer t.close()

	c, err := startClient(ctx, t.seeds, "bench-latency", cfg.ttl)
	if err != nil {
		return err
	}
	defer c.Stop()

	for i := 0; i < 20; i++ {
		if _, _, err := cycle(ctx, c, "bench/latency"); err != nil {
			return err
		}
	}

	var acquires, releases samples
	for deadline := time.Now().Add(cfg.duration); time.Now().Before(deadline); {
		acquire, release, err := cycle(ctx, c, "bench/latency")
		if err != nil {
			return err
		}
		acquires = append(acquires, acquire)
		releases = append(releases, release)
	}

	fmt.Printf("%d acquire/release cycles from one client\n", len(acquires))
	table(
		[]string{"op", "p50", "p99", "max"},
		[]string{"acquire", ms(acquires.pct(50)), ms(acquires.pct(99)), ms(acquires.max())},
		[]string{"release", ms(releases.pct(50)), ms(releases.pct(99)), ms(releases.max())},
	)
	return nil
}

// runThroughput has each client cycle its own lock as fast as it can. With
// no contention, the ceiling is how many commits Raft can batch per second.
func runThroughput(ctx context.Context, cfg config) error {
	t, err := newTarget(cfg)
	if err != nil {
		return err
	}
	defer t.close()

	rows := [][]string{{"clients", "cycles/s", "p50 cycle", "p99 cycle"}}
	for _, n := range cfg.clients {
		if n == 0 {
			continue
		}
		clients, err := startClients(ctx, t.seeds, "bench-throughput", 0, n, cfg.ttl)
		if err != nil {
			return err
		}

		var (
			mu     sync.Mutex
			cycles samples
			wg     sync.WaitGroup
			errs   = make(chan error, n)
		)
		deadline := time.Now().Add(cfg.duration)
		for i, c := range clients {
			wg.Add(1)
			go func() {
				defer wg.Done()
				lockName := fmt.Sprintf("bench/throughput/%d", i)
				var local samples
				for time.Now().Before(deadline) {
					acquire, release, err := cycle(ctx, c, lockName)
					if err != nil {
						errs <- err
						return
					}
					local = append(local, acquire+release)
				}
				mu.Lock()
				cycles = append(cycles, local...)
				mu.Unlock()
			}()
		}
		wg.Wait()
		stopAll(clients)
		close(errs)
		if err := <-errs; err != nil {
			return err
		}

		rate := float64(len(cycles)) / cfg.duration.Seconds()
		rows = append(rows, []string{strconv.Itoa(n), fmt.Sprintf("%.0f", rate), ms(cycles.pct(50)), ms(cycles.pct(99))})
	}
	table(rows...)
	return nil
}

// runHandoff has several clients compete for one lock through WaitAcquire.
// Clavis has no waiter queue, so a released lock goes to whichever client
// retries first. The handoff time and the spread of wins show what that
// costs.
func runHandoff(ctx context.Context, cfg config) error {
	const contenders = 4

	t, err := newTarget(cfg)
	if err != nil {
		return err
	}
	defer t.close()

	clients, err := startClients(ctx, t.seeds, "bench-handoff", 0, contenders, cfg.ttl)
	if err != nil {
		return err
	}
	defer stopAll(clients)

	var (
		mu          sync.Mutex
		lastRelease time.Time
		handoffs    samples
		wins        = make([]int, contenders)
		wg          sync.WaitGroup
		errs        = make(chan error, contenders)
	)
	runCtx, cancel := context.WithTimeout(ctx, cfg.duration)
	defer cancel()

	for i, c := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for runCtx.Err() == nil {
				lock, err := c.WaitAcquire(runCtx, "bench/handoff")
				if err != nil {
					if runCtx.Err() == nil {
						errs <- err
					}
					return
				}

				// A handoff runs from the moment the holder starts releasing
				// to the moment the next holder has the lock.
				mu.Lock()
				now := time.Now()
				if !lastRelease.IsZero() {
					handoffs = append(handoffs, now.Sub(lastRelease))
				}
				wins[i]++
				lastRelease = time.Now()
				mu.Unlock()

				if err := lock.Release(context.Background()); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		return err
	}

	fmt.Printf("%d handoffs between %d clients competing for one lock\n", len(handoffs), contenders)
	table(
		[]string{"p50", "p99", "max", "fewest wins", "most wins"},
		[]string{ms(handoffs.pct(50)), ms(handoffs.pct(99)), ms(handoffs.max()), strconv.Itoa(slices.Min(wins)), strconv.Itoa(slices.Max(wins))},
	)
	return nil
}

// runSessions adds idle sessions in steps and measures one client's lock
// latency at each step. Every session renews through Raft three times per
// TTL, so heartbeats alone become a steady write load on the leader.
func runSessions(ctx context.Context, cfg config) error {
	t, err := newTarget(cfg)
	if err != nil {
		return err
	}
	defer t.close()

	probe, err := startClient(ctx, t.seeds, "bench-sessions-probe", cfg.ttl)
	if err != nil {
		return err
	}
	defer probe.Stop()

	var idle []*client.Client
	defer func() { stopAll(idle) }()

	levels := slices.Clone(cfg.sessions)
	slices.Sort(levels)

	rows := [][]string{{"idle sessions", "heartbeats/s", "p50 cycle", "p99 cycle", "sessions lost"}}
	for _, level := range levels {
		if more := level - len(idle); more > 0 {
			started, err := startClients(ctx, t.seeds, "bench-idle", len(idle), more, cfg.ttl)
			if err != nil {
				return err
			}
			idle = append(idle, started...)
		}

		// Let the new sessions' heartbeats spread across a full interval so
		// the measurement sees steady-state load, not the creation burst.
		time.Sleep(cfg.ttl / 3)

		var cycles samples
		for deadline := time.Now().Add(cfg.duration); time.Now().Before(deadline); {
			acquire, release, err := cycle(ctx, probe, "bench/sessions")
			if err != nil {
				return err
			}
			cycles = append(cycles, acquire+release)
		}

		heartbeats := float64(level) * 3 / cfg.ttl.Seconds()
		rows = append(rows, []string{
			strconv.Itoa(level),
			fmt.Sprintf("%.0f", heartbeats),
			ms(cycles.pct(50)),
			ms(cycles.pct(99)),
			strconv.Itoa(countLost(idle)),
		})
	}
	table(rows...)
	return nil
}

// runFailover keeps several clients cycling their own locks, crashes the Raft
// leader, and measures how long each client waits for its next successful
// cycle. A client whose session fails closed during the election is lost.
func runFailover(ctx context.Context, cfg config) error {
	const workers = 8

	rows := [][]string{{"round", "election", "p50 recovery", "max recovery", "sessions lost"}}
	for round := 1; round <= cfg.rounds; round++ {
		t, err := newTarget(cfg)
		if err != nil {
			return err
		}
		clients, err := startClients(ctx, t.seeds, "bench-failover", 0, workers, cfg.ttl)
		if err != nil {
			t.close()
			return err
		}
		leader, err := t.cluster.WaitForLeader(10 * time.Second)
		if err != nil {
			t.close()
			return err
		}

		successes := make([][]time.Time, workers)
		runCtx, cancel := context.WithCancel(ctx)
		var wg sync.WaitGroup
		for i, c := range clients {
			wg.Add(1)
			go func() {
				defer wg.Done()
				lockName := fmt.Sprintf("bench/failover/%d", i)
				for runCtx.Err() == nil {
					_, _, err := cycle(runCtx, c, lockName)
					switch {
					case err == nil:
						successes[i] = append(successes[i], time.Now())
					case errors.Is(err, client.ErrLeaseUnavailable):
						return
					default:
						time.Sleep(10 * time.Millisecond)
					}
				}
			}()
		}

		time.Sleep(2 * time.Second)
		crashAt := time.Now()
		if err := t.cluster.StopNode(leader); err != nil {
			cancel()
			t.close()
			return err
		}
		if _, err := t.cluster.WaitForLeader(10 * time.Second); err != nil {
			cancel()
			t.close()
			return err
		}
		election := time.Since(crashAt)

		// Run for one TTL after the crash. Any session that is going to fail
		// closed because of this failover has done so by then.
		time.Sleep(cfg.ttl)
		cancel()
		wg.Wait()

		var recoveries samples
		for _, times := range successes {
			if i, _ := slices.BinarySearchFunc(times, crashAt, time.Time.Compare); i < len(times) {
				recoveries = append(recoveries, times[i].Sub(crashAt))
			}
		}
		lost := countLost(clients)
		stopAll(clients)
		t.close()

		rows = append(rows, []string{
			strconv.Itoa(round),
			ms(election),
			ms(recoveries.pct(50)),
			ms(recoveries.max()),
			fmt.Sprintf("%d/%d", lost, workers),
		})
	}
	fmt.Printf("%d clients cycling their own locks while the Raft leader crashes\n", workers)
	table(rows...)
	return nil
}

// runHandover crashes a lock holder without releasing and measures how long a
// waiting client takes to get the lock. The holder's lease expires between
// 2/3 TTL and one TTL after the crash, depending on when it last renewed;
// anything beyond that is detection overhead.
func runHandover(ctx context.Context, cfg config) error {
	// A short TTL keeps each round quick; the overhead does not depend on it.
	const ttl = 3 * time.Second

	t, err := newTarget(cfg)
	if err != nil {
		return err
	}
	defer t.close()

	var handovers samples
	for round := 0; round < cfg.rounds; round++ {
		lockName := fmt.Sprintf("bench/handover/%d", round)

		holder, err := startClient(ctx, t.seeds, fmt.Sprintf("bench-holder-%d", round), ttl)
		if err != nil {
			return err
		}
		if _, err := holder.Acquire(ctx, lockName); err != nil {
			return err
		}

		waiter, err := startClient(ctx, t.seeds, fmt.Sprintf("bench-waiter-%d", round), ttl)
		if err != nil {
			return err
		}
		acquired := make(chan error, 1)
		go func() {
			waitCtx, cancel := context.WithTimeout(ctx, 3*ttl)
			defer cancel()
			_, err := waiter.WaitAcquire(waitCtx, lockName)
			acquired <- err
		}()

		// Vary where the crash lands in the holder's heartbeat cycle.
		time.Sleep(500*time.Millisecond + time.Duration(round)*ttl/7)
		crashAt := time.Now()
		_ = holder.Stop()

		if err := <-acquired; err != nil {
			return fmt.Errorf("waiter never acquired the lock: %w", err)
		}
		handovers = append(handovers, time.Since(crashAt))
		_ = waiter.Stop()
	}

	fmt.Printf("lease TTL %s: the lease expires %s-%s after the crash, the rest is overhead\n", ttl, ttl*2/3, ttl)
	table(
		[]string{"rounds", "min", "p50", "max"},
		[]string{strconv.Itoa(cfg.rounds), ms(handovers.min()), ms(handovers.pct(50)), ms(handovers.max())},
	)
	return nil
}

// startClients opens n sessions in parallel, named prefix-from through
// prefix-(from+n-1).
func startClients(ctx context.Context, seeds []string, prefix string, from, n int, ttl time.Duration) ([]*client.Client, error) {
	clients := make([]*client.Client, n)
	errs := make([]error, n)
	sem := make(chan struct{}, 32)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			clients[i], errs[i] = startClient(ctx, seeds, fmt.Sprintf("%s-%d", prefix, from+i), ttl)
		}()
	}
	wg.Wait()

	if err := errors.Join(errs...); err != nil {
		stopAll(clients)
		return nil, err
	}
	return clients, nil
}

func stopAll(clients []*client.Client) {
	for _, c := range clients {
		if c != nil {
			_ = c.Stop()
		}
	}
}

// countLost returns how many sessions have failed closed.
func countLost(clients []*client.Client) int {
	lost := 0
	for _, c := range clients {
		select {
		case <-c.Done():
			lost++
		default:
		}
	}
	return lost
}
