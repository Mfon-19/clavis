// Command postgres-job-runner shows the strongest Clavis usage pattern:
// take a distributed lock, then use the fencing token in the downstream write
// path so stale workers cannot overwrite newer work.
//
// The example keeps the store in memory so it compiles without adding a SQL
// driver dependency. The comments show the Postgres compare-and-set pattern the
// in-memory store is standing in for.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Mfon-19/clavis/pkg/client"
)

const (
	jobName  = "jobs:nightly-report"
	lockName = "jobs:nightly-report"
)

var errStaleToken = errors.New("stale fencing token rejected")

func main() {
	ctx := context.Background()

	c, err := client.NewClientWithSeeds(seedAddrs(), ownerID())
	if err != nil {
		log.Fatal(err)
	}
	defer c.Stop()

	// One lease can protect all work this process performs. The heartbeat loop
	// keeps the lease alive until Stop is called or the client invalidates.
	if err := c.Start(ctx, 15*time.Second); err != nil {
		log.Fatal(err)
	}

	// WaitAcquire is the simplest shape for a singleton scheduled job. One
	// process wins, the rest keep backing off until the lock becomes free.
	lock, err := c.WaitAcquire(ctx, lockName)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := lock.Release(ctx); err != nil {
			log.Printf("release lock: %v", err)
		}
	}()

	store := newJobStore()
	token := lock.Token()

	if err := store.claim(ctx, jobName, token); err != nil {
		log.Fatal(err)
	}

	result, err := runNightlyReport(ctx)
	if err != nil {
		log.Fatal(err)
	}

	if err := store.saveResult(ctx, jobName, token, result); err != nil {
		log.Fatal(err)
	}

	log.Printf("job %q completed with fencing token %d", jobName, token)
}

// jobStore is an in-memory stand-in for a Postgres table that tracks the
// highest fencing token accepted for a given job.
type jobStore struct {
	mu          sync.Mutex
	lastToken   map[string]uint64
	lastPayload map[string]string
}

func newJobStore() *jobStore {
	return &jobStore{
		lastToken:   make(map[string]uint64),
		lastPayload: make(map[string]string),
	}
}

func (s *jobStore) claim(ctx context.Context, job string, token uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if token <= s.lastToken[job] {
		return errStaleToken
	}
	s.lastToken[job] = token
	return nil
}

func (s *jobStore) saveResult(ctx context.Context, job string, token uint64, payload string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if token < s.lastToken[job] {
		return errStaleToken
	}
	s.lastToken[job] = token
	s.lastPayload[job] = payload
	return nil
}

func runNightlyReport(ctx context.Context) (string, error) {
	// Replace this with the real job body. The key rule is that any write to a
	// database or external system should carry the fencing token from Clavis.
	time.Sleep(250 * time.Millisecond)
	return "report-generated", nil
}

func seedAddrs() []string {
	value := os.Getenv("CLAVIS_SEEDS")
	if value == "" {
		return []string{"127.0.0.1:9000"}
	}
	return strings.Split(value, ",")
}

func ownerID() string {
	if value := os.Getenv("CLAVIS_OWNER_ID"); value != "" {
		return value
	}
	host, _ := os.Hostname()
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

/*
In a real Postgres-backed implementation, the claim/save methods would use
compare-and-set writes shaped like this:

UPDATE job_state
   SET fencing_token = $2
 WHERE job_name = $1
   AND fencing_token < $2;

UPDATE reports
   SET payload = $2,
       fencing_token = $3
 WHERE report_name = $1
   AND fencing_token < $3;

That is the point of Clavis: the lock gives exclusive ownership, and the
fencing token gives downstream systems a way to reject stale owners.
*/
