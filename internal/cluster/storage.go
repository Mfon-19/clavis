package cluster

import (
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"os"
	"path/filepath"
)

// Storage groups the three persistence interfaces HashiCorp Raft needs:
//   - LogStore: append-only replicated command log
//   - StableStore: durable Raft metadata such as current term and votes
//   - SnapshotStore: compacted FSM snapshots used for catch-up and restart
//
// LogStore and StableStore share sone BoltDB file. Snapshots are stored in a
// sibling directory because HashiCorp Raft's file snapshot expects a directory,
// not a key/value database.
type Storage struct {
	LogStore      raft.LogStore
	StableStore   raft.StableStore
	SnapshotStore raft.SnapshotStore
}

func NewStorage(dataDir string) (*Storage, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	dbPath := filepath.Join(dataDir, "raft.db")

	boltDb, err := raftboltdb.New(raftboltdb.Options{
		Path: dbPath,
	})

	if err != nil {
		return nil, err
	}

	snapshotDir := filepath.Join(dataDir, "snapshots")
	snapShotStore, err := raft.NewFileSnapshotStore(snapshotDir, 3, os.Stderr)
	if err != nil {
		boltDb.Close()
		return nil, err
	}

	return &Storage{
		LogStore:      boltDb,
		StableStore:   boltDb,
		SnapshotStore: snapShotStore,
	}, nil
}

func (b *Storage) Close() error {
	if closer, ok := b.LogStore.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}
