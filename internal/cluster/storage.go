package cluster

import (
	"io"
	"path/filepath"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// Storage groups the persistence HashiCorp Raft needs. A single BoltDB file
// serves as both the append-only log store and the stable store (current term,
// votes). Snapshots are stored in a sibling directory because HashiCorp Raft's
// file snapshot store expects a directory, not a key/value database.
type Storage struct {
	Bolt          *raftboltdb.BoltStore
	SnapshotStore raft.SnapshotStore
}

// NewStorage opens the Raft stores under dataDir, which must already exist.
func NewStorage(dataDir string, logOutput io.Writer) (*Storage, error) {
	bolt, err := raftboltdb.New(raftboltdb.Options{
		Path: filepath.Join(dataDir, "raft.db"),
	})
	if err != nil {
		return nil, err
	}

	snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(dataDir, "snapshots"), 3, logOutput)
	if err != nil {
		bolt.Close()
		return nil, err
	}

	return &Storage{
		Bolt:          bolt,
		SnapshotStore: snapshotStore,
	}, nil
}

func (s *Storage) Close() error {
	return s.Bolt.Close()
}
