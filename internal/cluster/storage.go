package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	wal "github.com/hashicorp/raft-wal"
	"github.com/hashicorp/raft-wal/migrate"
)

const (
	walDirName        = "wal"
	legacyBoltName    = "raft.db"
	migratedBoltName  = "raft.db.migrated"
	migratingDirName  = "wal.migrating"
	snapshotsDirName  = "snapshots"
	migrateBatchBytes = 1 << 20
)

// Storage groups the persistence HashiCorp Raft needs. A write-ahead log
// serves as both the log store and the stable store (current term, votes); it
// syncs once per append, where BoltDB syncs twice. Snapshots are stored in a
// sibling directory because HashiCorp Raft's file snapshot store expects a
// directory, not a key/value database.
type Storage struct {
	WAL           *wal.WAL
	SnapshotStore raft.SnapshotStore
}

// NewStorage opens the Raft stores under dataDir, which must already exist.
// A data directory written by an older version with a BoltDB log store is
// migrated to the WAL first.
func NewStorage(dataDir string, logOutput io.Writer) (*Storage, error) {
	if err := migrateFromBolt(dataDir); err != nil {
		return nil, fmt.Errorf("migrate raft.db to wal: %w", err)
	}

	walDir := filepath.Join(dataDir, walDirName)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		return nil, err
	}
	log, err := wal.Open(walDir)
	if err != nil {
		return nil, err
	}

	snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(dataDir, snapshotsDirName), 3, logOutput)
	if err != nil {
		log.Close()
		return nil, err
	}

	return &Storage{
		WAL:           log,
		SnapshotStore: snapshotStore,
	}, nil
}

func (s *Storage) Close() error {
	return s.WAL.Close()
}

// HasRaftData reports whether dataDir already holds Raft state in either the
// current or the legacy format.
func HasRaftData(dataDir string) (bool, error) {
	for _, name := range []string{walDirName, legacyBoltName} {
		if _, err := os.Stat(filepath.Join(dataDir, name)); err == nil {
			return true, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	return false, nil
}

// migrateFromBolt copies a legacy BoltDB log and stable store into a new WAL.
//
// It is safe to interrupt at any point. The WAL is built in a scratch
// directory and renamed into place only when complete, and raft.db is kept,
// renamed to raft.db.migrated, as a backup once the WAL exists.
func migrateFromBolt(dataDir string) error {
	boltPath := filepath.Join(dataDir, legacyBoltName)
	walDir := filepath.Join(dataDir, walDirName)

	if _, err := os.Stat(boltPath); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}

	if _, err := os.Stat(walDir); errors.Is(err, os.ErrNotExist) {
		if err := copyBoltToWAL(boltPath, filepath.Join(dataDir, migratingDirName), walDir); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	return os.Rename(boltPath, filepath.Join(dataDir, migratedBoltName))
}

func copyBoltToWAL(boltPath, scratchDir, walDir string) error {
	if err := os.RemoveAll(scratchDir); err != nil {
		return err
	}
	if err := os.MkdirAll(scratchDir, 0o755); err != nil {
		return err
	}

	src, err := raftboltdb.New(raftboltdb.Options{Path: boltPath})
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := wal.Open(scratchDir)
	if err != nil {
		return err
	}

	if err := copyLogs(dst, src); err != nil {
		dst.Close()
		return err
	}
	if err := copyStable(dst, src); err != nil {
		dst.Close()
		return err
	}
	if err := dst.Close(); err != nil {
		return err
	}
	return os.Rename(scratchDir, walDir)
}

func copyLogs(dst, src raft.LogStore) error {
	last, err := src.LastIndex()
	if err != nil {
		return err
	}
	if last == 0 {
		return nil // no entries; CopyLogs cannot handle an empty store
	}
	return migrate.CopyLogs(context.Background(), dst, src, migrateBatchBytes, nil)
}

// copyStable copies the keys hashicorp/raft keeps in its stable store. It
// does not use migrate.CopyStable because that fails on a missing key, and a
// node that has never voted has no vote recorded.
func copyStable(dst, src raft.StableStore) error {
	for _, key := range []string{"CurrentTerm", "LastVoteTerm"} {
		val, err := src.GetUint64([]byte(key))
		if errors.Is(err, raftboltdb.ErrKeyNotFound) {
			continue
		} else if err != nil {
			return fmt.Errorf("read %s: %w", key, err)
		}
		if err := dst.SetUint64([]byte(key), val); err != nil {
			return fmt.Errorf("write %s: %w", key, err)
		}
	}

	val, err := src.Get([]byte("LastVoteCand"))
	if errors.Is(err, raftboltdb.ErrKeyNotFound) {
		return nil
	} else if err != nil {
		return fmt.Errorf("read LastVoteCand: %w", err)
	}
	return dst.Set([]byte("LastVoteCand"), val)
}
