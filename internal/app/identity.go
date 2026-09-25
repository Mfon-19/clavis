package app

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

const nodeIDFileName = "node-id"

// ResolveNodeID returns the stable identity for a data directory. Generated
// IDs are persisted before Raft starts so a restart cannot silently use a new
// ServerID against an existing configuration.
func ResolveNodeID(dataDir, requested string) (uuid.UUID, bool, error) {
	if dataDir == "" {
		return uuid.Nil, false, fmt.Errorf("data directory is required")
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return uuid.Nil, false, fmt.Errorf("create data directory: %w", err)
	}

	idPath := filepath.Join(dataDir, nodeIDFileName)
	storedID, stored, err := readNodeID(idPath)
	if err != nil {
		return uuid.Nil, false, err
	}

	var requestedID uuid.UUID
	if requested != "" {
		requestedID, err = uuid.Parse(requested)
		if err != nil {
			return uuid.Nil, false, fmt.Errorf("invalid node id: %w", err)
		}
		if requestedID == uuid.Nil {
			return uuid.Nil, false, fmt.Errorf("invalid node id: nil UUID is not allowed")
		}
	}

	if stored {
		if requested != "" && requestedID != storedID {
			return uuid.Nil, false, fmt.Errorf(
				"node id %s does not match persisted node id %s in %s",
				requestedID,
				storedID,
				idPath,
			)
		}
		return storedID, false, nil
	}

	generated := requested == ""
	selectedID := requestedID
	if generated {
		raftDBPath := filepath.Join(dataDir, "raft.db")
		if _, statErr := os.Stat(raftDBPath); statErr == nil {
			return uuid.Nil, false, fmt.Errorf(
				"existing Raft data has no persisted node id; restart with --node-id to establish its original identity",
			)
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return uuid.Nil, false, fmt.Errorf("inspect existing Raft data: %w", statErr)
		}
		selectedID = uuid.New()
	}

	if err := persistNodeID(idPath, selectedID); err != nil {
		return uuid.Nil, false, err
	}
	return selectedID, generated, nil
}

func readNodeID(path string) (uuid.UUID, bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return uuid.Nil, false, nil
	}
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("read persisted node id: %w", err)
	}

	id, err := uuid.Parse(strings.TrimSpace(string(data)))
	if err != nil || id == uuid.Nil {
		return uuid.Nil, false, fmt.Errorf("persisted node id in %s is invalid", path)
	}
	return id, true, nil
}

func persistNodeID(path string, id uuid.UUID) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("persist node id: %w", err)
	}

	_, writeErr := fmt.Fprintln(file, id.String())
	closeErr := file.Close()
	if writeErr != nil {
		return fmt.Errorf("persist node id: %w", writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("persist node id: %w", closeErr)
	}
	return nil
}
