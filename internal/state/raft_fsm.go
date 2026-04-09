package state

import (
	"encoding/json"
	"fmt"
	"github.com/Mfon-19/clavis/internal/domain"
	"github.com/Mfon-19/clavis/internal/raftlog"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
	"io"
)

// RaftFSM adapts the pure [FSM] to the [raft.FSM] interface required by
// HashiCorp Raft. It handles protobuf deserialization of log entries and
// JSON serialization of snapshots, keeping serialization concerns out of
// the core state machine
type RaftFSM struct {
	fsm *FSM
}

func NewRaftFSM() *RaftFSM {
	return &RaftFSM{
		fsm: NewFSM(),
	}
}

// Apply deserializes a committed Raft log entry and delegates to the pure FSM
func (rf *RaftFSM) Apply(log *raft.Log) any {
	var wrapper raftlog.CommandWrapper
	if err := proto.Unmarshal(log.Data, &wrapper); err != nil {
		return err
	}

	result, err := rf.fsm.Apply(&wrapper)
	if err != nil {
		return err
	}

	return result
}

func (rf *RaftFSM) Snapshot() (raft.FSMSnapshot, error) {
	rf.fsm.mu.RLock()
	defer rf.fsm.mu.RUnlock()

	snapshot := &fsmSnapshot{
		Locks:          make(map[string]*domain.Lock),
		Leases:         make(map[uint64]*domain.Lease),
		Members:        make(map[string]*domain.ClusterMember),
		FencingCounter: rf.fsm.fencingCounter,
		NextLeaseID:    rf.fsm.nextLeaseID,
	}

	// Deep-copy maps before returning the snapshot object. Raft may persist the
	// snapshot asynchronously while the live FSM continues to process new logs.
	for name, lock := range rf.fsm.locks {
		lockCopy := *lock
		snapshot.Locks[name] = &lockCopy
	}

	for id, lease := range rf.fsm.leases {
		leaseCopy := *lease
		snapshot.Leases[id] = &leaseCopy
	}

	for nodeID, member := range rf.fsm.members {
		memberCopy := *member
		snapshot.Members[nodeID] = &memberCopy
	}

	return snapshot, nil
}

// Restore replaces the FSM state from a JSON snapshot. This is called
// when a node falls behind and needs to catch up, or when a new node
// joins the cluster
func (rf *RaftFSM) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()

	var snap fsmSnapshot
	if err := json.NewDecoder(snapshot).Decode(&snap); err != nil {
		return err
	}

	rf.fsm.mu.Lock()
	defer rf.fsm.mu.Unlock()

	if err := validateSnapshot(&snap); err != nil {
		return err
	}

	rf.fsm.locks = snap.Locks
	rf.fsm.leases = snap.Leases
	rf.fsm.members = snap.Members
	rf.fsm.fencingCounter = snap.FencingCounter
	rf.fsm.nextLeaseID = snap.NextLeaseID

	return nil
}

func validateSnapshot(snap *fsmSnapshot) error {
	if snap.Locks == nil {
		return fmt.Errorf("snapshot locks missing")
	}
	if snap.Leases == nil {
		return fmt.Errorf("snapshot leases missing")
	}
	if snap.Members == nil {
		return fmt.Errorf("snapshot members missing")
	}

	for leaseID, lease := range snap.Leases {
		if lease == nil {
			return fmt.Errorf("snapshot lease %d is nil", leaseID)
		}
		if lease.ExpiresAtUnixNano <= 0 {
			return fmt.Errorf("snapshot lease %d has invalid expiry", leaseID)
		}
		if lease.TTL <= 0 {
			return fmt.Errorf("snapshot lease %d has invalid ttl", leaseID)
		}
	}

	for nodeID, member := range snap.Members {
		if member == nil {
			return fmt.Errorf("snapshot member %q is nil", nodeID)
		}
		if member.NodeID == "" || member.RaftAddress == "" || member.GRPCAddress == "" {
			return fmt.Errorf("snapshot member %q is incomplete", nodeID)
		}
	}

	return nil
}

func (rf *RaftFSM) GetFSM() *FSM {
	return rf.fsm
}

// fsmSnapshot is the serialized, point-in-time FSM state used by Raft snapshots
type fsmSnapshot struct {
	Locks          map[string]*domain.Lock          `json:"locks"`
	Leases         map[uint64]*domain.Lease         `json:"leases"`
	Members        map[string]*domain.ClusterMember `json:"members"`
	FencingCounter uint64                           `json:"fencing_counter"`
	NextLeaseID    uint64                           `json:"next_lease_id"`
}

// Persist writes the snapshot to Raft's sink. If encoding fails, the sink
// must be canceled so Raft does not treat the partial file as valid
func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(f); err != nil {
		sink.Cancel() // Fail snapshot on error
		return err
	}
	return sink.Close()
}

// Release is required by raft.FSMSnapshot. The snapshot owns no external
// resources so there is nothing to clean up
func (f fsmSnapshot) Release() {}
