package state

import (
	"github.com/Mfon-19/clavis/internal/domain"
	"github.com/hashicorp/raft"
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

func (rf *RaftFSM) Apply(log *raft.Log) any {
	// TODO: implement
	return 0
}

func (rf *RaftFSM) Snapshot() (raft.FSMSnapshot, error) {
	rf.fsm.mu.RLock()
	defer rf.fsm.mu.RUnlock()

	snapshot := &fsmSnapshot{
		Locks:          make(map[string]*domain.Lock),
		Leases:         make(map[uint64]*domain.Lease),
		Members:        make(map[string]*domain.ClusterMember),
		FencingCounter: rf.fsm.fencingCounter,
		NextLeaseID:    0,
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

// fsmSnapshot is the serialized, point-in-time FSM state used by Raft snapshots
type fsmSnapshot struct {
	Locks          map[string]*domain.Lock          `json:"locks"`
	Leases         map[uint64]*domain.Lease         `json:"leases"`
	Members        map[string]*domain.ClusterMember `json:"members"`
	FencingCounter uint64                           `json:"fencing_counter"`
	NextLeaseID    uint64                           `json:"next_lease_id"`
}

func (f fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	//TODO implement me
	panic("implement me")
}

func (f fsmSnapshot) Release() {
	//TODO implement me
	panic("implement me")
}
