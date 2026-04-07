package state

import (
	"github.com/Mfon-19/clavis/internal/domain"
	"sync"
)

type FSM struct {
	mu sync.RWMutex

	locks   map[string]*domain.Lock  // lock name -> Lock
	leases  map[string]*domain.Lease // lease ID -> Lease
	members map[string]*domain.ClusterMember

	fencingCounter uint64 // global monotonic fencing token counter
	nextLeaseID    uint64 // next leaseID to assign
}
