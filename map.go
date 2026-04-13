package node

import (
	"context"
	"sync"
	"time"

	"github.com/nikitakosatka/hive/pkg/hive"
)

// Version is a logical LWW version for one key.
// Ordering is lexicographic: (Counter, NodeID).
type Version struct {
	Counter uint64
	NodeID  string
}

// StateEntry stores one OR-Map key state.
type StateEntry struct {
	Value     string
	Tombstone bool
	Version   Version
}

// MapState is an exported snapshot representation used by Merge.
type MapState map[string]StateEntry

// CRDTMapNode is a state-based OR-Map with LWW values.
type CRDTMapNode struct {
	*hive.BaseNode

	mu         sync.RWMutex
	state      map[string]StateEntry
	counter    uint64
	nodeID     string
	allNodeIDs []string

	cancel context.CancelFunc
}

// NewCRDTMapNode creates a CRDT map node for the provided peer set.
func NewCRDTMapNode(id string, allNodeIDs []string) *CRDTMapNode {
	return &CRDTMapNode{
		BaseNode:   hive.NewBaseNode(id),
		state:      make(map[string]StateEntry),
		counter:    0,
		nodeID:     id,
		allNodeIDs: allNodeIDs,
	}
}

func (n *CRDTMapNode) syncLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stateCopy := n.State()
			for _, peerID := range n.allNodeIDs {
				if peerID == n.nodeID {
					continue
				}

				_ = n.Send(peerID, stateCopy)
			}
		}
	}
}

// Start starts message processing and anti-entropy broadcast (flood/gossip).
func (n *CRDTMapNode) Start(ctx context.Context) error {

	if err := n.BaseNode.Start(ctx); err != nil {
		return err
	}

	syncCtx, cancel := context.WithCancel(ctx)
	n.cancel = cancel

	go n.syncLoop(syncCtx)
	return nil
}

// Put writes a value with a fresh local version.
func (n *CRDTMapNode) Put(k, v string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.counter++
	newVer := Version{Counter: n.counter, NodeID: n.nodeID}
	n.state[k] = StateEntry{
		Value:     v,
		Tombstone: false,
		Version:   newVer,
	}
}

// Get returns the current visible value for key k.
func (n *CRDTMapNode) Get(k string) (string, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	entry, ok := n.state[k]
	if !ok || entry.Tombstone {
		return "", false
	}
	return entry.Value, true
}

// Delete marks the key as removed via a tombstone.
func (n *CRDTMapNode) Delete(k string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.counter++
	newVer := Version{Counter: n.counter, NodeID: n.nodeID}
	n.state[k] = StateEntry{
		Value:     "",
		Tombstone: true,
		Version:   newVer,
	}
}

// Merge joins local state with a remote state snapshot.
func (n *CRDTMapNode) Merge(remote MapState) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for key, remoteEntry := range remote {
		localEntry, exists := n.state[key]
		flag := false
		if localEntry.Version.Counter != remoteEntry.Version.Counter {
			flag = localEntry.Version.Counter < remoteEntry.Version.Counter
		} else {
			flag = localEntry.Version.NodeID < remoteEntry.Version.NodeID
		}

		if !exists || flag {
			n.state[key] = remoteEntry
		}
	}
}

// State returns a copy of the full CRDT state.
func (n *CRDTMapNode) State() MapState {
	n.mu.RLock()
	defer n.mu.RUnlock()

	cpy := make(MapState, len(n.state))
	for k, v := range n.state {
		cpy[k] = v
	}
	return cpy
}

// ToMap returns a value-only map view without tombstones.
func (n *CRDTMapNode) ToMap() map[string]string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	result := make(map[string]string, len(n.state))
	for k, entry := range n.state {
		if !entry.Tombstone {
			result[k] = entry.Value
		}
	}
	return result
}

// Receive applies remote state snapshots.
func (n *CRDTMapNode) Receive(msg *hive.Message) error {
	remoteState, ok := msg.Payload.(MapState)
	if !ok {
		// Ignore malformed messages
		return nil
	}
	n.Merge(remoteState)
	return nil
}
