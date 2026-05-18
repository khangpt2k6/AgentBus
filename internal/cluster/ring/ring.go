// Package ring implements a consistent hash ring with virtual nodes for
// session-keyed shard routing. It is read-mostly: AddNode/RemoveNode are
// called when cluster membership changes; LeaderFor is called on every
// publish to figure out which node owns a given shard.
//
// Concurrency: AddNode/RemoveNode take a write lock; LeaderFor takes a
// read lock. The ring uses sort.Search over a sorted []uint32 of virtual
// node positions, so LeaderFor is O(log V) where V = vnodes * nodeCount.
package ring

import (
	"encoding/binary"
	"hash/fnv"
	"sort"
	"sync"
)

// Ring is the consistent hash ring.
type Ring struct {
	vnodes int

	mu        sync.RWMutex
	positions []uint32          // sorted virtual-node positions
	owners    map[uint32]string // position -> nodeID
	nodes     map[string]struct{}
}

// New returns an empty ring whose physical nodes each get vnodes virtual
// nodes when added.
func New(vnodes int) *Ring {
	if vnodes <= 0 {
		vnodes = 128
	}
	return &Ring{
		vnodes: vnodes,
		owners: make(map[uint32]string),
		nodes:  make(map[string]struct{}),
	}
}

// AddNode registers a physical node. Idempotent.
func (r *Ring) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[nodeID]; ok {
		return
	}
	r.nodes[nodeID] = struct{}{}
	for i := 0; i < r.vnodes; i++ {
		pos := hashVNode(nodeID, uint32(i))
		r.owners[pos] = nodeID
	}
	r.rebuildPositions()
}

// RemoveNode unregisters a physical node and all its virtual nodes.
func (r *Ring) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[nodeID]; !ok {
		return
	}
	delete(r.nodes, nodeID)
	for pos, owner := range r.owners {
		if owner == nodeID {
			delete(r.owners, pos)
		}
	}
	r.rebuildPositions()
}

// LeaderFor returns the nodeID that owns the given shard. Empty if no
// nodes are registered.
func (r *Ring) LeaderFor(shardID uint32) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.positions) == 0 {
		return ""
	}
	key := hashVNode("shard", shardID)
	idx := sort.Search(len(r.positions), func(i int) bool {
		return r.positions[i] >= key
	})
	if idx == len(r.positions) {
		idx = 0
	}
	return r.owners[r.positions[idx]]
}

// Members returns the registered nodeIDs in sorted order.
func (r *Ring) Members() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.nodes))
	for n := range r.nodes {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func (r *Ring) rebuildPositions() {
	r.positions = r.positions[:0]
	for p := range r.owners {
		r.positions = append(r.positions, p)
	}
	sort.Slice(r.positions, func(i, j int) bool {
		return r.positions[i] < r.positions[j]
	})
}

// hashVNode hashes a (domain, seed) pair into a uint32 ring position.
// Writing the seed as 4 bytes before the domain string ensures that virtual
// nodes for the same node ID land at well-spread positions rather than
// clustering around a single region of the ring.
func hashVNode(domain string, seed uint32) uint32 {
	h := fnv.New32a()
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], seed)
	_, _ = h.Write(buf[:])
	_, _ = h.Write([]byte(domain))
	return h.Sum32()
}
