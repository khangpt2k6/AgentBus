package shardwal

import (
	"context"
	"sync"
)

// HighWaterMark tracks per-replica ack offsets and computes the cluster-
// wide low-watermark (min across replicas, which is the durably committed
// offset under quorum semantics with an "ack from all alive replicas"
// policy). For RF=N + acks=quorum, this returns the position at or below
// which we have quorum durability.
type HighWaterMark struct {
	mu      sync.Mutex
	cond    *sync.Cond
	selfID  string
	offsets map[string]uint64
	current uint64
}

// NewHWM creates a tracker, with selfID always considered a replica.
func NewHWM(selfID string) *HighWaterMark {
	h := &HighWaterMark{
		selfID:  selfID,
		offsets: map[string]uint64{selfID: 0},
	}
	h.cond = sync.NewCond(&h.mu)
	return h
}

// SetReplicas declares the current set of replicas. Replicas not in the
// list are removed from tracking (used when ISR shrinks/grows). selfID
// is always implicitly included.
func (h *HighWaterMark) SetReplicas(ids []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	want := map[string]struct{}{h.selfID: {}}
	for _, id := range ids {
		want[id] = struct{}{}
	}
	for id := range h.offsets {
		if _, ok := want[id]; !ok {
			delete(h.offsets, id)
		}
	}
	for id := range want {
		if _, ok := h.offsets[id]; !ok {
			h.offsets[id] = 0
		}
	}
	h.recomputeLocked()
	h.cond.Broadcast()
}

// Update sets replicaID's ack to offset. Monotonic per replica.
func (h *HighWaterMark) Update(replicaID string, offset uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	cur, ok := h.offsets[replicaID]
	if !ok {
		// Replica not currently in the set; ignore.
		return
	}
	if offset > cur {
		h.offsets[replicaID] = offset
		h.recomputeLocked()
		h.cond.Broadcast()
	}
}

// Mark returns the current high-water-mark (= min across replicas).
func (h *HighWaterMark) Mark() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.current
}

// WaitFor blocks until Mark() >= offset or ctx is done.
func (h *HighWaterMark) WaitFor(ctx context.Context, offset uint64) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	for h.current < offset {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Wake when ctx fires.
		done := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				h.mu.Lock()
				h.cond.Broadcast()
				h.mu.Unlock()
			case <-done:
			}
		}()
		h.cond.Wait()
		close(done)
	}
	return nil
}

func (h *HighWaterMark) recomputeLocked() {
	if len(h.offsets) == 0 {
		h.current = 0
		return
	}
	first := true
	var min uint64
	for _, off := range h.offsets {
		if first || off < min {
			min = off
			first = false
		}
	}
	h.current = min
}
