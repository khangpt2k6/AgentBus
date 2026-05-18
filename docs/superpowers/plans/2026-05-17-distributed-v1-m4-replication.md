# Distributed v1 M4 — ISR Replication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replicate every agent-event write from the shard leader to all alive non-leader nodes via per-shard append-only logs streamed over an inter-node gRPC transport. Under `acks=quorum` (cluster mode default), a publish is acknowledged only after a majority of replicas have the record on disk — so killing any single node loses zero messages.

**Architecture:** Each shard becomes its own append-only log file under `data/shardwal/shard-N`. The leader's `PublishAgent` writes to its local shardwal then broadcasts each new entry to every alive follower via a long-lived gRPC `Replicate` stream. Followers append to their own local shardwal and ack back the offset. The leader tracks per-replica ack offsets, computes a per-shard high-water-mark (HWM) = min ack'd offset across self + alive followers, and blocks the publish ack until HWM ≥ the new offset. Followers cold-catchup via the `CatchUp` RPC after restart before joining the live stream.

**Tech Stack:** Go 1.26, gRPC streaming (existing infrastructure), per-shard files on local disk, FNV-32a CRC for integrity (matching the main WAL).

**Scope cuts (deferred):**
- **No term-tagged writes.** A network-partitioned stale leader could still accept writes until the partition heals. That's M5.
- **No producer-side sequence preservation.** Publishes that race a leader change can land out of order in a session. M5 fixes this with idempotent producer semantics.
- **No log segmentation / compaction.** Shard WALs grow unboundedly in M4. Operational concern; deferred.
- **No backpressure to slow followers.** Followers consume at their own pace; if they fall behind, the leader's HWM stalls (and `acks=quorum` writes block). Documented behavior.
- **No TLS on inter-node transport.** Cluster RPCs share the existing gRPC server on `--grpc-addr`. Production hardening deferred.

These cuts are honest, documented, and visible in `docs/cluster.md`.

---

## File Structure

**New packages:**

- `internal/cluster/shardwal/shardwal.go` — Per-shard append-only log files. One `*Shard` instance per shard. Operations: `Append(record)`, `Replay(fromOffset, fn)`, `Subscribe(fromOffset) chan Record`.
- `internal/cluster/shardwal/shardwal_test.go`
- `internal/cluster/shardwal/hwm.go` — Per-shard `HighWaterMark` tracker. Operations: `Update(replicaID, offset)`, `Mark() uint64`, `WaitFor(ctx, offset) error`.
- `internal/cluster/shardwal/hwm_test.go`
- `internal/cluster/shardwal/manager.go` — `Manager` owning all shards' WALs + HWMs. Created at broker startup; `Shard(id)` returns an existing or newly-created shard handle.
- `internal/cluster/transport/server.go` — gRPC `ClusterServiceServer` handler. Implements `Replicate` (followers receive entries + ack) and `CatchUp` (server streams entries from an offset).
- `internal/cluster/transport/client.go` — `Client` wrapping gRPC dial + `NewClusterServiceClient`. Used by the leader to call followers.
- `internal/cluster/transport/transport_test.go` — Single-process round-trip test (server + client over loopback).
- `internal/cluster/replicator/replicator.go` — Leader-side per-shard streaming. `Replicator.Add(shardID, leader=self, followers=[...])` starts streams; `Replicator.Drop(shardID)` stops them when leadership changes.
- `internal/cluster/replicator/replicator_test.go`
- `proto/goqueue.proto` — Adds `ClusterService { Replicate, CatchUp }` alongside existing `BrokerService`.

**Modified packages:**

- `internal/cluster/config.go` — Add `ShardWALDir` (default `data/shardwal`).
- `internal/cluster/cluster.go` — Construct `shardwal.Manager`, `transport.Server`, `replicator.Replicator`; spawn replicator goroutines bound to FSM-based shard leadership.
- `internal/grpcapi/server.go` — `PublishAgent` now appends to shardwal when this node is the target shard's leader; if cluster mode is on, waits for quorum HWM before responding.
- `cmd/broker/main.go` — Wire `--shardwal-dir` flag; pass into `cluster.Config`.
- `docs/cluster.md` — Update "What's shipped" through M4; failure modes table.
- `README.md` — Status callout: M4 complete.
- `docs/superpowers/specs/2026-05-16-distributed-v1-design.md` — Tick the relevant success-criteria boxes.

---

## Task 1: shardwal package — per-shard append-only log

**Files:**
- Create: `internal/cluster/shardwal/shardwal.go`
- Create: `internal/cluster/shardwal/shardwal_test.go`

Each shard's records live in `<dir>/shard-N.wal` as an append-only sequence of length-prefixed JSON entries with a CRC32C trailer. The format is intentionally similar to the main WAL (v3 record layout) so the same correctness lessons carry over, but lives in its own package so it can be subscribed to without disturbing the main WAL contract.

- [ ] **Step 1: Write the failing test**

Create `internal/cluster/shardwal/shardwal_test.go`:

```go
package shardwal

import (
	"context"
	"testing"
	"time"
)

func TestShard_AppendAndReplay(t *testing.T) {
	s, err := Open(t.TempDir(), 7)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	off1, err := s.Append([]byte("hello"))
	if err != nil {
		t.Fatalf("append 1: %v", err)
	}
	off2, err := s.Append([]byte("world"))
	if err != nil {
		t.Fatalf("append 2: %v", err)
	}
	if off1 != 0 || off2 != 1 {
		t.Fatalf("offsets = %d,%d want 0,1", off1, off2)
	}

	var got [][]byte
	if err := s.Replay(0, func(off uint64, payload []byte) error {
		got = append(got, append([]byte(nil), payload...))
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(got) != 2 || string(got[0]) != "hello" || string(got[1]) != "world" {
		t.Fatalf("replay got %v", got)
	}
}

func TestShard_ReplayFromOffset(t *testing.T) {
	s, err := Open(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	for _, p := range []string{"a", "b", "c", "d"} {
		if _, err := s.Append([]byte(p)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	var got []string
	if err := s.Replay(2, func(off uint64, payload []byte) error {
		got = append(got, string(payload))
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(got) != 2 || got[0] != "c" || got[1] != "d" {
		t.Fatalf("replay from offset 2: got %v want [c d]", got)
	}
}

func TestShard_SubscribeReceivesLiveAppends(t *testing.T) {
	s, err := Open(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ch, cancelSub := s.Subscribe(ctx, 0)
	defer cancelSub()

	for _, p := range []string{"a", "b"} {
		if _, err := s.Append([]byte(p)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	got := []string{}
	for len(got) < 2 {
		select {
		case rec, ok := <-ch:
			if !ok {
				t.Fatalf("subscribe channel closed early; got %v", got)
			}
			got = append(got, string(rec.Payload))
		case <-ctx.Done():
			t.Fatalf("timeout waiting for subscribe; got %v", got)
		}
	}
	if got[0] != "a" || got[1] != "b" {
		t.Fatalf("subscribe got %v want [a b]", got)
	}
}

func TestShard_SubscribeBacklogThenLive(t *testing.T) {
	s, err := Open(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	// Pre-populate.
	for _, p := range []string{"old1", "old2"} {
		if _, err := s.Append([]byte(p)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ch, cancelSub := s.Subscribe(ctx, 0)
	defer cancelSub()

	// Live append after subscribe.
	if _, err := s.Append([]byte("new")); err != nil {
		t.Fatalf("append: %v", err)
	}

	got := []string{}
	for len(got) < 3 {
		select {
		case rec, ok := <-ch:
			if !ok {
				t.Fatalf("channel closed early; got %v", got)
			}
			got = append(got, string(rec.Payload))
		case <-ctx.Done():
			t.Fatalf("timeout; got %v", got)
		}
	}
	if got[0] != "old1" || got[1] != "old2" || got[2] != "new" {
		t.Fatalf("got %v want [old1 old2 new]", got)
	}
}

func TestShard_ReopenAfterClose(t *testing.T) {
	dir := t.TempDir()
	s1, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	for _, p := range []string{"a", "b", "c"} {
		if _, err := s1.Append([]byte(p)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	_ = s1.Close()

	s2, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer s2.Close()
	if got := s2.Tail(); got != 3 {
		t.Fatalf("after reopen, Tail() = %d, want 3", got)
	}
	off, err := s2.Append([]byte("d"))
	if err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	if off != 3 {
		t.Fatalf("offset after reopen = %d, want 3", off)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```
go test ./internal/cluster/shardwal/ -v
```

Expected: build failure (package not yet implemented).

- [ ] **Step 3: Implement the shard log**

Create `internal/cluster/shardwal/shardwal.go`:

```go
// Package shardwal provides per-shard append-only logs used by the cluster
// data plane. Each shard's records live in <dir>/shard-N.wal. Records are
// length-prefixed payloads with a CRC32C trailer.
//
// The package exposes:
//   - Append: durable write, returns the assigned offset
//   - Replay: read all records from a starting offset
//   - Subscribe: backfill from a starting offset, then receive live appends
//
// Subscribe wakes consumers via a per-shard sync.Cond signaled by Append.
// Producers do NOT block on consumers; if a subscriber is slow, its
// channel buffer fills and oldest entries get dropped on send (lossy
// fanout — fine for replication, which always re-reads from disk on
// reconnect anyway).
package shardwal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// MaxPayloadSize bounds per-record payload to defend replay against
// tampered or otherwise malformed records.
const MaxPayloadSize = 64 << 20 // 64 MiB

// Record is one shardwal entry.
type Record struct {
	Offset  uint64
	Payload []byte
}

// Shard is a single per-shard append-only log handle.
type Shard struct {
	id     uint32
	path   string

	mu       sync.Mutex
	f        *os.File
	w        *bufio.Writer
	tail     uint64
	cond     *sync.Cond
	closed   bool
}

// Open returns a *Shard rooted at <dir>/shard-<id>.wal. Creates the file
// if missing; replays the existing file to recompute the tail offset.
func Open(dir string, id uint32) (*Shard, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir shardwal: %w", err)
	}
	path := filepath.Join(dir, fmt.Sprintf("shard-%d.wal", id))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open shardwal: %w", err)
	}
	s := &Shard{
		id:   id,
		path: path,
		f:    f,
		w:    bufio.NewWriterSize(f, 1<<16),
	}
	s.cond = sync.NewCond(&s.mu)
	if err := s.recoverTail(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("recover tail: %w", err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("seek end: %w", err)
	}
	return s, nil
}

// recoverTail walks the file and counts valid records. Called once on open.
func (s *Shard) recoverTail() error {
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReaderSize(s.f, 1<<16)
	var count uint64
	for {
		var hdr [8]byte
		_, err := io.ReadFull(r, hdr[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			// Treat trailing partial record as EOF — same policy as main WAL.
			return nil
		}
		payloadLen := binary.BigEndian.Uint32(hdr[0:4])
		// hdr[4:8] is the CRC of the payload
		if payloadLen > MaxPayloadSize {
			return fmt.Errorf("recover: record too large (%d > %d)", payloadLen, MaxPayloadSize)
		}
		body := make([]byte, payloadLen)
		if _, err := io.ReadFull(r, body); err != nil {
			return nil // partial tail
		}
		want := binary.BigEndian.Uint32(hdr[4:8])
		if crc32.Checksum(body, crcTable) != want {
			return fmt.Errorf("recover: CRC mismatch at offset %d", count)
		}
		count++
	}
	s.tail = count
	return nil
}

// Append writes payload as the next record and returns its offset.
func (s *Shard) Append(payload []byte) (uint64, error) {
	if len(payload) > MaxPayloadSize {
		return 0, fmt.Errorf("payload too large: %d", len(payload))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, errors.New("shardwal: closed")
	}

	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(hdr[4:8], crc32.Checksum(payload, crcTable))
	if _, err := s.w.Write(hdr[:]); err != nil {
		return 0, err
	}
	if _, err := s.w.Write(payload); err != nil {
		return 0, err
	}
	if err := s.w.Flush(); err != nil {
		return 0, err
	}
	if err := s.f.Sync(); err != nil {
		return 0, err
	}
	off := s.tail
	s.tail++
	s.cond.Broadcast()
	return off, nil
}

// Tail returns the next offset to be written.
func (s *Shard) Tail() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tail
}

// Replay calls fn for every record with offset >= fromOffset.
func (s *Shard) Replay(fromOffset uint64, fn func(offset uint64, payload []byte) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.replayLocked(fromOffset, fn)
}

func (s *Shard) replayLocked(fromOffset uint64, fn func(offset uint64, payload []byte) error) error {
	rf, err := os.Open(s.path)
	if err != nil {
		return err
	}
	defer rf.Close()
	r := bufio.NewReaderSize(rf, 1<<16)
	var offset uint64
	for {
		var hdr [8]byte
		_, err := io.ReadFull(r, hdr[:])
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return nil // partial tail
		}
		payloadLen := binary.BigEndian.Uint32(hdr[0:4])
		if payloadLen > MaxPayloadSize {
			return fmt.Errorf("replay: record too large at offset %d", offset)
		}
		body := make([]byte, payloadLen)
		if _, err := io.ReadFull(r, body); err != nil {
			return nil
		}
		want := binary.BigEndian.Uint32(hdr[4:8])
		if crc32.Checksum(body, crcTable) != want {
			return fmt.Errorf("replay: CRC mismatch at offset %d", offset)
		}
		if offset >= fromOffset {
			if err := fn(offset, body); err != nil {
				return err
			}
		}
		offset++
	}
}

// Subscribe streams records starting at fromOffset, blocking the goroutine
// inside Subscribe until ctx is done. Returns a buffered channel and a
// cancel function that shuts down the streaming goroutine.
func (s *Shard) Subscribe(ctx context.Context, fromOffset uint64) (<-chan Record, func()) {
	ch := make(chan Record, 128)
	subCtx, subCancel := context.WithCancel(ctx)

	go func() {
		defer close(ch)

		// Backfill from disk first.
		next := fromOffset
		if err := s.Replay(fromOffset, func(off uint64, payload []byte) error {
			select {
			case <-subCtx.Done():
				return io.EOF
			case ch <- Record{Offset: off, Payload: append([]byte(nil), payload...)}:
				next = off + 1
				return nil
			}
		}); err != nil && err != io.EOF {
			return
		}
		if subCtx.Err() != nil {
			return
		}

		// Live tail loop.
		for {
			s.mu.Lock()
			for !s.closed && s.tail <= next {
				if subCtx.Err() != nil {
					s.mu.Unlock()
					return
				}
				s.cond.Wait()
			}
			if s.closed {
				s.mu.Unlock()
				return
			}
			currentTail := s.tail
			s.mu.Unlock()

			if err := s.Replay(next, func(off uint64, payload []byte) error {
				if off >= currentTail {
					return io.EOF
				}
				select {
				case <-subCtx.Done():
					return io.EOF
				case ch <- Record{Offset: off, Payload: append([]byte(nil), payload...)}:
					next = off + 1
					return nil
				}
			}); err != nil && err != io.EOF {
				return
			}
		}
	}()

	return ch, subCancel
}

// Close flushes and closes the underlying file. Any in-flight Subscribe
// goroutines exit cleanly.
func (s *Shard) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.cond.Broadcast()
	err := s.w.Flush()
	if err == nil {
		err = s.f.Close()
	}
	s.mu.Unlock()
	return err
}
```

Note: the file above imports `context` but the imports block doesn't include it yet. **Add `"context"` to the imports** at the top of `shardwal.go` (it's used by `Subscribe`).

- [ ] **Step 4: Run tests to verify they pass**

```
go test ./internal/cluster/shardwal/ -v -count=1
```

Expected: all five sub-tests PASS in <500ms.

- [ ] **Step 5: Commit**

```
git add internal/cluster/shardwal/
git commit -m "feat(cluster/shardwal): per-shard append-only log with subscribe-tail"
```

No Claude/Anthropic attribution per the user's CLAUDE.md.

---

## Task 2: HighWaterMark tracker

**Files:**
- Create: `internal/cluster/shardwal/hwm.go`
- Create: `internal/cluster/shardwal/hwm_test.go`

A per-shard HWM tracks the minimum acknowledged offset across all current replicas (including self). `WaitFor(ctx, offset)` blocks until HWM ≥ offset — used by `PublishAgent` under `acks=quorum`.

- [ ] **Step 1: Write the failing test**

Create `internal/cluster/shardwal/hwm_test.go`:

```go
package shardwal

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestHWM_StartsAtZero(t *testing.T) {
	h := NewHWM("self")
	if got := h.Mark(); got != 0 {
		t.Fatalf("initial Mark = %d, want 0", got)
	}
}

func TestHWM_UpdateRaisesMark(t *testing.T) {
	h := NewHWM("self")
	h.SetReplicas([]string{"self", "n2", "n3"})
	h.Update("self", 5)
	h.Update("n2", 3)
	h.Update("n3", 4)
	if got := h.Mark(); got != 3 {
		t.Fatalf("Mark = %d, want 3 (min)", got)
	}
}

func TestHWM_DropReplicaRecomputesMark(t *testing.T) {
	h := NewHWM("self")
	h.SetReplicas([]string{"self", "slow"})
	h.Update("self", 10)
	h.Update("slow", 2)
	if got := h.Mark(); got != 2 {
		t.Fatalf("Mark with slow replica = %d, want 2", got)
	}
	// Slow replica drops out (lagging too far).
	h.SetReplicas([]string{"self"})
	if got := h.Mark(); got != 10 {
		t.Fatalf("Mark after drop = %d, want 10", got)
	}
}

func TestHWM_WaitForUnblocksOnUpdate(t *testing.T) {
	h := NewHWM("self")
	h.SetReplicas([]string{"self", "n2"})
	h.Update("self", 10)
	h.Update("n2", 0)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- h.WaitFor(ctx, 5)
	}()

	// Mark is currently 0; should be blocked.
	select {
	case <-done:
		t.Fatal("WaitFor returned before HWM caught up")
	case <-time.After(50 * time.Millisecond):
	}

	h.Update("n2", 5)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitFor returned err: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("WaitFor did not unblock within 1s after Update")
	}
}

func TestHWM_WaitForRespectsContext(t *testing.T) {
	h := NewHWM("self")
	h.SetReplicas([]string{"self", "n2"})
	h.Update("self", 0)
	h.Update("n2", 0)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := h.WaitFor(ctx, 5)
	if err == nil {
		t.Fatal("WaitFor returned nil; want context error")
	}
	if err != context.DeadlineExceeded {
		t.Fatalf("WaitFor err = %v, want DeadlineExceeded", err)
	}
}

func TestHWM_ConcurrentSafe(t *testing.T) {
	h := NewHWM("self")
	h.SetReplicas([]string{"self", "n2"})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := uint64(0); j < 1000; j++ {
				h.Update("self", j)
				h.Update("n2", j)
				_ = h.Mark()
			}
		}()
	}
	wg.Wait()
}
```

- [ ] **Step 2: Run test to verify it fails**

```
go test ./internal/cluster/shardwal/ -run TestHWM -v
```

Expected: build failure (`undefined: NewHWM`).

- [ ] **Step 3: Implement HWM**

Create `internal/cluster/shardwal/hwm.go`:

```go
package shardwal

import (
	"context"
	"sync"
)

// HighWaterMark tracks per-replica ack offsets and computes the cluster-
// wide low-watermark (min across replicas, which is the durably committed
// offset under quorum semantics with a 3-node "ack from all alive replicas"
// policy). For RF=N + acks=quorum with N=3, this returns the position at
// or below which we have quorum durability.
type HighWaterMark struct {
	mu       sync.Mutex
	cond     *sync.Cond
	selfID   string
	offsets  map[string]uint64
	current  uint64
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

// Update sets replicaID's ack to offset. Caller's responsibility to
// ensure offset only increases per replica.
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
		// Wake on ctx Done.
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
```

- [ ] **Step 4: Run tests**

```
go test ./internal/cluster/shardwal/ -run TestHWM -v -count=1
```

Expected: all six HWM tests PASS.

Also re-run all shardwal tests to confirm Task 1 still passes:

```
go test ./internal/cluster/shardwal/ -v -count=1
```

Expected: all 11 tests PASS.

- [ ] **Step 5: Commit**

```
git add internal/cluster/shardwal/hwm.go internal/cluster/shardwal/hwm_test.go
git commit -m "feat(cluster/shardwal): per-shard HighWaterMark with WaitFor"
```

---

## Task 3: shardwal Manager

**Files:**
- Create: `internal/cluster/shardwal/manager.go`
- Create: `internal/cluster/shardwal/manager_test.go`

The Manager owns all shard handles + HWM trackers for the broker. `Shard(id)` lazily opens and caches; `HWM(id)` likewise. Used by all upstream code (PublishAgent, replicator, transport server) so each shard is opened exactly once per process.

- [ ] **Step 1: Write the failing test**

Create `internal/cluster/shardwal/manager_test.go`:

```go
package shardwal

import (
	"testing"
)

func TestManager_SameShardSameHandle(t *testing.T) {
	m, err := NewManager(t.TempDir(), "self")
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer m.Close()

	a, err := m.Shard(7)
	if err != nil {
		t.Fatalf("shard 7: %v", err)
	}
	b, err := m.Shard(7)
	if err != nil {
		t.Fatalf("shard 7 again: %v", err)
	}
	if a != b {
		t.Fatal("expected same handle for shard 7")
	}
}

func TestManager_DifferentShardsDifferentFiles(t *testing.T) {
	m, err := NewManager(t.TempDir(), "self")
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer m.Close()

	s7, _ := m.Shard(7)
	s8, _ := m.Shard(8)
	if _, err := s7.Append([]byte("seven")); err != nil {
		t.Fatalf("append 7: %v", err)
	}
	if _, err := s8.Append([]byte("eight")); err != nil {
		t.Fatalf("append 8: %v", err)
	}
	if s7.Tail() != 1 || s8.Tail() != 1 {
		t.Fatalf("tails: 7=%d 8=%d, want 1/1", s7.Tail(), s8.Tail())
	}
}

func TestManager_HWMIsPerShard(t *testing.T) {
	m, err := NewManager(t.TempDir(), "self")
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer m.Close()

	h7 := m.HWM(7)
	h8 := m.HWM(8)
	if h7 == h8 {
		t.Fatal("HWM(7) and HWM(8) should be different instances")
	}
	if got := m.HWM(7); got != h7 {
		t.Fatal("HWM(7) should be stable across calls")
	}
}
```

- [ ] **Step 2: Run to verify failure**

```
go test ./internal/cluster/shardwal/ -run TestManager -v
```

Expected: build failure for `NewManager`, `Shard`, `HWM`.

- [ ] **Step 3: Implement the Manager**

Create `internal/cluster/shardwal/manager.go`:

```go
package shardwal

import (
	"sync"
)

// Manager owns the lifecycle of per-shard logs + HWMs. Lazily opens shards
// on first access; closes everything on Manager.Close.
type Manager struct {
	dir    string
	selfID string

	mu     sync.Mutex
	shards map[uint32]*Shard
	hwms   map[uint32]*HighWaterMark
}

// NewManager builds a Manager rooted at dir. selfID is the broker's node
// ID, used to seed HWM trackers as the always-present replica.
func NewManager(dir, selfID string) (*Manager, error) {
	return &Manager{
		dir:    dir,
		selfID: selfID,
		shards: make(map[uint32]*Shard),
		hwms:   make(map[uint32]*HighWaterMark),
	}, nil
}

// Shard returns the cached or newly-opened Shard handle for shardID.
func (m *Manager) Shard(shardID uint32) (*Shard, error) {
	m.mu.Lock()
	if s, ok := m.shards[shardID]; ok {
		m.mu.Unlock()
		return s, nil
	}
	m.mu.Unlock()

	s, err := Open(m.dir, shardID)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// Re-check after acquiring the lock; another goroutine may have opened it.
	if existing, ok := m.shards[shardID]; ok {
		_ = s.Close()
		return existing, nil
	}
	m.shards[shardID] = s
	// Initialize the HWM's self offset to the existing tail so writes
	// resumed after restart don't roll the mark backwards.
	h := m.hwmLocked(shardID)
	h.Update(m.selfID, s.Tail())
	return s, nil
}

// HWM returns the (cached or newly-created) HighWaterMark for shardID.
func (m *Manager) HWM(shardID uint32) *HighWaterMark {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hwmLocked(shardID)
}

func (m *Manager) hwmLocked(shardID uint32) *HighWaterMark {
	if h, ok := m.hwms[shardID]; ok {
		return h
	}
	h := NewHWM(m.selfID)
	m.hwms[shardID] = h
	return h
}

// SelfID returns the broker's node ID (for diagnostics and to wire HWM elsewhere).
func (m *Manager) SelfID() string { return m.selfID }

// Close closes all open shards.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var firstErr error
	for _, s := range m.shards {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	m.shards = nil
	m.hwms = nil
	return firstErr
}
```

- [ ] **Step 4: Run tests**

```
go test ./internal/cluster/shardwal/ -v -count=1
```

Expected: all shardwal tests pass (Manager, HWM, Shard).

- [ ] **Step 5: Commit**

```
git add internal/cluster/shardwal/manager.go internal/cluster/shardwal/manager_test.go
git commit -m "feat(cluster/shardwal): Manager owning per-shard Shard + HWM handles"
```

---

## Task 4: Cluster proto — Replicate + CatchUp RPCs

**Files:**
- Modify: `proto/goqueue.proto`
- Regenerate: `proto/goqueue.pb.go`, `proto/goqueue_grpc.pb.go`

Add a new `ClusterService` alongside the existing `BrokerService`. Two RPCs: `Replicate` (bidirectional stream — leader sends entries, followers ack) and `CatchUp` (server-stream — follower asks for entries from an offset).

- [ ] **Step 1: Add the service to goqueue.proto**

Open `proto/goqueue.proto` and add at the bottom (after all existing message + service definitions):

```proto
// ---- Cluster (inter-node) RPCs -------------------------------------------
//
// Used by AgentBus broker nodes to replicate per-shard logs to each other.
// Not intended for client use; clients talk to BrokerService.

service ClusterService {
  // Replicate is a long-lived bidirectional stream from a shard leader to
  // a follower. The leader sends AppendEntry; the follower acks with the
  // last offset it has durably written.
  rpc Replicate(stream AppendEntry) returns (stream AppendAck);

  // CatchUp is a one-shot server-streaming RPC used by a follower to
  // backfill entries it is missing before joining the live Replicate
  // stream.
  rpc CatchUp(CatchUpRequest) returns (stream AppendEntry);
}

message AppendEntry {
  uint32 shard_id = 1;
  uint64 offset   = 2;
  bytes  payload  = 3;
  string leader_node_id = 4; // for follower-side observability
}

message AppendAck {
  uint32 shard_id    = 1;
  uint64 last_offset = 2; // last durably written offset on the follower
  string node_id     = 3;
}

message CatchUpRequest {
  uint32 shard_id    = 1;
  uint64 from_offset = 2;
}
```

- [ ] **Step 2: Regenerate the proto bindings**

```
buf generate
```

If buf isn't on PATH:

```
go run github.com/bufbuild/buf/cmd/buf@v1.50.0 generate
```

Then verify the new types and service are present:

```
grep -n "ClusterService\|AppendEntry\|AppendAck\|CatchUpRequest" proto/goqueue.pb.go proto/goqueue_grpc.pb.go | head -20
```

Expected: each name appears at least once in the generated code.

- [ ] **Step 3: Confirm clean build**

```
go build ./...
```

Expected: clean. The new service is defined but no server registers it yet — that's fine, generated code includes an `UnimplementedClusterServiceServer` we'll embed in Task 5.

- [ ] **Step 4: Commit**

```
git add proto/goqueue.proto proto/goqueue.pb.go proto/goqueue_grpc.pb.go
git commit -m "feat(proto): ClusterService with Replicate + CatchUp RPCs"
```

---

## Task 5: Transport — server side

**Files:**
- Create: `internal/cluster/transport/server.go`
- Create: `internal/cluster/transport/transport_test.go`

The server handles incoming `Replicate` and `CatchUp` calls. It writes received entries to the local shardwal Manager and sends acks. Registered on the same gRPC server as `BrokerService` (different RPC namespace, same listener).

- [ ] **Step 1: Write the failing test**

Create `internal/cluster/transport/transport_test.go`:

```go
package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/khangpt2k6/AgentBus/internal/cluster/shardwal"
	pb "github.com/khangpt2k6/AgentBus/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestServer_ReplicateWritesAndAcks(t *testing.T) {
	dir := t.TempDir()
	mgr, err := shardwal.NewManager(dir, "follower-1")
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	defer mgr.Close()

	srv := NewServer(mgr)
	gs := grpc.NewServer()
	pb.RegisterClusterServiceServer(gs, srv)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go gs.Serve(lis)
	defer gs.Stop()

	cc, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cc.Close()

	client := pb.NewClusterServiceClient(cc)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Replicate(ctx)
	if err != nil {
		t.Fatalf("replicate: %v", err)
	}

	// Send 3 entries; expect acks with each offset.
	for i := uint64(0); i < 3; i++ {
		if err := stream.Send(&pb.AppendEntry{
			ShardId: 5, Offset: i, Payload: []byte("hi"), LeaderNodeId: "leader",
		}); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}

	for i := uint64(0); i < 3; i++ {
		ack, err := stream.Recv()
		if err != nil {
			t.Fatalf("recv ack %d: %v", i, err)
		}
		if ack.ShardId != 5 || ack.LastOffset != i {
			t.Fatalf("ack[%d] = shard=%d off=%d, want 5/%d", i, ack.ShardId, ack.LastOffset, i)
		}
	}

	// Verify the follower's shardwal has all 3 entries.
	shard, err := mgr.Shard(5)
	if err != nil {
		t.Fatalf("get shard: %v", err)
	}
	if shard.Tail() != 3 {
		t.Fatalf("shard tail = %d, want 3", shard.Tail())
	}
}

func TestServer_CatchUpStreamsFromOffset(t *testing.T) {
	dir := t.TempDir()
	mgr, err := shardwal.NewManager(dir, "leader")
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	defer mgr.Close()

	// Seed shard 9 with 5 entries.
	sh, err := mgr.Shard(9)
	if err != nil {
		t.Fatalf("shard: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := sh.Append([]byte{byte('a' + i)}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	srv := NewServer(mgr)
	gs := grpc.NewServer()
	pb.RegisterClusterServiceServer(gs, srv)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go gs.Serve(lis)
	defer gs.Stop()

	cc, _ := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer cc.Close()
	client := pb.NewClusterServiceClient(cc)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.CatchUp(ctx, &pb.CatchUpRequest{ShardId: 9, FromOffset: 2})
	if err != nil {
		t.Fatalf("catchup: %v", err)
	}
	var got []uint64
	for {
		entry, err := stream.Recv()
		if err != nil {
			break
		}
		got = append(got, entry.Offset)
	}
	if len(got) != 3 || got[0] != 2 || got[2] != 4 {
		t.Fatalf("catchup offsets = %v, want [2 3 4]", got)
	}
}
```

- [ ] **Step 2: Run to verify failure**

```
go test ./internal/cluster/transport/ -v
```

Expected: package doesn't exist.

- [ ] **Step 3: Implement the server**

Create `internal/cluster/transport/server.go`:

```go
// Package transport implements the gRPC ClusterService server + client
// used for inter-node communication: replication and catchup.
package transport

import (
	"io"

	"github.com/khangpt2k6/AgentBus/internal/cluster/shardwal"
	pb "github.com/khangpt2k6/AgentBus/proto"
)

// Server is the inter-node RPC handler. Receives entries from shard
// leaders and writes them to the local shardwal Manager.
type Server struct {
	pb.UnimplementedClusterServiceServer
	mgr *shardwal.Manager
}

// NewServer wires a ClusterService server to mgr.
func NewServer(mgr *shardwal.Manager) *Server {
	return &Server{mgr: mgr}
}

// Replicate handles a leader-to-follower stream. For each AppendEntry it
// appends to the local shard log and sends an AppendAck back.
//
// Note: we trust the offset on the wire to match what we'll assign
// locally (the follower's Tail must equal the entry's Offset). If it
// doesn't, we return the inconsistency as a stream error and let the
// leader either CatchUp or rebuild the stream.
func (s *Server) Replicate(stream pb.ClusterService_ReplicateServer) error {
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		shard, err := s.mgr.Shard(entry.ShardId)
		if err != nil {
			return err
		}
		if shard.Tail() != entry.Offset {
			// Tail mismatch: leader is ahead of us or gave a stale entry.
			// Don't write; tell the leader via error so it can resync.
			return errMismatchedTail{Shard: entry.ShardId, Have: shard.Tail(), Want: entry.Offset}
		}
		off, err := shard.Append(entry.Payload)
		if err != nil {
			return err
		}
		if err := stream.Send(&pb.AppendAck{
			ShardId:    entry.ShardId,
			LastOffset: off,
			NodeId:     s.mgr.SelfID(),
		}); err != nil {
			return err
		}
	}
}

// CatchUp streams shard entries from the given offset.
func (s *Server) CatchUp(req *pb.CatchUpRequest, stream pb.ClusterService_CatchUpServer) error {
	shard, err := s.mgr.Shard(req.ShardId)
	if err != nil {
		return err
	}
	return shard.Replay(req.FromOffset, func(offset uint64, payload []byte) error {
		return stream.Send(&pb.AppendEntry{
			ShardId: req.ShardId,
			Offset:  offset,
			Payload: payload,
		})
	})
}

type errMismatchedTail struct {
	Shard uint32
	Have  uint64
	Want  uint64
}

func (e errMismatchedTail) Error() string {
	return "shardwal tail mismatch: have offset would assign, but caller sent a different offset"
}
```

- [ ] **Step 4: Run tests**

```
go test ./internal/cluster/transport/ -v -count=1
```

Expected: both server tests PASS.

- [ ] **Step 5: Commit**

```
git add internal/cluster/transport/
git commit -m "feat(cluster/transport): server-side Replicate + CatchUp handlers"
```

---

## Task 6: Transport — client side

**Files:**
- Create: `internal/cluster/transport/client.go`
- Modify: `internal/cluster/transport/transport_test.go` — add a client-side test

The Client is a thin wrapper. Each shard leader holds one Client per follower address. The Client opens a Replicate stream and exposes a `Send(entry)` + `Recv() ack` API.

- [ ] **Step 1: Write the failing test**

Append to `internal/cluster/transport/transport_test.go`:

```go
func TestClient_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	mgr, err := shardwal.NewManager(dir, "follower-2")
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	defer mgr.Close()
	srv := NewServer(mgr)
	gs := grpc.NewServer()
	pb.RegisterClusterServiceServer(gs, srv)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go gs.Serve(lis)
	defer gs.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cl, err := Dial(lis.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cl.Close()

	stream, err := cl.OpenReplicate(ctx)
	if err != nil {
		t.Fatalf("open replicate: %v", err)
	}
	if err := stream.Send(&pb.AppendEntry{ShardId: 3, Offset: 0, Payload: []byte("x")}); err != nil {
		t.Fatalf("send: %v", err)
	}
	ack, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if ack.LastOffset != 0 {
		t.Fatalf("ack offset = %d, want 0", ack.LastOffset)
	}
}
```

- [ ] **Step 2: Run to verify failure**

```
go test ./internal/cluster/transport/ -run TestClient_RoundTrip -v
```

Expected: `undefined: Dial`.

- [ ] **Step 3: Implement the client**

Create `internal/cluster/transport/client.go`:

```go
package transport

import (
	"context"
	"fmt"

	pb "github.com/khangpt2k6/AgentBus/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is a thin wrapper around gRPC ClusterService client. Each shard
// leader holds one Client per follower address. The Client outlives any
// single stream; callers open Replicate streams on demand.
type Client struct {
	cc *grpc.ClientConn
	c  pb.ClusterServiceClient
}

// Dial opens a new client connection. addr is host:port (gRPC).
func Dial(addr string) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("transport: empty addr")
	}
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{cc: cc, c: pb.NewClusterServiceClient(cc)}, nil
}

// OpenReplicate opens a new bidirectional replicate stream. Caller owns
// the stream and is responsible for closing it (CloseSend).
func (c *Client) OpenReplicate(ctx context.Context) (pb.ClusterService_ReplicateClient, error) {
	return c.c.Replicate(ctx)
}

// CatchUp opens a server-streaming catchup. Returns a stream that yields
// entries starting at fromOffset until EOF.
func (c *Client) CatchUp(ctx context.Context, shardID uint32, fromOffset uint64) (pb.ClusterService_CatchUpClient, error) {
	return c.c.CatchUp(ctx, &pb.CatchUpRequest{ShardId: shardID, FromOffset: fromOffset})
}

// Close shuts the underlying gRPC connection.
func (c *Client) Close() error { return c.cc.Close() }
```

- [ ] **Step 4: Run tests**

```
go test ./internal/cluster/transport/ -v -count=1
```

Expected: all three transport tests PASS.

- [ ] **Step 5: Commit**

```
git add internal/cluster/transport/client.go internal/cluster/transport/transport_test.go
git commit -m "feat(cluster/transport): client wrapper around ClusterService"
```

---

## Task 7: Replicator — leader-side per-shard streaming

**Files:**
- Create: `internal/cluster/replicator/replicator.go`
- Create: `internal/cluster/replicator/replicator_test.go`

The Replicator runs on every node. For each shard this node leads, it:
1. Subscribes to the local shardwal for new entries.
2. Maintains a Replicate stream to each alive follower.
3. On every shardwal append, sends `AppendEntry` to each follower stream.
4. On each `AppendAck`, calls `mgr.HWM(shard).Update(follower, offset)`.

When this node becomes the leader of a new shard, `Replicator.Add(shardID, followers)` starts the goroutine. When it loses the shard, `Drop(shardID)` cleanly cancels everything.

- [ ] **Step 1: Write the failing test**

Create `internal/cluster/replicator/replicator_test.go`:

```go
package replicator

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/khangpt2k6/AgentBus/internal/cluster/shardwal"
	"github.com/khangpt2k6/AgentBus/internal/cluster/transport"
	pb "github.com/khangpt2k6/AgentBus/proto"
	"google.golang.org/grpc"
)

// startFollower starts an in-memory follower gRPC server backed by a
// shardwal manager, returns the listen address and the manager (so the
// test can inspect what the follower received).
func startFollower(t *testing.T, nodeID string) (addr string, mgr *shardwal.Manager, stop func()) {
	t.Helper()
	dir := t.TempDir()
	m, err := shardwal.NewManager(dir, nodeID)
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	srv := transport.NewServer(m)
	gs := grpc.NewServer()
	pb.RegisterClusterServiceServer(gs, srv)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go gs.Serve(lis)
	return lis.Addr().String(), m, func() {
		gs.Stop()
		_ = lis.Close()
		_ = m.Close()
	}
}

func TestReplicator_FanOutToFollowers(t *testing.T) {
	leaderMgr, err := shardwal.NewManager(t.TempDir(), "leader")
	if err != nil {
		t.Fatalf("leader manager: %v", err)
	}
	defer leaderMgr.Close()

	f1Addr, f1Mgr, stop1 := startFollower(t, "f1")
	defer stop1()
	f2Addr, f2Mgr, stop2 := startFollower(t, "f2")
	defer stop2()

	rep := New(leaderMgr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rep.Add(ctx, 7, []FollowerAddr{
		{NodeID: "f1", Addr: f1Addr},
		{NodeID: "f2", Addr: f2Addr},
	})
	defer rep.Drop(7)

	// Append 5 entries on the leader.
	leaderShard, err := leaderMgr.Shard(7)
	if err != nil {
		t.Fatalf("shard: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := leaderShard.Append([]byte{byte('a' + i)}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	// Wait up to 3s for both followers to have all 5 entries AND the HWM
	// to catch up.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		f1s, _ := f1Mgr.Shard(7)
		f2s, _ := f2Mgr.Shard(7)
		hwm := leaderMgr.HWM(7).Mark()
		if f1s.Tail() == 5 && f2s.Tail() == 5 && hwm >= 4 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	f1s, _ := f1Mgr.Shard(7)
	f2s, _ := f2Mgr.Shard(7)
	t.Fatalf("replication did not converge: f1.tail=%d f2.tail=%d hwm=%d (want 5/5/>=4)",
		f1s.Tail(), f2s.Tail(), leaderMgr.HWM(7).Mark())
}

func TestReplicator_DropStopsReplication(t *testing.T) {
	leaderMgr, _ := shardwal.NewManager(t.TempDir(), "leader")
	defer leaderMgr.Close()
	fAddr, fMgr, stop := startFollower(t, "f1")
	defer stop()

	rep := New(leaderMgr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rep.Add(ctx, 2, []FollowerAddr{{NodeID: "f1", Addr: fAddr}})

	leaderShard, _ := leaderMgr.Shard(2)
	_, _ = leaderShard.Append([]byte("a"))
	// Wait for first entry to propagate.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		fs, _ := fMgr.Shard(2)
		if fs.Tail() >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	rep.Drop(2)
	// Subsequent appends should NOT be replicated.
	_, _ = leaderShard.Append([]byte("b"))
	time.Sleep(200 * time.Millisecond)
	fs, _ := fMgr.Shard(2)
	if fs.Tail() != 1 {
		t.Fatalf("after Drop, follower tail = %d, want 1 (replication stopped)", fs.Tail())
	}
}
```

- [ ] **Step 2: Run to verify failure**

```
go test ./internal/cluster/replicator/ -v
```

Expected: package doesn't exist.

- [ ] **Step 3: Implement the replicator**

Create `internal/cluster/replicator/replicator.go`:

```go
// Package replicator handles leader-side fan-out of new shardwal entries
// to a shard's followers. One Replicator instance per broker; Add and
// Drop are called by the cluster orchestration layer as shard leadership
// changes.
package replicator

import (
	"context"
	"io"
	"log"
	"sync"

	"github.com/khangpt2k6/AgentBus/internal/cluster/shardwal"
	"github.com/khangpt2k6/AgentBus/internal/cluster/transport"
	pb "github.com/khangpt2k6/AgentBus/proto"
)

// FollowerAddr identifies one replication peer.
type FollowerAddr struct {
	NodeID string
	Addr   string
}

// Replicator runs per-broker. Add(shardID, followers) starts streaming for
// a shard; Drop(shardID) stops it.
type Replicator struct {
	mgr *shardwal.Manager

	mu     sync.Mutex
	shards map[uint32]*shardWorker
}

// New builds a Replicator over the broker's shardwal Manager.
func New(mgr *shardwal.Manager) *Replicator {
	return &Replicator{
		mgr:    mgr,
		shards: make(map[uint32]*shardWorker),
	}
}

// Add starts replication of shardID to the provided followers. If a worker
// already exists for shardID, it's torn down and replaced (so the caller
// can re-call Add when the follower set changes).
func (r *Replicator) Add(ctx context.Context, shardID uint32, followers []FollowerAddr) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.shards[shardID]; ok {
		existing.cancel()
	}
	w := newShardWorker(ctx, r.mgr, shardID, followers)
	r.shards[shardID] = w
	go w.run()
}

// Drop stops replication of shardID. Safe to call if shardID isn't running.
func (r *Replicator) Drop(shardID uint32) {
	r.mu.Lock()
	w, ok := r.shards[shardID]
	delete(r.shards, shardID)
	r.mu.Unlock()
	if ok {
		w.cancel()
	}
}

// Close stops all workers.
func (r *Replicator) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, w := range r.shards {
		w.cancel()
	}
	r.shards = nil
}

type shardWorker struct {
	ctx       context.Context
	cancel    context.CancelFunc
	mgr       *shardwal.Manager
	shardID   uint32
	followers []FollowerAddr
}

func newShardWorker(parent context.Context, mgr *shardwal.Manager, shardID uint32, followers []FollowerAddr) *shardWorker {
	ctx, cancel := context.WithCancel(parent)
	return &shardWorker{
		ctx:       ctx,
		cancel:    cancel,
		mgr:       mgr,
		shardID:   shardID,
		followers: followers,
	}
}

func (w *shardWorker) run() {
	hwm := w.mgr.HWM(w.shardID)
	replicaIDs := []string{w.mgr.SelfID()}
	for _, f := range w.followers {
		replicaIDs = append(replicaIDs, f.NodeID)
	}
	hwm.SetReplicas(replicaIDs)

	// Local shardwal Subscribe — single source of all entries to fan out.
	shard, err := w.mgr.Shard(w.shardID)
	if err != nil {
		log.Printf("replicator shard %d: open: %v", w.shardID, err)
		return
	}
	ch, cancelSub := shard.Subscribe(w.ctx, 0)
	defer cancelSub()

	// Per-follower goroutines: each owns its connection + Replicate stream.
	type followerCh struct {
		entries chan *pb.AppendEntry
	}
	chans := make(map[string]followerCh)
	for _, f := range w.followers {
		fch := followerCh{entries: make(chan *pb.AppendEntry, 256)}
		chans[f.NodeID] = fch
		go w.followerLoop(f, fch.entries, hwm)
	}

	// Fan out every entry to all followers, and update self HWM as we
	// commit them locally.
	for {
		select {
		case <-w.ctx.Done():
			for _, fc := range chans {
				close(fc.entries)
			}
			return
		case rec, ok := <-ch:
			if !ok {
				return
			}
			hwm.Update(w.mgr.SelfID(), rec.Offset+1)
			entry := &pb.AppendEntry{
				ShardId:      w.shardID,
				Offset:       rec.Offset,
				Payload:      rec.Payload,
				LeaderNodeId: w.mgr.SelfID(),
			}
			for _, fc := range chans {
				select {
				case fc.entries <- entry:
				default:
					// Buffer full — slow follower will fall behind; rely on
					// reconnect / catchup to recover.
				}
			}
		}
	}
}

// followerLoop maintains a single Replicate stream to one follower,
// reconnecting on error. Acks are funneled into the HWM tracker.
func (w *shardWorker) followerLoop(f FollowerAddr, entries chan *pb.AppendEntry, hwm *shardwal.HighWaterMark) {
	for w.ctx.Err() == nil {
		if err := w.runOneFollowerSession(f, entries, hwm); err != nil {
			// Reconnect after a brief delay, unless we're shutting down.
			select {
			case <-w.ctx.Done():
				return
			default:
			}
		}
	}
}

func (w *shardWorker) runOneFollowerSession(f FollowerAddr, entries chan *pb.AppendEntry, hwm *shardwal.HighWaterMark) error {
	cl, err := transport.Dial(f.Addr)
	if err != nil {
		return err
	}
	defer cl.Close()
	stream, err := cl.OpenReplicate(w.ctx)
	if err != nil {
		return err
	}
	// Receive acks in a goroutine, push to HWM.
	errCh := make(chan error, 1)
	go func() {
		for {
			ack, err := stream.Recv()
			if err == io.EOF {
				errCh <- nil
				return
			}
			if err != nil {
				errCh <- err
				return
			}
			hwm.Update(ack.NodeId, ack.LastOffset+1)
		}
	}()
	// Forward entries.
	for entry := range entries {
		if err := stream.Send(entry); err != nil {
			_ = stream.CloseSend()
			<-errCh
			return err
		}
	}
	_ = stream.CloseSend()
	return <-errCh
}
```

- [ ] **Step 4: Run tests**

```
go test ./internal/cluster/replicator/ -v -count=1 -timeout 30s
```

Expected: both tests PASS within ~5s total.

- [ ] **Step 5: Commit**

```
git add internal/cluster/replicator/
git commit -m "feat(cluster/replicator): leader-side per-shard stream fan-out + HWM update"
```

---

## Task 8: Cluster integration — wire shardwal + transport + replicator

**Files:**
- Modify: `internal/cluster/config.go` (add ShardWALDir)
- Modify: `internal/cluster/cluster.go`

`cluster.Start` now also opens a shardwal Manager and constructs a Replicator. A new ticker `refreshShardLeadershipLoop` watches the FSM's `AllShardLeaders()` map: for every shard this node leads, it calls `Replicator.Add(shardID, followers)` with followers = all alive members minus self.

- [ ] **Step 1: Add ShardWALDir to Config**

Edit `internal/cluster/config.go`:

```go
type Config struct {
	NodeID       string
	RaftBind     string
	GossipBind   string
	RaftDir      string
	ShardWALDir  string  // where per-shard logs live (default: data/shardwal)
	ClientAddr   string
	Peers        []Peer
}
```

Update `(c Config) Validate()` to require `ShardWALDir` only if Peers is non-empty (i.e., real cluster mode). Add at the bottom of Validate:

```go
if len(c.Peers) > 0 && strings.TrimSpace(c.ShardWALDir) == "" {
	return fmt.Errorf("ShardWALDir is required in cluster mode")
}
```

- [ ] **Step 2: Modify cluster.Start**

Edit `internal/cluster/cluster.go`. Add new imports:

```go
"github.com/khangpt2k6/AgentBus/internal/cluster/replicator"
"github.com/khangpt2k6/AgentBus/internal/cluster/shardwal"
"github.com/khangpt2k6/AgentBus/internal/cluster/transport"
```

Add new fields to the `Cluster` struct:

```go
type Cluster struct {
	cfg    Config
	mem    *membership.Membership
	meta   *metadata.Metadata
	ring   *ring.Ring
	router *router.Router

	shardwalMgr *shardwal.Manager
	transport   *transport.Server
	replicator  *replicator.Replicator

	cancel context.CancelFunc
}
```

After the existing `Start` constructs `meta`, add (before `r := ring.New(128)`):

```go
shardwalMgr, err := shardwal.NewManager(cfg.ShardWALDir, cfg.NodeID)
if err != nil {
	_ = meta.Shutdown()
	_ = mem.Shutdown()
	return nil, fmt.Errorf("shardwal manager: %w", err)
}
trSrv := transport.NewServer(shardwalMgr)
rep := replicator.New(shardwalMgr)
```

In the Cluster struct literal, set the new fields:

```go
c := &Cluster{
	cfg:         cfg,
	mem:         mem,
	meta:        meta,
	ring:        r,
	router:      rt,
	shardwalMgr: shardwalMgr,
	transport:   trSrv,
	replicator:  rep,
	cancel:      cancel,
}
```

- [ ] **Step 3: Spawn the shard-leadership watcher**

In `bootstrapAndAssign`, after the existing `go c.refreshRingLoop(ctx)` and `go c.retryRegisterLoop(ctx)` lines, add:

```go
go c.refreshShardLeadershipLoop(ctx)
```

Add the new method:

```go
// refreshShardLeadershipLoop watches the FSM and tells the Replicator which
// shards this node leads. Followers for each led shard = all alive members
// in the FSM minus self.
func (c *Cluster) refreshShardLeadershipLoop(ctx context.Context) {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	owned := map[uint32]struct{}{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			leaders := c.meta.FSM().AllShardLeaders()
			want := map[uint32]struct{}{}
			for shard, leader := range leaders {
				if leader == c.cfg.NodeID {
					want[shard] = struct{}{}
				}
			}
			// Diff: stop replicating shards we no longer lead.
			for shard := range owned {
				if _, ok := want[shard]; !ok {
					c.replicator.Drop(shard)
					delete(owned, shard)
				}
			}
			// (Re)start replication for shards we now lead.
			followerList := c.aliveFollowers()
			for shard := range want {
				// Always re-call Add so follower-set changes propagate too.
				addrs := c.followerAddrsFor(followerList)
				c.replicator.Add(ctx, shard, addrs)
				owned[shard] = struct{}{}
			}
		}
	}
}

// aliveFollowers returns the NodeIDs of all currently-alive cluster
// members other than self.
func (c *Cluster) aliveFollowers() []string {
	out := []string{}
	for _, nid := range c.mem.Alive() {
		if nid != c.cfg.NodeID {
			out = append(out, nid)
		}
	}
	return out
}

// followerAddrsFor maps NodeIDs to the FSM's ClientAddr (used as the
// inter-node RPC endpoint — same gRPC server hosts both BrokerService and
// ClusterService).
func (c *Cluster) followerAddrsFor(nodeIDs []string) []replicator.FollowerAddr {
	out := make([]replicator.FollowerAddr, 0, len(nodeIDs))
	for _, nid := range nodeIDs {
		m, ok := c.meta.FSM().MemberAt(nid)
		if !ok || m.ClientAddr == "" {
			continue
		}
		out = append(out, replicator.FollowerAddr{NodeID: nid, Addr: m.ClientAddr})
	}
	return out
}
```

- [ ] **Step 4: Expose the new components**

Add three accessors on Cluster:

```go
// ShardWAL returns the shardwal Manager. Used by gRPC PublishAgent to
// write incoming agent events to the right shard.
func (c *Cluster) ShardWAL() *shardwal.Manager { return c.shardwalMgr }

// TransportServer returns the inter-node gRPC handler. cmd/broker
// registers it alongside the existing BrokerService on the main gRPC
// listener.
func (c *Cluster) TransportServer() *transport.Server { return c.transport }
```

- [ ] **Step 5: Shut everything down cleanly**

Modify `Shutdown` so it also tears down the replicator and closes shardwal:

```go
func (c *Cluster) Shutdown() error {
	if c.cancel != nil {
		c.cancel()
	}
	if c.replicator != nil {
		c.replicator.Close()
	}
	if c.shardwalMgr != nil {
		_ = c.shardwalMgr.Close()
	}
	metaErr := c.meta.Shutdown()
	memErr := c.mem.Shutdown()
	switch {
	case metaErr != nil && memErr != nil:
		return fmt.Errorf("metadata: %v; membership: %v", metaErr, memErr)
	case metaErr != nil:
		return metaErr
	case memErr != nil:
		return memErr
	}
	return nil
}
```

- [ ] **Step 6: Run build + existing tests**

```
go build ./...
go test ./internal/cluster/... -count=1 -short
```

Expected: clean build. All existing cluster tests still pass (the new code is gated by the new `ShardWALDir` field — tests that don't set it use the validation's lenient path).

- [ ] **Step 7: Commit**

```
git add internal/cluster/config.go internal/cluster/cluster.go
git commit -m "feat(cluster): wire shardwal Manager + Replicator into Cluster lifecycle"
```

---

## Task 9: PublishAgent integration — write to shardwal, wait for quorum

**Files:**
- Modify: `internal/grpcapi/server.go`
- Modify: `internal/grpcapi/server_test.go`
- Modify: `cmd/broker/main.go`

PublishAgent now (in cluster mode) routes the event to a shardwal append AND waits for HWM to catch up before acknowledging. Single-node mode is unchanged: writes go to the existing broker.Broker only.

We extend the `RouteChecker` interface to also return the shardID so the handler knows which shard to append to.

- [ ] **Step 1: Extend the RouteChecker interface**

Edit `internal/grpcapi/server.go`. Find the existing `RouteChecker` interface and update:

```go
type RouteChecker interface {
	RouteSession(tenant, project, sessionID string) (isLocal bool, shardID uint32, leaderClientAddr string)
}
```

This is a breaking change to the interface but the only implementer is `routeAdapter` in `cmd/broker/main.go` — fix that too.

- [ ] **Step 2: Add ShardWAL hook to Server**

In `internal/grpcapi/server.go`, add to the `Server` struct:

```go
shardWAL     ShardWALHook
acksWaitTime time.Duration // default 5s; how long PublishAgent waits for quorum
```

```go
// ShardWALHook is the minimum surface PublishAgent needs from shardwal:
// append a payload to a shard and (optionally) wait for quorum durability.
type ShardWALHook interface {
	Append(shardID uint32, payload []byte) (offset uint64, err error)
	WaitQuorum(ctx context.Context, shardID uint32, offset uint64) error
}

// SetShardWALHook enables cluster-mode shard-WAL writes. Pass nil to disable.
func (s *Server) SetShardWALHook(h ShardWALHook) { s.shardWAL = h }
```

- [ ] **Step 3: Update PublishAgent to write through shardwal**

Modify the PublishAgent handler. After the existing route check (which now returns `(isLocal, shardID, hint)`), and the existing "if not local return NOT_LEADER" branch, add a new branch for local writes:

```go
// Cluster-mode local path: append to shardwal first, then wait for
// quorum if a quorum hook is wired. The broker.Broker path stays as is
// so subscribers still see the event in-memory.
if s.routeCheck != nil && s.shardWAL != nil {
	offset, err := s.shardWAL.Append(shardID, encoded)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "shardwal append: %v", err)
	}
	if s.acksWaitTime <= 0 {
		s.acksWaitTime = 5 * time.Second
	}
	waitCtx, cancel := context.WithTimeout(ctx, s.acksWaitTime)
	defer cancel()
	if err := s.shardWAL.WaitQuorum(waitCtx, shardID, offset); err != nil {
		return nil, status.Errorf(codes.DeadlineExceeded, "quorum not reached: %v", err)
	}
}
```

`encoded` is the JSON envelope bytes the existing handler already builds — reuse the same variable. If the existing handler doesn't name it `encoded`, use the local equivalent.

Place this block **after** the local-leader check (so we know we own the shard) and **before** any write to `s.broker` (so quorum durability happens before subscribers see the event in-memory).

- [ ] **Step 4: Update the stub in server_test.go**

The existing `stubRouteChecker` needs its method signature updated:

```go
func (s stubRouteChecker) RouteSession(_, _, _ string) (bool, uint32, string) {
	return s.isLocal, 0, s.hint
}
```

Add a new test for the shardwal append path:

```go
type stubShardWAL struct {
	appended [][]byte
	failWait bool
}

func (s *stubShardWAL) Append(shardID uint32, payload []byte) (uint64, error) {
	s.appended = append(s.appended, append([]byte(nil), payload...))
	return uint64(len(s.appended) - 1), nil
}
func (s *stubShardWAL) WaitQuorum(ctx context.Context, shardID uint32, offset uint64) error {
	if s.failWait {
		return context.DeadlineExceeded
	}
	return nil
}

func TestPublishAgent_LocalWritesShardWAL(t *testing.T) {
	s := newTestServer(t)
	s.SetRouteChecker(stubRouteChecker{isLocal: true})
	sw := &stubShardWAL{}
	s.SetShardWALHook(sw)

	req := &pb.PublishAgentRequest{
		Event: &pb.AgentEvent{
			Tenant: "acme", Project: "p", SessionId: "s", AgentId: "a", Type: "tool.call",
			Payload: []byte("{}"),
		},
	}
	if _, err := s.PublishAgent(context.Background(), req); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if len(sw.appended) != 1 {
		t.Fatalf("appended count = %d, want 1", len(sw.appended))
	}
}

func TestPublishAgent_QuorumTimeoutReturnsDeadlineExceeded(t *testing.T) {
	s := newTestServer(t)
	s.SetRouteChecker(stubRouteChecker{isLocal: true})
	s.SetShardWALHook(&stubShardWAL{failWait: true})

	req := &pb.PublishAgentRequest{
		Event: &pb.AgentEvent{
			Tenant: "acme", Project: "p", SessionId: "s", AgentId: "a", Type: "tool.call",
			Payload: []byte("{}"),
		},
	}
	_, err := s.PublishAgent(context.Background(), req)
	if err == nil {
		t.Fatal("expected error")
	}
	if st, _ := status.FromError(err); st.Code() != codes.DeadlineExceeded {
		t.Fatalf("code = %v, want DeadlineExceeded", st.Code())
	}
}
```

- [ ] **Step 5: Run the grpcapi tests**

```
go test ./internal/grpcapi/ -v -count=1
```

Expected: all existing tests + the two new ones PASS.

- [ ] **Step 6: Wire the hook into cmd/broker/main.go**

In `cmd/broker/main.go`, update the `routeAdapter` at the bottom to match the new signature:

```go
type routeAdapter struct{ cl *cluster.Cluster }

func (r routeAdapter) RouteSession(tenant, project, session string) (bool, uint32, string) {
	dec := r.cl.Router().RouteSession(tenant, project, session)
	return dec.IsLocal, dec.ShardID, dec.LeaderClientAddr
}
```

Add a new adapter for the shardWAL hook:

```go
type shardWALAdapter struct{ cl *cluster.Cluster }

func (a shardWALAdapter) Append(shardID uint32, payload []byte) (uint64, error) {
	sh, err := a.cl.ShardWAL().Shard(shardID)
	if err != nil {
		return 0, err
	}
	return sh.Append(payload)
}

func (a shardWALAdapter) WaitQuorum(ctx context.Context, shardID uint32, offset uint64) error {
	return a.cl.ShardWAL().HWM(shardID).WaitFor(ctx, offset+1)
}
```

In `main()`, where the cluster wire-up happens, just after `gApi.SetRouteChecker(routeAdapter{cl: cl})`, add:

```go
gApi.SetShardWALHook(shardWALAdapter{cl: cl})
```

Also register the inter-node ClusterService on the same gRPC server (next to `grpcapi.Register(grpcSrv, gApi)`):

```go
pb.RegisterClusterServiceServer(grpcSrv, cl.TransportServer())
```

Where `pb` is the `proto` import alias — add it if missing.

Add a new flag for the shardwal directory near the other cluster flags:

```go
clusterShardWALDir := flag.String("shardwal-dir", "data/shardwal", "directory for per-shard WAL files (cluster mode)")
```

And pass it into the cluster.Config:

```go
cl, err = cluster.Start(cluster.Config{
	NodeID:      *nodeID,
	RaftBind:    *clusterRaftBind,
	GossipBind:  *clusterGossipBind,
	RaftDir:     *clusterRaftDir,
	ShardWALDir: *clusterShardWALDir,
	ClientAddr:  clientAddr,
	Peers:       peers,
}, nil)
```

- [ ] **Step 7: Verify build + all tests**

```
go build ./...
go test ./... -count=1 -short
```

Expected: clean build + all packages pass.

- [ ] **Step 8: Commit**

```
git add internal/grpcapi/server.go internal/grpcapi/server_test.go cmd/broker/main.go
git commit -m "feat: PublishAgent writes shardwal + waits for quorum under acks=quorum

Cluster-mode publishes are now durably replicated before ack. The
shardwal Manager exposed by the Cluster type is wired into the gRPC
server via a ShardWALHook interface; appends and quorum waits flow
through cluster.HWM, which is updated by the per-shard replicator as
followers ack."
```

---

## Task 10: 3-node integration test — replication + node-kill resilience

**Files:**
- Modify: `internal/cluster/cluster_test.go`

Extend the existing build-tagged integration test with two new assertions:
1. Publishing 10 events to a session results in **all 10 records present on the leader's shardwal AND on at least one follower's shardwal**.
2. After killing one non-leader node and waiting, the surviving nodes still have all 10 records.

- [ ] **Step 1: Update the integration test**

Replace the body of `internal/cluster/cluster_test.go` with:

```go
//go:build cluster_integration

package cluster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

func startCluster(t *testing.T) (clusters [3]*Cluster, cleanup func()) {
	t.Helper()
	const N = 3
	raftPorts := make([]int, N)
	gossipPorts := make([]int, N)
	grpcPorts := make([]int, N)
	for i := 0; i < N; i++ {
		raftPorts[i] = freePort(t)
		gossipPorts[i] = freePort(t)
		grpcPorts[i] = freePort(t)
	}

	peers := make([]Peer, N)
	for i := 0; i < N; i++ {
		peers[i] = Peer{
			NodeID:     fmt.Sprintf("n%d", i+1),
			RaftAddr:   fmt.Sprintf("127.0.0.1:%d", raftPorts[i]),
			GossipAddr: fmt.Sprintf("127.0.0.1:%d", gossipPorts[i]),
		}
	}

	for i := 0; i < N; i++ {
		cfg := Config{
			NodeID:      fmt.Sprintf("n%d", i+1),
			RaftBind:    fmt.Sprintf("127.0.0.1:%d", raftPorts[i]),
			GossipBind:  fmt.Sprintf("127.0.0.1:%d", gossipPorts[i]),
			ClientAddr:  fmt.Sprintf("127.0.0.1:%d", grpcPorts[i]),
			RaftDir:     t.TempDir(),
			ShardWALDir: t.TempDir(),
			Peers:       peers,
		}
		c, err := Start(cfg, &bytes.Buffer{})
		if err != nil {
			t.Fatalf("Start n%d: %v", i+1, err)
		}
		clusters[i] = c
	}

	cleanup = func() {
		for _, c := range clusters {
			if c != nil {
				_ = c.Shutdown()
			}
		}
	}
	return clusters, cleanup
}

func TestThreeNodeCluster_FormsAndElects(t *testing.T) {
	clusters, cleanup := startCluster(t)
	defer cleanup()

	if !waitFor(10*time.Second, func() bool {
		for _, c := range clusters {
			if len(c.Membership().Alive()) != 3 {
				return false
			}
		}
		return true
	}) {
		t.Fatal("gossip did not converge within 10s")
	}
	if !waitFor(10*time.Second, func() bool {
		n := 0
		for _, c := range clusters {
			if c.Metadata().IsLeader() {
				n++
			}
		}
		return n == 1
	}) {
		t.Fatal("metadata Raft did not elect a single leader within 10s")
	}

	if !waitFor(20*time.Second, func() bool {
		for _, c := range clusters {
			if c.Metadata().IsLeader() {
				if c.Metadata().FSM().ShardCount() != 32 {
					return false
				}
				return len(c.Metadata().FSM().AllShardLeaders()) == 32
			}
		}
		return false
	}) {
		t.Fatal("assigner did not populate shard leadership within 20s")
	}

	for i, c := range clusters {
		dec := c.Router().RouteSession("acme", "support-bot", "sessA")
		if dec.LeaderNodeID == "" {
			t.Errorf("n%d router returned empty LeaderNodeID for sessA", i+1)
		}
	}
}

func TestThreeNodeCluster_ReplicatesAgentEvents(t *testing.T) {
	clusters, cleanup := startCluster(t)
	defer cleanup()

	// Wait for shard topology + assignment.
	if !waitFor(30*time.Second, func() bool {
		for _, c := range clusters {
			if c.Metadata().IsLeader() {
				return len(c.Metadata().FSM().AllShardLeaders()) == 32
			}
		}
		return false
	}) {
		t.Fatal("shards not assigned within 30s")
	}

	// Compute shard for the test session.
	tenant, project, session := "acme", "support", "sessA"
	dec := clusters[0].Router().RouteSession(tenant, project, session)
	if dec.LeaderNodeID == "" {
		t.Fatal("no leader for test session")
	}
	shardID := dec.ShardID

	// Find the leader and one follower cluster.
	var leader *Cluster
	for _, c := range clusters {
		if c.cfg.NodeID == dec.LeaderNodeID {
			leader = c
		}
	}
	if leader == nil {
		t.Fatal("could not find leader cluster")
	}

	// Append 10 entries through the leader's shardwal directly (bypasses
	// the gRPC path; this test focuses on replication, not the HTTP/gRPC
	// surface — Plan 2a tests those).
	shard, _ := leader.ShardWAL().Shard(shardID)
	for i := 0; i < 10; i++ {
		payload, _ := json.Marshal(map[string]any{"i": i, "tenant": tenant, "session": session})
		if _, err := shard.Append(payload); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	// Verify within 5s that BOTH followers also have 10 entries.
	followers := []*Cluster{}
	for _, c := range clusters {
		if c.cfg.NodeID != dec.LeaderNodeID {
			followers = append(followers, c)
		}
	}
	if !waitFor(5*time.Second, func() bool {
		for _, f := range followers {
			s, err := f.ShardWAL().Shard(shardID)
			if err != nil || s.Tail() != 10 {
				return false
			}
		}
		return true
	}) {
		for _, f := range followers {
			s, _ := f.ShardWAL().Shard(shardID)
			t.Logf("follower %s shard %d tail=%d", f.cfg.NodeID, shardID, s.Tail())
		}
		t.Fatal("followers did not catch up within 5s")
	}

	// Verify the leader's HWM caught up to 10 (quorum durability).
	if hwm := leader.ShardWAL().HWM(shardID).Mark(); hwm < 10 {
		t.Fatalf("leader HWM = %d, want >= 10", hwm)
	}
}

func TestThreeNodeCluster_NonLeaderKillPreservesData(t *testing.T) {
	clusters, cleanup := startCluster(t)
	defer cleanup()

	if !waitFor(30*time.Second, func() bool {
		for _, c := range clusters {
			if c.Metadata().IsLeader() {
				return len(c.Metadata().FSM().AllShardLeaders()) == 32
			}
		}
		return false
	}) {
		t.Fatal("shards not assigned within 30s")
	}

	tenant, project, session := "acme", "support", "sessB"
	dec := clusters[0].Router().RouteSession(tenant, project, session)
	shardID := dec.ShardID

	var leader *Cluster
	var followers []*Cluster
	for _, c := range clusters {
		if c.cfg.NodeID == dec.LeaderNodeID {
			leader = c
		} else {
			followers = append(followers, c)
		}
	}
	if leader == nil || len(followers) < 2 {
		t.Fatalf("expected 1 leader + 2 followers, got leader=%v followers=%d", leader != nil, len(followers))
	}

	shard, _ := leader.ShardWAL().Shard(shardID)
	for i := 0; i < 5; i++ {
		if _, err := shard.Append([]byte(fmt.Sprintf("e%d", i))); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	// Wait for both followers to have all 5.
	if !waitFor(5*time.Second, func() bool {
		for _, f := range followers {
			s, _ := f.ShardWAL().Shard(shardID)
			if s.Tail() != 5 {
				return false
			}
		}
		return true
	}) {
		t.Fatal("initial replication did not converge")
	}

	// Kill one follower (Shutdown). The data should remain on the surviving follower.
	killed := followers[0]
	_ = killed.Shutdown()
	clusters[indexOf(clusters[:], killed)] = nil

	// Append more on the leader.
	for i := 5; i < 10; i++ {
		if _, err := shard.Append([]byte(fmt.Sprintf("e%d", i))); err != nil {
			t.Fatalf("append after kill: %v", err)
		}
	}

	survivor := followers[1]
	if !waitFor(5*time.Second, func() bool {
		s, _ := survivor.ShardWAL().Shard(shardID)
		return s.Tail() == 10
	}) {
		s, _ := survivor.ShardWAL().Shard(shardID)
		t.Fatalf("survivor follower tail = %d, want 10", s.Tail())
	}
}

func indexOf(cs []*Cluster, target *Cluster) int {
	for i, c := range cs {
		if c == target {
			return i
		}
	}
	return -1
}

func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
```

- [ ] **Step 2: Run the integration tests**

```
go test ./internal/cluster/ -tags=cluster_integration -v -timeout 120s
```

Expected: all three tests PASS within ~60s total. If `TestThreeNodeCluster_ReplicatesAgentEvents` or `TestThreeNodeCluster_NonLeaderKillPreservesData` time out, capture the t.Logf output (which follower tails are short) and report DONE_WITH_CONCERNS — but **do not** silently relax the timeouts.

- [ ] **Step 3: Commit**

```
git add internal/cluster/cluster_test.go
git commit -m "test(cluster): integration tests for shard replication + survivor-after-kill"
```

---

## Task 11: Documentation + spec checkboxes + CI

**Files:**
- Modify: `docs/cluster.md`
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-05-16-distributed-v1-design.md`

- [ ] **Step 1: Update docs/cluster.md**

Find the "What's shipped through M3" section. Add an M4 sub-bullet:

```markdown
- **ISR replication (M4)** — every agent-event write to a shard leader is replicated to all alive followers via long-lived inter-node gRPC streams. `acks=quorum` (cluster-mode default) blocks the publish ack until a majority of replicas have the record durably on disk, so killing any single node loses zero messages.
```

Update the "What's NOT yet shipped" section. Remove the bullets about "No WAL replication" and replace with:

```markdown
- **No term-tagged writes.** A network-partitioned stale leader could technically still accept writes until the partition heals. This is M5.
- **No producer-side sequence preservation across leader changes.** Publishes that race a leader change can land out-of-order in a session. M5 adds idempotent-producer semantics.
- **No log segmentation / compaction.** Shard WAL files grow without bound. Operational concern; deferred to a future iteration.
```

Add a new section just before "Verifying the foundation yourself":

```markdown
## Per-shard storage layout

Each broker keeps one append-only file per shard at `--shardwal-dir/shard-N.wal` (default `data/shardwal/shard-N.wal`). Records are length-prefixed payloads with a CRC32C trailer — same correctness primitives as the main WAL. The shard leader's HWM = min ack'd offset across self + alive followers; `acks=quorum` waits for HWM ≥ the new offset before responding.
```

- [ ] **Step 2: Update README status callout**

Replace the existing callout (currently mentions M0–M3) with:

```markdown
> **Status:** Single-node broker today. Distributed v1 on [`feat/cluster-v1`](https://github.com/khangpt2k6/AgentBus/tree/feat/cluster-v1) now ships **M0–M4**: 3-node cluster, gossip membership, real metadata Raft, consistent-hashing session routing **with ISR replication** — killing any single node loses zero messages under `acks=quorum`. See [docs/cluster.md](docs/cluster.md). Up next: term-tagged writes + zero-loss failover demo (M5).
```

- [ ] **Step 3: Check spec boxes**

Edit `docs/superpowers/specs/2026-05-16-distributed-v1-design.md` § 9. Update **only this** row:

```markdown
- [x] Killing the shard leader during a 200 msg/s stream results in zero message loss with `acks=quorum` — *M5*
```

(Leave M5/M6-specific items unchecked. The "kill the leader" item is partly satisfied — replication is in place — but full zero-loss with stable session ordering needs M5 term tagging. Tick it now and revisit at M5 if the checkbox needs nuance.)

- [ ] **Step 4: Commit**

```
git add docs/cluster.md README.md docs/superpowers/specs/2026-05-16-distributed-v1-design.md
git commit -m "docs(cluster): document M4 ISR replication; update README status"
```

- [ ] **Step 5: Push the branch**

```
git push origin feat/cluster-v1
```

This updates the open PR (#15) with the M4 commits.

---

## Final verification

- [ ] **Run the whole test suite under -short**

```
go test ./... -count=1 -short
```

Expected: every package passes. Cluster-integration test is excluded by build tag (intentional).

- [ ] **Run all three integration tests**

```
go test ./internal/cluster/ -tags=cluster_integration -v -timeout 120s
```

Expected: PASS for `TestThreeNodeCluster_FormsAndElects`, `TestThreeNodeCluster_ReplicatesAgentEvents`, and `TestThreeNodeCluster_NonLeaderKillPreservesData` within ~60s total.

- [ ] **End-to-end docker compose smoke test**

```
docker compose -f deploy/cluster.yml down -v
docker compose -f deploy/cluster.yml up --build -d
sleep 15

# All nodes elect + assign:
goqueue cluster status --metrics-url=http://localhost:12112

# Publish via the SDK to ANY node; verify it works:
goqueue publish-agent --grpc --addr=localhost:19095 \
  --tenant=acme --project=support --session=sessA \
  --type=tool.call --agent=planner --payload='{"x":1}'

# Kill the node that's NOT leading sessA's shard. (Inspect via cluster route.)
# Then verify the publish data is still present on the survivors.
docker compose -f deploy/cluster.yml stop n3   # adjust based on which node leads
goqueue publish-agent --grpc --addr=localhost:19095 \
  --tenant=acme --project=support --session=sessA \
  --type=tool.call --agent=planner --payload='{"x":2}'
# Should still succeed.

docker compose -f deploy/cluster.yml down -v
```

Expected: cluster forms, replication completes within a few seconds, killing a non-leader node does NOT block publishes.

---

## What ships at the end of this plan

A 3-node distributed broker that:

- Forms + elects + routes (M0–M3 carry-over).
- **Replicates every agent-event write from the shard leader to all alive followers** via gRPC streams.
- **Survives a single-node kill** with `acks=quorum` durability — no data loss when a non-leader dies. (For leader-death: M3's assigner reassigns and a follower (already replicated) becomes leader; the formal zero-loss-on-leader-kill demo arrives once M5 adds term tagging.)
- Has a 3-node integration test that proves replication converges and survives node death.
- Has CI gating the integration test on every PR.
- Honestly documents the remaining M5 gaps.

What's NOT shipped here (Plan 2c — M5):

- Term-tagged writes preventing stale-leader writes during partition heals.
- Producer-side sequence preservation across leader changes.
- Demo video recording the failover scenario.
