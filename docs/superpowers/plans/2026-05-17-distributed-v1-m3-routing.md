# Distributed v1 M3 — Session Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route AI-agent events to the correct shard leader across a 3-node cluster. Each `tenant/project/session` hashes deterministically to one of 32 shards; each shard has one elected leader node; clients that hit the wrong node get `NOT_LEADER` with a hint and retry transparently.

**Architecture:** Three new internal packages stack on top of the M2 foundation: `ring` (custom consistent hashing with virtual nodes — the unique-to-AgentBus piece worth blogging), `assigner` (a leader-driven round-robin shard-to-node assigner that writes through the existing metadata Raft FSM), and `router` (a small read-side facade combining the ring with FSM and membership state). The data plane stays node-local; M4 adds replication and M5 adds failover.

**Tech Stack:** Go 1.26, hashicorp/raft (already wired in M2), hashicorp/memberlist (already wired in M2), gRPC for inter-node and client RPC, FNV-32a for the hash function.

**Scope cuts (deferred):**
- No WAL replication. A publish lands in *one* node's local WAL — if that node dies, the data is gone. Replication is M4.
- No failover. If a shard leader dies in M3, that shard is stuck unassigned until manual restart or M5 ships.
- No term-tagging on writes. Stale leaders can technically still accept writes during a partition. Term-tagged writes are M5.

These limitations are visible to users (`docs/cluster.md` already documents them). Do not paper over them in M3.

---

## File Structure

**New packages:**

- `internal/cluster/ring/ring.go` — Consistent hash ring with virtual nodes; `Ring.LeaderFor(shardID uint32) string`.
- `internal/cluster/ring/ring_test.go` — Ring distribution + rebalancing tests.
- `internal/cluster/assigner/assigner.go` — Goroutine that runs only on the metadata Raft leader; assigns unassigned shards to alive members round-robin via `Metadata.Apply`.
- `internal/cluster/assigner/assigner_test.go`
- `internal/cluster/router/router.go` — `Router` type composing membership + metadata FSM + ring. Public methods: `ShardFor(sessionKey string) uint32`, `LeaderFor(shardID uint32) (nodeID, clientAddr string, isLocal bool)`.
- `internal/cluster/router/router_test.go`
- `internal/cli/cluster_route.go` — `goqueue cluster route --tenant ... --session ...` debug subcommand.

**Modified packages:**

- `internal/cluster/metadata/fsm.go` — Add `OpSetShardCount` op + `ShardCount() uint32` accessor. Extend `Members()` to carry client (gRPC) address by storing a richer `Member` struct (keeps wire compatibility via an additive snapshot version bump).
- `internal/cluster/membership/membership.go` — Add a `Meta []byte` field to `Config`, expose `Member(nodeID) (addr, meta, ok)` so the router can read each node's broadcast gRPC address from gossip.
- `internal/cluster/cluster.go` — Build router + assigner during `Start`; expose `Router() *router.Router`.
- `internal/grpcapi/server.go` — When the broker is in cluster mode AND the incoming `PublishAgent` event names a session, call `Router.LeaderFor()`. If not local, return a gRPC error with the `leader_hint` metadata.
- `agentbus/publish.go` — On gRPC `FailedPrecondition` with code `NOT_LEADER`, read the leader hint, reconnect, retry once.
- `internal/cli/root.go` — Register the new `cluster route` subcommand.
- `docs/cluster.md` — Add an M3 section describing routing behavior and remaining gaps.
- `README.md` — Status callout: foundation + routing now shipped; M4 next.
- `docs/superpowers/specs/2026-05-16-distributed-v1-design.md` — Check off the routing success criterion.

---

## Task 1: Ring package — consistent hashing with virtual nodes

**Files:**
- Create: `internal/cluster/ring/ring.go`
- Create: `internal/cluster/ring/ring_test.go`

The ring is the unique-to-AgentBus piece. Standard consistent hashing: each physical node owns N virtual nodes spread around a 32-bit hash ring. Looking up a shard hashes the shardID and walks clockwise to the nearest virtual node.

- [ ] **Step 1: Write the failing test**

Create `internal/cluster/ring/ring_test.go`:

```go
package ring

import (
	"fmt"
	"testing"
)

func TestRing_LeaderForIsDeterministic(t *testing.T) {
	r := New(128)
	r.AddNode("n1")
	r.AddNode("n2")
	r.AddNode("n3")

	first := r.LeaderFor(7)
	for i := 0; i < 100; i++ {
		if got := r.LeaderFor(7); got != first {
			t.Fatalf("LeaderFor(7) inconsistent: %q vs %q", got, first)
		}
	}
	if first == "" {
		t.Fatalf("LeaderFor(7) returned empty")
	}
}

func TestRing_EmptyReturnsEmpty(t *testing.T) {
	r := New(128)
	if got := r.LeaderFor(0); got != "" {
		t.Fatalf("empty ring should return \"\", got %q", got)
	}
}

func TestRing_DistributionIsRoughlyEven(t *testing.T) {
	r := New(128)
	r.AddNode("n1")
	r.AddNode("n2")
	r.AddNode("n3")

	counts := map[string]int{}
	const N = 30000
	for i := uint32(0); i < N; i++ {
		counts[r.LeaderFor(i)]++
	}
	// Expect each node to own roughly 1/3 of the keys. Allow ±25% slack
	// because consistent hashing distribution is variance-bounded, not
	// perfectly uniform.
	want := N / 3
	for node, n := range counts {
		if n < want*3/4 || n > want*5/4 {
			t.Errorf("node %s got %d keys, want ~%d (±25%%)", node, n, want)
		}
	}
}

func TestRing_RemoveNodeRedistributes(t *testing.T) {
	r := New(64)
	r.AddNode("n1")
	r.AddNode("n2")
	r.AddNode("n3")

	// Capture assignments before removal.
	before := map[uint32]string{}
	for i := uint32(0); i < 100; i++ {
		before[i] = r.LeaderFor(i)
	}

	r.RemoveNode("n2")

	moved := 0
	for i := uint32(0); i < 100; i++ {
		after := r.LeaderFor(i)
		if after == "n2" {
			t.Fatalf("shard %d still mapped to removed node n2", i)
		}
		if after != before[i] {
			moved++
		}
	}
	// Removing n2 should move *roughly* one-third of keys (the ones n2 had).
	// Consistent hashing keeps the other two-thirds where they were.
	if moved < 15 || moved > 60 {
		t.Errorf("removing one of three nodes moved %d/100 keys; want roughly 33", moved)
	}
}

// Sanity: AddNode is idempotent.
func TestRing_AddNodeIdempotent(t *testing.T) {
	r := New(32)
	r.AddNode("n1")
	r.AddNode("n1") // second call should be a no-op
	r.AddNode("n2")

	got := r.Members()
	if len(got) != 2 {
		t.Fatalf("Members() = %v, want 2 distinct nodes", got)
	}
}

func TestRing_MembersIsStable(t *testing.T) {
	r := New(8)
	r.AddNode("n3")
	r.AddNode("n1")
	r.AddNode("n2")
	got := r.Members()
	want := []string{"n1", "n2", "n3"} // sorted
	if fmt.Sprintf("%v", got) != fmt.Sprintf("%v", want) {
		t.Fatalf("Members() = %v, want %v (sorted)", got, want)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```
go test ./internal/cluster/ring/ -v
```

Expected: build failures (`undefined: New`, `undefined: AddNode`, etc.).

- [ ] **Step 3: Implement the ring**

Create `internal/cluster/ring/ring.go`:

```go
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
	"hash/fnv"
	"sort"
	"strconv"
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
		pos := hash(nodeID + "#" + strconv.Itoa(i))
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
	key := hash("shard-" + strconv.FormatUint(uint64(shardID), 10))
	idx := sort.Search(len(r.positions), func(i int) bool {
		return r.positions[i] >= key
	})
	if idx == len(r.positions) {
		idx = 0 // wrap around
	}
	return r.owners[r.positions[idx]]
}

// Members returns the registered nodeIDs in sorted order. Useful for
// debugging and stable comparisons.
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

func hash(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}
```

- [ ] **Step 4: Run tests to verify they pass**

```
go test ./internal/cluster/ring/ -v -count=1
```

Expected: all six sub-tests PASS in under 100ms.

- [ ] **Step 5: Commit**

```
git add internal/cluster/ring/
git commit -m "feat(cluster/ring): consistent hash ring with virtual nodes for shard routing"
```

---

## Task 2: Metadata FSM — shard count + richer Member type

**Files:**
- Modify: `internal/cluster/metadata/fsm.go`
- Modify: `internal/cluster/metadata/fsm_test.go`

The current FSM stores `members map[string]string` (NodeID → RaftAddr) and `shardLeaders map[uint32]string` (ShardID → NodeID). We extend it with:
1. A `ShardCount` field (default 32) so the cluster has a fixed shard topology.
2. A richer `Member` struct that also carries the gRPC client address — so the router can tell a redirected client where to reconnect.
3. A bumped snapshot format that includes both new fields.

Old `Members()` is preserved (returns NodeID → RaftAddr) so existing tests don't break; new `MemberAt(nodeID) Member` is added for full lookup.

- [ ] **Step 1: Write the failing test additions**

Append to `internal/cluster/metadata/fsm_test.go` (after the existing tests):

```go
func TestFSM_RegisterMemberWithClientAddr(t *testing.T) {
	f := NewFSM()
	applyCmd(t, f, Command{Op: OpRegisterMember, NodeID: "n1", Addr: "127.0.0.1:7001", ClientAddr: "127.0.0.1:9095"})
	m, ok := f.MemberAt("n1")
	if !ok {
		t.Fatalf("MemberAt(\"n1\") = false, want true")
	}
	if m.RaftAddr != "127.0.0.1:7001" {
		t.Errorf("RaftAddr = %q", m.RaftAddr)
	}
	if m.ClientAddr != "127.0.0.1:9095" {
		t.Errorf("ClientAddr = %q", m.ClientAddr)
	}
}

func TestFSM_SetShardCount(t *testing.T) {
	f := NewFSM()
	if got := f.ShardCount(); got != 0 {
		t.Fatalf("initial ShardCount = %d, want 0", got)
	}
	applyCmd(t, f, Command{Op: OpSetShardCount, Shard: 32})
	if got := f.ShardCount(); got != 32 {
		t.Fatalf("after set, ShardCount = %d, want 32", got)
	}
}

func TestFSM_SnapshotRestorePreservesV2Fields(t *testing.T) {
	src := NewFSM()
	applyCmd(t, src, Command{Op: OpRegisterMember, NodeID: "n1", Addr: "raft:1", ClientAddr: "grpc:1"})
	applyCmd(t, src, Command{Op: OpSetShardCount, Shard: 32})
	applyCmd(t, src, Command{Op: OpSetShardLeader, Shard: 7, NodeID: "n1"})

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&memSink{Buffer: &buf}); err != nil {
		t.Fatalf("persist: %v", err)
	}

	dst := NewFSM()
	if err := dst.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if got := dst.ShardCount(); got != 32 {
		t.Errorf("restored ShardCount = %d, want 32", got)
	}
	if m, ok := dst.MemberAt("n1"); !ok || m.ClientAddr != "grpc:1" {
		t.Errorf("restored ClientAddr = %v (ok=%v)", m, ok)
	}
	if got := dst.ShardLeader(7); got != "n1" {
		t.Errorf("restored ShardLeader(7) = %q", got)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

```
go test ./internal/cluster/metadata/ -run "TestFSM_RegisterMember|TestFSM_SetShardCount|TestFSM_SnapshotRestorePreservesV2Fields" -v
```

Expected: build errors (`undefined: OpRegisterMember`, `undefined: OpSetShardCount`, `undefined: MemberAt`, `undefined: ShardCount`, missing `ClientAddr` field on Command).

- [ ] **Step 3: Implement the FSM extensions**

Replace the body of `internal/cluster/metadata/fsm.go` with:

```go
// Package metadata implements the cluster's metadata control plane:
// a hashicorp/raft FSM whose committed state is the source of truth for
// cluster membership and shard leadership assignments.
package metadata

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

// Op identifies the mutation requested by a Raft log entry.
type Op string

const (
	OpAddMember      Op = "add_member"      // legacy: NodeID + Addr only (raft addr)
	OpRegisterMember Op = "register_member" // full record: NodeID + Addr + ClientAddr
	OpRemoveMember   Op = "remove_member"
	OpSetShardLeader Op = "set_shard_leader"
	OpSetShardCount  Op = "set_shard_count" // Shard carries the count
)

// Command is the JSON envelope serialized into every Raft log entry.
type Command struct {
	Op         Op     `json:"op"`
	NodeID     string `json:"node_id,omitempty"`
	Addr       string `json:"addr,omitempty"`        // raft transport addr
	ClientAddr string `json:"client_addr,omitempty"` // gRPC address for clients
	Shard      uint32 `json:"shard,omitempty"`
}

// Member is a registered cluster node with all the addresses needed to
// route clients and replicate data.
type Member struct {
	NodeID     string
	RaftAddr   string
	ClientAddr string
}

// FSM is the in-memory projection of all committed Raft log entries.
type FSM struct {
	mu           sync.RWMutex
	members      map[string]Member // nodeID -> full record
	shardLeaders map[uint32]string // shardID -> nodeID
	shardCount   uint32
}

func NewFSM() *FSM {
	return &FSM{
		members:      make(map[string]Member),
		shardLeaders: make(map[uint32]string),
	}
}

// Apply is called by Raft for every committed log entry on every node.
func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("fsm: bad command: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch cmd.Op {
	case OpAddMember:
		if cmd.NodeID == "" {
			return fmt.Errorf("add_member: empty node_id")
		}
		// Preserve any existing ClientAddr on a legacy add.
		existing := f.members[cmd.NodeID]
		f.members[cmd.NodeID] = Member{
			NodeID:     cmd.NodeID,
			RaftAddr:   cmd.Addr,
			ClientAddr: existing.ClientAddr,
		}
	case OpRegisterMember:
		if cmd.NodeID == "" {
			return fmt.Errorf("register_member: empty node_id")
		}
		f.members[cmd.NodeID] = Member{
			NodeID:     cmd.NodeID,
			RaftAddr:   cmd.Addr,
			ClientAddr: cmd.ClientAddr,
		}
	case OpRemoveMember:
		delete(f.members, cmd.NodeID)
		for s, nid := range f.shardLeaders {
			if nid == cmd.NodeID {
				delete(f.shardLeaders, s)
			}
		}
	case OpSetShardLeader:
		if cmd.NodeID == "" {
			return fmt.Errorf("set_shard_leader: empty node_id")
		}
		f.shardLeaders[cmd.Shard] = cmd.NodeID
	case OpSetShardCount:
		f.shardCount = cmd.Shard
	default:
		return fmt.Errorf("unknown op %q", cmd.Op)
	}
	return nil
}

// Members returns a copy of the raft-addr map for safe read by callers.
// Preserves the legacy NodeID -> RaftAddr shape used by existing callers
// (cluster.Cluster.Status()). For full records, use MemberAt.
func (f *FSM) Members() map[string]string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]string, len(f.members))
	for k, v := range f.members {
		out[k] = v.RaftAddr
	}
	return out
}

// MemberAt returns the full Member record for nodeID.
func (f *FSM) MemberAt(nodeID string) (Member, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	m, ok := f.members[nodeID]
	return m, ok
}

// ShardLeader returns the nodeID currently leading shardID, or "" if unset.
func (f *FSM) ShardLeader(shardID uint32) string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.shardLeaders[shardID]
}

// ShardCount returns the configured cluster-wide shard count. 0 means
// the cluster hasn't bootstrapped a shard topology yet.
func (f *FSM) ShardCount() uint32 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.shardCount
}

// AllShardLeaders returns a copy of the shard-leader map.
func (f *FSM) AllShardLeaders() map[uint32]string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[uint32]string, len(f.shardLeaders))
	for k, v := range f.shardLeaders {
		out[k] = v
	}
	return out
}

// snapshotPayload v2: members carry ClientAddr; shardCount is included.
type snapshotPayload struct {
	Version      int               `json:"v"`
	Members      map[string]Member `json:"members"`
	ShardLeaders map[uint32]string `json:"shard_leaders"`
	ShardCount   uint32            `json:"shard_count"`
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	payload := snapshotPayload{
		Version:      2,
		Members:      make(map[string]Member, len(f.members)),
		ShardLeaders: make(map[uint32]string, len(f.shardLeaders)),
		ShardCount:   f.shardCount,
	}
	for k, v := range f.members {
		payload.Members[k] = v
	}
	for k, v := range f.shardLeaders {
		payload.ShardLeaders[k] = v
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &fsmSnapshot{data: b}, nil
}

// Restore loads a snapshot. Tolerates v1 snapshots (members as map[string]string).
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	// Probe the version field.
	var probe struct {
		Version int `json:"v"`
	}
	_ = json.Unmarshal(data, &probe)

	f.mu.Lock()
	defer f.mu.Unlock()
	switch probe.Version {
	case 2:
		var p snapshotPayload
		if err := json.Unmarshal(data, &p); err != nil {
			return err
		}
		f.members = p.Members
		f.shardLeaders = p.ShardLeaders
		f.shardCount = p.ShardCount
	default:
		// v1 layout: members as map[string]string.
		var p struct {
			Members      map[string]string `json:"members"`
			ShardLeaders map[uint32]string `json:"shard_leaders"`
		}
		if err := json.Unmarshal(data, &p); err != nil {
			return err
		}
		f.members = make(map[string]Member, len(p.Members))
		for nid, raftAddr := range p.Members {
			f.members[nid] = Member{NodeID: nid, RaftAddr: raftAddr}
		}
		f.shardLeaders = p.ShardLeaders
		f.shardCount = 0
	}
	if f.members == nil {
		f.members = make(map[string]Member)
	}
	if f.shardLeaders == nil {
		f.shardLeaders = make(map[uint32]string)
	}
	return nil
}

type fsmSnapshot struct{ data []byte }

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
```

- [ ] **Step 4: Run all metadata tests to confirm both new and old pass**

```
go test ./internal/cluster/metadata/ -v -count=1
```

Expected: every test passes — the four new ones plus all of Task 5's original FSM tests and the Task 6 single-node Raft test.

- [ ] **Step 5: Commit**

```
git add internal/cluster/metadata/fsm.go internal/cluster/metadata/fsm_test.go
git commit -m "feat(cluster/metadata): shard count + Member with ClientAddr; v2 snapshot

Adds OpRegisterMember (full record) and OpSetShardCount, plus a richer
Member type carrying gRPC client addr so M3 router can redirect publishers
to the correct shard leader. Snapshot bumps to v2; Restore reads both
versions so existing on-disk state survives the upgrade."
```

---

## Task 3: Membership — expose per-node metadata

**Files:**
- Modify: `internal/cluster/membership/membership.go`
- Modify: `internal/cluster/membership/membership_test.go`

The router needs to know each node's gRPC address. Two reasonable sources: the metadata FSM (Task 2 added `ClientAddr` to `Member`) or gossip. We use the **FSM** as the source of truth and gossip as the freshness check. This means membership doesn't need a metadata channel in M3 — but we still want a `Member(nodeID)` lookup so callers can ask "is this node currently alive?" without parsing `Alive()` repeatedly.

- [ ] **Step 1: Write the failing test**

Add to `internal/cluster/membership/membership_test.go`:

```go
func TestMembership_MemberLookup(t *testing.T) {
	if testing.Short() {
		t.Skip("network-bound; skipped with -short")
	}
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	m, err := Start(Config{NodeID: "solo", GossipBind: addr})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer m.Shutdown()

	if got, ok := m.Member("solo"); !ok || got.NodeID != "solo" {
		t.Errorf("Member(\"solo\") = %+v ok=%v", got, ok)
	}
	if _, ok := m.Member("nope"); ok {
		t.Errorf("Member(\"nope\") returned ok=true")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```
go test ./internal/cluster/membership/ -run TestMembership_MemberLookup -v
```

Expected: `undefined: m.Member` build failure.

- [ ] **Step 3: Add the lookup method**

Insert into `internal/cluster/membership/membership.go` after the existing `Alive()` method:

```go
// MemberInfo is what callers see about another node — just its ID and
// the address gossip is using to talk to it. For richer metadata
// (client/gRPC address, etc.) callers consult the metadata FSM.
type MemberInfo struct {
	NodeID string
	Addr   string
}

// Member looks up a single node by ID. Returns (MemberInfo, true) if the
// node is currently alive in this node's gossip view; ({}, false) otherwise.
func (m *Membership) Member(nodeID string) (MemberInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.dead || m.ml == nil {
		return MemberInfo{}, false
	}
	for _, n := range m.ml.Members() {
		if n.Name == nodeID {
			return MemberInfo{NodeID: n.Name, Addr: n.Address()}, true
		}
	}
	return MemberInfo{}, false
}
```

- [ ] **Step 4: Run tests to verify they pass**

```
go test ./internal/cluster/membership/ -v -count=1
```

Expected: both `TestMembership_TwoNodesFormCluster` (existing) and `TestMembership_MemberLookup` PASS.

- [ ] **Step 5: Commit**

```
git add internal/cluster/membership/
git commit -m "feat(cluster/membership): Member(nodeID) liveness lookup for the router"
```

---

## Task 4: Router — composing ring + FSM + membership

**Files:**
- Create: `internal/cluster/router/router.go`
- Create: `internal/cluster/router/router_test.go`

The router is the read-side facade. Given a `tenant/project/session` key, it answers two questions: "which shard does this hash to?" and "which node leads that shard right now?" — returning whether the call is local plus the gRPC address to redirect to if not.

The router does NOT mutate anything. It reads from the metadata FSM (Task 2) and the membership liveness check (Task 3). Shard assignments come from the assigner (Task 5).

- [ ] **Step 1: Write the failing test**

Create `internal/cluster/router/router_test.go`:

```go
package router

import (
	"testing"

	"github.com/khangpt2k6/AgentBus/internal/cluster/metadata"
	"github.com/khangpt2k6/AgentBus/internal/cluster/ring"
)

// fakeMembership lets tests assert which nodes the router considers alive
// without spinning up real gossip.
type fakeMembership struct{ alive map[string]bool }

func (f *fakeMembership) IsAlive(nodeID string) bool { return f.alive[nodeID] }

func newFSMWith(t *testing.T, shardCount uint32, members map[string]string, leaders map[uint32]string) *metadata.FSM {
	t.Helper()
	f := metadata.NewFSM()
	applyOrFail(t, f, metadata.Command{Op: metadata.OpSetShardCount, Shard: shardCount})
	for nid, clientAddr := range members {
		applyOrFail(t, f, metadata.Command{
			Op:         metadata.OpRegisterMember,
			NodeID:     nid,
			Addr:       "raft://" + nid,
			ClientAddr: clientAddr,
		})
	}
	for s, nid := range leaders {
		applyOrFail(t, f, metadata.Command{Op: metadata.OpSetShardLeader, Shard: s, NodeID: nid})
	}
	return f
}

func applyOrFail(t *testing.T, f *metadata.FSM, c metadata.Command) {
	t.Helper()
	// We're testing the FSM API, not the Raft Apply path.
	// Encode + Apply via a synthetic raft.Log.
	helper := newApplyHelper(t)
	helper.apply(f, c)
}

func TestRouter_LocalDispatch(t *testing.T) {
	f := newFSMWith(t, 32,
		map[string]string{"n1": "n1:9095", "n2": "n2:9095"},
		map[uint32]string{0: "n1", 1: "n2"},
	)
	r := New("n1", f, &fakeMembership{alive: map[string]bool{"n1": true, "n2": true}}, freshRing("n1", "n2"))

	dec := r.RouteSession("acme", "support", "sessA")
	if dec.IsLocal {
		// good
	} else {
		// We don't know which shard sessA hashes to without running, so
		// just assert the response is internally consistent.
		if dec.LeaderNodeID == "n1" {
			t.Errorf("inconsistent: LeaderNodeID=n1 but IsLocal=false")
		}
	}
	if dec.ShardID >= 32 {
		t.Errorf("ShardID = %d out of range", dec.ShardID)
	}
}

func TestRouter_RedirectIncludesClientAddr(t *testing.T) {
	f := newFSMWith(t, 4,
		map[string]string{"n1": "n1:9095", "n2": "n2:9095"},
		map[uint32]string{0: "n2", 1: "n2", 2: "n2", 3: "n2"}, // pin all shards to n2
	)
	r := New("n1", f, &fakeMembership{alive: map[string]bool{"n1": true, "n2": true}}, freshRing("n1", "n2"))

	dec := r.RouteSession("acme", "support", "any-session")
	if dec.IsLocal {
		t.Fatalf("expected redirect to n2, got local")
	}
	if dec.LeaderNodeID != "n2" {
		t.Errorf("LeaderNodeID = %q, want n2", dec.LeaderNodeID)
	}
	if dec.LeaderClientAddr != "n2:9095" {
		t.Errorf("LeaderClientAddr = %q, want n2:9095", dec.LeaderClientAddr)
	}
}

func TestRouter_NoLeaderWhenUnassigned(t *testing.T) {
	// Shard count = 4, but no SetShardLeader commands ran.
	f := newFSMWith(t, 4,
		map[string]string{"n1": "n1:9095"},
		map[uint32]string{},
	)
	r := New("n1", f, &fakeMembership{alive: map[string]bool{"n1": true}}, freshRing("n1"))

	dec := r.RouteSession("acme", "support", "sessA")
	if dec.LeaderNodeID != "" {
		t.Errorf("expected empty LeaderNodeID for unassigned shard, got %q", dec.LeaderNodeID)
	}
	if dec.IsLocal {
		t.Errorf("expected IsLocal=false for unassigned shard")
	}
}

func TestRouter_DeadLeaderProducesEmptyLeader(t *testing.T) {
	f := newFSMWith(t, 4,
		map[string]string{"n1": "n1:9095", "n2": "n2:9095"},
		map[uint32]string{0: "n2", 1: "n2", 2: "n2", 3: "n2"},
	)
	// n2 is recorded in FSM but not in our liveness view.
	r := New("n1", f, &fakeMembership{alive: map[string]bool{"n1": true}}, freshRing("n1", "n2"))

	dec := r.RouteSession("acme", "support", "sessA")
	if dec.LeaderNodeID != "" {
		t.Errorf("expected empty LeaderNodeID when leader is dead, got %q", dec.LeaderNodeID)
	}
}

func freshRing(nodes ...string) *ring.Ring {
	r := ring.New(64)
	for _, n := range nodes {
		r.AddNode(n)
	}
	return r
}
```

Also create `internal/cluster/router/helper_test.go` (separate file so the helper isn't compiled into the binary):

```go
package router

import (
	"encoding/json"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/khangpt2k6/AgentBus/internal/cluster/metadata"
)

// applyHelper provides a tiny shim for tests that need to call FSM.Apply
// without standing up a real Raft node. It's only used in tests.
type applyHelper struct{ t *testing.T }

func newApplyHelper(t *testing.T) *applyHelper { return &applyHelper{t: t} }

func (h *applyHelper) apply(f *metadata.FSM, c metadata.Command) {
	h.t.Helper()
	b, err := json.Marshal(c)
	if err != nil {
		h.t.Fatalf("marshal cmd: %v", err)
	}
	if resp := f.Apply(&raft.Log{Data: b}); resp != nil {
		if err, ok := resp.(error); ok {
			h.t.Fatalf("apply: %v", err)
		}
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

```
go test ./internal/cluster/router/ -v
```

Expected: package doesn't exist.

- [ ] **Step 3: Implement the router**

Create `internal/cluster/router/router.go`:

```go
// Package router is the read-side facade used by gRPC handlers to decide
// whether to serve a session-keyed publish locally or redirect the client
// to the current shard leader. The router holds no mutable state of its
// own; it composes:
//
//   - Ring: hash sessionKey -> shardID
//   - FSM:  shardID -> NodeID (leader), NodeID -> ClientAddr
//   - Membership: liveness check ("is that leader alive right now?")
//
// All methods are safe for concurrent use because the underlying
// components are.
package router

import (
	"strings"

	"github.com/khangpt2k6/AgentBus/internal/cluster/metadata"
	"github.com/khangpt2k6/AgentBus/internal/cluster/ring"
)

// LivenessChecker is the minimum membership-like interface the router needs.
// internal/cluster/membership.Membership satisfies this via IsAlive.
type LivenessChecker interface {
	IsAlive(nodeID string) bool
}

// Decision is the result of routing one session.
type Decision struct {
	ShardID          uint32
	LeaderNodeID     string // "" if no leader is currently assigned or alive
	LeaderClientAddr string // empty if LeaderNodeID == ""
	IsLocal          bool   // true if this node is the leader
}

// Router is the read-side routing facade.
type Router struct {
	selfNodeID string
	fsm        *metadata.FSM
	live       LivenessChecker
	r          *ring.Ring
}

// New builds a router for `selfNodeID` over the given subsystems.
func New(selfNodeID string, fsm *metadata.FSM, live LivenessChecker, r *ring.Ring) *Router {
	return &Router{selfNodeID: selfNodeID, fsm: fsm, live: live, r: r}
}

// SessionKey is the deterministic key used for shard hashing.
// Mirrors agentstream.SessionKey but lives here to avoid an import cycle.
func SessionKey(tenant, project, sessionID string) string {
	return strings.TrimSpace(tenant) + "/" + strings.TrimSpace(project) + "/" + strings.TrimSpace(sessionID)
}

// RouteSession is the primary entry point. It hashes the session to a
// shard, looks up the current leader, and returns a Decision the gRPC
// handler can act on.
func (rt *Router) RouteSession(tenant, project, sessionID string) Decision {
	count := rt.fsm.ShardCount()
	if count == 0 {
		// No topology yet — caller should fall back to local handling.
		return Decision{}
	}
	key := SessionKey(tenant, project, sessionID)
	shard := hashToShard(key, count)

	leader := rt.fsm.ShardLeader(shard)
	if leader == "" || !rt.live.IsAlive(leader) {
		// Either no assignment yet, or the assigned leader is currently
		// not alive. Caller policy: return NOT_LEADER with an empty hint
		// and let the SDK retry after a backoff. The assigner will fill
		// the gap on the next tick.
		return Decision{ShardID: shard}
	}

	if leader == rt.selfNodeID {
		return Decision{ShardID: shard, LeaderNodeID: leader, IsLocal: true}
	}

	m, ok := rt.fsm.MemberAt(leader)
	if !ok || m.ClientAddr == "" {
		// FSM doesn't know how to reach the leader for clients yet.
		return Decision{ShardID: shard, LeaderNodeID: leader}
	}
	return Decision{ShardID: shard, LeaderNodeID: leader, LeaderClientAddr: m.ClientAddr}
}

// Ring exposes the underlying ring for the assigner.
func (rt *Router) Ring() *ring.Ring { return rt.r }

// FSM exposes the underlying FSM for the assigner.
func (rt *Router) FSM() *metadata.FSM { return rt.fsm }

func hashToShard(key string, count uint32) uint32 {
	// FNV-32a, identical to the ring's hash to keep behavior easy to reason about.
	h := fnv32a(key)
	return h % count
}

func fnv32a(s string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime32
	}
	return h
}
```

- [ ] **Step 4: Run tests to verify they pass**

```
go test ./internal/cluster/router/ -v -count=1
```

Expected: all four tests PASS.

- [ ] **Step 5: Commit**

```
git add internal/cluster/router/
git commit -m "feat(cluster/router): session-key routing facade over ring + FSM + membership"
```

---

## Task 5: Assigner — leader-driven shard placement

**Files:**
- Create: `internal/cluster/assigner/assigner.go`
- Create: `internal/cluster/assigner/assigner_test.go`

The assigner runs as a goroutine on every node, but only does work when this node is the metadata Raft leader. It periodically (every 2s) inspects the FSM and writes shard-leader assignments via `Metadata.Apply` for:
- Shards whose current leader is dead.
- Shards that have no leader yet.

Assignment is round-robin over alive members for stability.

- [ ] **Step 1: Write the failing test**

Create `internal/cluster/assigner/assigner_test.go`:

```go
package assigner

import (
	"testing"

	"github.com/khangpt2k6/AgentBus/internal/cluster/metadata"
)

type fakeApplier struct {
	cmds []metadata.Command
}

func (f *fakeApplier) Apply(cmd metadata.Command) error {
	f.cmds = append(f.cmds, cmd)
	return nil
}

type fakeLive struct{ alive map[string]bool }

func (f *fakeLive) IsAlive(nodeID string) bool { return f.alive[nodeID] }
func (f *fakeLive) AliveMembers() []string {
	out := make([]string, 0, len(f.alive))
	for nid, a := range f.alive {
		if a {
			out = append(out, nid)
		}
	}
	return out
}

func TestAssigner_AssignsUnassignedShards(t *testing.T) {
	fsm := metadata.NewFSM()
	apply(t, fsm, metadata.Command{Op: metadata.OpSetShardCount, Shard: 4})
	apply(t, fsm, metadata.Command{Op: metadata.OpRegisterMember, NodeID: "n1", Addr: "r1", ClientAddr: "c1"})
	apply(t, fsm, metadata.Command{Op: metadata.OpRegisterMember, NodeID: "n2", Addr: "r2", ClientAddr: "c2"})

	app := &fakeApplier{}
	live := &fakeLive{alive: map[string]bool{"n1": true, "n2": true}}

	cmds := Plan(fsm, live, app)
	if cmds != 4 {
		t.Fatalf("Plan() issued %d commands, want 4 (one per shard)", cmds)
	}
	// All four shards should now have a leader set.
	leaders := fsm.AllShardLeaders()
	if len(leaders) != 4 {
		t.Errorf("AllShardLeaders len = %d, want 4", len(leaders))
	}
	// Distribution should be 2-and-2 (round-robin over sorted nodeIDs).
	counts := map[string]int{}
	for _, nid := range leaders {
		counts[nid]++
	}
	if counts["n1"] != 2 || counts["n2"] != 2 {
		t.Errorf("uneven distribution: %v", counts)
	}
}

func TestAssigner_LeavesAssignedAlone(t *testing.T) {
	fsm := metadata.NewFSM()
	apply(t, fsm, metadata.Command{Op: metadata.OpSetShardCount, Shard: 2})
	apply(t, fsm, metadata.Command{Op: metadata.OpRegisterMember, NodeID: "n1", Addr: "r1", ClientAddr: "c1"})
	apply(t, fsm, metadata.Command{Op: metadata.OpRegisterMember, NodeID: "n2", Addr: "r2", ClientAddr: "c2"})
	apply(t, fsm, metadata.Command{Op: metadata.OpSetShardLeader, Shard: 0, NodeID: "n1"})
	apply(t, fsm, metadata.Command{Op: metadata.OpSetShardLeader, Shard: 1, NodeID: "n2"})

	app := &fakeApplier{}
	live := &fakeLive{alive: map[string]bool{"n1": true, "n2": true}}

	if cmds := Plan(fsm, live, app); cmds != 0 {
		t.Fatalf("Plan() with no changes issued %d commands, want 0", cmds)
	}
}

func TestAssigner_ReassignsDeadLeader(t *testing.T) {
	fsm := metadata.NewFSM()
	apply(t, fsm, metadata.Command{Op: metadata.OpSetShardCount, Shard: 2})
	apply(t, fsm, metadata.Command{Op: metadata.OpRegisterMember, NodeID: "n1", Addr: "r1", ClientAddr: "c1"})
	apply(t, fsm, metadata.Command{Op: metadata.OpRegisterMember, NodeID: "n2", Addr: "r2", ClientAddr: "c2"})
	apply(t, fsm, metadata.Command{Op: metadata.OpSetShardLeader, Shard: 0, NodeID: "n2"})
	apply(t, fsm, metadata.Command{Op: metadata.OpSetShardLeader, Shard: 1, NodeID: "n2"})

	app := &fakeApplier{}
	// n2 is dead; only n1 alive.
	live := &fakeLive{alive: map[string]bool{"n1": true, "n2": false}}

	if cmds := Plan(fsm, live, app); cmds != 2 {
		t.Fatalf("Plan() with dead leader issued %d commands, want 2", cmds)
	}
	leaders := fsm.AllShardLeaders()
	if leaders[0] != "n1" || leaders[1] != "n1" {
		t.Errorf("shards not reassigned to n1: %v", leaders)
	}
}

func apply(t *testing.T, fsm *metadata.FSM, c metadata.Command) {
	t.Helper()
	// We use the same applier the assigner does, so the test fully
	// exercises the FSM round-trip.
	if err := (&fsmApplier{fsm: fsm}).Apply(c); err != nil {
		t.Fatalf("apply: %v", err)
	}
}

// fsmApplier lets tests apply commands directly through the FSM without
// a real Raft node.
type fsmApplier struct{ fsm *metadata.FSM }

func (a *fsmApplier) Apply(c metadata.Command) error {
	// We borrow the same JSON encoding the production Apply path uses.
	helper := jsonApplyHelper{}
	return helper.do(a.fsm, c)
}
```

Create `internal/cluster/assigner/helper_test.go`:

```go
package assigner

import (
	"encoding/json"
	"fmt"

	"github.com/hashicorp/raft"
	"github.com/khangpt2k6/AgentBus/internal/cluster/metadata"
)

type jsonApplyHelper struct{}

func (h jsonApplyHelper) do(f *metadata.FSM, c metadata.Command) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	if resp := f.Apply(&raft.Log{Data: b}); resp != nil {
		if err, ok := resp.(error); ok {
			return fmt.Errorf("fsm: %w", err)
		}
	}
	return nil
}
```

- [ ] **Step 2: Run tests to verify they fail**

```
go test ./internal/cluster/assigner/ -v
```

Expected: package doesn't exist.

- [ ] **Step 3: Implement the assigner**

Create `internal/cluster/assigner/assigner.go`:

```go
// Package assigner runs on the metadata Raft leader and round-robins
// unassigned (or dead-leader) shards onto alive cluster members via
// metadata.Command writes.
//
// Plan() is the pure decision function (tested directly). RunLoop() is
// the production goroutine; it ticks every 2s and only calls Plan when
// this node is the Raft leader.
package assigner

import (
	"context"
	"sort"
	"time"

	"github.com/khangpt2k6/AgentBus/internal/cluster/metadata"
)

// Applier is the write-side interface the assigner needs. In production
// this is satisfied by an adapter around metadata.Metadata.Apply.
type Applier interface {
	Apply(metadata.Command) error
}

// Liveness reports which cluster members are currently alive.
type Liveness interface {
	IsAlive(nodeID string) bool
	AliveMembers() []string
}

// LeaderChecker reports whether this node is currently the Raft leader.
type LeaderChecker interface {
	IsLeader() bool
}

// Plan inspects FSM state, decides which shards need (re)assignment, and
// writes the corresponding commands via applier. Returns the number of
// commands issued.
func Plan(fsm *metadata.FSM, live Liveness, applier Applier) int {
	count := fsm.ShardCount()
	if count == 0 {
		return 0
	}
	alive := live.AliveMembers()
	if len(alive) == 0 {
		return 0
	}
	sort.Strings(alive)

	leaders := fsm.AllShardLeaders()
	issued := 0
	next := 0
	for s := uint32(0); s < count; s++ {
		current := leaders[s]
		if current != "" && live.IsAlive(current) {
			continue
		}
		target := alive[next%len(alive)]
		next++
		err := applier.Apply(metadata.Command{
			Op:     metadata.OpSetShardLeader,
			Shard:  s,
			NodeID: target,
		})
		if err == nil {
			issued++
		}
	}
	return issued
}

// RunLoop ticks every 2s and runs Plan when this node is the Raft leader.
// Exits when ctx is canceled.
func RunLoop(ctx context.Context, leader LeaderChecker, fsm *metadata.FSM, live Liveness, applier Applier) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if !leader.IsLeader() {
				continue
			}
			_ = Plan(fsm, live, applier)
		}
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

```
go test ./internal/cluster/assigner/ -v -count=1
```

Expected: all three tests PASS.

- [ ] **Step 5: Commit**

```
git add internal/cluster/assigner/
git commit -m "feat(cluster/assigner): leader-driven round-robin shard placement"
```

---

## Task 6: Cluster wiring — register self, build router + assigner

**Files:**
- Modify: `internal/cluster/cluster.go`
- Modify: `internal/cluster/cluster_test.go` (the integration test)

The cluster facade ties everything together. On `Start`, after metadata Raft is up, the node:
1. Waits up to 5s to find a Raft leader.
2. Applies `OpRegisterMember` with its own RaftAddr + ClientAddr.
3. If shard count isn't set yet, applies `OpSetShardCount` (default 32).
4. Spawns the assigner goroutine.

We add a `ClientAddr` field to `Config` (the gRPC address `cmd/broker/main.go` already knows) and a `Router() *router.Router` accessor.

- [ ] **Step 1: Add `ClientAddr` to Config + a routing-aware `Router()` accessor**

Update `internal/cluster/config.go` — add a single field to `Config`:

```go
type Config struct {
	NodeID     string
	RaftBind   string
	GossipBind string
	RaftDir    string
	ClientAddr string  // gRPC address clients should dial; used in redirect hints
	Peers      []Peer
}
```

No changes to `ParsePeers` or `Validate` (ClientAddr is optional in single-node mode).

- [ ] **Step 2: Wire router + assigner in cluster.go**

Replace the body of `internal/cluster/cluster.go` with:

```go
package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/khangpt2k6/AgentBus/internal/cluster/assigner"
	"github.com/khangpt2k6/AgentBus/internal/cluster/membership"
	"github.com/khangpt2k6/AgentBus/internal/cluster/metadata"
	"github.com/khangpt2k6/AgentBus/internal/cluster/ring"
	"github.com/khangpt2k6/AgentBus/internal/cluster/router"
)

// Default shard count when the assigner first bootstraps a cluster.
// Picked so 3-5 node deployments distribute work, while remaining cheap
// to track in the FSM.
const defaultShardCount = 32

// Cluster bundles the membership and metadata subsystems behind a single
// lifecycle, plus the M3 router/assigner used to route session traffic.
type Cluster struct {
	cfg    Config
	mem    *membership.Membership
	meta   *metadata.Metadata
	ring   *ring.Ring
	router *router.Router

	cancel context.CancelFunc // cancels the assigner + member-bootstrap loop
}

// Status is a snapshot of cluster state for /readyz or `cluster status` CLI.
type Status struct {
	NodeID         string
	AliveMembers   []string
	MetadataLeader string
	IsLeader       bool
	Role           string
	Term           uint64
	ShardCount     uint32
	FSMembers      map[string]string
}

// Start brings up both subsystems. On any error, partial state is cleaned up.
func Start(cfg Config, logOut io.Writer) (*Cluster, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if logOut == nil {
		logOut = os.Stderr
	}

	join := make([]string, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		if p.NodeID == cfg.NodeID {
			continue
		}
		addr := p.GossipAddr
		if addr == "" {
			addr = p.RaftAddr
		}
		join = append(join, addr)
	}
	mem, err := membership.Start(membership.Config{
		NodeID:     cfg.NodeID,
		GossipBind: cfg.GossipBind,
		JoinAddrs:  join,
		LogOutput:  logOut,
	})
	if err != nil {
		return nil, fmt.Errorf("membership start: %w", err)
	}

	peers := make([]metadata.Peer, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		peers = append(peers, metadata.Peer{NodeID: p.NodeID, Addr: p.RaftAddr})
	}
	meta, err := metadata.Start(metadata.Options{
		NodeID:        cfg.NodeID,
		BindAddr:      cfg.RaftBind,
		AdvertiseAddr: cfg.RaftBind,
		DataDir:       cfg.RaftDir,
		Bootstrap:     len(peers) > 0,
		InitialPeers:  peers,
		LogOutput:     logOut,
	})
	if err != nil {
		_ = mem.Shutdown()
		return nil, fmt.Errorf("metadata start: %w", err)
	}

	r := ring.New(128)
	rt := router.New(cfg.NodeID, meta.FSM(), aliveAdapter{mem: mem}, r)
	ctx, cancel := context.WithCancel(context.Background())

	c := &Cluster{
		cfg:    cfg,
		mem:    mem,
		meta:   meta,
		ring:   r,
		router: rt,
		cancel: cancel,
	}

	go c.bootstrapAndAssign(ctx, logOut)
	return c, nil
}

// bootstrapAndAssign performs the one-time self-registration with the
// metadata FSM and then runs the assigner goroutine for the life of the
// cluster.
func (c *Cluster) bootstrapAndAssign(ctx context.Context, logOut io.Writer) {
	// Wait for *some* leader to emerge before trying to Apply.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if c.meta.Leader() != "" {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Register ourselves (only the leader's Apply will succeed; followers'
	// Apply gets routed/redirected by raft library itself or simply fails,
	// in which case we retry next tick).
	c.registerSelf()

	// Ensure shard count is bootstrapped. Only the leader's call matters.
	if c.meta.IsLeader() && c.meta.FSM().ShardCount() == 0 {
		_ = c.applyCmd(metadata.Command{Op: metadata.OpSetShardCount, Shard: defaultShardCount})
	}

	// Keep the ring in sync with FSM members. Cheap; runs on every tick.
	go c.refreshRingLoop(ctx)

	// Run the assigner until shutdown.
	assigner.RunLoop(ctx, leaderChecker{m: c.meta}, c.meta.FSM(), aliveAdapter{mem: c.mem}, applierAdapter{m: c.meta})

	_ = logOut // reserved for future structured-logging output
}

func (c *Cluster) registerSelf() {
	_ = c.applyCmd(metadata.Command{
		Op:         metadata.OpRegisterMember,
		NodeID:     c.cfg.NodeID,
		Addr:       c.cfg.RaftBind,
		ClientAddr: c.cfg.ClientAddr,
	})
}

func (c *Cluster) refreshRingLoop(ctx context.Context) {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.refreshRingOnce()
		}
	}
}

func (c *Cluster) refreshRingOnce() {
	want := map[string]struct{}{}
	for nid := range c.meta.FSM().Members() {
		want[nid] = struct{}{}
	}
	have := map[string]struct{}{}
	for _, n := range c.ring.Members() {
		have[n] = struct{}{}
	}
	for n := range want {
		if _, ok := have[n]; !ok {
			c.ring.AddNode(n)
		}
	}
	for n := range have {
		if _, ok := want[n]; !ok {
			c.ring.RemoveNode(n)
		}
	}
}

func (c *Cluster) applyCmd(cmd metadata.Command) error {
	b, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	return c.meta.Apply(b, 2*time.Second)
}

// Router returns the read-side routing facade. Callers use this in gRPC
// handlers to decide local-vs-redirect.
func (c *Cluster) Router() *router.Router { return c.router }

// Membership returns the gossip subsystem (read-only handle).
func (c *Cluster) Membership() *membership.Membership { return c.mem }

// Metadata returns the Raft subsystem (read-only handle).
func (c *Cluster) Metadata() *metadata.Metadata { return c.meta }

// Status returns a snapshot of current cluster state.
func (c *Cluster) Status() Status {
	return Status{
		NodeID:         c.cfg.NodeID,
		AliveMembers:   c.mem.Alive(),
		MetadataLeader: c.meta.Leader(),
		IsLeader:       c.meta.IsLeader(),
		Role:           c.meta.State(),
		Term:           c.meta.Term(),
		ShardCount:     c.meta.FSM().ShardCount(),
		FSMembers:      c.meta.FSM().Members(),
	}
}

// Shutdown stops both subsystems and the assigner goroutine.
func (c *Cluster) Shutdown() error {
	if c.cancel != nil {
		c.cancel()
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

// --- adapters bridging cluster subsystems to assigner/router interfaces ---

type aliveAdapter struct{ mem *membership.Membership }

func (a aliveAdapter) IsAlive(nodeID string) bool {
	_, ok := a.mem.Member(nodeID)
	return ok
}
func (a aliveAdapter) AliveMembers() []string { return a.mem.Alive() }

type leaderChecker struct{ m *metadata.Metadata }

func (l leaderChecker) IsLeader() bool { return l.m.IsLeader() }

type applierAdapter struct{ m *metadata.Metadata }

func (a applierAdapter) Apply(c metadata.Command) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return a.m.Apply(b, 2*time.Second)
}
```

- [ ] **Step 3: Update the integration test for M3 expectations**

Replace the body of `internal/cluster/cluster_test.go` with:

```go
//go:build cluster_integration

package cluster

import (
	"bytes"
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

func TestThreeNodeCluster_FormsAndElects(t *testing.T) {
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
			NodeID:   fmt.Sprintf("n%d", i+1),
			RaftAddr: fmt.Sprintf("127.0.0.1:%d", raftPorts[i]),
		}
	}

	var clusters [N]*Cluster
	for i := 0; i < N; i++ {
		cfg := Config{
			NodeID:     fmt.Sprintf("n%d", i+1),
			RaftBind:   fmt.Sprintf("127.0.0.1:%d", raftPorts[i]),
			GossipBind: fmt.Sprintf("127.0.0.1:%d", gossipPorts[i]),
			ClientAddr: fmt.Sprintf("127.0.0.1:%d", grpcPorts[i]),
			RaftDir:    t.TempDir(),
			Peers:      peers,
		}
		c, err := Start(cfg, &bytes.Buffer{})
		if err != nil {
			t.Fatalf("Start n%d: %v", i+1, err)
		}
		clusters[i] = c
		defer c.Shutdown()
	}

	if !waitFor(10*time.Second, func() bool {
		for _, c := range clusters {
			if len(c.Membership().Alive()) != N {
				return false
			}
		}
		return true
	}) {
		t.Fatal("gossip did not converge within 10s")
	}

	if !waitFor(10*time.Second, func() bool {
		leaders := 0
		for _, c := range clusters {
			if c.Metadata().IsLeader() {
				leaders++
			}
		}
		return leaders == 1
	}) {
		t.Fatal("metadata Raft did not elect a single leader within 10s")
	}

	// M3 expectation: within another 10s, the assigner has populated
	// shard leadership for all 32 shards, distributed across the 3 nodes.
	if !waitFor(15*time.Second, func() bool {
		// Pick the metadata leader; its FSM is authoritative-on-this-node.
		for _, c := range clusters {
			if c.Metadata().IsLeader() {
				leaders := c.Metadata().FSM().AllShardLeaders()
				if c.Metadata().FSM().ShardCount() != 32 {
					return false
				}
				return len(leaders) == 32
			}
		}
		return false
	}) {
		for i, c := range clusters {
			t.Logf("n%d ShardCount=%d AssignedLeaders=%d", i+1,
				c.Metadata().FSM().ShardCount(), len(c.Metadata().FSM().AllShardLeaders()))
		}
		t.Fatal("assigner did not populate shard leadership within 15s")
	}

	// And the router on every node should route a sample session somewhere
	// non-empty.
	for i, c := range clusters {
		dec := c.Router().RouteSession("acme", "support-bot", "sessA")
		if dec.LeaderNodeID == "" {
			t.Errorf("n%d router returned empty LeaderNodeID for sessA", i+1)
		}
	}
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

- [ ] **Step 4: Verify all existing tests still pass + integration test passes**

```
go test ./... -count=1 -short
go test ./internal/cluster/ -tags=cluster_integration -run TestThreeNode -v -timeout 90s
```

Expected: every package passes under `-short`. The integration test passes within 30s.

- [ ] **Step 5: Commit**

```
git add internal/cluster/cluster.go internal/cluster/cluster_test.go internal/cluster/config.go
git commit -m "feat(cluster): self-register members + run assigner; expose Router()"
```

---

## Task 7: gRPC PublishAgent — emit NOT_LEADER when not local

**Files:**
- Modify: `internal/grpcapi/server.go`

The broker's gRPC `PublishAgent` handler currently writes every agent event to its local broker. With a router available, we add a pre-check: parse the incoming event envelope, ask the router, and if not local, return `FailedPrecondition` with a `leader-hint` trailer the SDK can read.

We only intercept publishes that carry a valid agentstream envelope. Non-envelope publishes (legacy regular `Publish`) remain node-local.

- [ ] **Step 1: Read the existing PublishAgent handler**

Run:

```
grep -n "PublishAgent" internal/grpcapi/server.go
```

Find the handler. It will be something like `func (s *Server) PublishAgent(ctx context.Context, req *pb.PublishAgentRequest) (*pb.PublishAgentResponse, error)`. Note exactly where it accepts the request and where it begins writing to `s.broker`.

- [ ] **Step 2: Add a `ClusterRouter` field + setter to Server**

In `internal/grpcapi/server.go`, find the `Server` struct definition. Add a field:

```go
type Server struct {
	// ... existing fields ...
	routeCheck RouteChecker // nil in single-node mode
}

// RouteChecker is the minimum surface PublishAgent needs from the cluster
// router. Defined as an interface so single-node mode and tests don't
// import the cluster package.
type RouteChecker interface {
	RouteSession(tenant, project, sessionID string) (isLocal bool, leaderClientAddr string)
}

// SetRouteChecker enables cluster-mode routing checks. Pass nil to disable.
func (s *Server) SetRouteChecker(rc RouteChecker) { s.routeCheck = rc }
```

- [ ] **Step 3: Add the redirect short-circuit in PublishAgent**

At the very top of the `PublishAgent` handler body (before any writes to `s.broker`), insert:

```go
if s.routeCheck != nil {
	tenant, project, session := req.GetEvent().GetTenant(), req.GetEvent().GetProject(), req.GetEvent().GetSessionId()
	if tenant != "" && project != "" && session != "" {
		isLocal, hint := s.routeCheck.RouteSession(tenant, project, session)
		if !isLocal {
			st := status.New(codes.FailedPrecondition, "not the leader of this session's shard")
			st, _ = st.WithDetails(&pb.NotLeaderError{LeaderAddr: hint})
			return nil, st.Err()
		}
	}
}
```

Make sure the imports include:

```go
"google.golang.org/grpc/codes"
"google.golang.org/grpc/status"
```

(They almost certainly already do.)

The `pb.NotLeaderError` message doesn't exist yet — Step 4 adds it.

- [ ] **Step 4: Add the NotLeaderError proto message**

Edit `proto/goqueue.proto`. Find the `message PublishAgentResponse` (or wherever the agent-event messages live). After that block, add:

```proto
// NotLeaderError is returned as a status detail (alongside
// FailedPrecondition) when a client publishes to a node that does not
// own the target shard.
message NotLeaderError {
  // gRPC address (host:port) of the current leader. May be empty if the
  // leader is unknown or unreachable — the SDK should back off and retry.
  string leader_addr = 1;
}
```

Then regenerate:

```
buf generate
```

If `buf` isn't installed, use the existing project pattern (the README's Quality Gates job runs `buf generate`). Confirm `proto/goqueue.pb.go` now contains `type NotLeaderError struct`.

- [ ] **Step 5: Write a unit test for the redirect short-circuit**

Append to `internal/grpcapi/server_test.go`:

```go
type stubRouteChecker struct {
	isLocal bool
	hint    string
}

func (s stubRouteChecker) RouteSession(_, _, _ string) (bool, string) {
	return s.isLocal, s.hint
}

func TestPublishAgent_RedirectsWhenNotLocal(t *testing.T) {
	s := newTestServer(t)
	s.SetRouteChecker(stubRouteChecker{isLocal: false, hint: "n2-host:9095"})

	req := &pb.PublishAgentRequest{
		Event: &pb.AgentEvent{
			Tenant:    "acme",
			Project:   "support",
			SessionId: "sess-1",
			Type:      "tool.call",
			AgentId:   "planner",
		},
	}
	_, err := s.PublishAgent(context.Background(), req)
	if err == nil {
		t.Fatal("expected NOT_LEADER error")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("got code %v, want FailedPrecondition", st.Code())
	}
	// Detail check
	details := st.Details()
	if len(details) == 0 {
		t.Fatal("expected NotLeaderError detail")
	}
	hint, ok := details[0].(*pb.NotLeaderError)
	if !ok {
		t.Fatalf("detail type = %T, want *NotLeaderError", details[0])
	}
	if hint.LeaderAddr != "n2-host:9095" {
		t.Errorf("LeaderAddr = %q", hint.LeaderAddr)
	}
}

func TestPublishAgent_LocalProceeds(t *testing.T) {
	s := newTestServer(t)
	s.SetRouteChecker(stubRouteChecker{isLocal: true})

	req := &pb.PublishAgentRequest{
		Event: &pb.AgentEvent{
			Tenant:    "acme",
			Project:   "support",
			SessionId: "sess-1",
			Type:      "tool.call",
			AgentId:   "planner",
			Payload:   []byte("{}"),
		},
	}
	if _, err := s.PublishAgent(context.Background(), req); err != nil {
		t.Fatalf("local PublishAgent: %v", err)
	}
}
```

(`newTestServer` already exists in the file; reuse it.)

- [ ] **Step 6: Run the new tests**

```
go test ./internal/grpcapi/ -run "TestPublishAgent_Redirects|TestPublishAgent_LocalProceeds" -v -count=1
```

Expected: both PASS.

- [ ] **Step 7: Commit**

```
git add proto/goqueue.proto proto/goqueue.pb.go internal/grpcapi/server.go internal/grpcapi/server_test.go
git commit -m "feat(grpcapi): PublishAgent emits NOT_LEADER with hint when not shard-local"
```

---

## Task 8: SDK transparent redirect

**Files:**
- Modify: `agentbus/publish.go`
- Modify: `agentbus/client.go`
- Create: `agentbus/redirect_test.go`

The SDK absorbs the cluster topology so end users don't have to. On `FailedPrecondition` with a `NotLeaderError` detail, the SDK:
1. Reads the leader hint.
2. Dials the leader (caching the resulting connection for subsequent publishes to the same session — micro-optimization deferred to M5; for M3 we redial every redirect).
3. Retries the publish exactly once.

If the retry also gets `NOT_LEADER` (rare, but happens during reassignment), we surface the original error so the caller can decide to back off.

- [ ] **Step 1: Read the current SDK Publish flow**

```
grep -n "PublishAgent\|func.*Connect" agentbus/*.go
```

Identify (a) the call site that invokes the gRPC `PublishAgent` and (b) how the gRPC connection is owned (client struct, factory, etc.).

- [ ] **Step 2: Add a redirect helper**

Create `agentbus/redirect.go`:

```go
package agentbus

import (
	"context"
	"errors"

	pb "github.com/khangpt2k6/AgentBus/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// notLeaderHint extracts a leader address hint from a gRPC error returned
// by PublishAgent. Returns ("", false) if the error is not a redirect.
func notLeaderHint(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	st, ok := status.FromError(err)
	if !ok {
		return "", false
	}
	if st.Code() != codes.FailedPrecondition {
		return "", false
	}
	for _, d := range st.Details() {
		if hint, ok := d.(*pb.NotLeaderError); ok {
			return hint.LeaderAddr, true
		}
	}
	return "", false
}

// dialLeader builds a transient gRPC client connection to the redirect
// target. The caller is responsible for closing it.
func dialLeader(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	if addr == "" {
		return nil, errors.New("agentbus: empty leader hint; cluster has no current leader for this shard")
	}
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}
```

- [ ] **Step 3: Wrap PublishAgent in the SDK with a single redirect retry**

Edit `agentbus/publish.go`. Find the function that calls `pb.QueueClient.PublishAgent(...)` (likely `(c *Client) PublishAgent(ctx, ev)`). Wrap its body. The exact existing code may differ; here is the canonical pattern to apply:

```go
func (c *Client) PublishAgent(ctx context.Context, ev AgentEvent) (*PublishResult, error) {
	res, err := c.publishAgentOnce(ctx, c.grpc, ev)
	if err == nil {
		return res, nil
	}
	hint, isRedirect := notLeaderHint(err)
	if !isRedirect {
		return nil, err
	}
	conn, dialErr := dialLeader(ctx, hint)
	if dialErr != nil {
		return nil, err // surface original, not dial; original has more detail
	}
	defer conn.Close()
	return c.publishAgentOnce(ctx, pb.NewQueueClient(conn), ev)
}

// publishAgentOnce is the previously-inline body of PublishAgent, now
// parameterized by the gRPC client to call.
func (c *Client) publishAgentOnce(ctx context.Context, qc pb.QueueClient, ev AgentEvent) (*PublishResult, error) {
	// ... existing call ...
}
```

The exact diff depends on the current code. Apply the smallest possible edit that achieves: (a) the original happy path is preserved when no redirect; (b) on `FailedPrecondition + NotLeaderError`, one retry against the hinted address is attempted; (c) the original error surfaces if the retry also fails.

- [ ] **Step 4: Write an integration-style SDK test**

Create `agentbus/redirect_test.go`:

```go
package agentbus

import (
	"context"
	"net"
	"testing"
	"time"

	pb "github.com/khangpt2k6/AgentBus/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stubServer accepts PublishAgent and either redirects (first call) or
// accepts (second call), then records that both happened.
type stubServer struct {
	pb.UnimplementedQueueServer
	calls       int
	redirectTo  string
	t           *testing.T
}

func (s *stubServer) PublishAgent(ctx context.Context, req *pb.PublishAgentRequest) (*pb.PublishAgentResponse, error) {
	s.calls++
	if s.calls == 1 {
		st := status.New(codes.FailedPrecondition, "not the leader of this session's shard")
		st, _ = st.WithDetails(&pb.NotLeaderError{LeaderAddr: s.redirectTo})
		return nil, st.Err()
	}
	return &pb.PublishAgentResponse{Offset: 42}, nil
}

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

func startStub(t *testing.T, redirectTo string) (addr string, srv *stubServer, stop func()) {
	t.Helper()
	port := freePort(t)
	addr = "127.0.0.1:" + itoa(port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	srv = &stubServer{redirectTo: redirectTo, t: t}
	pb.RegisterQueueServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	return addr, srv, func() { gs.Stop(); _ = lis.Close() }
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [16]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func TestSDK_PublishAgentFollowsRedirect(t *testing.T) {
	// Stub A redirects to stub B; stub B accepts.
	addrB, stubB, stopB := startStub(t, "")
	defer stopB()
	addrA, stubA, stopA := startStub(t, addrB)
	defer stopA()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := Connect(ctx, addrA)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close()

	if _, err := c.PublishAgent(ctx, AgentEvent{
		Tenant: "acme", Project: "support", SessionID: "s1",
		AgentID: "p1", Type: "tool.call",
		Payload: []byte("{}"),
	}); err != nil {
		t.Fatalf("PublishAgent: %v", err)
	}

	if stubA.calls != 1 || stubB.calls != 1 {
		t.Fatalf("call counts: A=%d B=%d, want 1/1", stubA.calls, stubB.calls)
	}
}
```

- [ ] **Step 5: Run the SDK tests**

```
go test ./agentbus/ -v -count=1
```

Expected: existing SDK tests pass, plus the new `TestSDK_PublishAgentFollowsRedirect`.

- [ ] **Step 6: Commit**

```
git add agentbus/
git commit -m "feat(sdk): PublishAgent transparently follows NOT_LEADER redirects"
```

---

## Task 9: `goqueue cluster route` debug subcommand

**Files:**
- Create: `internal/cli/cluster_route.go`
- Modify: `internal/cli/cluster.go`

A small debug command: given `--tenant`, `--project`, `--session`, prints which shard the session hashes to and which node currently leads that shard. Hits the broker's `/api/stats` plus a new `/api/route` endpoint.

- [ ] **Step 1: Add a `/api/route` HTTP handler in `cmd/broker/main.go`**

Find the mux setup. Insert after `/api/stats`:

```go
mux.HandleFunc("/api/route", func(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	q := r.URL.Query()
	tenant, project, session := q.Get("tenant"), q.Get("project"), q.Get("session")
	if cl == nil {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"cluster_enabled": false,
			"reason":          "broker is running in single-node mode; routing is a no-op",
		})
		return
	}
	dec := cl.Router().RouteSession(tenant, project, session)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"cluster_enabled":  true,
		"tenant":           tenant,
		"project":          project,
		"session":          session,
		"shard_id":         dec.ShardID,
		"leader_node_id":   dec.LeaderNodeID,
		"leader_client":    dec.LeaderClientAddr,
		"is_local":         dec.IsLocal,
	})
})
```

(The `cl` variable already exists in main.go from Task 9 of Plan 1. `dec` requires importing the router package; the import is `"github.com/khangpt2k6/AgentBus/internal/cluster/router"` if you need the `Decision` type, but referencing fields by name doesn't actually require the type to be in scope.)

- [ ] **Step 2: Add the CLI subcommand**

Create `internal/cli/cluster_route.go`:

```go
package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/spf13/cobra"
)

type clusterRouteResponse struct {
	ClusterEnabled bool   `json:"cluster_enabled"`
	Reason         string `json:"reason,omitempty"`
	Tenant         string `json:"tenant,omitempty"`
	Project        string `json:"project,omitempty"`
	Session        string `json:"session,omitempty"`
	ShardID        uint32 `json:"shard_id"`
	LeaderNodeID   string `json:"leader_node_id"`
	LeaderClient   string `json:"leader_client"`
	IsLocal        bool   `json:"is_local"`
}

func newClusterRouteCmd() *cobra.Command {
	var metricsURL, tenant, project, session string
	c := &cobra.Command{
		Use:   "route",
		Short: "Show which shard + leader a session would route to",
		RunE: func(_ *cobra.Command, _ []string) error {
			if tenant == "" || project == "" || session == "" {
				return fmt.Errorf("--tenant, --project, and --session are all required")
			}
			cli := &http.Client{Timeout: 3 * time.Second}
			q := url.Values{}
			q.Set("tenant", tenant)
			q.Set("project", project)
			q.Set("session", session)
			resp, err := cli.Get(metricsURL + "/api/route?" + q.Encode())
			if err != nil {
				return fmt.Errorf("fetch route: %w", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
			}
			var out clusterRouteResponse
			if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
				return fmt.Errorf("decode: %w", err)
			}
			if !out.ClusterEnabled {
				fmt.Printf("Cluster: not enabled (%s)\n", out.Reason)
				return nil
			}
			fmt.Printf("Session: %s/%s/%s\n", out.Tenant, out.Project, out.Session)
			fmt.Printf("Shard:   %d\n", out.ShardID)
			if out.LeaderNodeID == "" {
				fmt.Println("Leader:  (none — shard unassigned or current leader is dead)")
			} else {
				fmt.Printf("Leader:  %s (client addr: %s)\n", out.LeaderNodeID, out.LeaderClient)
			}
			fmt.Printf("Local:   %v\n", out.IsLocal)
			return nil
		},
	}
	c.Flags().StringVar(&metricsURL, "metrics-url", "http://localhost:2112", "broker metrics/admin URL")
	c.Flags().StringVar(&tenant, "tenant", "", "tenant (required)")
	c.Flags().StringVar(&project, "project", "", "project (required)")
	c.Flags().StringVar(&session, "session", "", "session id (required)")
	return c
}
```

- [ ] **Step 3: Wire the subcommand into the `cluster` group**

Edit `internal/cli/cluster.go`. Find the `newClusterCmd` function and add the route subcommand:

```go
func newClusterCmd(_ *options) *cobra.Command {
	c := &cobra.Command{
		Use:   "cluster",
		Short: "Inspect AgentBus cluster state",
	}
	c.AddCommand(newClusterStatusCmd())
	c.AddCommand(newClusterRouteCmd())
	return c
}
```

- [ ] **Step 4: Verify it builds and the help text shows**

```
go build -o bin/goqueue.exe ./cmd/goqueue
./bin/goqueue.exe cluster route --help
```

Expected: usage shows the four flags.

- [ ] **Step 5: Smoke-test against a live single-node cluster**

```
go build -o bin/broker.exe ./cmd/broker
./bin/broker.exe --cluster --node-id=n1 \
  --raft-bind=127.0.0.1:7001 --gossip-bind=127.0.0.1:8001 \
  --raft-dir=data/raft-n1 --peers=n1@127.0.0.1:7001 \
  --tcp-addr=:19090 --grpc-addr=:19095 --metrics-addr=:12112 \
  --wal-path=data/test.wal
```

(Run in the background — `&` on bash, `Start-Process` on PowerShell.) Wait ~6s for assignment to settle, then:

```
./bin/goqueue.exe cluster route \
  --metrics-url=http://localhost:12112 \
  --tenant=acme --project=support --session=sessA
```

Expected:
```
Session: acme/support/sessA
Shard:   <some number 0-31>
Leader:  n1 (client addr: 127.0.0.1:19095)
Local:   true
```

Kill the broker, clean up.

- [ ] **Step 6: Commit**

```
git add cmd/broker/main.go internal/cli/cluster_route.go internal/cli/cluster.go
git commit -m "feat(cli): goqueue cluster route subcommand + /api/route handler"
```

---

## Task 10: Wire the router into broker startup

**Files:**
- Modify: `cmd/broker/main.go`

The gRPC server now accepts a `RouteChecker`. In cluster mode, we adapt the `cluster.Cluster.Router()` to that interface and inject it.

- [ ] **Step 1: Add a thin adapter to cmd/broker/main.go**

Inside `main()` after the cluster startup block (right after the goroutine that bridges Raft state), find where the gRPC server is constructed:

```go
grpcSrv := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
grpcapi.Register(grpcSrv, grpcapi.NewServer(b, groups, m, logFile))
```

Capture the server and inject the route checker:

```go
gApi := grpcapi.NewServer(b, groups, m, logFile)
if cl != nil {
	gApi.SetRouteChecker(routeAdapter{cl: cl})
}
grpcSrv := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
grpcapi.Register(grpcSrv, gApi)
```

At the bottom of `cmd/broker/main.go` (alongside the existing types) add:

```go
type routeAdapter struct{ cl *cluster.Cluster }

func (r routeAdapter) RouteSession(tenant, project, session string) (bool, string) {
	dec := r.cl.Router().RouteSession(tenant, project, session)
	return dec.IsLocal, dec.LeaderClientAddr
}
```

You'll need an import for `"github.com/khangpt2k6/AgentBus/internal/cluster"` — it's likely already there from Plan 1's Task 9. Confirm with:

```
grep "internal/cluster\"" cmd/broker/main.go
```

- [ ] **Step 2: Verify build and tests**

```
go build ./...
go test ./... -count=1 -short
go test ./internal/cluster/ -tags=cluster_integration -run TestThreeNode -v -timeout 90s
```

Expected: all green.

- [ ] **Step 3: Smoke-test a 3-node cluster sees real cross-node redirects**

```
docker compose -f deploy/cluster.yml up --build -d
sleep 12
# Query all three nodes for the same session — they should agree on the leader.
for port in 12112 12113 12114; do
  echo "--- node on port $port ---"
  ./bin/goqueue.exe cluster route \
    --metrics-url=http://localhost:$port \
    --tenant=acme --project=support --session=sessA
done
docker compose -f deploy/cluster.yml down -v
```

Expected: all three nodes report the *same* `Shard`, *same* `Leader`, but only the leader node shows `Local: true`.

- [ ] **Step 4: Commit**

```
git add cmd/broker/main.go
git commit -m "feat(broker): inject cluster router into gRPC server for NOT_LEADER redirects"
```

---

## Task 11: Documentation + spec checkboxes

**Files:**
- Modify: `docs/cluster.md`
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-05-16-distributed-v1-design.md`

- [ ] **Step 1: Update docs/cluster.md with the M3 section**

Replace the existing "What's NOT yet in this milestone" section in `docs/cluster.md` with:

```markdown
## What's shipped through M3

- **Gossip membership** (SWIM) — nodes find each other within ~3s.
- **Metadata Raft** — strongly-consistent cluster state with on-disk durability.
- **Session routing** — each `tenant/project/session` hashes to one of 32 shards; each shard has one elected leader. Publishes that arrive at the wrong node return `NOT_LEADER` with the leader's gRPC address; the AgentBus SDK retries transparently.
- **Debug tooling** — `goqueue cluster route` prints exactly where a session would land.

## What's NOT yet shipped

- **No WAL replication.** A publish lands in *one* node's WAL. If that node dies, the data is gone. Replication is M4.
- **No failover.** If a shard leader dies, the assigner reassigns within ~2s — but any data the dead leader held is lost (no replicas yet). M4 fixes the data-loss risk; M5 makes failover seamless.
- **No term-tagged writes.** A network-partitioned stale leader can technically still accept writes until the partition heals. Term-tagging is M5.
```

Also update the failure modes table to reflect M3 routing behavior. Replace it with:

```markdown
## Failure modes (M3 routing only)

| What you do | What happens |
|-------------|--------------|
| Publish a session to the wrong node | SDK gets `NOT_LEADER`, transparently redirects to the leader, retries. User sees no error. |
| Kill a non-leader node | Within ~3s, gossip marks it dead. Assigner reassigns the shards it held (data on it is lost — M4 fixes this). |
| Kill the metadata Raft leader | Within ~1s a new metadata leader is elected. Then within ~2s the new leader's assigner pass picks up shard reassignment for any shards held by the killed node. |
| Network partition | Minority side cannot elect a new metadata leader OR run the assigner; majority side keeps routing. |
```

- [ ] **Step 2: Update README status callout**

Find the existing callout (currently mentioning M0-M2 foundation). Replace with:

```markdown
> **Status:** Single-node broker today. The distributed v1 **foundation + routing** (3-node cluster forms, gossips, elects a real metadata Raft leader, **and routes session traffic across nodes via consistent hashing**) ships on the [`feat/cluster-v1`](https://github.com/khangpt2k6/AgentBus/tree/feat/cluster-v1) branch — see [docs/cluster.md](docs/cluster.md) for usage and [docs/superpowers/specs/2026-05-16-distributed-v1-design.md](docs/superpowers/specs/2026-05-16-distributed-v1-design.md) for the full design. Up next: ISR replication (M4) and seamless failover (M5).
```

- [ ] **Step 3: Check spec boxes**

Edit `docs/superpowers/specs/2026-05-16-distributed-v1-design.md` § 9. Update only this row:

```markdown
- [x] Publishing to two different sessions lands them on different shards (verified by `goqueue cluster route`) — *M3*
```

(The other boxes for M5/M6 stay unchecked.)

- [ ] **Step 4: Commit**

```
git add docs/cluster.md README.md docs/superpowers/specs/2026-05-16-distributed-v1-design.md
git commit -m "docs(cluster): document M3 routing; check spec box for shard routing"
```

---

## Final verification

- [ ] **Run the entire test suite**

```
go test ./... -count=1
go test ./internal/cluster/ -tags=cluster_integration -run TestThreeNode -v -timeout 90s
```

Expected: every package passes; integration test PASSES within 30s (more than M2's 10s because we now also wait for shard assignment).

- [ ] **Smoke-test 3-node cluster end-to-end via the Compose stack**

```
docker compose -f deploy/cluster.yml up --build -d
sleep 15

# Cluster formed:
for port in 12112 12113 12114; do
  echo "--- status on $port ---"
  ./bin/goqueue.exe cluster status --metrics-url=http://localhost:$port
done

# Routing works across all 3 nodes:
for port in 12112 12113 12114; do
  echo "--- route on $port ---"
  ./bin/goqueue.exe cluster route \
    --metrics-url=http://localhost:$port \
    --tenant=acme --project=support --session=sessA
done

# Actually publishing through the SDK should not return an error even when
# the client points at the "wrong" node:
./bin/goqueue.exe publish-agent --grpc --addr=localhost:19095 \
  --tenant=acme --project=support --session=sessA --agent=planner \
  --type=tool.call --payload='{"q":"hi"}'

docker compose -f deploy/cluster.yml down -v
```

Expected: all `cluster status` outputs converge on one leader, all `cluster route` outputs report the same shard+leader for `sessA`, and the publish succeeds regardless of which node `--addr` points to.

- [ ] **Push the branch**

```
git push origin feat/cluster-v1
```

---

## What ships at the end of this plan

A broker that:

- Forms a 3-node cluster with real Raft metadata + SWIM gossip (M0–M2 baseline).
- Routes every agent-session publish to the shard leader via consistent hashing.
- Returns `NOT_LEADER` with a usable gRPC hint when a client hits the wrong node.
- Has an SDK that follows the redirect transparently — users see one call, the SDK handles the rest.
- Has a `goqueue cluster route` debug CLI for operators.
- Has a 3-node integration test asserting routing converges within ~30s.

What's intentionally **not** shipped here (deferred to Plan 2b — M4):

- WAL replication between shard leaders and followers (data is currently single-copy per shard).
- Quorum acks (`acks=quorum`).
- Follower-catchup-after-restart.

And Plan 2c — M5:

- Term-tagged writes (preventing stale-leader writes during partition heals).
- Producer-side sequence preservation across leader changes.
