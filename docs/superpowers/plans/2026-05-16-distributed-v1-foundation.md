# Distributed v1 Foundation (M0–M2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the cluster *foundation* layer of AgentBus distributed v1 — three broker nodes form a cluster via SWIM gossip, elect a metadata Raft leader, and expose cluster status to operators. No data is yet routed or replicated across the cluster; the data plane is built in Plan 2.

**Architecture:** New `internal/cluster/` package family that wraps `hashicorp/memberlist` (gossip membership) and `hashicorp/raft` + `raft-boltdb` (metadata consensus). A top-level `Cluster` type composes both subsystems. The broker binary gains opt-in `--cluster` mode that starts the `Cluster` alongside the existing single-node services; with `--cluster=false` (default) behavior is byte-identical to today.

**Tech Stack:** Go 1.26, `github.com/hashicorp/raft`, `github.com/hashicorp/raft-boltdb/v2`, `github.com/hashicorp/memberlist`, existing `cobra` CLI, existing `prometheus/client_golang` metrics.

**Spec:** [docs/superpowers/specs/2026-05-16-distributed-v1-design.md](../specs/2026-05-16-distributed-v1-design.md)

**SDK constraint:** The public `agentbus/` Go SDK MUST NOT change in this plan. Cluster mode is broker-side only in M0–M2. SDK-visible changes (NOT_LEADER redirect) ship in Plan 2 (M3).

**Shell portability:** Smoke-test snippets use POSIX shell idioms (`&`, `kill %1`, `wait`) for brevity. On Windows PowerShell, run the broker in a separate terminal and use `Stop-Process` to terminate. The canonical proof of correctness is the cross-platform `go test` integration suite in Task 8 — smoke tests are supplementary manual checks.

---

## File Structure

**New files:**

| Path | Responsibility |
|---|---|
| `internal/cluster/config.go` | `Config` struct + parsing of `--peers` flag |
| `internal/cluster/config_test.go` | Unit tests for config parsing |
| `internal/cluster/membership/membership.go` | Wraps `memberlist.Memberlist`; emits Alive/Dead events |
| `internal/cluster/membership/membership_test.go` | In-process 2-node gossip test |
| `internal/cluster/metadata/fsm.go` | Raft FSM: members map + shard→leader map |
| `internal/cluster/metadata/fsm_test.go` | Unit tests for FSM Apply/Snapshot/Restore |
| `internal/cluster/metadata/metadata.go` | Wraps `hashicorp/raft` (transport, log store, snapshot store, bootstrap) |
| `internal/cluster/metadata/metadata_test.go` | Single-node Raft bootstrap test |
| `internal/cluster/cluster.go` | Top-level `Cluster` type composing membership + metadata; lifecycle (Start/Stop) |
| `internal/cluster/cluster_test.go` | 3-node in-process integration test (gated by build tag) |
| `internal/cli/cluster.go` | `goqueue cluster status` subcommand |
| `deploy/cluster.yml` | Docker Compose for 3-node cluster |
| `docs/cluster.md` | Operator-facing docs for cluster mode |

**Modified files:**

| Path | Change |
|---|---|
| `go.mod` / `go.sum` | Add raft + memberlist + raft-boltdb |
| `README.md` | Lines ~115–119: move "single-node today" admission above install instructions; link the spec |
| `cmd/broker/main.go` | Add cluster flags; if `--cluster`, start `Cluster` and wire its state into existing `raftRuntimeState` so dashboard reflects real Raft instead of manual overrides |
| `internal/cli/root.go` | Register `cluster` subcommand |

**Branch:** all work lands on `feat/cluster-v1` branch; merge to `main` only after Plan 1 is complete and reviewed.

---

## Task 1: Create branch + README honesty pass (M0)

**Files:**
- Modify: `README.md` (lines 115–119)

- [ ] **Step 1: Create the feature branch**

```bash
git checkout -b feat/cluster-v1
git status
```

Expected: on branch `feat/cluster-v1`, working tree clean (or the spec/plan files committed on main already).

- [ ] **Step 2: Move the "single-node today" admission to the top of the README**

In [README.md](../../../README.md), the current "Current Scope" section (around lines 115–121) explains the single-node reality but sits *below* install instructions. Move a one-line honest scope note to immediately after the architecture image, so any skeptical reader sees it before scrolling.

Apply this edit to README.md — insert the callout immediately after the "Architecture" section (after the closing `</table>` of the layer table) and before `## Current Scope`:

```markdown
> **Status:** Single-node broker today. Distributed v1 (3-node cluster with metadata Raft + ISR replication + leader failover) is in active development on the [`feat/cluster-v1`](https://github.com/khangpt2k6/AgentBus/tree/feat/cluster-v1) branch — design spec in [docs/superpowers/specs/2026-05-16-distributed-v1-design.md](docs/superpowers/specs/2026-05-16-distributed-v1-design.md).
```

The existing `## Current Scope` section then becomes a more detailed restatement, which is fine.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs(readme): surface single-node status above install steps"
```

Expected: commit succeeds, no Claude attribution in the message (per global CLAUDE.md).

---

## Task 2: Add hashicorp/raft + memberlist + raft-boltdb dependencies

**Files:**
- Modify: `go.mod`, `go.sum`

- [ ] **Step 1: Add the three libraries**

```bash
go get github.com/hashicorp/raft@latest
go get github.com/hashicorp/raft-boltdb/v2@latest
go get github.com/hashicorp/memberlist@latest
```

Expected: `go.mod` gains three new `require` lines; `go.sum` populated.

- [ ] **Step 2: Verify the project still builds**

```bash
go build ./...
```

Expected: no errors. If module-resolution errors appear, run `go mod tidy` and re-build.

- [ ] **Step 3: Run existing tests to ensure no regression**

```bash
go test ./... -count=1
```

Expected: all existing tests pass (the new libraries are not yet referenced).

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum
git commit -m "deps: add hashicorp/raft, raft-boltdb/v2, memberlist for cluster v1"
```

---

## Task 3: cluster.Config — parsing and validation

**Files:**
- Create: `internal/cluster/config.go`
- Create: `internal/cluster/config_test.go`

The `Config` is shared by membership + metadata subsystems. Keeping it in its own package level makes the dependency graph clean and lets the broker's `main.go` populate one struct from flags.

- [ ] **Step 1: Write the failing test**

Create `internal/cluster/config_test.go`:

```go
package cluster

import (
	"reflect"
	"testing"
)

func TestParsePeers_Valid(t *testing.T) {
	got, err := ParsePeers("n1@127.0.0.1:7001,n2@127.0.0.1:7002,n3@127.0.0.1:7003")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []Peer{
		{NodeID: "n1", RaftAddr: "127.0.0.1:7001"},
		{NodeID: "n2", RaftAddr: "127.0.0.1:7002"},
		{NodeID: "n3", RaftAddr: "127.0.0.1:7003"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ParsePeers mismatch:\ngot:  %#v\nwant: %#v", got, want)
	}
}

func TestParsePeers_EmptyStringIsEmptySlice(t *testing.T) {
	got, err := ParsePeers("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want empty slice, got %v", got)
	}
}

func TestParsePeers_RejectsMalformed(t *testing.T) {
	cases := []string{
		"missing-at-sign:7001",
		"n1@",
		"@127.0.0.1:7001",
		"n1@127.0.0.1",       // no port
		"n1@127.0.0.1:notnum", // bad port
	}
	for _, c := range cases {
		if _, err := ParsePeers(c); err == nil {
			t.Errorf("ParsePeers(%q) want error, got nil", c)
		}
	}
}

func TestConfig_ValidateRequiresNodeID(t *testing.T) {
	c := Config{
		Peers:       []Peer{{NodeID: "n1", RaftAddr: "127.0.0.1:7001"}},
		RaftBind:    "127.0.0.1:7001",
		GossipBind:  "127.0.0.1:8001",
		RaftDir:     "/tmp/raft",
	}
	if err := c.Validate(); err == nil {
		t.Fatal("want error for empty NodeID")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./internal/cluster/ -run TestParsePeers -v
```

Expected: FAIL — `package cluster` does not exist yet.

- [ ] **Step 3: Implement `Config` and `ParsePeers`**

Create `internal/cluster/config.go`:

```go
// Package cluster contains the distributed-mode subsystems for AgentBus.
// It is opt-in: a broker that does not pass --cluster does not import or
// initialize any code here.
package cluster

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// Peer identifies one node in the cluster by its stable NodeID and the
// TCP address its Raft transport listens on.
type Peer struct {
	NodeID   string
	RaftAddr string
}

// Config bundles every knob the cluster subsystems need. Populated from
// CLI flags in cmd/broker/main.go when --cluster is set.
type Config struct {
	NodeID     string
	RaftBind   string
	GossipBind string
	RaftDir    string
	Peers      []Peer
}

// ParsePeers reads the --peers flag value, comma-separated "id@host:port".
// Empty string is valid and returns an empty slice (single-node bootstrap).
func ParsePeers(raw string) ([]Peer, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]Peer, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		at := strings.Index(p, "@")
		if at <= 0 || at == len(p)-1 {
			return nil, fmt.Errorf("peer %q must be of the form id@host:port", p)
		}
		id := strings.TrimSpace(p[:at])
		addr := strings.TrimSpace(p[at+1:])
		if id == "" {
			return nil, fmt.Errorf("peer %q has empty node id", p)
		}
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, fmt.Errorf("peer %q has invalid address: %w", p, err)
		}
		if _, err := strconv.Atoi(port); err != nil {
			return nil, fmt.Errorf("peer %q has non-numeric port: %w", p, err)
		}
		if host == "" {
			return nil, fmt.Errorf("peer %q has empty host", p)
		}
		out = append(out, Peer{NodeID: id, RaftAddr: addr})
	}
	return out, nil
}

// Validate checks the Config has the minimum required fields populated.
func (c Config) Validate() error {
	if strings.TrimSpace(c.NodeID) == "" {
		return fmt.Errorf("NodeID is required")
	}
	if strings.TrimSpace(c.RaftBind) == "" {
		return fmt.Errorf("RaftBind is required")
	}
	if strings.TrimSpace(c.GossipBind) == "" {
		return fmt.Errorf("GossipBind is required")
	}
	if strings.TrimSpace(c.RaftDir) == "" {
		return fmt.Errorf("RaftDir is required")
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
go test ./internal/cluster/ -run TestParsePeers -v
go test ./internal/cluster/ -run TestConfig -v
```

Expected: PASS for all sub-tests.

- [ ] **Step 5: Commit**

```bash
git add internal/cluster/config.go internal/cluster/config_test.go
git commit -m "feat(cluster): Config + ParsePeers for --peers flag"
```

---

## Task 4: cluster/membership — wrap hashicorp/memberlist

**Files:**
- Create: `internal/cluster/membership/membership.go`
- Create: `internal/cluster/membership/membership_test.go`

The membership subsystem owns the gossip layer. It exposes:
- `Start(cfg) (*Membership, error)` — joins or bootstraps a gossip cluster.
- `Alive() []string` — current list of live NodeIDs.
- `Events() <-chan Event` — node-join / node-leave notifications.
- `Shutdown() error` — graceful stop.

- [ ] **Step 1: Write the failing test**

Create `internal/cluster/membership/membership_test.go`:

```go
package membership

import (
	"fmt"
	"net"
	"testing"
	"time"
)

// freePort returns an OS-assigned free TCP/UDP port on 127.0.0.1.
// Memberlist uses both TCP (for full-state sync) and UDP (for ping/gossip)
// on the same port number, so we just need a free integer.
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

func TestMembership_TwoNodesFormCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("network-bound; skipped with -short")
	}
	p1, p2 := freePort(t), freePort(t)
	addr1 := fmt.Sprintf("127.0.0.1:%d", p1)
	addr2 := fmt.Sprintf("127.0.0.1:%d", p2)

	m1, err := Start(Config{NodeID: "n1", GossipBind: addr1})
	if err != nil {
		t.Fatalf("start m1: %v", err)
	}
	defer m1.Shutdown()

	m2, err := Start(Config{NodeID: "n2", GossipBind: addr2, JoinAddrs: []string{addr1}})
	if err != nil {
		t.Fatalf("start m2: %v", err)
	}
	defer m2.Shutdown()

	// Wait up to 3s for both nodes to see each other.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(m1.Alive()) == 2 && len(m2.Alive()) == 2 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("nodes did not converge: m1.Alive()=%v m2.Alive()=%v", m1.Alive(), m2.Alive())
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./internal/cluster/membership/ -v
```

Expected: FAIL — `package membership` does not exist.

- [ ] **Step 3: Implement membership wrapper**

Create `internal/cluster/membership/membership.go`:

```go
// Package membership wraps hashicorp/memberlist so the rest of the
// cluster code talks to a small AgentBus-specific surface instead of
// directly to memberlist's broader API.
package membership

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"

	"github.com/hashicorp/memberlist"
)

// Config is the minimum the membership subsystem needs at startup.
type Config struct {
	NodeID     string
	GossipBind string
	JoinAddrs  []string // empty = bootstrap; non-empty = join existing
}

// EventType discriminates between node lifecycle signals.
type EventType int

const (
	EventJoin EventType = iota
	EventLeave
)

// Event is delivered on Events() when membership changes.
type Event struct {
	Type   EventType
	NodeID string
	Addr   string
}

// Membership is the live handle to a running gossip cluster member.
type Membership struct {
	ml     *memberlist.Memberlist
	events chan Event

	mu   sync.RWMutex
	dead bool
}

// Start creates the local member and joins the cluster if JoinAddrs is set.
func Start(cfg Config) (*Membership, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("NodeID is required")
	}
	host, portStr, err := net.SplitHostPort(cfg.GossipBind)
	if err != nil {
		return nil, fmt.Errorf("GossipBind %q invalid: %w", cfg.GossipBind, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("GossipBind %q port not numeric: %w", cfg.GossipBind, err)
	}

	mc := memberlist.DefaultLocalConfig()
	mc.Name = cfg.NodeID
	mc.BindAddr = host
	mc.BindPort = port
	mc.AdvertiseAddr = host
	mc.AdvertisePort = port
	mc.LogOutput = io.Discard // suppress library chatter; callers use Events() instead

	m := &Membership{
		events: make(chan Event, 128),
	}
	mc.Events = &delegate{out: m.events}

	ml, err := memberlist.Create(mc)
	if err != nil {
		return nil, fmt.Errorf("memberlist create: %w", err)
	}
	m.ml = ml

	if len(cfg.JoinAddrs) > 0 {
		if _, err := ml.Join(cfg.JoinAddrs); err != nil {
			_ = ml.Shutdown()
			return nil, fmt.Errorf("memberlist join: %w", err)
		}
	}
	return m, nil
}

// Alive returns the NodeIDs of all currently-alive cluster members,
// including this node itself.
func (m *Membership) Alive() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.dead || m.ml == nil {
		return nil
	}
	members := m.ml.Members()
	out := make([]string, 0, len(members))
	for _, n := range members {
		out = append(out, n.Name)
	}
	return out
}

// Events returns the channel of lifecycle events. Buffered; callers must
// drain it or events are dropped.
func (m *Membership) Events() <-chan Event { return m.events }

// Shutdown leaves the cluster gracefully and stops the local listener.
func (m *Membership) Shutdown() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.dead {
		return nil
	}
	m.dead = true
	if m.ml == nil {
		return nil
	}
	// Best-effort graceful leave; ignore timeout error.
	_ = m.ml.Leave(0)
	return m.ml.Shutdown()
}

type delegate struct {
	out chan<- Event
}

func (d *delegate) NotifyJoin(n *memberlist.Node) {
	d.send(Event{Type: EventJoin, NodeID: n.Name, Addr: n.Address()})
}
func (d *delegate) NotifyLeave(n *memberlist.Node) {
	d.send(Event{Type: EventLeave, NodeID: n.Name, Addr: n.Address()})
}
func (d *delegate) NotifyUpdate(n *memberlist.Node) {} // not used in v1

func (d *delegate) send(ev Event) {
	select {
	case d.out <- ev:
	default:
		// Channel full; drop. Production code may want a counter here.
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
go test ./internal/cluster/membership/ -v -count=1
```

Expected: PASS within ~1–2 seconds.

- [ ] **Step 5: Commit**

```bash
git add internal/cluster/membership/
git commit -m "feat(cluster/membership): SWIM gossip wrapper over hashicorp/memberlist"
```

---

## Task 5: cluster/metadata/fsm — Raft FSM with members + shard map

**Files:**
- Create: `internal/cluster/metadata/fsm.go`
- Create: `internal/cluster/metadata/fsm_test.go`

The FSM is the deterministic state machine that every Raft node applies committed log entries against. Keep it small in v1: a members map and a shard→leader map. All mutations go through JSON-encoded commands.

- [ ] **Step 1: Write the failing test**

Create `internal/cluster/metadata/fsm_test.go`:

```go
package metadata

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/hashicorp/raft"
)

func applyCmd(t *testing.T, f *FSM, c Command) interface{} {
	t.Helper()
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return f.Apply(&raft.Log{Data: b})
}

func TestFSM_AddAndRemoveMember(t *testing.T) {
	f := NewFSM()
	applyCmd(t, f, Command{Op: OpAddMember, NodeID: "n1", Addr: "127.0.0.1:7001"})
	applyCmd(t, f, Command{Op: OpAddMember, NodeID: "n2", Addr: "127.0.0.1:7002"})
	if got, want := len(f.Members()), 2; got != want {
		t.Fatalf("Members len = %d, want %d", got, want)
	}
	applyCmd(t, f, Command{Op: OpRemoveMember, NodeID: "n1"})
	if got, want := len(f.Members()), 1; got != want {
		t.Fatalf("after remove, len = %d, want %d", got, want)
	}
}

func TestFSM_SetShardLeader(t *testing.T) {
	f := NewFSM()
	applyCmd(t, f, Command{Op: OpSetShardLeader, Shard: 7, NodeID: "n3"})
	if got := f.ShardLeader(7); got != "n3" {
		t.Fatalf("ShardLeader(7) = %q, want %q", got, "n3")
	}
	if got := f.ShardLeader(8); got != "" {
		t.Fatalf("ShardLeader(8) = %q, want empty", got)
	}
}

func TestFSM_SnapshotRestoreRoundTrip(t *testing.T) {
	src := NewFSM()
	applyCmd(t, src, Command{Op: OpAddMember, NodeID: "n1", Addr: "a:1"})
	applyCmd(t, src, Command{Op: OpAddMember, NodeID: "n2", Addr: "a:2"})
	applyCmd(t, src, Command{Op: OpSetShardLeader, Shard: 4, NodeID: "n2"})

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
	if got, want := len(dst.Members()), 2; got != want {
		t.Fatalf("restored Members len = %d, want %d", got, want)
	}
	if got := dst.ShardLeader(4); got != "n2" {
		t.Fatalf("restored ShardLeader(4) = %q, want %q", got, "n2")
	}
}

// memSink satisfies raft.SnapshotSink for in-memory testing.
type memSink struct{ *bytes.Buffer }

func (m *memSink) ID() string     { return "test-snap" }
func (m *memSink) Cancel() error  { return nil }
func (m *memSink) Close() error   { return nil }
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./internal/cluster/metadata/ -run TestFSM -v
```

Expected: FAIL — `package metadata` not found.

- [ ] **Step 3: Implement the FSM**

Create `internal/cluster/metadata/fsm.go`:

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
	OpAddMember      Op = "add_member"
	OpRemoveMember   Op = "remove_member"
	OpSetShardLeader Op = "set_shard_leader"
)

// Command is the JSON envelope serialized into every Raft log entry.
// Keeping the wire format JSON keeps debugging easy; switch to protobuf
// only if FSM throughput becomes a bottleneck (it won't in v1).
type Command struct {
	Op      Op     `json:"op"`
	NodeID  string `json:"node_id,omitempty"`
	Addr    string `json:"addr,omitempty"`
	Shard   uint32 `json:"shard,omitempty"`
}

// FSM is the in-memory projection of all committed Raft log entries.
type FSM struct {
	mu           sync.RWMutex
	members      map[string]string // nodeID -> raft addr
	shardLeaders map[uint32]string // shardID -> nodeID
}

func NewFSM() *FSM {
	return &FSM{
		members:      make(map[string]string),
		shardLeaders: make(map[uint32]string),
	}
}

// Apply is called by Raft for every committed log entry on every node.
// Must be deterministic and side-effect-free outside the FSM itself.
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
		f.members[cmd.NodeID] = cmd.Addr
	case OpRemoveMember:
		delete(f.members, cmd.NodeID)
		// Drop any shard leadership held by the removed node.
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
	default:
		return fmt.Errorf("unknown op %q", cmd.Op)
	}
	return nil
}

// Members returns a copy of the members map for safe read by callers.
func (f *FSM) Members() map[string]string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]string, len(f.members))
	for k, v := range f.members {
		out[k] = v
	}
	return out
}

// ShardLeader returns the nodeID currently leading shardID, or "" if unset.
func (f *FSM) ShardLeader(shardID uint32) string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.shardLeaders[shardID]
}

// snapshotPayload is the on-disk format of an FSM snapshot.
type snapshotPayload struct {
	Members      map[string]string `json:"members"`
	ShardLeaders map[uint32]string `json:"shard_leaders"`
}

// Snapshot is invoked by Raft when it decides to compact the log.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	payload := snapshotPayload{
		Members:      make(map[string]string, len(f.members)),
		ShardLeaders: make(map[uint32]string, len(f.shardLeaders)),
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

// Restore loads a snapshot into the FSM, replacing all in-memory state.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var payload snapshotPayload
	if err := json.NewDecoder(rc).Decode(&payload); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.members = payload.Members
	f.shardLeaders = payload.ShardLeaders
	if f.members == nil {
		f.members = make(map[string]string)
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

- [ ] **Step 4: Run test to verify it passes**

```bash
go test ./internal/cluster/metadata/ -run TestFSM -v
```

Expected: PASS for all three sub-tests.

- [ ] **Step 5: Commit**

```bash
git add internal/cluster/metadata/fsm.go internal/cluster/metadata/fsm_test.go
git commit -m "feat(cluster/metadata): FSM for members + shard-leader map with snapshot/restore"
```

---

## Task 6: cluster/metadata/metadata — Raft node wrapper

**Files:**
- Create: `internal/cluster/metadata/metadata.go`
- Create: `internal/cluster/metadata/metadata_test.go`

The wrapper handles boilerplate: BoltDB stores, file snapshot store, TCP transport, BootstrapCluster, and exposing a small API (`Apply`, `IsLeader`, `Leader`, `LeaderCh`, `Shutdown`).

- [ ] **Step 1: Write the failing test**

Create `internal/cluster/metadata/metadata_test.go`:

```go
package metadata

import (
	"encoding/json"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func TestSingleNodeBootstrap_BecomesLeader(t *testing.T) {
	if testing.Short() {
		t.Skip("network-bound; skipped with -short")
	}
	dir := t.TempDir()
	addr := freeAddr(t)
	m, err := Start(Options{
		NodeID:        "n1",
		BindAddr:      addr,
		AdvertiseAddr: addr,
		DataDir:       dir,
		Bootstrap:     true,
		InitialPeers: []Peer{
			{NodeID: "n1", Addr: addr},
		},
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer m.Shutdown()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if m.IsLeader() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !m.IsLeader() {
		t.Fatalf("single-node Raft did not become leader within 5s")
	}

	// Submit one command and verify it lands in the FSM.
	cmd, _ := json.Marshal(Command{Op: OpAddMember, NodeID: "n1", Addr: addr})
	if err := m.Apply(cmd, 2*time.Second); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := m.FSM().Members()["n1"]; got != addr {
		t.Fatalf("FSM Members[n1] = %q, want %q", got, addr)
	}

	// Sanity: data dir exists with a Raft state file.
	if _, err := filepath.Glob(filepath.Join(dir, "raft.db")); err != nil {
		t.Fatalf("glob: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./internal/cluster/metadata/ -run TestSingleNode -v
```

Expected: FAIL — `Start`, `Options`, etc. not defined.

- [ ] **Step 3: Implement the Raft wrapper**

Create `internal/cluster/metadata/metadata.go`:

```go
package metadata

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// Peer is the bootstrap-time description of a cluster member.
type Peer struct {
	NodeID string
	Addr   string
}

// Options bundles every knob the Raft wrapper needs.
type Options struct {
	NodeID        string
	BindAddr      string // listen address for the Raft transport
	AdvertiseAddr string // address peers should dial to reach this node
	DataDir       string // where bolt logs + snapshots live
	Bootstrap     bool   // call BootstrapCluster on first start
	InitialPeers  []Peer // server list when Bootstrap is true
	LogOutput     io.Writer // optional; defaults to os.Stderr
}

// Metadata is the running handle to a Raft node.
type Metadata struct {
	raft *raft.Raft
	fsm  *FSM
	tx   raft.Transport
}

// Start brings up a Raft node. If opts.Bootstrap is true and the on-disk
// state is fresh, BootstrapCluster is called with opts.InitialPeers.
func Start(opts Options) (*Metadata, error) {
	if opts.NodeID == "" {
		return nil, fmt.Errorf("NodeID required")
	}
	if opts.BindAddr == "" {
		return nil, fmt.Errorf("BindAddr required")
	}
	if opts.AdvertiseAddr == "" {
		opts.AdvertiseAddr = opts.BindAddr
	}
	if opts.DataDir == "" {
		return nil, fmt.Errorf("DataDir required")
	}
	if opts.LogOutput == nil {
		opts.LogOutput = os.Stderr
	}
	if err := os.MkdirAll(opts.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir DataDir: %w", err)
	}

	fsm := NewFSM()

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(opts.DataDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("bolt log store: %w", err)
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(opts.DataDir, "raft-stable.db"))
	if err != nil {
		return nil, fmt.Errorf("bolt stable store: %w", err)
	}
	snapStore, err := raft.NewFileSnapshotStore(opts.DataDir, 2, opts.LogOutput)
	if err != nil {
		return nil, fmt.Errorf("file snapshot store: %w", err)
	}

	advAddr, err := net.ResolveTCPAddr("tcp", opts.AdvertiseAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve advertise: %w", err)
	}
	tx, err := raft.NewTCPTransportWithLogger(
		opts.BindAddr,
		advAddr,
		3,
		10*time.Second,
		log.New(opts.LogOutput, "raft-tcp: ", log.LstdFlags),
	)
	if err != nil {
		return nil, fmt.Errorf("tcp transport: %w", err)
	}

	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(opts.NodeID)
	cfg.LogOutput = opts.LogOutput
	// Snappier election timings for local-cluster demos; production users
	// can override via env later.
	cfg.HeartbeatTimeout = 500 * time.Millisecond
	cfg.ElectionTimeout = 500 * time.Millisecond
	cfg.LeaderLeaseTimeout = 250 * time.Millisecond
	cfg.CommitTimeout = 50 * time.Millisecond

	r, err := raft.NewRaft(cfg, fsm, logStore, stableStore, snapStore, tx)
	if err != nil {
		return nil, fmt.Errorf("raft new: %w", err)
	}

	if opts.Bootstrap {
		servers := make([]raft.Server, 0, len(opts.InitialPeers))
		for _, p := range opts.InitialPeers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(p.NodeID),
				Address: raft.ServerAddress(p.Addr),
			})
		}
		fut := r.BootstrapCluster(raft.Configuration{Servers: servers})
		if err := fut.Error(); err != nil && err != raft.ErrCantBootstrap {
			return nil, fmt.Errorf("bootstrap: %w", err)
		}
	}

	return &Metadata{raft: r, fsm: fsm, tx: tx}, nil
}

// FSM returns the underlying state machine for read-only inspection.
func (m *Metadata) FSM() *FSM { return m.fsm }

// IsLeader reports whether this node is currently the Raft leader.
func (m *Metadata) IsLeader() bool {
	return m.raft.State() == raft.Leader
}

// Leader returns the current leader's transport address, or "" if unknown.
func (m *Metadata) Leader() string {
	addr, _ := m.raft.LeaderWithID()
	return string(addr)
}

// LeaderCh forwards Raft's leadership notification channel.
// Receivers get `true` when this node becomes leader, `false` when it loses.
func (m *Metadata) LeaderCh() <-chan bool { return m.raft.LeaderCh() }

// Apply submits a serialized Command to the Raft log. Returns once it has
// been committed and applied by the FSM, or an error.
func (m *Metadata) Apply(cmd []byte, timeout time.Duration) error {
	fut := m.raft.Apply(cmd, timeout)
	if err := fut.Error(); err != nil {
		return err
	}
	if resp := fut.Response(); resp != nil {
		if e, ok := resp.(error); ok {
			return e
		}
	}
	return nil
}

// Shutdown stops Raft and closes the transport. Safe to call once.
func (m *Metadata) Shutdown() error {
	fut := m.raft.Shutdown()
	return fut.Error()
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
go test ./internal/cluster/metadata/ -run TestSingleNode -v -timeout 30s
```

Expected: PASS within ~2 seconds. (A single-node Raft elects itself almost immediately.)

- [ ] **Step 5: Commit**

```bash
git add internal/cluster/metadata/metadata.go internal/cluster/metadata/metadata_test.go
git commit -m "feat(cluster/metadata): Raft node wrapper with bolt store + TCP transport"
```

---

## Task 7: cluster.Cluster — top-level composition

**Files:**
- Create: `internal/cluster/cluster.go`

`Cluster` orchestrates membership + metadata. Its job in M0–M2 is small: start both, ensure metadata's bootstrap peer list matches the configured peers, expose a `Status()` method, and propagate Shutdown.

- [ ] **Step 1: Write the implementation**

Create `internal/cluster/cluster.go`:

```go
package cluster

import (
	"fmt"
	"io"
	"os"

	"github.com/khangpt2k6/AgentBus/internal/cluster/membership"
	"github.com/khangpt2k6/AgentBus/internal/cluster/metadata"
)

// Cluster bundles the membership and metadata subsystems behind a single
// lifecycle. Broker main wires one of these up when --cluster is set.
type Cluster struct {
	cfg  Config
	mem  *membership.Membership
	meta *metadata.Metadata
}

// Status is a snapshot of cluster state for /readyz or `cluster status` CLI.
type Status struct {
	NodeID         string
	AliveMembers   []string
	MetadataLeader string
	IsLeader       bool
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

	// Membership: build join list from cfg.Peers minus self.
	join := make([]string, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		if p.NodeID == cfg.NodeID {
			continue
		}
		// Translate raft address host to use the gossip port. In v1 we
		// assume the same host serves both transports; production may
		// add an explicit GossipAddr per peer.
		join = append(join, p.RaftAddr)
	}
	mem, err := membership.Start(membership.Config{
		NodeID:     cfg.NodeID,
		GossipBind: cfg.GossipBind,
		JoinAddrs:  join,
	})
	if err != nil {
		return nil, fmt.Errorf("membership start: %w", err)
	}

	// Metadata: bootstrap with the full peer list (idempotent on subsequent runs).
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

	return &Cluster{cfg: cfg, mem: mem, meta: meta}, nil
}

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
		FSMembers:      c.meta.FSM().Members(),
	}
}

// Shutdown stops both subsystems. Errors are joined; Membership shutdown
// is best-effort even if Metadata shutdown fails.
func (c *Cluster) Shutdown() error {
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

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/cluster/...
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add internal/cluster/cluster.go
git commit -m "feat(cluster): top-level Cluster composition over membership + metadata"
```

---

## Task 8: 3-node in-process integration test

**Files:**
- Create: `internal/cluster/cluster_test.go`

This is the milestone test. Three `Cluster` instances on free localhost ports converge to a single membership view and one metadata Raft leader. Gated behind the `cluster_integration` build tag to keep `go test ./...` fast for unrelated work.

- [ ] **Step 1: Write the failing test**

Create `internal/cluster/cluster_test.go`:

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
	for i := 0; i < N; i++ {
		raftPorts[i] = freePort(t)
		gossipPorts[i] = freePort(t)
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

	// Wait for gossip convergence: all nodes see all peers.
	if !waitFor(10*time.Second, func() bool {
		for _, c := range clusters {
			if len(c.Membership().Alive()) != N {
				return false
			}
		}
		return true
	}) {
		for i, c := range clusters {
			t.Logf("n%d Alive=%v", i+1, c.Membership().Alive())
		}
		t.Fatal("gossip did not converge within 10s")
	}

	// Wait for exactly one metadata Raft leader to emerge.
	if !waitFor(10*time.Second, func() bool {
		leaders := 0
		for _, c := range clusters {
			if c.Metadata().IsLeader() {
				leaders++
			}
		}
		return leaders == 1
	}) {
		for i, c := range clusters {
			t.Logf("n%d IsLeader=%v Leader=%q", i+1, c.Metadata().IsLeader(), c.Metadata().Leader())
		}
		t.Fatal("metadata Raft did not elect a single leader within 10s")
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

- [ ] **Step 2: Run the integration test**

```bash
go test ./internal/cluster/ -tags=cluster_integration -run TestThreeNode -v -timeout 60s
```

Expected: PASS within ~5–10 seconds. If it fails:
- Check that no other process holds ports 7000–9000 range.
- Confirm `hashicorp/raft` and `memberlist` are both at recent versions.
- The Raft election timings in `metadata.go` are tuned tight (500ms); slow CI may need them relaxed — note this if it happens.

- [ ] **Step 3: Commit**

```bash
git add internal/cluster/cluster_test.go
git commit -m "test(cluster): 3-node in-process integration test (cluster_integration tag)"
```

---

## Task 9: Wire --cluster mode into cmd/broker/main.go

**Files:**
- Modify: `cmd/broker/main.go`

Add new flags, gate cluster startup behind `--cluster`, and (when enabled) replace the manual `raftRuntimeState` updates with values from the real `Cluster.Status()` so the existing `/api/stats` and Prometheus metrics start reflecting real Raft.

- [ ] **Step 1: Read the current flag block + state struct**

The current flags are at [cmd/broker/main.go:34-47](../../../cmd/broker/main.go#L34-L47) and the `raftRuntimeState` is at lines 428–466. We keep `raftRuntimeState` for backward compat in single-node mode but populate it from the cluster in cluster mode.

- [ ] **Step 2: Add cluster flags + import the package**

Add to the import block in [cmd/broker/main.go](../../../cmd/broker/main.go) (after the existing project imports):

```go
	"github.com/khangpt2k6/AgentBus/internal/cluster"
```

Add these flags immediately after the existing `adminToken` flag (line 46):

```go
	clusterEnabled := flag.Bool("cluster", false, "enable distributed cluster mode")
	clusterPeers := flag.String("peers", "", "comma-separated id@host:port peer list (e.g. n1@127.0.0.1:7001,n2@127.0.0.1:7002)")
	clusterRaftBind := flag.String("raft-bind", "127.0.0.1:7001", "Raft transport listen address (cluster mode)")
	clusterGossipBind := flag.String("gossip-bind", "127.0.0.1:8001", "Gossip listen address (cluster mode)")
	clusterRaftDir := flag.String("raft-dir", "data/raft", "directory for Raft state (cluster mode)")
```

- [ ] **Step 3: Start the cluster after `rootCtx` is set up**

The cluster-start block uses `m`, `state`, `nodeID`, and `rootCtx`. `rootCtx` is created on line 116 of the current main.go, so the block must go *after* that line. Insert immediately after `defer stop()` (line 117):

```go
	var cl *cluster.Cluster
	if *clusterEnabled {
		peers, err := cluster.ParsePeers(*clusterPeers)
		if err != nil {
			log.Fatalf("invalid --peers: %v", err)
		}
		cl, err = cluster.Start(cluster.Config{
			NodeID:     *nodeID,
			RaftBind:   *clusterRaftBind,
			GossipBind: *clusterGossipBind,
			RaftDir:    *clusterRaftDir,
			Peers:      peers,
		}, nil)
		if err != nil {
			log.Fatalf("cluster start: %v", err)
		}
		log.Printf("cluster mode enabled: node_id=%s raft=%s gossip=%s peers=%d",
			*nodeID, *clusterRaftBind, *clusterGossipBind, len(peers))

		// Bridge real Raft state into the existing raftRuntimeState +
		// Prometheus gauges every 2s.
		go func() {
			t := time.NewTicker(2 * time.Second)
			defer t.Stop()
			prevLeader := ""
			for {
				select {
				case <-rootCtx.Done():
					return
				case <-t.C:
					st := cl.Status()
					role := "follower"
					if st.IsLeader {
						role = "leader"
					}
					state.Update(raftStateUpdateRequest{
						Role:     role,
						LeaderID: st.MetadataLeader,
						Term:     state.Get().Term, // term comes from inside Raft; expose later
					})
					if st.MetadataLeader != prevLeader && st.MetadataLeader != "" {
						m.IncRaftLeaderChange(state.Get().NodeID)
						prevLeader = st.MetadataLeader
					}
					cur := state.Get()
					m.SetRaftState(cur.NodeID, cur.Role, cur.LeaderID, cur.Term)
				}
			}
		}()
	}
```

**Note:** the goroutine references `rootCtx` (declared at line 116) — placing the block right after `defer stop()` (line 117) keeps that reference valid.

- [ ] **Step 4: Shut the cluster down cleanly on exit**

In the shutdown block (around line 393, after `ready.Store(false)`), before `metricsSrv.Shutdown(...)`:

```go
	if cl != nil {
		if err := cl.Shutdown(); err != nil {
			log.Printf("cluster shutdown: %v", err)
		}
	}
```

- [ ] **Step 5: Verify single-node mode is unchanged**

```bash
go build -o bin/broker ./cmd/broker
./bin/broker --tcp-addr=:19090 --grpc-addr=:19095 --metrics-addr=:12112 --wal-path=data/test.wal &
sleep 1
curl -s http://localhost:12112/api/stats | grep -i node_id
kill %1
wait
```

Expected: starts cleanly without `--cluster`, `/api/stats` shows `"node_id":"node-1"` and `"role":"standalone"` — the existing behavior.

- [ ] **Step 6: Smoke-test cluster mode with one node**

```bash
./bin/broker --cluster --node-id=n1 \
  --raft-bind=127.0.0.1:7001 --gossip-bind=127.0.0.1:8001 \
  --raft-dir=data/raft-n1 --peers=n1@127.0.0.1:7001 \
  --tcp-addr=:19090 --grpc-addr=:19095 --metrics-addr=:12112 \
  --wal-path=data/test.wal &
sleep 3
curl -s http://localhost:12112/api/stats | python -c "import sys, json; s=json.load(sys.stdin); print('role=',s['role'],'leader=',s['leader_id'])"
kill %1
wait
rm -rf data/raft-n1 data/test.wal
```

Expected output after a few seconds: `role= leader leader= n1` — the real Raft has elected itself.

- [ ] **Step 7: Commit**

```bash
git add cmd/broker/main.go
git commit -m "feat(broker): --cluster flag wires real Raft state into dashboard surface"
```

---

## Task 10: `goqueue cluster status` CLI subcommand

**Files:**
- Create: `internal/cli/cluster.go`
- Modify: `internal/cli/root.go`

The CLI talks to the broker's `/api/stats` HTTP endpoint, which now reflects real Raft when `--cluster` was used. Adding a dedicated `cluster status` command (rather than just `cat /api/stats | jq`) keeps the operator UX consistent with the rest of `goqueue`.

- [ ] **Step 1: Implement the subcommand**

Create `internal/cli/cluster.go`:

```go
package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/spf13/cobra"
)

type clusterStatusResponse struct {
	NodeID   string `json:"node_id"`
	Role     string `json:"role"`
	LeaderID string `json:"leader_id"`
	Term     int64  `json:"term"`
	Uptime   string `json:"uptime"`
}

func newClusterCmd(_ *options) *cobra.Command {
	c := &cobra.Command{
		Use:   "cluster",
		Short: "Inspect AgentBus cluster state",
	}
	c.AddCommand(newClusterStatusCmd())
	return c
}

func newClusterStatusCmd() *cobra.Command {
	var metricsURL string
	c := &cobra.Command{
		Use:   "status",
		Short: "Print the current node's view of cluster state",
		RunE: func(_ *cobra.Command, _ []string) error {
			cli := &http.Client{Timeout: 3 * time.Second}
			resp, err := cli.Get(metricsURL + "/api/stats")
			if err != nil {
				return fmt.Errorf("fetch stats: %w", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
			}
			var out clusterStatusResponse
			if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
				return fmt.Errorf("decode: %w", err)
			}
			fmt.Printf("Node:    %s\n", out.NodeID)
			fmt.Printf("Role:    %s\n", out.Role)
			fmt.Printf("Leader:  %s\n", out.LeaderID)
			fmt.Printf("Term:    %d\n", out.Term)
			fmt.Printf("Uptime:  %s\n", out.Uptime)
			return nil
		},
	}
	c.Flags().StringVar(&metricsURL, "metrics-url", "http://localhost:2112", "broker metrics/admin URL")
	return c
}
```

- [ ] **Step 2: Register the command in root.go**

Edit [internal/cli/root.go:35-36](../../../internal/cli/root.go#L35-L36) — add the cluster command registration alongside the others:

```go
	root.AddCommand(newClusterCmd(opts))
```

- [ ] **Step 3: Verify it builds and runs against a live cluster node**

```bash
go build -o bin/goqueue ./cmd/goqueue
./bin/broker --cluster --node-id=n1 \
  --raft-bind=127.0.0.1:7001 --gossip-bind=127.0.0.1:8001 \
  --raft-dir=data/raft-n1 --peers=n1@127.0.0.1:7001 \
  --tcp-addr=:19090 --grpc-addr=:19095 --metrics-addr=:12112 \
  --wal-path=data/test.wal &
sleep 3
./bin/goqueue cluster status --metrics-url=http://localhost:12112
kill %1
wait
rm -rf data/raft-n1 data/test.wal
```

Expected output:
```
Node:    n1
Role:    leader
Leader:  n1
Term:    1
Uptime:  3s
```

- [ ] **Step 4: Commit**

```bash
git add internal/cli/cluster.go internal/cli/root.go
git commit -m "feat(cli): goqueue cluster status subcommand"
```

---

## Task 11: docker-compose for 3-node local cluster

**Files:**
- Create: `deploy/cluster.yml`

A copy-pasteable 3-node demo that operators (and your Show HN audience) can `docker compose up` to see the cluster form.

- [ ] **Step 1: Write the compose file**

Create `deploy/cluster.yml`:

```yaml
# 3-node AgentBus cluster for local demos.
# Run: docker compose -f deploy/cluster.yml up --build
#
# After ~5s, each node's /api/stats shows real Raft state. Try:
#   goqueue cluster status --metrics-url=http://localhost:12112
#   goqueue cluster status --metrics-url=http://localhost:12113
#   goqueue cluster status --metrics-url=http://localhost:12114

version: "3.9"

services:
  n1:
    build: ..
    command:
      - --cluster
      - --node-id=n1
      - --raft-bind=0.0.0.0:7001
      - --gossip-bind=0.0.0.0:8001
      - --raft-dir=/data/raft
      - --peers=n1@n1:7001,n2@n2:7001,n3@n3:7001
      - --tcp-addr=:9090
      - --grpc-addr=:9095
      - --metrics-addr=:2112
      - --wal-path=/data/wal
    ports:
      - "19090:9090"
      - "19095:9095"
      - "12112:2112"
    volumes:
      - n1-data:/data
    networks:
      - agentbus

  n2:
    build: ..
    command:
      - --cluster
      - --node-id=n2
      - --raft-bind=0.0.0.0:7001
      - --gossip-bind=0.0.0.0:8001
      - --raft-dir=/data/raft
      - --peers=n1@n1:7001,n2@n2:7001,n3@n3:7001
      - --tcp-addr=:9090
      - --grpc-addr=:9095
      - --metrics-addr=:2112
      - --wal-path=/data/wal
    ports:
      - "29090:9090"
      - "29095:9095"
      - "12113:2112"
    volumes:
      - n2-data:/data
    networks:
      - agentbus

  n3:
    build: ..
    command:
      - --cluster
      - --node-id=n3
      - --raft-bind=0.0.0.0:7001
      - --gossip-bind=0.0.0.0:8001
      - --raft-dir=/data/raft
      - --peers=n1@n1:7001,n2@n2:7001,n3@n3:7001
      - --tcp-addr=:9090
      - --grpc-addr=:9095
      - --metrics-addr=:2112
      - --wal-path=/data/wal
    ports:
      - "39090:9090"
      - "39095:9095"
      - "12114:2112"
    volumes:
      - n3-data:/data
    networks:
      - agentbus

volumes:
  n1-data: {}
  n2-data: {}
  n3-data: {}

networks:
  agentbus:
    driver: bridge
```

- [ ] **Step 2: Manually verify the cluster forms**

```bash
docker compose -f deploy/cluster.yml up --build -d
sleep 8
./bin/goqueue cluster status --metrics-url=http://localhost:12112
./bin/goqueue cluster status --metrics-url=http://localhost:12113
./bin/goqueue cluster status --metrics-url=http://localhost:12114
docker compose -f deploy/cluster.yml down -v
```

Expected: all three nodes report the *same* `Leader:` value, exactly one of them shows `Role: leader`, the other two `Role: follower`.

- [ ] **Step 3: Commit**

```bash
git add deploy/cluster.yml
git commit -m "deploy: docker-compose for 3-node cluster local demo"
```

---

## Task 12: Operator docs + README update

**Files:**
- Create: `docs/cluster.md`
- Modify: `README.md`

- [ ] **Step 1: Write the cluster operator doc**

Create `docs/cluster.md`:

```markdown
# Cluster Mode (Distributed v1 Foundation)

> Status: M0–M2 shipped — cluster forms, gossips, elects a metadata Raft leader.
> Data is **not yet** routed or replicated across nodes; producers and consumers
> still talk to a single broker. Routing + replication ship in Plan 2 (M3–M5).

## Running a local 3-node cluster

```bash
docker compose -f deploy/cluster.yml up --build
```

Three brokers come up on `localhost`:

| Node | gRPC | metrics + admin |
|---|---|---|
| n1 | 19095 | 12112 |
| n2 | 29095 | 12113 |
| n3 | 39095 | 12114 |

Inspect cluster state:

```bash
goqueue cluster status --metrics-url=http://localhost:12112
# Node:    n1
# Role:    leader            ← exactly one node should show 'leader'
# Leader:  n1
# Term:    1
# Uptime:  8s
```

## Running the broker binary directly

```bash
broker --cluster --node-id=n1 \
  --raft-bind=127.0.0.1:7001 \
  --gossip-bind=127.0.0.1:8001 \
  --raft-dir=data/raft-n1 \
  --peers=n1@127.0.0.1:7001,n2@127.0.0.1:7002,n3@127.0.0.1:7003 \
  --metrics-addr=:2112
```

Repeat with `--node-id=n2/--raft-bind=:7002/--gossip-bind=:8002/--raft-dir=data/raft-n2` and so on.

## What's not yet in this milestone

- **No cross-node data routing.** A `Publish` to n1 lands in n1's local topic only.
- **No replication.** Each node has an independent WAL.
- **No NOT_LEADER redirect on the SDK.** Clients still talk to whichever node they connect to.

These ship in [Plan 2 (data plane)](superpowers/plans/) — coming next.

## Failure modes (foundation only)

- **Kill the metadata Raft leader:** within ~1s a new leader is elected. Verify via `cluster status` showing a different `Leader:`.
- **Kill any non-leader:** `Alive` count drops within ~3s; remaining nodes continue.
- **Partition (network):** the minority side cannot elect a metadata leader. Cluster control halts on that side; data plane (single-node behavior) keeps serving.
```

- [ ] **Step 2: Link the doc from the README's status callout**

Update the callout you inserted in Task 1 to also link `docs/cluster.md`:

```markdown
> **Status:** Single-node broker today. Distributed v1 foundation (3-node cluster + metadata Raft) is on the [`feat/cluster-v1`](https://github.com/khangpt2k6/AgentBus/tree/feat/cluster-v1) branch — see [docs/cluster.md](docs/cluster.md) for usage and [docs/superpowers/specs/2026-05-16-distributed-v1-design.md](docs/superpowers/specs/2026-05-16-distributed-v1-design.md) for the full design.
```

- [ ] **Step 3: Commit**

```bash
git add docs/cluster.md README.md
git commit -m "docs(cluster): operator guide for distributed v1 foundation"
```

---

## Final verification

- [ ] **Run the whole test suite**

```bash
go test ./... -count=1
```

Expected: all tests pass.

- [ ] **Run the integration test**

```bash
go test ./internal/cluster/ -tags=cluster_integration -run TestThreeNode -v -timeout 60s
```

Expected: PASS.

- [ ] **Smoke-test the docker compose stack**

```bash
docker compose -f deploy/cluster.yml up --build -d
sleep 10
for port in 12112 12113 12114; do
  echo "--- node on $port ---"
  ./bin/goqueue cluster status --metrics-url=http://localhost:$port
done
docker compose -f deploy/cluster.yml down -v
```

Expected: all three nodes converge on the same `Leader:` value, exactly one is `Role: leader`.

- [ ] **Push the branch (do NOT merge yet — wait for Plan 2)**

```bash
git push -u origin feat/cluster-v1
```

---

## What ships at the end of this plan

A broker that:

- Runs in single-node mode by default with byte-identical behavior to today's code.
- Runs in 3-node cluster mode when `--cluster` is set, with gossip membership and a real metadata Raft leader.
- Shows real cluster state via `goqueue cluster status` and `/api/stats`.
- Has an integration test that proves a 3-node in-process cluster forms and elects within 10 seconds.
- Documents what works and what doesn't, so reviewers/recruiters see honest scope.

What's intentionally **not** shipped here (deferred to Plan 2):

- Session → shard routing.
- ISR replication.
- NOT_LEADER redirect.
- Data plane failover.
- Per-shard leadership reassignment on node death (the FSM has the *map*; nothing yet *writes* to it from broker code — that's M3).
