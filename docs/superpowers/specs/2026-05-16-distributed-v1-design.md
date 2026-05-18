# AgentBus Distributed v1 — Design Spec

**Status:** Approved for implementation
**Date:** 2026-05-16
**Author:** Brainstorm session, AgentBus repo
**Supersedes:** `docs/distributed-v1-design.md` (kept as historical record; this is the authoritative spec)

---

## 1. Goal

Turn AgentBus from a single-node broker into a 3-node distributed broker that:

1. Routes session traffic intelligently across nodes via consistent hashing.
2. Replicates each shard's WAL to followers for durability.
3. Survives any single node death with **zero message loss** under `acks=quorum`.
4. Preserves the existing single-node mode (opt-in clustering via `--cluster` flag).
5. Ships a 2-minute demo video that runs live end-to-end.

## 2. Non-goals (v1)

Explicitly deferred to v2 or later. Document in `docs/distributed-v2-roadmap.md` so reviewers see we considered each:

- Elastic membership (adding/removing nodes at runtime via Raft membership change).
- Live session migration on rebalance.
- Follower reads (leader-only read path in v1).
- Cross-region / WAN replication.
- Exactly-once producer semantics.
- Dynamic topic partition count changes.
- TLS / authN / authZ on inter-node or client RPCs (separate workstream).

## 3. Architecture

### 3.1 Layered design

Each layer uses the right primitive for its problem shape. Two layers use battle-tested libraries (commodity work); three are written from scratch (the differentiator and learning).

| Layer | Primitive | Library / Custom | Rationale |
|---|---|---|---|
| Membership + failure detection | SWIM gossip | `hashicorp/memberlist` | Eventually-consistent, scales to hundreds of nodes, no coordinator. Same protocol as Consul/Serf. |
| Cluster metadata | Raft | `hashicorp/raft` | Small log, low throughput, needs strong consistency. The *only* layer where Raft belongs. |
| Session → shard routing | Consistent hashing + virtual nodes | **Custom (~200 LOC)** | Unique to AgentBus (session-keyed). Educational, blog-worthy. |
| Per-shard durability | Leader + ISR replication | **Custom (~600 LOC)** | The actual meat. Streaming WAL tail, quorum acks, follower catchup. |
| NOT_LEADER redirect | Term-tagged response + client retry | **Custom (~100 LOC)** | Standard pattern (Kafka, etcd, CockroachDB). |

### 3.2 Package layout

```
cmd/broker                  existing — accepts new --cluster, --node-id, --peers, --raft-dir flags
cmd/goqueue                 existing — CLI transparently handles NOT_LEADER redirect

internal/broker             existing — unchanged at API; cluster adapter wraps it
internal/wal                existing — gains follower-driven "tail reader" API for replication
internal/agentstream        existing — SessionKey() is the hash input to the shard ring
internal/consumer           existing — offsets routed through internal __consumer_offsets shard
internal/grpcapi            existing — responses carry NOT_LEADER + LeaderHint when applicable

internal/cluster/membership NEW — wraps hashicorp/memberlist; exposes Alive()/Dead() events
internal/cluster/metadata   NEW — wraps hashicorp/raft; FSM = nodes + topics + shard→leader map
internal/cluster/ring       NEW — consistent hash ring with virtual nodes; SessionKey → shardID → leaderNodeID
internal/cluster/replicator NEW — per-shard ISR: leader streams WAL tail to followers; ack on quorum
internal/cluster/router     NEW — front door: "I lead this shard" / "redirect to X" / "buffer until metadata settles"
internal/cluster/transport  NEW — internal gRPC service for AppendEntries (data plane) + leadership signals
proto/cluster.proto         NEW — wire format for internal RPCs
```

### 3.3 Data flow: a single Publish in cluster mode

```
client → any node's gRPC Publish
       → router: hash(sessionKey) → shardID → metadata.LeaderFor(shardID)
         │
         ├─ I am leader of shard:
         │    1. wal.Append (durable locally first — WAL-first invariant preserved)
         │    2. replicator.BroadcastToFollowers(record, term)
         │    3. wait for quorum ack  (skip if acks=1)
         │    4. respond OK to client
         │
         └─ I am NOT leader:
              → respond {NOT_LEADER, leader_hint: "node-2:9095", term: 42}
              → client reconnects to node-2 and retries
```

### 3.4 Data flow: leader failover

```
1. Node-2 (current leader of shard 7) crashes
2. memberlist gossip detects node-2 marked Dead within ~3s
3. Metadata Raft leader proposes: "reassign shard 7 leadership from node-2 to node-4, term++"
4. Raft commits, FSM updates shard→leader map and bumps term
5. All nodes apply the FSM update → ring.LeaderFor(shard 7) returns node-4
6. In-flight clients that hit a stale node get NOT_LEADER + new leader_hint and retry
7. Node-4 replays its local WAL tail against the term boundary before accepting writes
```

### 3.5 Key invariants

These are the load-bearing correctness properties. Violating any of them is a bug.

1. **WAL-first publish.** Existing invariant. Preserved: leader writes locally durable before broadcasting to followers.
2. **Term-tagged writes.** Every shard-leader write carries the leadership term from the metadata FSM. Followers reject writes from stale terms. Prevents split-brain: a deposed leader cannot durably commit after the network heals.
3. **Quorum durability.** Under `acks=quorum`, a write is acknowledged only after ⌈(N+1)/2⌉ replicas (including leader) have it on disk. With N=3, that's 2 nodes — survives any 1-node death.
4. **Replay completeness.** After failover, any surviving node can serve a session replay that includes all events written *before* the failover, in order. Events written *across* the failover are best-effort ordered (see §4 known gap).
5. **Single-node mode unaffected.** `--cluster=false` (default) preserves byte-identical behavior to today's broker. Cluster code paths gated behind the flag.

## 4. Known gaps (honestly documented)

Failing to document these in v1 is the difference between a junior and senior project.

1. **Producer ordering across failover is eventual, not strict.** If a client publishes A, B, C to leader L1 and L1 crashes between B and C, C may arrive at the new leader L2 before B (if B was in flight). v1 ships *eventual* per-session ordering across leader changes. Strict ordering requires producer-side sequence numbers + broker dedup (Kafka's idempotent producer) — tracked for v1.5.
2. **Metadata Raft quorum is a SPOF for cluster control.** Lose 2 of 3 metadata nodes → no new leader elections possible. Data plane continues serving against last-known leadership map, but cannot heal further failures. Honest tradeoff.
3. **No live session migration on shard reassignment.** When shard 7 moves from n2 to n4 after failover, n4 serves shard 7 from its own (replicated) WAL tail. There is no in-flight buffer migration; producers see brief NOT_LEADER errors and retry.
4. **No backpressure across cluster.** A slow follower in one shard can't slow producers on other shards — but within a shard, slow follower → lower quorum ack rate → producer-visible latency increase. Documented behavior, not a bug.

## 5. Demo script (the artifact)

The 2-minute video that ships with the Show HN. Three acts plus an honesty slide.

### Act 1 — Cluster forms and routes intelligently (~30s)

```bash
docker compose -f deploy/cluster.yml up

goqueue cluster status --addr localhost:9001
# Metadata leader: n2
# Shards: 32 (8 leader + 16 follower roles per node)
# Members: n1 (alive), n2 (alive, controller), n3 (alive)

goqueue publish-agent --addr localhost:9001 --tenant acme --session A --type tool.call --payload '{"q":"x"}'
goqueue publish-agent --addr localhost:9001 --tenant acme --session B --type tool.call --payload '{"q":"y"}'

goqueue cluster route --tenant acme --session A   # → shard 12, leader n3
goqueue cluster route --tenant acme --session B   # → shard 5,  leader n1
```

### Act 2 — Failover with zero message loss (~45s, the money shot)

```bash
goqueue stress publish --tenant acme --session A --rate 200/s &
goqueue session tail --tenant acme --session A   # shows continuous offsets

kill -9 $(pgrep -f "node-id=n3")
# Within ~3s: memberlist marks n3 dead, metadata Raft reassigns shard 12
# Publisher sees brief NOT_LEADER errors, retries, lands on new leader
# Session tail shows zero gap in offsets

goqueue session replay --tenant acme --session A | wc -l
# Count matches publisher's emitted count exactly — zero loss
```

### Act 3 — Replay survives leader changes (~30s)

```bash
goqueue session replay --addr localhost:9002 --tenant acme --session A | head
# Full session reconstructed in order — events from before AND after failover
```

### Act 4 — Honest scope slide (~15s)

> v1 ships: fixed cluster, metadata Raft, ISR data replication, leader failover.
> v1 does NOT ship: elastic membership, follower reads, exactly-once. Tracked in `docs/distributed-v2-roadmap.md`.

## 6. Implementation milestones

Six milestones, each independently shippable. Each ends in a "you could stop here" demoable state. Killable checkpoints at M3, M4, M5 if timeline shifts.

| # | Milestone | Ships when | Effort |
|---|---|---|---|
| **M0** | Repo cleanup + honest README | Decorative `raft-*` fields removed or renamed; README top says "single-node today, distributed v1 on `feat/cluster-v1`"; cluster work behind `--cluster` flag | ~2 hr |
| **M1** | **Membership** | 3 nodes start, see each other via memberlist; `goqueue cluster status` lists alive nodes; killing one shows Dead within ~3s | ~3 days |
| **M2** | **Metadata Raft** | hashicorp/raft wired; FSM stores members + dummy shard map; killing Raft leader elects new one | ~4 days |
| **M3** | **Ring + routing** (no replication yet) | Publishes go through router; ring assigns shards to nodes; non-leaders return NOT_LEADER + hint; client transparently redirects. **Single replica; data lost if leader dies** | ~5 days |
| **M4** | **ISR replication** | Per-shard followers pull from leader; quorum acks; follower catchup after restart | ~7 days |
| **M5** | **Failover** | Metadata Raft reassigns shard leaders; new leader rejects stale-term writes; producers retry seamlessly; **zero message loss with `acks=quorum`** | ~4 days |
| **M6** | **Polish + ship** | `__consumer_offsets` shard wired; Grafana cluster dashboards; `deploy/cluster.yml`; Helm StatefulSet update; blog post draft; demo video | ~3 days |

**Total: ~26 working days (~5 weeks). +1 week buffer for Raft/replication gotchas. Target ship: 6 weeks from start.**

## 7. Risk register

Ranked by likelihood × impact on schedule.

1. **WAL tail-streaming API design.** Current WAL is append-only with no "give me records since offset X as they arrive" interface. Building this without breaking existing replay is M4's hardest part.
2. **Producer retry semantics on NOT_LEADER.** Needs careful sequence preservation. If client retries out of order, session ordering breaks during failover. Mitigation in v1: serialize per-session retries client-side (no parallel in-flight writes for a given session) so eventual ordering still holds. If that proves insufficient during M5 testing, pull §4 gap 1 (producer-side sequence numbers + broker dedup) forward into v1.
3. **hashicorp/raft snapshot integration.** FSM grows over time; snapshots needed. API has sharp edges; budget half a day for "why is my snapshot empty."
4. **Multi-node integration test harness.** Needed from M1 to pay off across M2–M5. Will require in-process spawn of N brokers with port allocation, teardown, and crash injection. Worth investing upfront.

## 8. Open questions (decide during implementation, not blockers for v1)

These are intentionally left open; we'll pick during the relevant milestone. Listed here so they don't get forgotten.

- **Shard count.** Fixed at 32? 64? Configurable? Default 32 unless benchmark shows otherwise.
- **Virtual nodes per physical node in the ring.** Default 128 (Cassandra-style) unless distribution analysis shows skew.
- **Replication factor default.** Probably 3 (leader + 2 followers) for a 3-node cluster. Configurable per topic in v2.
- **Quorum ack default.** `acks=1` for backward compat in single-node mode; `acks=quorum` for cluster mode unless overridden.
- **Snapshot cadence for metadata Raft.** Time-based or log-size-based? hashicorp/raft default is usually fine; revisit if FSM grows.

## 9. Success criteria

v1 is shipped when all of these are true. Items marked `[x]` shipped in the **foundation milestone** (M0–M2) on `feat/cluster-v1`. Items marked `[ ]` are scoped to Plan 2 (M3–M5: data plane routing, ISR replication, failover) and Plan 3 (M6: polish + demo video).

- [x] `--cluster` mode starts a 3-node cluster from `docker compose -f deploy/cluster.yml up`
- [x] `goqueue cluster status` shows cluster membership and current metadata Raft leader
- [x] Publishing to two different sessions lands them on different shards (verified by `goqueue cluster route`) — *M3*
- [ ] Killing the shard leader during a 200 msg/s stream results in zero message loss with `acks=quorum` — *M5*
- [ ] `goqueue session replay` after failover reconstructs the full session including events written across the failover — *M5*
- [x] Single-node mode (`--cluster=false`) passes existing test suite unchanged
- [x] Multi-node integration test suite runs in CI and gates merges
- [x] README clearly distinguishes single-node from cluster mode and links to this spec
- [ ] Demo video recorded and linked in README — *M6*
- [ ] `docs/distributed-v2-roadmap.md` documents the deferred work from §2 and §4 — *M6*

## 10. References

- This spec supersedes the lightweight [docs/distributed-v1-design.md](../../distributed-v1-design.md).
- Raft library: https://github.com/hashicorp/raft
- Memberlist library: https://github.com/hashicorp/memberlist
- Kafka's KRaft mode (the canonical "Raft for metadata, ISR for data" architecture).
- Cassandra's consistent hashing with virtual nodes (the canonical "session-keyed" routing model).
