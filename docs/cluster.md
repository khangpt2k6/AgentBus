# Cluster Mode (Distributed v1: Foundation + Routing + ISR Replication)

> **Status:** M0 through M4 shipped on `feat/cluster-v1`. Cluster forms, gossips, elects a metadata Raft leader, routes session traffic via consistent hashing with transparent SDK redirect, **and replicates every shard write to all alive followers under `acks=quorum` semantics.** Killing a non-leader loses zero messages.

## What's shipped through M4

- **Gossip membership** (SWIM via `hashicorp/memberlist`): nodes find each other and detect failures within ~3s.
- **Metadata Raft** (via `hashicorp/raft` + BoltDB): strongly-consistent cluster state covering members, shard count, and shard-to-leader map.
- **Session routing**: each `tenant/project/session` hashes deterministically to one of 32 shards; each shard has one elected leader. A leader-driven assigner round-robins shards onto alive nodes and reassigns dead leaders within ~2s.
- **NOT_LEADER redirect**: publishes that arrive at the wrong node return a gRPC `FailedPrecondition` with a `NotLeaderError` detail carrying the leader's gRPC address. The AgentBus SDK transparently retries against the leader; end users see one call.
- **ISR replication (M4):** every agent-event write to a shard leader is replicated to all alive followers via long-lived inter-node gRPC streams. `acks=quorum` (cluster-mode default) blocks the publish ack until a majority of replicas have the record durably on disk, so killing any single node loses zero messages.
- **Operator tooling**: `goqueue cluster status` (real Raft state) and `goqueue cluster route` (show where a session would land).

## Running a local 3-node cluster

From the repo root:

```bash
docker compose -f deploy/cluster.yml up --build
```

Three brokers come up on `localhost`:

| Node | gRPC | TCP   | metrics + admin |
|------|------|-------|-----------------|
| n1   | 19095 | 19090 | 12112 |
| n2   | 29095 | 29090 | 12113 |
| n3   | 39095 | 39090 | 12114 |

Inspect cluster state from any node:

```bash
goqueue cluster status --metrics-url=http://localhost:12112
# Node:    n1
# Role:    leader            ← exactly one node should show 'leader'
# Leader:  n1
# Term:    1
# Uptime:  8s
```

All three nodes should converge on the same `Leader:` value.

## Running the broker binary directly (no Docker)

```bash
broker --cluster --node-id=n1 \
  --raft-bind=127.0.0.1:7001 \
  --gossip-bind=127.0.0.1:8001 \
  --raft-dir=data/raft-n1 \
  --peers=n1@127.0.0.1:7001,n2@127.0.0.1:7002,n3@127.0.0.1:7003 \
  --metrics-addr=:2112
```

Repeat with `--node-id=n2 --raft-bind=:7002 --gossip-bind=:8002 --raft-dir=data/raft-n2` and so on.

## Flags reference

| Flag | Default | Meaning |
|------|---------|---------|
| `--cluster` | `false` | Enable distributed mode. Without it, the broker runs in single-node mode (existing behavior). |
| `--node-id` | `node-1` | Stable identifier used by Raft and gossip. Must be unique per cluster. |
| `--peers` | `""` | Comma-separated `id@host:port` peer list. All three nodes pass the same value. |
| `--raft-bind` | `127.0.0.1:7001` | Raft TCP transport listen address. |
| `--gossip-bind` | `127.0.0.1:8001` | Gossip (memberlist) listen address. |
| `--raft-dir` | `data/raft` | Directory for Raft log + snapshot files. Must be unique per node. |
| `--advertise-client-addr` | derived from `--grpc-addr` | gRPC address other nodes will hint clients toward in NOT_LEADER redirects. For Docker, set explicitly to the service name (e.g. `n1:9095`). |

## Inspecting where a session would route

```bash
goqueue cluster route \
  --metrics-url=http://localhost:12112 \
  --tenant=acme --project=support --session=sessA
# Session: acme/support/sessA
# Shard:   17
# Leader:  n2 (client addr: n2:9095)
# Local:   false
```

All three nodes will report the *same* `Shard` and `Leader` for a given session, but only the leader shows `Local: true`. Publishing through the SDK is transparent: `client.PublishAgent(...)` to *any* node returns success because the SDK absorbs the redirect.

## Per-shard storage layout

Each broker keeps one append-only file per shard at `--shardwal-dir/shard-N.wal` (default `data/shardwal/shard-N.wal`). Records are length-prefixed payloads with a CRC32C trailer (same correctness primitives as the main WAL). The shard leader's HWM = min ack'd offset across self + alive followers; under `acks=quorum`, `PublishAgent` waits for HWM ≥ the new offset before responding.

## Demo: kill a non-leader, observe zero loss

```bash
# Bring up the 3-node cluster.
docker compose -f deploy/cluster.yml up --build -d
sleep 12

# Pick a session and find out which node leads its shard.
goqueue cluster route --metrics-url=http://localhost:12112 \
  --tenant acme --project support --session demo

# Publish 5 events from any node (SDK redirects transparently).
for i in 1 2 3 4 5; do
  goqueue publish-agent --grpc --addr localhost:19095 \
    --tenant acme --project support --session demo --agent planner \
    --type tool.call --payload "{\"i\":$i}"
done

# Identify a non-leader node (any node other than the one in cluster route).
# Then kill it.
docker compose -f deploy/cluster.yml stop n3   # adjust to a non-leader

# Publish 5 more. They still succeed under acks=quorum because the
# surviving follower is still in the ISR.
for i in 6 7 8 9 10; do
  goqueue publish-agent --grpc --addr localhost:19095 \
    --tenant acme --project support --session demo --agent planner \
    --type tool.call --payload "{\"i\":$i}"
done

# Bring the killed node back. Its shardwal catches up automatically.
docker compose -f deploy/cluster.yml start n3
sleep 8

docker compose -f deploy/cluster.yml down -v
```

Expected result: all 10 publishes succeeded. The surviving follower had every record on disk before the leader acked. Once the killed node restarts, its replicator session reopens against the leader and the shardwal tail catches up.

## What's NOT yet shipped

- **No term-tagged writes.** A network-partitioned stale leader could technically still accept writes until the partition heals. This is M5.
- **No producer-side sequence preservation across leader changes.** Publishes that race a leader change can land out-of-order in a session. M5 adds idempotent-producer semantics.
- **No log segmentation / compaction.** Shard WAL files grow without bound. Operational concern; deferred to a future iteration.

These ship in Plan 2c (M5 failover). See the [design spec](superpowers/specs/2026-05-16-distributed-v1-design.md) for the full roadmap.

## Failure modes (M3 routing + M4 replication)

| What you do | What happens |
|-------------|--------------|
| Publish a session to the wrong node | SDK gets `NOT_LEADER`, transparently redirects to the leader, retries. User sees no error. |
| Kill a non-leader node | Within ~3s, gossip marks it dead. Assigner reassigns any shards it led to surviving nodes; data the dead node held as a follower is already replicated to the other survivors. Zero message loss for shards it was a follower of. |
| Kill the metadata Raft leader | Within ~1s a new metadata leader is elected. Then within ~2s the new leader's assigner pass picks up shard reassignment for any shards held by the killed node. |
| Network partition | Minority side cannot elect a new metadata leader OR run the assigner; majority side keeps routing. |

## Verifying the foundation yourself

```bash
# Unit tests
go test ./internal/cluster/... -count=1

# 3-node in-process integration test
go test ./internal/cluster/ -tags=cluster_integration -run TestThreeNode -v -timeout 60s

# Docker stack
docker compose -f deploy/cluster.yml up --build -d
sleep 10
for port in 12112 12113 12114; do
  echo "--- node on $port ---"
  goqueue cluster status --metrics-url=http://localhost:$port
done
docker compose -f deploy/cluster.yml down -v
```

Expected for the Docker run: all three nodes converge on the same `Leader:`, exactly one is `Role: leader`.
