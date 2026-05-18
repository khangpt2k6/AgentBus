# Cluster Mode (Distributed v1 — Foundation + Routing)

> **Status:** M0–M3 shipped on `feat/cluster-v1` — cluster forms, gossips, elects a metadata Raft leader, **and routes session traffic across nodes via consistent hashing with transparent SDK redirect.**
> Data is **not yet replicated** — a publish lands in *one* node's WAL only. ISR replication is M4; seamless failover is M5.

## What's shipped through M3

- **Gossip membership** (SWIM via `hashicorp/memberlist`) — nodes find each other and detect failures within ~3s.
- **Metadata Raft** (via `hashicorp/raft` + BoltDB) — strongly-consistent cluster state: members, shard count, shard→leader map.
- **Session routing** — each `tenant/project/session` hashes deterministically to one of 32 shards; each shard has one elected leader. A leader-driven assigner (the metadata Raft leader) round-robins shards onto alive nodes and reassigns dead leaders within ~2s.
- **NOT_LEADER redirect** — publishes that arrive at the wrong node return a gRPC `FailedPrecondition` with a `NotLeaderError` detail carrying the leader's gRPC address. The AgentBus SDK transparently retries against the leader; end users see one call.
- **Operator tooling** — `goqueue cluster status` (real Raft state) and `goqueue cluster route` (show where a session would land).

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

All three nodes will report the *same* `Shard` and `Leader` for a given session, but only the leader shows `Local: true`. Publishing through the SDK is transparent — `client.PublishAgent(...)` to *any* node returns success because the SDK absorbs the redirect.

## What's NOT yet shipped

- **No WAL replication.** A publish lands in *one* node's WAL. If that node dies, the data is gone. Replication is M4.
- **No failover.** If a shard leader dies, the assigner reassigns within ~2s — but any data the dead leader held is lost (no replicas yet). M4 fixes the data-loss risk; M5 makes failover seamless.
- **No term-tagged writes.** A network-partitioned stale leader could technically still accept writes until the partition heals. Term-tagging is M5.

These ship in Plan 2b (M4 ISR replication) and Plan 2c (M5 failover). See the [design spec](superpowers/specs/2026-05-16-distributed-v1-design.md) for the full roadmap.

## Failure modes (M3 routing)

| What you do | What happens |
|-------------|--------------|
| Publish a session to the wrong node | SDK gets `NOT_LEADER`, transparently redirects to the leader, retries. User sees no error. |
| Kill a non-leader node | Within ~3s, gossip marks it dead. Assigner reassigns the shards it held (data on it is lost — M4 fixes this). |
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
