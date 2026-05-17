# Cluster Mode (Distributed v1 Foundation)

> **Status:** M0–M2 shipped on `feat/cluster-v1` — cluster forms, gossips, elects a metadata Raft leader.
> Data is **not yet** routed or replicated across nodes; producers and consumers still talk to a single
> broker per session. Session routing + ISR replication ship in Plan 2 (M3–M5).

## What the foundation gives you

- **Gossip membership** (SWIM via `hashicorp/memberlist`) — nodes find each other and detect failures within ~3s.
- **Metadata Raft** (via `hashicorp/raft` + BoltDB) — strongly-consistent cluster state: members, shard→leader map.
- **Honest cluster status** — `/api/stats` and `goqueue cluster status` now reflect the *real* elected leader, not the manual placeholder of older versions.

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

## What's NOT yet in this milestone

- **No cross-node data routing.** A `Publish` to `n1` lands in `n1`'s local topic only — there is no `NOT_LEADER` redirect on the data plane.
- **No replication.** Each node has an independent WAL.
- **No SDK awareness of cluster topology.** Clients still talk to whichever node they connect to.

These ship in Plan 2 (M3 routing → M4 ISR replication → M5 failover). See the [design spec](superpowers/specs/2026-05-16-distributed-v1-design.md) for the full roadmap.

## Failure modes (foundation only)

| What you do | What happens |
|-------------|--------------|
| Kill the metadata Raft leader | Within ~1s a new leader is elected. `cluster status` on remaining nodes shows a different `Leader:`. |
| Kill any non-leader | `Alive` count drops within ~3s via gossip. Remaining nodes continue. |
| Network partition | Minority side cannot elect a metadata leader. Cluster control halts on that side; data plane (single-node behavior) keeps serving. |

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
