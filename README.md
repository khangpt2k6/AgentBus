<!--
  Agent Bus - README
  Animated header uses capsule-render + readme-typing-svg (public services).
-->

<div align="center">

<img src="https://capsule-render.vercel.app/api?type=waving&color=0:1A2A6C,50:6E48AA,100:00BCD4&height=210&section=header&text=Agent%20Bus&fontSize=72&fontColor=FFFFFF&fontAlignY=38&desc=A%20distributed%20event%20backbone%20for%20AI%20agents&descSize=18&descAlignY=60&animation=fadeIn" alt="Agent Bus" />

<br />

<a href="https://github.com/khangpt2k6/AgentBus/releases">
  <img src="https://readme-typing-svg.demolab.com?font=JetBrains+Mono&weight=600&size=22&duration=3200&pause=900&color=6E48AA&center=true&vCenter=true&width=760&lines=Raft-replicated%2C+zero-data-loss+failover;Group-commit+WAL+%E2%80%94+34x+append+throughput;Session-ordered+agent+event+streams;Prometheus+%2B+OpenTelemetry+built-in" alt="Typing tagline" />
</a>

<p>
  <a href="https://github.com/khangpt2k6/AgentBus/releases">
    <img src="https://img.shields.io/github/v/release/khangpt2k6/AgentBus?style=for-the-badge&color=6E48AA&labelColor=1A1A2E" alt="Release" />
  </a>
  <img src="https://img.shields.io/badge/Go-1.26-00ADD8?style=for-the-badge&logo=go&logoColor=white&labelColor=1A1A2E" alt="Go" />
  <img src="https://img.shields.io/badge/Raft-consensus-F0467E?style=for-the-badge&labelColor=1A1A2E" alt="Raft" />
  <img src="https://img.shields.io/badge/gRPC%20%2B%20TCP-API-2496ED?style=for-the-badge&labelColor=1A1A2E" alt="API" />
  <img src="https://img.shields.io/badge/License-MIT-2EA44F?style=for-the-badge&labelColor=1A1A2E" alt="License" />
</p>

<p>
  <a href="https://khangpt2k6.github.io/AgentBus/"><b>📖 Documentation</b></a> ·
  <a href="visualization/index.html"><b>🖥️ Interactive architecture</b></a> ·
  <a href="#quick-start"><b>Quick start</b></a> ·
  <a href="#benchmarks"><b>Benchmarks</b></a>
</p>

</div>

A **distributed message broker written from scratch in Go**, specialized for AI-agent event streams. It routes each `tenant/project/session` to a stable partition for in-order streaming, persists every write to a crash-safe WAL, and runs as a Raft-replicated cluster with automatic failover - all from a single binary, no Kafka-scale ops.

> Not a Kafka replacement. Focused on AI-native streaming where ordering, replay, and low operational overhead matter.



<img width="1414" height="910" alt="Adobe Express - Screen Recording 2026-06-25 130203" src="https://github.com/user-attachments/assets/65d495ff-cf27-4695-b453-290661069eae" />

## Engineering highlights

| | |
|---|---|
| **Raft consensus** | 3-node cluster, leader election, automatic failover. Killing any non-leader loses **zero messages** under quorum acks - verified by a kill-the-leader test. |
| **Group-commit WAL** | Append-only durability with full replay on restart. Group-commit fsync gives a **34× append-throughput** gain. |
| **Distributed internals** | Gossip membership (SWIM), consistent-hash session routing, replication with per-follower cursors and idempotent catch-up. |
| **Dual API** | gRPC + raw TCP wire protocols (plus a CLI). Transparent `NOT_LEADER` redirect follow in the SDK. |
| **Agent-native** | Session-ordered envelopes, broker-side retry/DLQ, per-tenant rate limiting (noisy-agent isolation). |
| **Observability** | Prometheus metrics + OpenTelemetry traces (session-grouped) + Grafana dashboards, out of the box. |

🖥️ **See it live:** open [`visualization/index.html`](visualization/index.html) for an animated architecture walkthrough.

## Architecture

| Layer | Responsibility |
|---|---|
| **Client APIs** | gRPC, TCP, and CLI surfaces for producers and consumers |
| **Session Router** | Picks a partition per `tenant/project/session` to keep ordering stable |
| **Partitioned Topics** | Append-only ring buffers with offset + eviction tracking |
| **Retry + DLQ** | Broker-native policy that auto-routes events past max-attempts |
| **WAL** | Append-only durability with group-commit fsync and full replay |
| **Cluster** | Raft metadata consensus, SWIM gossip membership, consistent-hash sharding, quorum replication |
| **Observability** | Prometheus counters + OpenTelemetry traces via Grafana / Tempo |

Full design notes in the [documentation](https://khangpt2k6.github.io/AgentBus/).

## Quick start

```bash
# Install (Linux / macOS)
curl -sSfL https://raw.githubusercontent.com/khangpt2k6/AgentBus/main/install.sh | sh

# Run the broker
broker --tcp-addr=:9090 --grpc-addr=:9095 --metrics-addr=:2112 --wal-path=data/agentbus.wal

# Publish + consume
goqueue publish --grpc --addr localhost:9095 --topic orders "hello"
goqueue consume --grpc --addr localhost:9095 --topic orders --group payment-service --partition -1
```

Docker, Helm, the Go SDK, cluster mode, and session replay are all covered in the docs:

- **[Installation](https://khangpt2k6.github.io/AgentBus/getting-started/)** · binaries, Docker, Helm, source
- **[Integrate the Go SDK](https://khangpt2k6.github.io/AgentBus/integrate/)** · publish agent events from your app
- **[Concepts](https://khangpt2k6.github.io/AgentBus/concepts/sessions/)** · sessions & ordering, retry/DLQ, WAL & replay
- **[Deploy](https://khangpt2k6.github.io/AgentBus/deploy/docker/)** · Docker, Kubernetes, systemd
- **[Observability](https://khangpt2k6.github.io/AgentBus/observability/)** · metrics, traces, dashboards
- **[CLI reference](https://khangpt2k6.github.io/AgentBus/reference/cli/)** · every command and flag

## Benchmarks

Reproducible from [bench/](bench/) - local developer machine, 256 B payload:

| Path | Throughput |
|---|---:|
| In-process publish | **~4.3M** msgs/sec |
| TCP localhost, end-to-end | **~45K** msgs/sec |

```bash
GOQUEUE_BENCH=1 go test ./bench -run TestThroughputReport -count=1 -v
```

> Local benchmark evidence, not production SLA claims.

---

<div align="center">

<sub>Pure-Go distributed broker · Raft · WAL · gRPC · OpenTelemetry · MIT licensed</sub>
<br />
<sub><a href="https://github.com/khangpt2k6/AgentBus/issues">Issues</a> · <a href="https://github.com/khangpt2k6/AgentBus/releases">Releases</a> · <a href="https://khangpt2k6.github.io/AgentBus/">Docs</a></sub>

<br /><br />

<img src="https://capsule-render.vercel.app/api?type=waving&color=0:00BCD4,50:6E48AA,100:1A2A6C&height=110&section=footer&animation=fadeIn" alt="footer" />

</div>
