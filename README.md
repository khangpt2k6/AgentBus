<!--
  Agent Bus - README
  Animated header uses capsule-render + readme-typing-svg (public services).
-->

<div align="center">

<img src="https://capsule-render.vercel.app/api?type=waving&color=0:1A2A6C,50:6E48AA,100:00BCD4&height=210&section=header&text=Agent%20Bus&fontSize=72&fontColor=FFFFFF&fontAlignY=38&desc=A%20distributed%20event%20backbone%20for%20AI%20agents&descSize=18&descAlignY=60&animation=fadeIn" alt="Agent Bus" />

<br />

<a href="https://github.com/khangpt2k6/AgentBus/releases">
  <img src="https://readme-typing-svg.demolab.com?font=JetBrains+Mono&weight=600&size=22&duration=3200&pause=900&color=6E48AA&center=true&vCenter=true&width=760&lines=Raft-replicated%2C+zero-data-loss+failover;Durable+workflow+execution+on+the+log;Group-commit+WAL+-+34x+append+throughput;Session-ordered+agent+event+streams;Prometheus+%2B+OpenTelemetry+built-in" alt="Typing tagline" />
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

A **distributed message broker and workflow execution runtime written from scratch in Go**, specialized for AI-agent event streams. It routes each `tenant/project/session` to a stable partition for in-order streaming, persists every write to a crash-safe WAL, runs as a Raft-replicated cluster with automatic failover, and schedules durable task execution (lease, heartbeat, retry, exactly-once completion) on top of the same log - all from a single binary, no Kafka-scale ops.

> Not a Kafka replacement. Focused on AI-native streaming where ordering, replay, and low operational overhead matter.



<img width="1414" height="910" alt="Adobe Express - Screen Recording 2026-06-25 130203" src="https://github.com/user-attachments/assets/65d495ff-cf27-4695-b453-290661069eae" />

## Engineering highlights

| | |
|---|---|
| **Raft consensus** | 3-node cluster, leader election, automatic failover. Killing any non-leader loses **zero messages** under quorum acks - verified by a kill-the-leader test. |
| **Durable workflow execution** | Submit / lease / heartbeat / retry / exactly-once completion, event-sourced on the replicated log. State is a pure fold over events, so crash recovery and debugging replay are **deterministic** - a restarted broker rebuilds byte-identical execution state. Sustains **2K+ concurrent executions and 45K+ workflow events/sec** under k6 (see benchmarks). |
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
| **Workflow runtime** | Coordinator that leases tasks to workers (FIFO per task type, batched RPCs), sweeps expired leases into retries, and fences stale completions by attempt number; every transition is a durable log event |
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

# Durable workflow execution
goqueue workflow submit --tenant acme --project etl --id job-1 --task-type transform --input '{"rows":100}'
goqueue workflow status --tenant acme --project etl --id job-1
goqueue workflow history --tenant acme --project etl --id job-1   # deterministic replay of state transitions
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

**Workflow runtime under k6** - reproducible from [load/](load/), single broker, WAL fsync interval 250ms, k6 and broker sharing one laptop (Ryzen AI 7 350). Every submit / lease / heartbeat / complete / retry is an individual durable log event; rates measured server-side from `goqueue_wf_events_total`:

| Metric | Measured |
|---|---:|
| Workflow events/sec, best 60s window | **50.8K** |
| Workflow events/sec, avg over 134s run | **44.8K** |
| Peak 10s window | **53.7K** |
| Long-running executions held concurrently (leased + heartbeating, full run) | **2,500** |
| Peak concurrently running executions | **38.5K** |
| Peak in-flight tracked executions | **60.7K** |

```bash
# broker running locally, then:
k6 run -e BATCH=32 -e SUBMIT_RATE=520 -e WORKER_VUS=288 -e HOLD_COUNT=2500 load/k6/workflow_load.js
go run ./load/metricsampler   # server-side rates while it runs
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
