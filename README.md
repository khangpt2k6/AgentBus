<!--
  Agent Bus — README
  Animated header uses capsule-render + readme-typing-svg (public services).
-->

<div align="center">

<img src="https://capsule-render.vercel.app/api?type=waving&color=0:1A2A6C,50:6E48AA,100:00BCD4&height=210&section=header&text=Agent%20Bus&fontSize=72&fontColor=FFFFFF&fontAlignY=38&desc=A%20session-ordered%20event%20bus%20for%20AI%20agents&descSize=18&descAlignY=60&animation=fadeIn" alt="Agent Bus" />

<br />

<a href="https://github.com/khangpt2k6/AgentBus/releases">
  <img src="https://readme-typing-svg.demolab.com?font=JetBrains+Mono&weight=600&size=22&duration=3200&pause=900&color=6E48AA&center=true&vCenter=true&width=760&lines=Stable+per-session+ordering;Replay+from+WAL+on+restart;Prometheus+%2B+OpenTelemetry+built-in;Single+Go+binary%2C+zero-ops+by+default" alt="Typing tagline" />
</a>

<p>
  <a href="https://github.com/khangpt2k6/AgentBus/releases">
    <img src="https://img.shields.io/github/v/release/khangpt2k6/AgentBus?style=for-the-badge&color=6E48AA&labelColor=1A1A2E" alt="Release" />
  </a>
  <img src="https://img.shields.io/badge/Go-1.22%2B-00ADD8?style=for-the-badge&logo=go&logoColor=white&labelColor=1A1A2E" alt="Go" />
  <a href="https://github.com/khangpt2k6/AgentBus/pkgs/container/goqueue">
    <img src="https://img.shields.io/badge/Docker-ghcr.io-2496ED?style=for-the-badge&logo=docker&logoColor=white&labelColor=1A1A2E" alt="Docker" />
  </a>
  <img src="https://img.shields.io/badge/License-MIT-2EA44F?style=for-the-badge&labelColor=1A1A2E" alt="License" />
  <a href="https://github.com/khangpt2k6/AgentBus/stargazers">
    <img src="https://img.shields.io/github/stars/khangpt2k6/AgentBus?style=for-the-badge&color=FFB000&labelColor=1A1A2E" alt="Stars" />
  </a>
</p>

<p>
  <a href="#-install"><b>Install</b></a> ·
  <a href="#-quick-start"><b>Quick Start</b></a> ·
  <a href="#-replay-a-failed-agent-run"><b>Replay</b></a> ·
  <a href="docs/"><b>Docs</b></a> ·
  <a href="examples/"><b>Examples</b></a> ·
  <a href="#-benchmarks"><b>Benchmarks</b></a>
</p>

</div>

---

## The Problem It Solves

> **How do you run multi-agent AI event streams with stable ordering, replay, and incident-level observability — without heavyweight broker operations?**

Multi-agent systems produce bursty streams of token chunks, tool calls, retries, and handoffs. Teams usually pick between two bad options:

- **Lightweight queues** — easy to run, hard to replay and debug
- **Heavyweight brokers** — powerful, expensive to operate at small scale

`Agent Bus` targets that gap.

---

## What It Is

`Agent Bus` is a **session-ordered event bus for AI agents** packaged as a single Go binary.

<table>
<tr>
<td width="50%" valign="top">

### Session-ordered routing
Routes by `tenant / project / session` so each conversation streams in order, on a stable partition.

</td>
<td width="50%" valign="top">

### Crash-safe replay
Append-only WAL with full replay on restart. Reconstruct any session, in order, after the fact.

</td>
</tr>
<tr>
<td width="50%" valign="top">

### Observability by default
Prometheus counters, OpenTelemetry traces, and a built-in WASM dashboard ship out of the box.

</td>
<td width="50%" valign="top">

### Zero-ops baseline
One Go binary. Docker Compose is optional, not required to run or develop against it.

</td>
</tr>
</table>

> It is **not** a Kafka replacement. It is focused on AI-native streaming where low operational overhead matters.

---

## Architecture

![Agent Bus architecture: agent tools publish events through TCP, gRPC, and CLI clients into the Agent Bus Core (session router, partition topics, consumer groups), with observability and durability layers attached.](<Agent tools.png>)

<table>
<thead>
<tr><th align="left">Layer</th><th align="left">Responsibility</th></tr>
</thead>
<tbody>
<tr><td><b>Client APIs</b></td><td>TCP, gRPC, and CLI surfaces for producers and consumers</td></tr>
<tr><td><b>Session Router</b></td><td>Picks a partition per <code>tenant/project/session</code> to keep ordering stable</td></tr>
<tr><td><b>Partitioned Topics</b></td><td>Append-only ring buffers with offset and eviction tracking</td></tr>
<tr><td><b>Retry + DLQ</b></td><td>Broker-native policy that auto-routes failed events on max-attempt</td></tr>
<tr><td><b>WAL</b></td><td>Append-only durability with full replay on restart</td></tr>
<tr><td><b>Observability</b></td><td>Prometheus counters and OpenTelemetry traces, surfaced via Grafana or the built-in WASM dashboard</td></tr>
</tbody>
</table>

> **Status:** Single-node broker today. The distributed v1 **foundation** (3-node cluster forms, gossips, elects a real metadata Raft leader) ships on the [`feat/cluster-v1`](https://github.com/khangpt2k6/AgentBus/tree/feat/cluster-v1) branch — see [docs/cluster.md](docs/cluster.md) for usage and [docs/superpowers/specs/2026-05-16-distributed-v1-design.md](docs/superpowers/specs/2026-05-16-distributed-v1-design.md) for the full design through M5 (routing, ISR replication, failover).

---

## Current Scope

- Single-node broker runtime today.
- Docker Compose can spin up multiple nodes for local topology and observability demos.
- `raft-*` fields are state labels for the dashboard, not real consensus replication. The distributed-v1 design lives in [docs/superpowers/specs/2026-05-16-distributed-v1-design.md](docs/superpowers/specs/2026-05-16-distributed-v1-design.md).

---

## 📦 Install

Pick whichever fits your platform.

### One-line installer · Linux / macOS

```bash
curl -sSfL https://raw.githubusercontent.com/khangpt2k6/AgentBus/main/install.sh | sh
```

Installs `broker` and `goqueue` to `/usr/local/bin` (falls back to `$HOME/.local/bin`). Pin a version with `sh -s -- --version v0.1.0`.

### Docker

```bash
docker run --rm -p 9090:9090 -p 9095:9095 -p 2112:2112 \
  ghcr.io/khangpt2k6/goqueue:latest \
  --tcp-addr=:9090 --grpc-addr=:9095 --metrics-addr=:2112
```

### Kubernetes (Helm)

```bash
helm install agentbus oci://ghcr.io/khangpt2k6/charts/agentbus
```

Provisions a Deployment, Service, PVC for the WAL, and (optional) ServiceMonitor for Prometheus. Full guide: [docs/deploy/kubernetes.md](docs/deploy/kubernetes.md).

<details>
<summary><b>Manual download</b></summary>

Grab the archive for your OS/arch from [Releases](https://github.com/khangpt2k6/AgentBus/releases), verify with `checksums.txt`, extract, and put `broker` + `goqueue` on your `PATH`.

</details>

<details>
<summary><b>Build from source</b></summary>

```bash
git clone https://github.com/khangpt2k6/AgentBus.git
cd AgentBus
go build -o bin/broker  ./cmd/broker
go build -o bin/goqueue ./cmd/goqueue
```

</details>

### Use the Go SDK in your own project

```bash
go get github.com/khangpt2k6/AgentBus/agentbus@latest
```

```go
import "github.com/khangpt2k6/AgentBus/agentbus"

client, _ := agentbus.Connect(ctx, "localhost:9095")
defer client.Close()

client.PublishAgent(ctx, agentbus.AgentEvent{
    Tenant: "acme", Project: "support-bot", SessionID: "sess-42",
    AgentID: "planner", Type: "tool.call",
    Payload: []byte(`{"tool":"search"}`),
})
```

Full guide: [docs/integrate.md](docs/integrate.md). Runnable examples in [`examples/`](examples/).

---

## 🚀 Quick Start

### Run the broker

```bash
broker --tcp-addr=:9090 --grpc-addr=:9095 --metrics-addr=:2112 --wal-path=data/agentbus.wal
```

### Publish and consume — TCP

```bash
goqueue publish --addr localhost:9090 --topic orders "hello tcp"
goqueue consume --addr localhost:9090 --topic orders --group payment-service
```

### Publish and consume — gRPC

```bash
goqueue publish --grpc --addr localhost:9095 --topic orders "hello grpc"
goqueue consume --grpc --addr localhost:9095 --topic orders --group payment-service --partition -1
```

### Key-based routing

```bash
goqueue publish --grpc --addr localhost:9095 --topic orders --key user-42 "order-a"
goqueue publish --grpc --addr localhost:9095 --topic orders --key user-42 "order-b"
```

### Publish an agent event (session-ordered)

```bash
goqueue publish-agent --grpc --addr localhost:9095 \
  --tenant acme --project support-bot --session sess-42 --agent planner \
  --type tool.call --step retrieve-context --attempt 1 \
  --payload '{"tool":"search","query":"latest order status"}'
```

---

## 🔁 Replay a failed agent run

When a multi-agent workflow fails, dump the whole conversation by session ID:

```bash
goqueue session replay --grpc --addr localhost:9095 \
  --tenant acme --project support-bot --session sess-42
```

```text
[14:02:11.394] off=21084  tool.call      retrieve-context  agent=planner
    {"tool":"search","query":"latest order"}
[14:02:11.811] off=21085  tool.result    retrieve-context  agent=planner
    {"results":[],"latency_ms":417}
[14:02:14.118] off=21088  tool.call      retrieve-context  (attempt 3)  agent=planner
    {"tool":"search","db":"cold-storage"}
[14:02:14.613] off=21090  handoff                          agent=planner
    {"from_agent":"planner","to_agent":"escalator","reason":"3 failed attempts"}
```

Live tail with `goqueue session tail`. Full guide: [docs/debug-agent-run.md](docs/debug-agent-run.md). Self-hosted, no vendor required.

### Retry or route a failed agent event to DLQ

```bash
goqueue retry-agent --grpc --addr localhost:9095 \
  --topic agent-events --max-attempts 3 --delay 2s \
  --event '{"version":"v1","type":"tool.call","tenant":"acme","project":"support-bot","session_id":"sess-42","agent_id":"planner","attempt":1,"created_at":"2026-04-03T10:00:00Z","payload":{"tool":"search","query":"latest order status"}}'
```

If `attempt + 1 > max-attempts`, the event is routed to `<topic>.dlq` (or `--dlq-topic` if specified).

> **Note** · The CLI binary is still named `goqueue` in code. A package rename will follow in a separate pass to keep the build clean.

---

## 📊 WASM Dashboard

<details>
<summary><b>Build and run the dashboard</b></summary>

```powershell
$env:GOOS="js"
$env:GOARCH="wasm"
go build -o web/app.wasm ./cmd/dashboard
Remove-Item Env:GOOS
Remove-Item Env:GOARCH
go run ./cmd/dashboard --broker http://localhost:2112 --addr :8080 --wasm-dir web
```

Open `http://localhost:8080`.

</details>

---

## 🛠️ Automation

<table>
<tr>
<td width="50%" valign="top">

**macOS / Linux**

```bash
make dev
make test
make lint
make up
make down
make clean
```

</td>
<td width="50%" valign="top">

**Windows · PowerShell**

```powershell
./scripts/goqueue.ps1 dev
./scripts/goqueue.ps1 test
./scripts/goqueue.ps1 lint
./scripts/goqueue.ps1 up
./scripts/goqueue.ps1 down
./scripts/goqueue.ps1 clean
```

</td>
</tr>
</table>

Use `make help` or `./scripts/goqueue.ps1 help` to list available tasks.

---

## 🔭 Observability Stack

```bash
docker compose up --build
```

<table>
<thead>
<tr><th align="left">Service</th><th align="left">URL</th></tr>
</thead>
<tbody>
<tr><td>Grafana</td><td><code>http://localhost:3000</code></td></tr>
<tr><td>Prometheus</td><td><code>http://localhost:9099</code></td></tr>
<tr><td>Tempo</td><td><code>http://localhost:3200</code></td></tr>
<tr><td>Broker metrics</td><td><code>http://localhost:2112/metrics</code></td></tr>
<tr><td>Broker readiness</td><td><code>http://localhost:2112/readyz</code></td></tr>
</tbody>
</table>

**Agent-focused counters**

- `goqueue_agent_events_published_total`
- `goqueue_agent_event_retries_total`
- `goqueue_agent_event_dlq_total`

---

## 📈 Benchmarks

Reproducible from [bench/bench_test.go](bench/bench_test.go).

```bash
GOQUEUE_BENCH=1 go test ./bench -run TestThroughputReport -count=1 -v
GOQUEUE_BENCH=1 go test ./bench -run TestTCPThroughputReport -count=1 -v
GOQUEUE_BENCH=1 go test ./bench -run TestLatencyReport -count=1 -v
GOQUEUE_BENCH=1 go test ./bench -run TestAgentEventThroughputAndMetricsReport -count=1 -v
```

Reference local numbers · developer machine · 256 B payload:

<table>
<thead>
<tr><th align="left">Path</th><th align="right">Throughput</th></tr>
</thead>
<tbody>
<tr><td>In-process publish</td><td align="right"><b>~4.3M</b> msgs/sec</td></tr>
<tr><td>TCP localhost · end-to-end</td><td align="right"><b>~45K</b> msgs/sec</td></tr>
</tbody>
</table>

> Treat these as local benchmark evidence, not production SLA claims.

---

## 🗂️ Project Layout

```text
cmd/broker            broker server entrypoint
cmd/goqueue           CLI for publish / consume / agent flows
cmd/dashboard         dashboard server + WASM build target
internal/broker       routing, topic, partition logic
internal/wal          write-ahead log and replay
internal/agentstream  AI event envelope + session key + retry policy
internal/grpcapi      gRPC service implementation
internal/metrics      Prometheus metrics
internal/telemetry    OpenTelemetry setup
proto                 gRPC / protobuf contracts
web                   Go WASM dashboard source
bench                 reproducible benchmark harness
```

---

## 💡 Why This Project

- Models the real concerns of an event bus — ordering, replay, lag, and visibility.
- Uses practical interfaces (TCP and gRPC) instead of a toy API.
- Stays readable enough to extend and experiment with.
- Has a clear path to a real distributed v1.

---

<div align="center">

<sub>Built with Go · Licensed under MIT · <a href="https://github.com/khangpt2k6/AgentBus/issues">Issues</a> · <a href="https://github.com/khangpt2k6/AgentBus/releases">Releases</a></sub>

<br /><br />

<img src="https://capsule-render.vercel.app/api?type=waving&color=0:00BCD4,50:6E48AA,100:1A2A6C&height=110&section=footer&animation=fadeIn" alt="footer" />

</div>
