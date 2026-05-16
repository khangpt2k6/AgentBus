# Agent Bus

A compact, partitioned event bus in Go built around one practical question:

**How do you run multi-agent AI event streams with stable ordering, replay, and incident-level observability without heavyweight broker operations?**

---

## What you get

- :material-sort: **Stable per-session ordering** — events for `tenant/project/session` always land on the same partition.
- :material-database-refresh: **Crash replay** — append-only WAL rebuilds in-memory state on restart.
- :material-chart-bell-curve: **Built-in observability** — Prometheus counters and OpenTelemetry traces, surfaced via Grafana or the bundled WASM dashboard.
- :material-package: **Single binary** — runs anywhere Go runs. Docker Compose stack is optional.

It is not a Kafka replacement. It is focused on AI-native streaming where low operational overhead matters.

---

## 60-second tour

=== "Install"

    ```bash
    curl -sSfL https://raw.githubusercontent.com/khangpt2k6/GoQueue/main/install.sh | sh
    ```

=== "Run"

    ```bash
    broker --tcp-addr=:9090 --grpc-addr=:9095 --metrics-addr=:2112
    ```

=== "Publish + consume"

    ```bash
    goqueue publish --addr localhost:9090 --topic orders "hello"
    goqueue consume --addr localhost:9090 --topic orders --group demo
    ```

---

## Where to next

- New here? Start with **[Getting Started](getting-started.md)**.
- Want to understand the model? Read **[Sessions & Ordering](concepts/sessions.md)**.
- Going to production? See **[Deploy → Docker](deploy/docker.md)** or **[systemd](deploy/systemd.md)**.

---

## Project status

Single-node broker today. The multi-node `raft-*` fields you see in the dashboard are **state labels for the UI, not real consensus**. The distributed-v1 design lives in [Design Notes → Distributed v1](distributed-v1-design.md).
