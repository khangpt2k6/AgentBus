# Getting Started

By the end of this page you'll have Agent Bus running locally and have sent your first session-ordered AI event through it.

## 1. Install

=== "One-line installer (Linux/macOS)"

    ```bash
    curl -sSfL https://raw.githubusercontent.com/khangpt2k6/GoQueue/main/install.sh | sh
    ```

    Pin a version: `... | sh -s -- --version v0.1.0`.

=== "Docker"

    ```bash
    docker run --rm -p 9090:9090 -p 9095:9095 -p 2112:2112 \
      ghcr.io/khangpt2k6/goqueue:latest \
      --tcp-addr=:9090 --grpc-addr=:9095 --metrics-addr=:2112
    ```

=== "Build from source"

    ```bash
    git clone https://github.com/khangpt2k6/GoQueue.git
    cd GoQueue
    go build -o bin/broker  ./cmd/broker
    go build -o bin/goqueue ./cmd/goqueue
    ```

Verify:

```bash
broker --help
goqueue --help
```

## 2. Run the broker

```bash
broker \
  --tcp-addr=:9090 \
  --grpc-addr=:9095 \
  --metrics-addr=:2112 \
  --wal-path=./data/agentbus.wal
```

You should see startup logs binding to those three ports. The WAL file is created on first write.

## 3. Send a plain message

In a second terminal:

```bash
goqueue publish --addr localhost:9090 --topic orders "hello tcp"
goqueue consume --addr localhost:9090 --topic orders --group demo
```

The consumer prints the message. You've just published and consumed through the TCP API.

## 4. Send an agent event (the real use case)

Agent events carry session metadata so the broker can preserve per-session ordering:

```bash
goqueue publish-agent --grpc --addr localhost:9095 \
  --tenant acme --project support-bot --session sess-42 \
  --agent planner --type tool.call --step retrieve-context --attempt 1 \
  --payload '{"tool":"search","query":"latest order status"}'
```

Read it back as a consumer group:

```bash
goqueue consume --grpc --addr localhost:9095 \
  --topic agent-events --group payment-service --partition -1
```

## 5. See it in metrics

```bash
curl -s http://localhost:2112/metrics | grep goqueue_agent
```

You should see counters like `goqueue_agent_events_published_total` and (after retries) `goqueue_agent_event_retries_total`.

## 6. Next

- Why same-session events stay in order: **[Sessions & Ordering](concepts/sessions.md)**
- What happens on consumer failure: **[Retry & DLQ](concepts/retry-dlq.md)**
- Crash recovery story: **[WAL & Replay](concepts/wal-replay.md)**
- Production: **[Deploy → Docker](deploy/docker.md)**
