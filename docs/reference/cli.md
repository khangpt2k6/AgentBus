# CLI Reference

Two binaries: **`broker`** (the server) and **`goqueue`** (the client).

!!! note
    The CLI binary is still named `goqueue` in code. A package rename to `agentbus` is planned in a separate pass.

---

## `broker`

Run the server. Authoritative reference is `broker --help`.

| Flag | Purpose | Default |
|---|---|---|
| `--tcp-addr` | TCP listen address | `:9090` |
| `--grpc-addr` | gRPC listen address | `:9095` |
| `--metrics-addr` | Prometheus + readiness listen address | `:2112` |
| `--wal-path` | Path to the WAL file | `data/agentbus.wal` |

Example:

```bash
broker \
  --tcp-addr=:9090 \
  --grpc-addr=:9095 \
  --metrics-addr=:2112 \
  --wal-path=/var/lib/agentbus/agentbus.wal
```

---

## `goqueue`

Subcommand-style CLI for producers, consumers, and agent-event helpers.

### `publish`

```bash
goqueue publish --addr localhost:9090 --topic orders "hello"
goqueue publish --grpc --addr localhost:9095 --topic orders "hello grpc"
goqueue publish --grpc --addr localhost:9095 --topic orders --key user-42 "msg"
```

| Flag | Meaning |
|---|---|
| `--addr` | broker address |
| `--grpc` | use the gRPC API instead of TCP |
| `--topic` | destination topic |
| `--key` | optional routing key (same key → same partition) |

### `consume`

```bash
goqueue consume --addr localhost:9090 --topic orders --group payment-service
goqueue consume --grpc --addr localhost:9095 --topic orders --group payment-service --partition -1
```

| Flag | Meaning |
|---|---|
| `--group` | consumer group ID |
| `--partition` | specific partition, or `-1` for all (gRPC only) |

### `publish-agent`

Publish a structured agent event:

```bash
goqueue publish-agent --grpc --addr localhost:9095 \
  --tenant acme --project support-bot --session sess-42 \
  --agent planner --type tool.call --step retrieve-context --attempt 1 \
  --payload '{"tool":"search","query":"latest order status"}'
```

| Flag | Meaning |
|---|---|
| `--tenant` / `--project` / `--session` | the routing triple — see [Sessions](../concepts/sessions.md) |
| `--agent` | agent identifier (free-form) |
| `--type` | event type (`tool.call`, `token.chunk`, etc.) |
| `--step` | step name within the session |
| `--attempt` | attempt number (1-based) |
| `--payload` | JSON payload string |

### `retry-agent`

Apply the retry-or-DLQ policy to an existing event. See [Retry & DLQ](../concepts/retry-dlq.md).

```bash
goqueue retry-agent --grpc --addr localhost:9095 \
  --topic agent-events --max-attempts 3 --delay 2s \
  --event '{...full event JSON...}'
```

| Flag | Meaning |
|---|---|
| `--max-attempts` | upper bound on `attempt`; one more pushes to DLQ |
| `--delay` | wait before re-publishing on the retry path |
| `--dlq-topic` | override DLQ destination (default: `<topic>.dlq`) |

---

For exhaustive flags and recent additions, always trust `broker --help` / `goqueue <subcommand> --help`.
