# Integrate AgentBus into your Go project

The `github.com/khangpt2k6/AgentBus/agentbus` package is the official Go SDK. Add it to your module, point it at a running broker, and you're publishing and consuming in a few lines.

## Install

```bash
go get github.com/khangpt2k6/AgentBus/agentbus@latest
```

## Minimal example

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "log"
    "time"

    "github.com/khangpt2k6/AgentBus/agentbus"
)

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    client, err := agentbus.Connect(ctx, "localhost:9095")
    if err != nil { log.Fatal(err) }
    defer client.Close()

    res, err := client.Publish(ctx, "orders", []byte("hello"))
    if err != nil { log.Fatal(err) }
    fmt.Printf("published partition=%d offset=%d\n", res.Partition, res.Offset)
}
```

That's the whole "hello world."

---

## Publishing

The SDK offers three publish modes — pick by what kind of ordering you need.

| Method | When | Routing |
|---|---|---|
| [`Publish`](#publish) | Fire-and-forget messages, no ordering required | Round-robin across partitions |
| [`PublishWithKey`](#publishwithkey) | Per-entity ordering (e.g. per-user) | hash(key) → partition |
| [`PublishToPartition`](#publishtopartition) | You know exactly where it goes | Explicit |
| [`PublishAgent`](#publishagent) | AI agent events with session ordering | hash(tenant/project/session) |

### `Publish`

```go
res, err := client.Publish(ctx, "topic", []byte("payload"))
```

### `PublishWithKey`

```go
res, err := client.PublishWithKey(ctx, "orders", "user-42", []byte("..."))
```

Same `user-42` key always lands on the same partition, so its messages stay in order.

### `PublishToPartition`

```go
res, err := client.PublishToPartition(ctx, "orders", 2, []byte("..."))
```

Use when you maintain partition assignment yourself.

### `PublishAgent`

For multi-agent workflows where per-session ordering matters. The SDK builds the standard JSON envelope and routes by `tenant/project/session`.

```go
res, err := client.PublishAgent(ctx, agentbus.AgentEvent{
    Tenant:    "acme",
    Project:   "support-bot",
    SessionID: "sess-42",
    AgentID:   "planner",
    Type:      "tool.call",
    Step:      "retrieve-context",
    Attempt:   1,
    Payload:   []byte(`{"tool":"search","query":"last order"}`),
})
```

`PublishAgent` defaults to topic `agent-events`. Use `PublishAgentTo` if you want a different destination (e.g. a DLQ).

---

## Consuming

`Subscribe` opens a server-streaming consumer. Call `Next` in a loop until the context is canceled or the stream ends.

```go
sub, err := client.Subscribe(ctx, "agent-events", "billing-service")
if err != nil { log.Fatal(err) }
defer sub.Close()

for {
    msg, err := sub.Next(ctx)
    if errors.Is(err, agentbus.ErrSubscriptionClosed) {
        break
    }
    if err != nil {
        log.Printf("recv: %v", err)
        break
    }
    fmt.Printf("offset=%d payload=%s\n", msg.Offset, msg.Payload)
}
```

Consumer groups: the broker tracks the last delivered offset per `(topic, group, partition)`. A second consumer joining the same group resumes from where the first left off.

### Pin to a specific partition

```go
sub, err := client.SubscribeWithOptions(ctx, "orders", "billing", agentbus.SubscribeOptions{
    Partition: 2,
})
```

The default (`Partition: -1`) reads from one partition chosen by hashing the group name. For full-fanout consumption, start one `Subscribe` per partition.

---

## TLS

For production / cross-host deployments:

```go
import "google.golang.org/grpc/credentials"

creds, _ := credentials.NewClientTLSFromFile("ca.pem", "")
client, err := agentbus.Connect(ctx, "broker.example.com:9095",
    agentbus.WithTLS(creds),
)
```

For mTLS / advanced setups, use `WithDialOption(grpc.WithTransportCredentials(...))`.

---

## Patterns

### Idempotent consumer

Retries happen. Make your handler safe to run twice on the same message:

```go
msg, _ := sub.Next(ctx)

if alreadyProcessed(msg.Offset) {
    continue
}
process(msg)
markProcessed(msg.Offset)
```

### Fan out to workers

```go
work := make(chan agentbus.Message, 64)
for i := 0; i < runtime.NumCPU(); i++ {
    go worker(work)
}
for {
    msg, err := sub.Next(ctx)
    if err != nil { return }
    work <- msg
}
```

Single goroutine drives `Next`; workers process. Don't share a `Subscription` across goroutines.

### Graceful shutdown

```go
ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer cancel()

client, _ := agentbus.Connect(ctx, addr)
defer client.Close()

// ... publish / subscribe loops respect ctx ...

<-ctx.Done()
```

Subscription Next returns `ErrSubscriptionClosed` when ctx is canceled — drain in your loop.

---

## Runnable examples

The repo ships two complete examples:

- [`examples/basic/`](https://github.com/khangpt2k6/AgentBus/tree/main/examples/basic) — publish + subscribe round-trip
- [`examples/agent-events/`](https://github.com/khangpt2k6/AgentBus/tree/main/examples/agent-events) — `PublishAgent` with envelope decoding

Clone and run after starting a broker:

```bash
docker run --rm -p 9095:9095 ghcr.io/khangpt2k6/goqueue:latest --grpc-addr=:9095
# in another terminal
go run ./examples/basic
```

---

## Stability

The SDK is at `v0.1.x` — the API surface is small on purpose. Breaking changes are possible before v1; they'll be called out in release notes. Wire format is stable: the broker accepts records from older SDKs and vice versa within the v1 envelope.
