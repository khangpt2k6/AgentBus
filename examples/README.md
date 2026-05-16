# Examples

Runnable programs that show how to use the AgentBus Go SDK from your own code.

Each example needs a running broker. The simplest path:

```bash
docker run --rm -p 9095:9095 ghcr.io/khangpt2k6/goqueue:latest --grpc-addr=:9095
```

Then in another terminal:

| Example | What it shows |
|---|---|
| [`basic/`](basic/main.go) | Connect, Publish, Subscribe round-trip |
| [`agent-events/`](agent-events/main.go) | `PublishAgent` with per-session ordering and structured envelope decoding |

Run them:

```bash
go run ./examples/basic
go run ./examples/agent-events
```

To use the SDK in your own module:

```bash
go get github.com/khangpt2k6/AgentBus/agentbus@latest
```

See [docs/integrate.md](../docs/integrate.md) for the integration guide.
