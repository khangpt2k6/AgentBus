# Deploy with Docker

The fastest way to run Agent Bus in production-shaped infrastructure.

## Just the broker

```bash
docker run -d --name agentbus \
  -p 9090:9090 -p 9095:9095 -p 2112:2112 \
  -v $(pwd)/data:/data \
  ghcr.io/khangpt2k6/goqueue:latest \
  --tcp-addr=:9090 \
  --grpc-addr=:9095 \
  --metrics-addr=:2112 \
  --wal-path=/data/agentbus.wal
```

Image tags:

| Tag | When to use |
|---|---|
| `latest` | latest released version |
| `v0.1.0` (etc.) | pin to a specific release |
| `sha-<7chars>` | pin to a commit |

## Full observability stack

The repository ships a `docker-compose.yml` that runs the broker plus Grafana, Prometheus, and Tempo:

```bash
git clone https://github.com/khangpt2k6/AgentBus.git
cd GoQueue
docker compose up --build
```

| Service | URL |
|---|---|
| Grafana | <http://localhost:3000> |
| Prometheus | <http://localhost:9099> |
| Tempo | <http://localhost:3200> |
| Broker metrics | <http://localhost:2112/metrics> |
| Broker readiness | <http://localhost:2112/readyz> |

## Health checks

The broker exposes `/readyz` and `/metrics` on the metrics port. For Kubernetes / Compose health probes:

```yaml
healthcheck:
  test: ["CMD", "wget", "-qO-", "http://localhost:2112/readyz"]
  interval: 10s
  timeout: 3s
  retries: 3
  start_period: 5s
```

## Persistent storage

The WAL is the only stateful component. Mount it on a persistent volume:

```yaml
services:
  broker:
    image: ghcr.io/khangpt2k6/goqueue:latest
    volumes:
      - agentbus-data:/data
    command:
      - --tcp-addr=:9090
      - --grpc-addr=:9095
      - --metrics-addr=:2112
      - --wal-path=/data/agentbus.wal

volumes:
  agentbus-data:
```

Lose the volume, lose the log. Plan backups accordingly until replicated WAL ships (see [Distributed v1](../distributed-v1-design.md)).

## Resources

Reasonable starting point for a single-node broker:

| Resource | Suggestion |
|---|---|
| CPU | 1 vCPU |
| RAM | 256–512 MiB |
| Disk | depends on retention; size for `peak msgs/sec × avg payload × retention` |

Re-benchmark for your payload sizes — see [Benchmarking](../benchmarking.md).
