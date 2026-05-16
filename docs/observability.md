# Observability

Agent Bus emits Prometheus metrics and OpenTelemetry traces out of the box. Point Grafana at the included dashboards and you have incident-level visibility in minutes.

## Metrics endpoint

```
http://<broker>:2112/metrics
```

Scrape with Prometheus on whatever interval suits your retention budget — 10–15s is a good default.

## Counters that matter

Agent-specific:

| Counter | What it means | When to alert |
|---|---|---|
| `goqueue_agent_events_published_total` | every accepted agent event (originals + retries) | sudden drop = upstream agents are silent |
| `goqueue_agent_event_retries_total` | events re-published with `attempt+1` | sustained rate > X% of published = a tool/agent is broken |
| `goqueue_agent_event_dlq_total` | events that hit `max-attempts` and went to DLQ | any non-zero rate is worth a look |

Plus the standard broker counters (publishes, consumes, lag) — see `internal/metrics/`.

## Traces

OTEL traces flow to Tempo (in the bundled stack) or any OTLP-compatible backend. The default span hierarchy is:

```
publish (producer)
  └─ broker.route
      └─ wal.append
broker.deliver (consumer pull)
  └─ consumer.handle (downstream span)
```

This makes it trivial to see *which session, on which partition, took how long*.

## The bundled stack

```bash
docker compose up --build
```

| Service | URL | Purpose |
|---|---|---|
| Grafana | <http://localhost:3000> | dashboards |
| Prometheus | <http://localhost:9099> | metrics store |
| Tempo | <http://localhost:3200> | trace store |
| Broker metrics | <http://localhost:2112/metrics> | scrape target |
| Broker readiness | <http://localhost:2112/readyz> | health probe |

Grafana provisioning is in `docker/grafana/` — datasources and dashboards are pre-loaded.

## The WASM dashboard (no extra stack)

If you don't want to run Grafana, the bundled WASM dashboard reads the broker's metrics endpoint directly:

```powershell
$env:GOOS="js"
$env:GOARCH="wasm"
go build -o web/app.wasm ./cmd/dashboard
Remove-Item Env:GOOS
Remove-Item Env:GOARCH
go run ./cmd/dashboard --broker http://localhost:2112 --addr :8080 --wasm-dir web
```

Open <http://localhost:8080>.

## Suggested alerts

Starter PromQL:

```promql
# DLQ should normally be flat
rate(goqueue_agent_event_dlq_total[5m]) > 0

# retry storm — more than 10% of published events are retries
rate(goqueue_agent_event_retries_total[5m])
  / rate(goqueue_agent_events_published_total[5m]) > 0.10

# broker silence — no publishes for 5 minutes during business hours
rate(goqueue_agent_events_published_total[5m]) == 0
```
