# GoQueue Benchmarking Guide

## Purpose

Provide reproducible local benchmark evidence with clear workload scope.

## Environment Metadata Template

Record these values with benchmark output:

- OS and kernel version
- CPU model and core count
- RAM size
- Go version (`go version`)
- `GOMAXPROCS`
- commit SHA

## Report Commands

```bash
GOQUEUE_BENCH=1 go test ./bench -run TestThroughputReport -count=1 -v
GOQUEUE_BENCH=1 go test ./bench -run TestTCPThroughputReport -count=1 -v
GOQUEUE_BENCH=1 go test ./bench -run TestLatencyReport -count=1 -v
GOQUEUE_BENCH=1 go test ./bench -run TestAgentEventThroughputAndMetricsReport -count=1 -v
```

## Microbenchmark Commands

```bash
go test ./bench -bench BenchmarkPublishInMemory -benchmem -count=3
go test ./bench -bench BenchmarkTCPPublish -benchmem -count=3
go test ./bench -bench BenchmarkGRPCPublish -benchmem -count=3
```

## Reporting Rules

- Separate `in-process`, `TCP localhost`, and `gRPC localhost` results.
- Include payload size and message count with every number.
- Include latency percentiles (`p50`, `p95`, `p99`) where applicable.
- For agent workloads, include retry/DLQ counters with labeled values.
- Never label local benchmark numbers as production SLA.
