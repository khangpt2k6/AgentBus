# GoQueue Benchmarking Guide

## Purpose
Provide reproducible benchmark evidence with clear workload boundaries.

## Environment metadata to capture
- OS and kernel version
- CPU model and core count
- RAM size
- Go version (`go version`)
- `GOMAXPROCS`
- Commit SHA

## Report commands
```bash
GOQUEUE_BENCH=1 go test ./bench -run TestThroughputReport -count=1 -v
GOQUEUE_BENCH=1 go test ./bench -run TestTCPThroughputReport -count=1 -v
GOQUEUE_BENCH=1 go test ./bench -run TestLatencyReport -count=1 -v
```

## Microbenchmark commands
```bash
go test ./bench -bench BenchmarkPublishInMemory -benchmem -count=3
go test ./bench -bench BenchmarkTCPPublish -benchmem -count=3
go test ./bench -bench BenchmarkGRPCPublish -benchmem -count=3
```

## Reporting rules
- Separate `in-process`, `TCP localhost`, and `gRPC localhost` results.
- Include payload size and message count with every number.
- Include latency percentiles (`p50`, `p95`, `p99`) where applicable.
- Do not label local benchmark numbers as production SLAs.
# GoQueue Benchmarking Guide

## Purpose
Provide reproducible local benchmark evidence with clear workload scope.

## Environment metadata template
Record these values with benchmark output:
- OS and kernel version
- CPU model and core count
- RAM size
- Go version (`go version`)
- `GOMAXPROCS`
- Commit SHA

## Commands
Run benchmark reports:

```bash
GOQUEUE_BENCH=1 go test ./bench -run TestThroughputReport -count=1 -v
GOQUEUE_BENCH=1 go test ./bench -run TestTCPThroughputReport -count=1 -v
GOQUEUE_BENCH=1 go test ./bench -run TestLatencyReport -count=1 -v
```

Run microbenchmarks:

```bash
go test ./bench -bench BenchmarkPublishInMemory -benchmem -count=3
go test ./bench -bench BenchmarkTCPPublish -benchmem -count=3
go test ./bench -bench BenchmarkGRPCPublish -benchmem -count=3
```

## Reporting format
- Clearly separate:
  - `In-process throughput`
  - `TCP localhost end-to-end throughput`
  - `gRPC localhost publish throughput`
  - `Latency percentiles (p50/p95/p99)`
- Never label local host benchmarks as production throughput.
- Include payload size and message count in every reported number.
