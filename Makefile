GO ?= go
COMPOSE ?= docker compose

BROKER_TCP_ADDR ?= :9090
BROKER_GRPC_ADDR ?= :9095
BROKER_METRICS_ADDR ?= :2112
BROKER_WAL_PATH ?= data/goqueue.wal

.PHONY: help dev test lint fmt up down logs bench

help:
	@echo "Available targets:"
	@echo "  make dev    - run local broker"
	@echo "  make test   - run all Go tests once"
	@echo "  make lint   - run go vet + internal tests"
	@echo "  make fmt    - format Go code"
	@echo "  make up     - start docker compose stack"
	@echo "  make down   - stop docker compose stack"
	@echo "  make logs   - tail broker logs"
	@echo "  make bench  - run benchmark reports"

dev:
	$(GO) run ./cmd/broker --tcp-addr=$(BROKER_TCP_ADDR) --grpc-addr=$(BROKER_GRPC_ADDR) --metrics-addr=$(BROKER_METRICS_ADDR) --wal-path=$(BROKER_WAL_PATH)

test:
	$(GO) test ./... -count=1

lint:
	$(GO) vet ./...
	$(GO) test ./internal/... -count=1

fmt:
	$(GO) fmt ./...

up:
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down --remove-orphans

logs:
	$(COMPOSE) logs -f broker-1 broker-2 broker-3

bench:
	GOQUEUE_BENCH=1 $(GO) test ./bench -run TestThroughputReport -count=1 -v
	GOQUEUE_BENCH=1 $(GO) test ./bench -run TestTCPThroughputReport -count=1 -v
	GOQUEUE_BENCH=1 $(GO) test ./bench -run TestLatencyReport -count=1 -v
