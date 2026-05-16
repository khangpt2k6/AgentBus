# Helm chart for AgentBus

Single-node AgentBus broker on Kubernetes. The chart provisions:

- **Deployment** (1 replica, `Recreate` strategy — the WAL is single-writer)
- **Service** with named ports (`tcp`, `grpc`, `metrics`)
- **PersistentVolumeClaim** for the WAL (8 Gi default, RWO)
- **ServiceAccount** with token automount disabled
- **ServiceMonitor** for Prometheus Operator (off by default)
- Optional **Ingress** (off by default)

## Install

```bash
# From a clone (current path)
helm install agentbus ./deploy/helm/agentbus

# Or from the OCI registry once the chart is published to GHCR:
helm install agentbus oci://ghcr.io/khangpt2k6/charts/agentbus --version 0.1.0
```

## Common overrides

```bash
# Persist the WAL on a 50 GiB volume
helm install agentbus ./deploy/helm/agentbus \
  --set persistence.size=50Gi \
  --set persistence.storageClass=gp3

# Pin a specific broker image build
helm install agentbus ./deploy/helm/agentbus \
  --set image.tag=v0.4.0

# Wire up Prometheus (Prometheus Operator stack)
helm install agentbus ./deploy/helm/agentbus \
  --set serviceMonitor.enabled=true

# Send traces to an OTEL collector in the cluster
helm install agentbus ./deploy/helm/agentbus \
  --set otel.enabled=true \
  --set otel.endpoint=otel-collector.observability.svc.cluster.local:4317
```

Full configuration reference: see [`values.yaml`](agentbus/values.yaml).

## Verify

```bash
kubectl -n default get pods -l app.kubernetes.io/name=agentbus
kubectl -n default port-forward svc/agentbus 9095:9095

# Another terminal:
go install github.com/khangpt2k6/AgentBus/cmd/goqueue@latest
goqueue publish --grpc --addr localhost:9095 --topic smoke "hello k8s"
goqueue consume --grpc --addr localhost:9095 --topic smoke --group demo --partition 0
```

## Why `Recreate` strategy

The PVC for the WAL is ReadWriteOnce. A rolling update would try to start a new pod while the old one still holds the volume lock — it would deadlock on `FailedAttachVolume`. `Recreate` causes a brief downtime per upgrade but is the correct choice for a single-node, stateful workload. Don't change it unless you also change the persistence strategy.

## Why `replicaCount: 1`

AgentBus is single-node today. Two replicas would give you two independent brokers sharing nothing — they would NOT shard topics or replicate WAL. Multi-node with replication lives in the [distributed-v1 design notes](https://github.com/khangpt2k6/AgentBus/blob/main/docs/distributed-v1-design.md); not yet implemented.

## Upgrade

```bash
helm upgrade agentbus ./deploy/helm/agentbus --set image.tag=v0.5.0
```

WAL format is forward-compatible within a major version (v1/v2/v3 records all replay under v0.4.x). Cross-major upgrades will be called out in release notes.

## Uninstall

```bash
helm uninstall agentbus

# The PVC is intentionally NOT deleted — destroy it manually if you also
# want the WAL data gone:
kubectl delete pvc agentbus-data
```
