# Deploy on Kubernetes

AgentBus ships an official Helm chart. One command and you have a broker pod, a Service exposing TCP/gRPC/metrics, a PVC for the WAL, and (optionally) a Prometheus Operator `ServiceMonitor`.

## TL;DR

```bash
# Install latest chart from GHCR
helm install agentbus oci://ghcr.io/khangpt2k6/charts/agentbus

# Or pin a version
helm install agentbus oci://ghcr.io/khangpt2k6/charts/agentbus --version 0.4.0
```

That's it. Pod comes up, WAL goes on an 8 GiB persistent volume, broker listens on `:9090` (TCP), `:9095` (gRPC), `:2112` (metrics).

## Verify

```bash
kubectl get pods -l app.kubernetes.io/name=agentbus
kubectl get svc agentbus
kubectl logs -l app.kubernetes.io/name=agentbus -f
```

Port-forward to test from your laptop:

```bash
kubectl port-forward svc/agentbus 9095:9095

# In another terminal:
go install github.com/khangpt2k6/AgentBus/cmd/goqueue@latest
goqueue publish --grpc --addr localhost:9095 --topic smoke "hello k8s"
goqueue consume --grpc --addr localhost:9095 --topic smoke --group demo --partition 0
```

## Common configurations

=== "Bigger WAL"

    ```bash
    helm install agentbus oci://ghcr.io/khangpt2k6/charts/agentbus \
      --set persistence.size=50Gi \
      --set persistence.storageClass=gp3
    ```

=== "Prometheus Operator"

    Requires `kube-prometheus-stack` installed.

    ```bash
    helm install agentbus oci://ghcr.io/khangpt2k6/charts/agentbus \
      --set serviceMonitor.enabled=true \
      --set serviceMonitor.labels.release=prometheus
    ```

    The `release: prometheus` label is what makes the operator pick up the
    ServiceMonitor — adjust to match your stack's selector.

=== "OTEL collector"

    ```bash
    helm install agentbus oci://ghcr.io/khangpt2k6/charts/agentbus \
      --set otel.enabled=true \
      --set otel.endpoint=otel-collector.observability.svc.cluster.local:4317 \
      --set otel.resourceAttributes='deployment.environment=prod,team=ai-platform'
    ```

    See [OpenTelemetry tracing](../otel-tracing.md) for what to search in Tempo.

=== "Custom image tag"

    ```bash
    helm install agentbus oci://ghcr.io/khangpt2k6/charts/agentbus \
      --set image.tag=v0.4.0
    ```

=== "Bring your own values file"

    ```yaml
    # values.yaml
    persistence:
      size: 100Gi
      storageClass: io2
    resources:
      requests:
        cpu: 500m
        memory: 512Mi
      limits:
        cpu: 2
        memory: 2Gi
    serviceMonitor:
      enabled: true
    podLabels:
      team: ai-platform
    ```

    ```bash
    helm install agentbus oci://ghcr.io/khangpt2k6/charts/agentbus -f values.yaml
    ```

## Architecture notes

**Single-replica by design.** `replicaCount` defaults to 1 and the deployment uses `Recreate`. The WAL is a single-writer, ReadWriteOnce PVC. Bumping replicas does NOT shard topics or replicate — it would start two independent brokers fighting over the same volume mount. Distributed-v1 is in the [design notes](../distributed-v1-design.md), not built.

**Recreate strategy.** Rolling updates would deadlock on `FailedAttachVolume` (new pod can't mount while old pod still holds the RWO PVC). `Recreate` causes a brief downtime per upgrade — acceptable for a stateful single-node service.

**Liveness/readiness on `/readyz`.** Probes hit the metrics port. Broker logs gRPC errors to stdout — `kubectl logs` is your friend.

**No automount of SA token.** The broker doesn't talk to the Kubernetes API, so the ServiceAccount token isn't mounted. Saves a tiny attack surface.

**Read-only root filesystem.** Security context locks down the container; only `/data` (PVC) and `/tmp` (emptyDir) are writable.

## Upgrade

```bash
helm upgrade agentbus oci://ghcr.io/khangpt2k6/charts/agentbus --version 0.5.0
```

WAL format is forward-compatible within a major version. Cross-major upgrades will be called out in release notes.

## Uninstall

```bash
helm uninstall agentbus
```

**Note**: this does NOT delete the PVC. WAL data persists across reinstalls of the same release name. To wipe data:

```bash
kubectl delete pvc agentbus-data
```

## Full values reference

See [`deploy/helm/agentbus/values.yaml`](https://github.com/khangpt2k6/AgentBus/blob/main/deploy/helm/agentbus/values.yaml) — it's documented inline.
