# Deploy on a VM (systemd)

For when you want the broker running on a Linux VM without Docker.

## 1. Install the binary

```bash
curl -sSfL https://raw.githubusercontent.com/khangpt2k6/GoQueue/main/install.sh | sh
which broker
# /usr/local/bin/broker
```

## 2. Create a service user and data dir

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin agentbus
sudo mkdir -p /var/lib/agentbus
sudo chown agentbus:agentbus /var/lib/agentbus
```

## 3. Install the unit file

```bash
sudo tee /etc/systemd/system/agentbus.service >/dev/null <<'EOF'
[Unit]
Description=Agent Bus broker
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=agentbus
Group=agentbus
ExecStart=/usr/local/bin/broker \
  --tcp-addr=:9090 \
  --grpc-addr=:9095 \
  --metrics-addr=:2112 \
  --wal-path=/var/lib/agentbus/agentbus.wal
Restart=on-failure
RestartSec=2
LimitNOFILE=65535

# hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
PrivateDevices=true
ReadWritePaths=/var/lib/agentbus

[Install]
WantedBy=multi-user.target
EOF
```

## 4. Start it

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now agentbus
sudo systemctl status agentbus
journalctl -u agentbus -f
```

## 5. Verify

```bash
curl -s http://localhost:2112/readyz
goqueue publish --addr localhost:9090 --topic smoke "hello"
goqueue consume --addr localhost:9090 --topic smoke --group test
```

## Upgrading

```bash
sudo systemctl stop agentbus
curl -sSfL https://raw.githubusercontent.com/khangpt2k6/GoQueue/main/install.sh | sudo sh -s -- --version v0.2.0
sudo systemctl start agentbus
```

The WAL format is forward-compatible within a major version; cross-major upgrades will be called out in release notes.

## Reverse-proxying TLS

The broker doesn't terminate TLS itself. Front it with Caddy / nginx / Envoy if you need TLS on the TCP or gRPC ports. A minimal Caddy config:

```caddyfile
api.example.com {
    reverse_proxy h2c://127.0.0.1:9095
}
```
