param(
  [Parameter(Mandatory = $false, Position = 0)]
  [ValidateSet("help", "dev", "test", "lint", "fmt", "up", "down", "logs", "bench", "clean")]
  [string]$Task = "help"
)

$ErrorActionPreference = "Stop"

function Invoke-Checked {
  param(
    [Parameter(Mandatory = $true)]
    [scriptblock]$Command
  )

  & $Command
  if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
  }
}

switch ($Task) {
  "help" {
    Write-Host "Available tasks:"
    Write-Host "  ./scripts/goqueue.ps1 dev    - run local broker"
    Write-Host "  ./scripts/goqueue.ps1 test   - run all Go tests once"
    Write-Host "  ./scripts/goqueue.ps1 lint   - run go vet + internal tests"
    Write-Host "  ./scripts/goqueue.ps1 fmt    - format Go code"
    Write-Host "  ./scripts/goqueue.ps1 up     - start docker compose stack"
    Write-Host "  ./scripts/goqueue.ps1 down   - stop docker compose stack"
    Write-Host "  ./scripts/goqueue.ps1 logs   - tail broker logs"
    Write-Host "  ./scripts/goqueue.ps1 bench  - run benchmark reports"
    Write-Host "  ./scripts/goqueue.ps1 clean  - remove generated artifacts"
  }
  "dev" {
    Invoke-Checked { go run ./cmd/broker --tcp-addr=:9090 --grpc-addr=:9095 --metrics-addr=:2112 --wal-path=data/goqueue.wal }
  }
  "test" {
    Invoke-Checked { go test ./... -count=1 }
  }
  "lint" {
    Invoke-Checked { go vet ./... }
    Invoke-Checked { go test ./internal/... -count=1 }
  }
  "fmt" {
    Invoke-Checked { go fmt ./... }
  }
  "up" {
    Invoke-Checked { docker compose up --build -d }
  }
  "down" {
    Invoke-Checked { docker compose down --remove-orphans }
  }
  "logs" {
    Invoke-Checked { docker compose logs -f broker-1 broker-2 broker-3 }
  }
  "bench" {
    Invoke-Checked { $env:GOQUEUE_BENCH = "1"; go test ./bench -run TestThroughputReport -count=1 -v }
    Invoke-Checked { $env:GOQUEUE_BENCH = "1"; go test ./bench -run TestTCPThroughputReport -count=1 -v }
    Invoke-Checked { $env:GOQUEUE_BENCH = "1"; go test ./bench -run TestLatencyReport -count=1 -v }
  }
  "clean" {
    Invoke-Checked { go clean -testcache }
    if (Test-Path "coverage.out") { Remove-Item "coverage.out" -Force }
    if (Test-Path "coverage.txt") { Remove-Item "coverage.txt" -Force }
  }
}
