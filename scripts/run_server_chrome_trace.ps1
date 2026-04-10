# Start `rustdb server` with Chrome tracing enabled (JSON for chrome://tracing).
# Hypothesis: compare span durations in the trace — see docs/network/quic-and-quinn.md § Profiling.
#
# Usage (from repo root):
#   .\scripts\run_server_chrome_trace.ps1
#   # other terminal (server-name must match cert SAN: use 127.0.0.1 when server uses --host 127.0.0.1):
#   cargo run --bin rustdb_load -- --addr 127.0.0.1:5432 --cert server.der --server-name 127.0.0.1 --queries 30 --concurrency 1 --sql "SELECT 1"
#   # Ctrl+C this script to flush trace.json
#
param(
  [string]$TracePath = "",
  [int]$Port = 5432,
  [string]$HostListen = "127.0.0.1",
  # If set, prints a second-terminal `rustdb_load` command for trace-under-load (concurrency > 1).
  [int]$LoadConcurrency = 0,
  [int]$LoadQueries = 10000,
  [int]$LoadStreamBatch = 1
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

if (-not $TracePath) {
  $TracePath = Join-Path $Root "target\rustdb-chrome-trace.json"
}
elseif (-not [System.IO.Path]::IsPathRooted($TracePath)) {
  $TracePath = Join-Path $Root $TracePath
}
$parent = Split-Path -Parent $TracePath
if ($parent -and -not (Test-Path $parent)) {
  New-Item -ItemType Directory -Force -Path $parent | Out-Null
}
# tracing-chrome creates/truncates this path; file need not exist beforehand.
$env:RUSTDB_TRACE_CHROME_PATH = $TracePath
if (-not $env:RUST_LOG) { $env:RUST_LOG = "info" }

$certOut = Join-Path $Root "server.der"
Write-Host "RUSTDB_TRACE_CHROME_PATH=$($env:RUSTDB_TRACE_CHROME_PATH)"
Write-Host ('Listening UDP {0}:{1} - cert -> {2}' -f $HostListen, $Port, $certOut)
Write-Host ('rustdb_load: use --server-name {0} (same as --host above).' -f $HostListen)
Write-Host 'After load test, press Ctrl+C here to flush the trace file.'
Write-Host ""

if ($LoadConcurrency -gt 0) {
  $loadLine = @(
    "cargo run --bin rustdb_load -- --addr ${HostListen}:${Port} --cert `"$certOut`" --server-name $HostListen",
    "--queries $LoadQueries --concurrency $LoadConcurrency --connection-mode shared",
    "--stream-batch $LoadStreamBatch --quic-max-streams 256 --sql `"SELECT 1`""
  ) -join " "
  Write-Host "=== Trace under load (wait for QUIC listening below, then run in a second terminal) ===" -ForegroundColor Cyan
  Write-Host $loadLine
  Write-Host ""
}

cargo run -- server --host $HostListen --port $Port --cert-out $certOut
