$ErrorActionPreference = "Stop"

# PostgreSQL TPC-C-ish baseline (postgres_tpcc) via Docker Desktop / Windows-friendly.
# Same schema/mix as rustdb_tpcc; RustDB itself is exercised with QUIC tools, not this script.

$ROOT = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $ROOT

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
  Write-Error "docker CLI not found in PATH."
}

$env:PGBENCH_OUT_DIR = if ($env:PGBENCH_OUT_DIR) { $env:PGBENCH_OUT_DIR } else { "tpcc-out" }
$env:PGBENCH_CONTAINER_NAME = if ($env:PGBENCH_CONTAINER_NAME) { $env:PGBENCH_CONTAINER_NAME } else { "postgres-tpcc-bench" }
$env:PGBENCH_POSTGRES_PORT = if ($env:PGBENCH_POSTGRES_PORT) { $env:PGBENCH_POSTGRES_PORT } else { "15440" }
$env:PGBENCH_DB = if ($env:PGBENCH_DB) { $env:PGBENCH_DB } else { "tpcc_bench" }
$env:PGBENCH_POSTGRES_USER = if ($env:PGBENCH_POSTGRES_USER) { $env:PGBENCH_POSTGRES_USER } else { "postgres" }
$env:PGBENCH_POSTGRES_PASSWORD = if ($env:PGBENCH_POSTGRES_PASSWORD) { $env:PGBENCH_POSTGRES_PASSWORD } else { "postgres" }
$env:PGBENCH_CLIENTS = if ($env:PGBENCH_CLIENTS) { $env:PGBENCH_CLIENTS } else { "64" }
$env:PGBENCH_DURATION = if ($env:PGBENCH_DURATION) { $env:PGBENCH_DURATION } else { "300" }
if (-not $env:TPCC_MIX) {
  $env:TPCC_MIX = "new_order=0.45,payment=0.43,order_status=0.04,delivery=0.04,stock_level=0.04"
}
$env:MIX = $env:TPCC_MIX

& bash.exe -lc "cd '$($ROOT.Path.Replace('\', '/'))' && chmod +x scripts/bench_postgres_tpcc.sh && ./scripts/bench_postgres_tpcc.sh"
if ($LASTEXITCODE -ne 0) { throw "bench_postgres_tpcc.sh failed (exit $LASTEXITCODE)" }

Write-Host ""
Write-Host "=== Next: compare with RustDB ==="
Write-Host "Run scripts/tpcc_throughput_ci.sh with matching CONCURRENCY / DURATION_SECS / MIX."
