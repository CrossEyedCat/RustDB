# Optimization matrix from docs/plan: select_table, concurrency 128, shared, stream_batch 1 vs 8/16.
# Prerequisites: `rustdb server` (or Docker) listening at --addr with cert at --cert; `cargo build --bin rustdb_load`.
#
# Example (from repo root, server on 127.0.0.1:5432 with server.der):
#   .\scripts\run_optimization_matrix.ps1
#   .\scripts\run_optimization_matrix.ps1 -Addr "127.0.0.1:15432" -Cert "path\to\server.der"

param(
  [string]$Addr = "127.0.0.1:5432",
  [string]$ServerName = "127.0.0.1",
  [string]$Cert = "server.der",
  [int]$Queries = 10000,
  [string]$OutDir = ""
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

if (-not $OutDir) {
  $ts = Get-Date -Format "yyyyMMdd-HHmmss"
  $OutDir = Join-Path $Root "target\optimization_matrix\$ts"
}

$cargoExe = Join-Path $Root "target\debug\rustdb_load.exe"
if (-not (Test-Path $cargoExe)) {
  Write-Host "Building rustdb_load..."
  cargo build --bin rustdb_load
}

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$certPath = if ([System.IO.Path]::IsPathRooted($Cert)) { $Cert } else { Join-Path $Root $Cert }

$py = @(
  "scripts/bench_sqlite_vs_rustdb.py",
  "--out-dir", $OutDir,
  "--addr", $Addr,
  "--server-name", $ServerName,
  "--cert", $certPath,
  "--scenarios", "select_table",
  "--concurrency", "128",
  "--queries", "$Queries",
  "--rustdb-connection-modes", "shared",
  "--rustdb-baseline-stream-batch", "1",
  "--rustdb-stream-sweep", "1,8,16",
  "--rustdb-quic-max-streams", "256"
)

Write-Host "Output: $OutDir"
Write-Host "python $($py -join ' ')"
& python @py
