# Second-terminal load for Chrome tracing: high concurrency (or long run) so the trace shows
# network.* vs dispatch_client_frame under contention. Start the traced server first
# (scripts/run_server_chrome_trace.ps1), then run this script.
#
# Example:
#   .\scripts\trace_under_load.ps1
#   .\scripts\trace_under_load.ps1 -Concurrency 128 -Queries 20000 -StreamBatch 8

param(
  [string]$Addr = "127.0.0.1:5432",
  [string]$ServerName = "127.0.0.1",
  [string]$Cert = "server.der",
  [int]$Concurrency = 128,
  [int]$Queries = 10000,
  [int]$StreamBatch = 1,
  [int]$QuicMaxStreams = 256
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

$certPath = if ([System.IO.Path]::IsPathRooted($Cert)) { $Cert } else { Join-Path $Root $Cert }

$cargoExe = Join-Path $Root "target\debug\rustdb_load.exe"
if (-not (Test-Path $cargoExe)) {
  Write-Host "Building rustdb_load..."
  cargo build --bin rustdb_load
}

$cargoArguments = @(
  "run", "--bin", "rustdb_load", "--",
  "--addr", $Addr,
  "--cert", $certPath,
  "--server-name", $ServerName,
  "--concurrency", "$Concurrency",
  "--queries", "$Queries",
  "--connection-mode", "shared",
  "--stream-batch", "$StreamBatch",
  "--quic-max-streams", "$QuicMaxStreams",
  "--sql", "SELECT 1"
)

Write-Host "cargo $($cargoArguments -join ' ')"
& cargo @cargoArguments
