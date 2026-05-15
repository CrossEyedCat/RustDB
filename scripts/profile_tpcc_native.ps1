# Native short TPC-C-ish profile (no Docker): seed, QUIC server, rustdb_tpcc, analyze logs.
# PowerShell, repo root. Default UDP 15436 — change if in use.
#
#   .\scripts\profile_tpcc_native.ps1
#   $env:CONCURRENCY='8'; $env:DURATION_SECS='10'; .\scripts\profile_tpcc_native.ps1

param(
    [int] $UdpPort = 15436,
    [int] $Concurrency = 6,
    [int] $DurationSecs = 6,
    [string] $Mix = "new_order=0.45,payment=0.43,order_status=0.04,delivery=0.04,stock_level=0.04"
)

$ErrorActionPreference = "Stop"
if ($env:CONCURRENCY) { [int]$Concurrency = $env:CONCURRENCY }
if ($env:DURATION_SECS) { [int]$DurationSecs = $env:DURATION_SECS }
if ($env:UDP_PORT) { [int]$UdpPort = $env:UDP_PORT }

$Repo = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $Repo

$Out = Join-Path $Repo "tpcc-local-out"
New-Item -ItemType Directory -Force -Path $Out | Out-Null
$DataDir = Join-Path $Out "data"
Remove-Item -Recurse -Force $DataDir -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path $DataDir | Out-Null
$DataDirAbs = (Resolve-Path $DataDir).Path.Replace('\', '/')

$MiniConfig = Join-Path $Out "config.profile.toml"
$miniToml = @"
name = "rustdb"
data_directory = "$DataDirAbs"
max_connections = 100
connection_timeout = 30
query_timeout = 120
language = "en"

[network]
host = "127.0.0.1"
port = $UdpPort
max_connections = 100
"@
$utf8NoBom = New-Object System.Text.UTF8Encoding $false
[System.IO.File]::WriteAllText($MiniConfig, $miniToml.TrimEnd() + "`n", $utf8NoBom)

$SeedFiltered = Join-Path $Out "tpcc_seed.filtered.sql"
$SeedSrc = Join-Path $Repo "scripts\tpcc_seed.sql"
$seedLines = @(Get-Content $SeedSrc | Where-Object {
    $t = $_.Trim()
    $t -and -not $t.StartsWith("--")
})
[System.IO.File]::WriteAllLines($SeedFiltered, $seedLines, $utf8NoBom)

Write-Host "==> cargo build (rustdb + rustdb_tpcc)"
cargo build -q --bin rustdb --bin rustdb_tpcc

$RustdbExe = Join-Path $Repo "target\debug\rustdb.exe"
if (-not (Test-Path $RustdbExe)) { $RustdbExe = Join-Path $Repo "target\debug\rustdb" }

Write-Host "==> seed"
& $RustdbExe --config $MiniConfig query --batch-file $SeedFiltered | Out-Null

$Cert = Join-Path $Out "server.der"
$ServerOut = Join-Path $Out "server_stdout.log"
$ServerErr = Join-Path $Out "server_stderr.log"
Remove-Item $Cert -ErrorAction SilentlyContinue
Remove-Item $ServerOut -ErrorAction SilentlyContinue
Remove-Item $ServerErr -ErrorAction SilentlyContinue

$env:RUSTDB_SQL_PHASE_LOG = "1"
$env:RUST_LOG = "info"

Write-Host "==> server (stdout -> $ServerOut, stderr -> $ServerErr)"
$arg = "--config `"$MiniConfig`" server --host 127.0.0.1 --port $UdpPort --cert-out `"$Cert`""
$server = Start-Process -FilePath $RustdbExe -ArgumentList $arg `
    -WorkingDirectory $Repo -PassThru -WindowStyle Hidden `
    -RedirectStandardError $ServerErr -RedirectStandardOutput $ServerOut

for ($i = 0; $i -lt 200; $i++) {
    if ((Test-Path $Cert) -and (Get-Item $Cert).Length -gt 0) { break }
    Start-Sleep -Milliseconds 150
}
if (-not (Test-Path $Cert) -or (Get-Item $Cert).Length -eq 0) {
    try { Stop-Process -Id $server.Id -Force } catch {}
    throw "server.der not ready; see $ServerOut / $ServerErr"
}

$TxnLog = Join-Path $Out "tpcc_txn.log"
$JsonOut = Join-Path $Out "tpcc.json"
$TpccExe = Join-Path $Repo "target\debug\rustdb_tpcc.exe"
if (-not (Test-Path $TpccExe)) { $TpccExe = Join-Path $Repo "target\debug\rustdb_tpcc" }

Write-Host "==> rustdb_tpcc ($Concurrency workers, ${DurationSecs}s)"
& $TpccExe `
    --addr "127.0.0.1:$UdpPort" `
    --cert $Cert `
    --server-name 127.0.0.1 `
    --concurrency $Concurrency `
    --duration-seconds $DurationSecs `
    --mix $Mix `
    --txn-log $TxnLog `
    --json | Set-Content -Encoding utf8 $JsonOut

Write-Host "==> stop server"
try { Stop-Process -Id $server.Id -Force } catch {}
$server.WaitForExit(8000) | Out-Null

Write-Host "`n==> analyze_tpcc_txn_log.py"
python (Join-Path $Repo "scripts\analyze_tpcc_txn_log.py") $TxnLog --only-ok

$PhaseLog = Join-Path $Out "server_combined.log"
Get-Content $ServerOut, $ServerErr -ErrorAction SilentlyContinue | Set-Content -Encoding utf8 $PhaseLog

Write-Host "`n==> summarize_sql_phase_log.py ($PhaseLog)"
python (Join-Path $Repo "scripts\summarize_sql_phase_log.py") $PhaseLog

Write-Host "`nCPU (Linux): attach with scripts/profile_cpu_linux.sh <PID> <secs> (perf record -g)"
Write-Host "Artifacts: $Out"
