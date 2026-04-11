# Smoke benchmark via GHCR image (same as bench_via_ghcr_image.sh).
# Use this on Windows when `bash` resolves to WSL without a distro.
#
#   .\scripts\bench_via_ghcr_image.ps1
#   $env:RUSTDB_IMAGE = "ghcr.io/crosseyedcat/rustdb:main-type-sha"; .\scripts\bench_via_ghcr_image.ps1

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

$RUSTDB_IMAGE = if ($env:RUSTDB_IMAGE) { $env:RUSTDB_IMAGE } else { "ghcr.io/crosseyedcat/rustdb:main" }
$OUT_DIR = if ($env:OUT_DIR) { $env:OUT_DIR } else { Join-Path $Root "target\bench_docker_ghcr" }
$CONTAINER_NAME = if ($env:CONTAINER_NAME) { $env:CONTAINER_NAME } else { "rustdb-bench-ghcr" }
$VOL_NAME = if ($env:VOL_NAME) { $env:VOL_NAME } else { "rustdb_bench_ghcr_data" }
$UDP_PORT = if ($env:UDP_PORT) { [int]$env:UDP_PORT } else { 8080 }
$QUERIES_PER_POINT = if ($env:QUERIES_PER_POINT) { [int]$env:QUERIES_PER_POINT } else { 500 }
$CONCURRENCY = if ($env:CONCURRENCY) { $env:CONCURRENCY } else { "1,8" }

# Avoid PowerShell treating docker's stderr (e.g. "No such container") as a terminating stream error.
function Remove-DockerContainerIfExists([string]$Name) {
  cmd.exe /c "docker rm -f `"$Name`" >nul 2>&1" | Out-Null
}

Write-Host "==> pull $RUSTDB_IMAGE"
docker pull $RUSTDB_IMAGE

Write-Host "==> volume + seed bench_t"
docker volume create $VOL_NAME 2>$null | Out-Null
docker run --rm -v "${VOL_NAME}:/app/data" $RUSTDB_IMAGE rustdb query "CREATE TABLE bench_t (a INTEGER)"
docker run --rm -v "${VOL_NAME}:/app/data" $RUSTDB_IMAGE rustdb query "INSERT INTO bench_t (a) VALUES (1)"

Write-Host "==> start QUIC server (UDP :$UDP_PORT)"
Remove-DockerContainerIfExists $CONTAINER_NAME
docker run -d --name $CONTAINER_NAME `
  -v "${VOL_NAME}:/app/data" `
  -p "${UDP_PORT}:8080/udp" `
  $RUSTDB_IMAGE `
  rustdb server --host 0.0.0.0 --port 8080 --cert-out /app/data/server.der

Start-Sleep -Seconds 2
docker logs $CONTAINER_NAME 2>&1 | Select-Object -Last 25

New-Item -ItemType Directory -Force -Path $OUT_DIR | Out-Null
$CERT = Join-Path $OUT_DIR "server.der"
Write-Host "==> copy cert -> $CERT"
docker cp "${CONTAINER_NAME}:/app/data/server.der" $CERT

$LOAD = Join-Path $Root "target\debug\rustdb_load.exe"
if (-not (Test-Path $LOAD)) {
  $LOAD = Join-Path $Root "target\debug\rustdb_load"
}
if (-not (Test-Path $LOAD)) {
  Write-Host "==> build rustdb_load"
  cargo build --bin rustdb_load
}

Write-Host "==> bench_sqlite_vs_rustdb.py"
$pyExe = "python"
if (-not (Get-Command $pyExe -ErrorAction SilentlyContinue)) { $pyExe = "python3" }
$benchArgs = @(
  (Join-Path $Root "scripts\bench_sqlite_vs_rustdb.py"),
  "--out-dir", $OUT_DIR,
  "--cert", $CERT,
  "--addr", "127.0.0.1:$UDP_PORT",
  "--server-name", "localhost",
  "--scenarios", "select_literal,select_table",
  "--concurrency", $CONCURRENCY,
  "--queries", "$QUERIES_PER_POINT",
  "--rustdb-baseline-stream-batch", "1",
  "--rustdb-stream-sweep", "none"
)
if ($env:POSTGRES_DSN) {
  $benchArgs += @("--postgres-dsn", $env:POSTGRES_DSN)
}
& $pyExe @benchArgs

Write-Host "==> wrote $OUT_DIR\bench.md"
Write-Host "==> cleanup container"
Remove-DockerContainerIfExists $CONTAINER_NAME
Write-Host "==> done"
