$ErrorActionPreference = "Stop"

# PostgreSQL baseline via pgbench (Docker Desktop / Windows-friendly).
#
# RustDB does NOT speak PostgreSQL wire protocol, so pgbench cannot target RustDB directly.
# This script produces a repeatable Postgres baseline in Docker that you can compare against
# RustDB load results (e.g. rustdb_tpcc / rustdb_load).

$DB_NAME = $env:PGBENCH_DB
if (-not $DB_NAME) { $DB_NAME = "pgbench_rustdb_compare" }

$SCALE = $env:PGBENCH_SCALE
if (-not $SCALE) { $SCALE = "10" }

$CLIENTS = $env:PGBENCH_CLIENTS
if (-not $CLIENTS) { $CLIENTS = "64" }

$JOBS = $env:PGBENCH_JOBS
if (-not $JOBS) { $JOBS = "16" }

$DURATION = $env:PGBENCH_DURATION
if (-not $DURATION) { $DURATION = "300" }

$POSTGRES_IMAGE = $env:PGBENCH_POSTGRES_IMAGE
if (-not $POSTGRES_IMAGE) { $POSTGRES_IMAGE = "postgres:16-alpine" }

$CONTAINER_NAME = $env:PGBENCH_CONTAINER_NAME
if (-not $CONTAINER_NAME) { $CONTAINER_NAME = "pgbench-postgres" }

$POSTGRES_PASSWORD = $env:PGBENCH_POSTGRES_PASSWORD
if (-not $POSTGRES_PASSWORD) { $POSTGRES_PASSWORD = "postgres" }

$POSTGRES_USER = $env:PGBENCH_POSTGRES_USER
if (-not $POSTGRES_USER) { $POSTGRES_USER = "postgres" }

$POSTGRES_PORT = $env:PGBENCH_POSTGRES_PORT
if (-not $POSTGRES_PORT) { $POSTGRES_PORT = "15440" }

$OUT_DIR = $env:PGBENCH_OUT_DIR
if (-not $OUT_DIR) { $OUT_DIR = "pgbench-out" }

$ROOT = Resolve-Path (Join-Path $PSScriptRoot "..")
$OUT_DIR_ABS = Join-Path $ROOT $OUT_DIR
New-Item -ItemType Directory -Force -Path $OUT_DIR_ABS | Out-Null

$TS = Get-Date -Format "yyyyMMdd-HHmmss"
$OUT_TXT = Join-Path $OUT_DIR_ABS ("pgbench-$TS.txt")

function Cleanup {
  try { docker rm -f $CONTAINER_NAME 2>$null | Out-Null } catch {}
}

try {
  Write-Host "=== PostgreSQL pgbench ==="
  Write-Host "DB: $DB_NAME, scale: $SCALE, clients: $CLIENTS, jobs: $JOBS, duration: ${DURATION}s"
  Write-Host "docker image: $POSTGRES_IMAGE"
  Write-Host "host port: $POSTGRES_PORT -> container 5432"
  Write-Host ""

  Write-Host "==> start postgres container"
  Cleanup
  docker run -d --name $CONTAINER_NAME `
    -e "POSTGRES_PASSWORD=$POSTGRES_PASSWORD" `
    -e "POSTGRES_USER=$POSTGRES_USER" `
    -e "POSTGRES_DB=$DB_NAME" `
    -p "${POSTGRES_PORT}:5432" `
    $POSTGRES_IMAGE | Out-Null

  Write-Host "==> wait for postgres readiness"
  $ready = $false
  for ($i = 0; $i -lt 240; $i++) {
    docker exec $CONTAINER_NAME pg_isready -h 127.0.0.1 -p 5432 -U $POSTGRES_USER *> $null
    if ($LASTEXITCODE -eq 0) {
      $ready = $true
      break
    }
    Start-Sleep -Milliseconds 500
  }
  if (-not $ready) {
    Write-Error "Postgres did not become ready"
  }

  Write-Host "==> pgbench init (scale=$SCALE)"
  docker exec $CONTAINER_NAME pgbench -h 127.0.0.1 -p 5432 -i -s $SCALE -U $POSTGRES_USER $DB_NAME | Out-Null
  if ($LASTEXITCODE -ne 0) { throw "pgbench init failed" }

  Write-Host "==> run pgbench"
  $header = @(
    "== pgbench ==",
    "image: $POSTGRES_IMAGE",
    "db: $DB_NAME",
    "scale: $SCALE",
    "clients: $CLIENTS",
    "jobs: $JOBS",
    "duration_s: $DURATION",
    ""
  )
  $header -join "`n" | Out-File -FilePath $OUT_TXT -Encoding utf8
  docker exec $CONTAINER_NAME pgbench -h 127.0.0.1 -p 5432 -c $CLIENTS -j $JOBS -T $DURATION -U $POSTGRES_USER $DB_NAME `
    | Tee-Object -FilePath $OUT_TXT -Append
  if ($LASTEXITCODE -ne 0) { throw "pgbench run failed" }

  Write-Host ""
  Write-Host "==> wrote: $OUT_TXT"
  Write-Host ""
  Write-Host "=== Next: compare with RustDB ==="
  Write-Host "RustDB cannot be benchmarked with pgbench (different protocol)."
  Write-Host "Use the same duration/concurrency and run one of:"
  Write-Host "- rustdb_tpcc: scripts/tpcc_throughput_ci.sh (QUIC load, produces tpcc.txt/json)"
  Write-Host "- rustdb_load: scripts/bench_saturation_rustdb_postgres.py (QPS/p99 sweep vs Postgres DSN)"
}
finally {
  Cleanup
}

