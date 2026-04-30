#!/usr/bin/env bash
# PostgreSQL baseline via pgbench (Docker-first).
#
# RustDB does NOT speak PostgreSQL wire protocol, so pgbench cannot be run against RustDB directly.
# This script provides a repeatable **PostgreSQL** baseline run (Docker/Linux container) that you
# can compare against RustDB load results (e.g. `rustdb_tpcc`, `rustdb_load`).

set -euo pipefail

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker CLI not found in PATH."
  echo "Hint (Windows/WSL): enable Docker Desktop WSL integration, or run the PowerShell script:"
  echo "  scripts/pgbench_compare.ps1"
  exit 1
fi

DB_NAME="${PGBENCH_DB:-pgbench_rustdb_compare}"
SCALE="${PGBENCH_SCALE:-10}"
CLIENTS="${PGBENCH_CLIENTS:-64}"
JOBS="${PGBENCH_JOBS:-16}"
DURATION="${PGBENCH_DURATION:-300}"

POSTGRES_IMAGE="${PGBENCH_POSTGRES_IMAGE:-postgres:16-alpine}"
CONTAINER_NAME="${PGBENCH_CONTAINER_NAME:-pgbench-postgres}"
POSTGRES_PASSWORD="${PGBENCH_POSTGRES_PASSWORD:-postgres}"
POSTGRES_USER="${PGBENCH_POSTGRES_USER:-postgres}"
POSTGRES_PORT="${PGBENCH_POSTGRES_PORT:-15440}"

OUT_DIR="${PGBENCH_OUT_DIR:-pgbench-out}"
OUT_DIR_ABS="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/${OUT_DIR}"
mkdir -p "$OUT_DIR_ABS"
TS="$(date +%Y%m%d-%H%M%S)"
OUT_TXT="$OUT_DIR_ABS/pgbench-${TS}.txt"

cleanup() {
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "=== PostgreSQL pgbench ==="
echo "DB: $DB_NAME, scale: $SCALE, clients: $CLIENTS, jobs: $JOBS, duration: ${DURATION}s"
echo "docker image: $POSTGRES_IMAGE"
echo "host port: $POSTGRES_PORT -> container 5432"
echo ""

echo "==> start postgres container"
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
docker run -d --name "$CONTAINER_NAME" \
  -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  -e POSTGRES_USER="$POSTGRES_USER" \
  -e POSTGRES_DB="$DB_NAME" \
  -p "${POSTGRES_PORT}:5432" \
  "$POSTGRES_IMAGE" >/dev/null

echo "==> wait for postgres readiness"
ready=0
for _ in $(seq 1 240); do
  if docker exec "$CONTAINER_NAME" pg_isready -h 127.0.0.1 -p 5432 -U "$POSTGRES_USER" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 0.5
done
if [[ "$ready" -ne 1 ]]; then
  echo "ERROR: Postgres did not become ready"
  docker logs "$CONTAINER_NAME" 2>&1 | tail -n 200 || true
  exit 1
fi

echo "==> pgbench init (scale=$SCALE)"
docker exec "$CONTAINER_NAME" pgbench -h 127.0.0.1 -p 5432 -i -s "$SCALE" -U "$POSTGRES_USER" "$DB_NAME" >/dev/null

echo "Running pgbench (TPC-B-like)..."
{
  echo "== pgbench =="
  echo "image: $POSTGRES_IMAGE"
  echo "db: $DB_NAME"
  echo "scale: $SCALE"
  echo "clients: $CLIENTS"
  echo "jobs: $JOBS"
  echo "duration_s: $DURATION"
  echo ""
  docker exec "$CONTAINER_NAME" pgbench \
    -h 127.0.0.1 \
    -p 5432 \
    -c "$CLIENTS" \
    -j "$JOBS" \
    -T "$DURATION" \
    -U "$POSTGRES_USER" \
    "$DB_NAME"
} | tee "$OUT_TXT"

echo ""
echo "==> wrote: $OUT_TXT"
echo ""
echo "=== Next: compare with RustDB ==="
echo "RustDB cannot be benchmarked with pgbench (different protocol)."
echo "Use the same duration/concurrency and run one of:"
echo "- rustdb_tpcc: scripts/tpcc_throughput_ci.sh (QUIC load, produces tpcc.txt/json)"
echo "- rustdb_load: scripts/bench_saturation_rustdb_postgres.py (QPS/p99 sweep vs Postgres DSN)"
