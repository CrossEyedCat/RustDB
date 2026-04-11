#!/usr/bin/env bash
# Smoke benchmark (SQLite vs RustDB in Docker) using a pre-built GHCR image.
#
# Usage:
#   chmod +x scripts/bench_via_ghcr_image.sh
#   ./scripts/bench_via_ghcr_image.sh
#
# Image tag: CI publishes tags like `main-<gitsha>`, not the literal `main-type-sha`.
# Examples:
#   RUSTDB_IMAGE=ghcr.io/crosseyedcat/rustdb:main ./scripts/bench_via_ghcr_image.sh
#   RUSTDB_IMAGE=ghcr.io/crosseyedcat/rustdb:main-7a3b2c1d ./scripts/bench_via_ghcr_image.sh
#
# Requires: Docker, Python 3, `cargo` (builds rustdb_load from this repo against the running server).
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Override to pin a build, e.g. `main-7a3b2c1d` from ghcr.io package tags (not the literal `main-type-sha` unless published).
RUSTDB_IMAGE="${RUSTDB_IMAGE:-ghcr.io/crosseyedcat/rustdb:main}"
OUT_DIR="${OUT_DIR:-$ROOT/target/bench_docker_ghcr}"
CONTAINER_NAME="${CONTAINER_NAME:-rustdb-bench-ghcr}"
VOL_NAME="${VOL_NAME:-rustdb_bench_ghcr_data}"
UDP_PORT="${UDP_PORT:-8080}"
QUERIES_PER_POINT="${QUERIES_PER_POINT:-500}"
CONCURRENCY="${CONCURRENCY:-1,8}"
POSTGRES_DSN="${POSTGRES_DSN:-}"

echo "==> pull $RUSTDB_IMAGE"
docker pull "$RUSTDB_IMAGE"

echo "==> prepare volume + seed bench_t (before server locks DB)"
docker volume create "$VOL_NAME" >/dev/null 2>&1 || true
docker run --rm -v "$VOL_NAME:/app/data" "$RUSTDB_IMAGE" \
  rustdb query "CREATE TABLE bench_t (a INTEGER)"
docker run --rm -v "$VOL_NAME:/app/data" "$RUSTDB_IMAGE" \
  rustdb query "INSERT INTO bench_t (a) VALUES (1)"

echo "==> start QUIC server (UDP :$UDP_PORT)"
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
docker run -d --name "$CONTAINER_NAME" \
  -v "$VOL_NAME:/app/data" \
  -p "${UDP_PORT}:8080/udp" \
  "$RUSTDB_IMAGE" \
  rustdb server --host 0.0.0.0 --port 8080 --cert-out /app/data/server.der

sleep 2
docker logs "$CONTAINER_NAME" 2>&1 | tail -n 25

mkdir -p "$OUT_DIR"
CERT="$OUT_DIR/server.der"
echo "==> copy leaf cert to $CERT"
docker cp "$CONTAINER_NAME:/app/data/server.der" "$CERT"

LOAD="$ROOT/target/debug/rustdb_load"
if [[ ! -x "$LOAD" ]]; then
  echo "==> build rustdb_load"
  cargo build --bin rustdb_load
fi

echo "==> run bench_sqlite_vs_rustdb.py (RustDB + SQLite; set POSTGRES_DSN for Postgres)"
PY=python3
command -v python3 >/dev/null 2>&1 || PY=python

POSTGRES_ARGS=()
if [[ -n "$POSTGRES_DSN" ]]; then
  POSTGRES_ARGS=(--postgres-dsn "$POSTGRES_DSN")
fi
"$PY" scripts/bench_sqlite_vs_rustdb.py \
  --out-dir "$OUT_DIR" \
  --cert "$CERT" \
  --addr "127.0.0.1:${UDP_PORT}" \
  --server-name localhost \
  --scenarios select_literal,select_table \
  --concurrency "$CONCURRENCY" \
  --queries "$QUERIES_PER_POINT" \
  --rustdb-baseline-stream-batch 1 \
  --rustdb-stream-sweep none \
  "${POSTGRES_ARGS[@]}"

echo "==> wrote $OUT_DIR/bench.md and bench.csv"
echo "==> stop container: docker rm -f $CONTAINER_NAME"
docker rm -f "$CONTAINER_NAME"

echo "==> done"
