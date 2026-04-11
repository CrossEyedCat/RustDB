#!/usr/bin/env bash
# Optimization matrix: select_table, c=128, shared, stream_batch 1,8,16 — see scripts/run_optimization_matrix.ps1
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

OUT_DIR="${OUT_DIR:-$ROOT/target/optimization_matrix/$(date +%Y%m%d-%H%M%S)}"
ADDR="${ADDR:-127.0.0.1:5432}"
SERVER_NAME="${SERVER_NAME:-127.0.0.1}"
CERT="${CERT:-$ROOT/server.der}"
QUERIES="${QUERIES:-10000}"

if [[ ! -f "$ROOT/target/debug/rustdb_load" ]]; then
  echo "Building rustdb_load..."
  cargo build --bin rustdb_load
fi

mkdir -p "$OUT_DIR"

exec python3 scripts/bench_sqlite_vs_rustdb.py \
  --out-dir "$OUT_DIR" \
  --addr "$ADDR" \
  --server-name "$SERVER_NAME" \
  --cert "$CERT" \
  --scenarios select_table \
  --concurrency 128 \
  --queries "$QUERIES" \
  --rustdb-connection-modes shared \
  --rustdb-baseline-stream-batch 1 \
  --rustdb-stream-sweep 1,8,16 \
  --rustdb-quic-max-streams 256
