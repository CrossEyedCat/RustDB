#!/usr/bin/env bash
# Companion to trace_under_load.ps1 — second terminal load for Chrome tracing.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

ADDR="${ADDR:-127.0.0.1:5432}"
SERVER_NAME="${SERVER_NAME:-127.0.0.1}"
CERT="${CERT:-$ROOT/server.der}"
CONCURRENCY="${CONCURRENCY:-128}"
QUERIES="${QUERIES:-10000}"
STREAM_BATCH="${STREAM_BATCH:-1}"
QUIC_MAX="${QUIC_MAX:-256}"

if [[ ! -f "$ROOT/target/debug/rustdb_load" ]]; then
  cargo build --bin rustdb_load
fi

exec cargo run --bin rustdb_load -- \
  --addr "$ADDR" \
  --cert "$CERT" \
  --server-name "$SERVER_NAME" \
  --concurrency "$CONCURRENCY" \
  --queries "$QUERIES" \
  --connection-mode shared \
  --stream-batch "$STREAM_BATCH" \
  --quic-max-streams "$QUIC_MAX" \
  --sql "SELECT 1"
