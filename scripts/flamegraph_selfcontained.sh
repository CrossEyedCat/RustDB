#!/usr/bin/env bash
# Local RustDB server + `cargo flamegraph` on `rustdb_load`.
# Works on Linux CI (perf) and inside the rustdb-prof Docker image (`CARGO=/usr/local/cargo/bin/cargo`).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CARGO_BIN="${CARGO:-cargo}"

OUT_SVG="${OUT_SVG:-$ROOT/profile-out/flame_rustdb_load_select1.svg}"
CERT_PATH="${CERT_PATH:-$ROOT/profile-out/flame_server.der}"
# Dev cert is issued for the bind IP (127.0.0.1); SNI must match.
SERVER_NAME="${SERVER_NAME:-127.0.0.1}"
ADDR="${ADDR:-127.0.0.1:15432}"

CONCURRENCY="${CONCURRENCY:-128}"
QUERIES="${QUERIES:-20000}"
SQL="${SQL:-SELECT 1}"
MODE="${MODE:-shared}"
STREAM_BATCH="${STREAM_BATCH:-1}"

mkdir -p "$(dirname "$OUT_SVG")"
rm -f "$CERT_PATH"

echo "==> build rustdb + rustdb_load (release)"
"$CARGO_BIN" build -q --release --bin rustdb --bin rustdb_load

echo "==> start rustdb server at $ADDR"
"$ROOT/target/release/rustdb" \
  server --host 127.0.0.1 --port 15432 --cert-out "$CERT_PATH" --exit-after-secs 300 &
SERVER_PID=$!

cleanup() {
  kill "$SERVER_PID" >/dev/null 2>&1 || true
  wait "$SERVER_PID" >/dev/null 2>&1 || true
}
trap cleanup EXIT

for _ in $(seq 1 600); do
  if [[ -s "$CERT_PATH" ]]; then
    break
  fi
  sleep 0.1
done
if [[ ! -s "$CERT_PATH" ]]; then
  echo "ERROR: server cert not created at $CERT_PATH" >&2
  exit 2
fi

echo "==> run flamegraph (rustdb_load -> $ADDR)"
exec "$CARGO_BIN" flamegraph \
  --output "$OUT_SVG" \
  --bin rustdb_load -- \
    --addr "$ADDR" \
    --cert "$CERT_PATH" \
    --server-name "$SERVER_NAME" \
    --concurrency "$CONCURRENCY" \
    --queries "$QUERIES" \
    --sql "$SQL" \
    --connection-mode "$MODE" \
    --stream-batch "$STREAM_BATCH"
