#!/usr/bin/env bash
# Quick CPU profiling run (Linux): rustdb server + rustdb_load + perf.
#
# Intended for CI runners / Linux dev boxes.
#
# Usage:
#   ./scripts/profile_quic_select_table.sh
#   CONCURRENCY=128 QUERIES=20000 ./scripts/profile_quic_select_table.sh
set -euo pipefail

HOST_PORT="${HOST_PORT:-15432}"
CONCURRENCY="${CONCURRENCY:-128}"
QUERIES="${QUERIES:-20000}"
SERVER_NAME="${SERVER_NAME:-localhost}"

OUT_DIR="${OUT_DIR:-profile-out}"
mkdir -p "$OUT_DIR"

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "==> build rustdb + client + load (release, with debuginfo)"
export RUSTFLAGS="${RUSTFLAGS:-} -C debuginfo=2"
cargo build -q --release --bin rustdb --bin rustdb_quic_client --bin rustdb_load

echo "==> start server"
rm -f "$OUT_DIR/server.der" >/dev/null 2>&1 || true
./target/release/rustdb --config ./config.toml server --host 127.0.0.1 --port 5432 --cert-out "$OUT_DIR/server.der" >/dev/null 2>&1 &
SERVER_PID=$!

for i in $(seq 1 80); do
  if [[ -s "$OUT_DIR/server.der" ]]; then
    break
  fi
  sleep 0.1
done
test -s "$OUT_DIR/server.der"

echo "==> warmup (create table + one row)"
./target/release/rustdb_quic_client --addr "127.0.0.1:5432" --cert "$OUT_DIR/server.der" --server-name "$SERVER_NAME" "CREATE TABLE bench_t (a INTEGER)" >/dev/null 2>&1 || true
./target/release/rustdb_quic_client --addr "127.0.0.1:5432" --cert "$OUT_DIR/server.der" --server-name "$SERVER_NAME" "INSERT INTO bench_t (a) VALUES (1)" >/dev/null 2>&1 || true

echo "==> perf record (pid=$SERVER_PID) while running load"
perf record -F 99 -g --call-graph dwarf -p "$SERVER_PID" -o "$OUT_DIR/perf.data" -- \
  ./target/release/rustdb_load \
    --addr "127.0.0.1:5432" \
    --cert "$OUT_DIR/server.der" \
    --server-name "$SERVER_NAME" \
    --connection-mode shared \
    --concurrency "$CONCURRENCY" \
    --queries "$QUERIES" \
    --sql "SELECT a FROM bench_t WHERE a = 1" \
    --json \
  | tee "$OUT_DIR/load.jsonl"

echo "==> perf report (top)"
perf report --stdio -i "$OUT_DIR/perf.data" --no-children --percent-limit 1 > "$OUT_DIR/perf_report.txt" || true

echo "==> done"
echo "Artifacts:"
ls -la "$OUT_DIR"

