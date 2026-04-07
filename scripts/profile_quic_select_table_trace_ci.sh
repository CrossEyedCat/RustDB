#!/usr/bin/env bash
# CPU-ish profiling for GitHub Actions where `perf` is not available:
# record Chrome trace events from `tracing` while running QUIC load.
set -euo pipefail

OUT_DIR="${OUT_DIR:-profile-out}"
CONCURRENCY="${CONCURRENCY:-128}"
QUERIES="${QUERIES:-20000}"

mkdir -p "$OUT_DIR"

cleanup() {
  docker rm -f rustdb-trace-server >/dev/null 2>&1 || true
  docker volume rm -f rustdb-trace-vol >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "==> build host tools"
cargo build -q --bin rustdb_load --bin rustdb_quic_client

echo "==> start server container (with trace file)"
docker rm -f rustdb-trace-server >/dev/null 2>&1 || true
docker volume rm -f rustdb-trace-vol >/dev/null 2>&1 || true
docker volume create rustdb-trace-vol >/dev/null
docker run -d --name rustdb-trace-server -p 15432:5432/udp \
  -v rustdb-trace-vol:/app/data \
  -v "$PWD/$OUT_DIR:/out" \
  -e RUSTDB_TRACE_CHROME_PATH=/out/trace.json \
  "$RUSTDB_IMAGE" \
  sh -c "rustdb --config /app/config/config.toml server --host 0.0.0.0 --port 5432 --cert-out /tmp/server.der" >/dev/null

echo "==> extract TLS cert"
for i in $(seq 1 80); do
  if docker exec rustdb-trace-server sh -c "test -s /tmp/server.der" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
if ! docker exec rustdb-trace-server sh -c "test -s /tmp/server.der" >/dev/null 2>&1; then
  echo "ERROR: /tmp/server.der was not created by server"
  echo "==> container logs (last 200 lines)"
  docker logs rustdb-trace-server 2>&1 | tail -n 200 || true
  echo "==> /tmp listing"
  docker exec rustdb-trace-server sh -c "ls -la /tmp || true" 2>&1 || true
  exit 1
fi
docker cp rustdb-trace-server:/tmp/server.der "$OUT_DIR/server.der"
test -s "$OUT_DIR/server.der"

echo "==> warmup"
./target/debug/rustdb_quic_client --addr 127.0.0.1:15432 --cert "$OUT_DIR/server.der" --server-name localhost "CREATE TABLE bench_t (a INTEGER)" >/dev/null 2>&1 || true
./target/debug/rustdb_quic_client --addr 127.0.0.1:15432 --cert "$OUT_DIR/server.der" --server-name localhost "INSERT INTO bench_t (a) VALUES (1)" >/dev/null 2>&1 || true

echo "==> run load (shared + 128 concurrency)"
./target/debug/rustdb_load \
  --addr 127.0.0.1:15432 \
  --cert "$OUT_DIR/server.der" \
  --server-name localhost \
  --connection-mode shared \
  --concurrency "$CONCURRENCY" \
  --queries "$QUERIES" \
  --sql "SELECT a FROM bench_t WHERE a = 1" \
  --json \
  | tee "$OUT_DIR/load.jsonl"

echo "==> stop server (flush trace)"
docker rm -f rustdb-trace-server >/dev/null 2>&1 || true

echo "==> done"
ls -la "$OUT_DIR"

