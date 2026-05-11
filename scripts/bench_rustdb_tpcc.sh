#!/usr/bin/env bash
#
# RustDB TPC-C benchmark over QUIC (GHCR image). Builds schema + loads data inside rustdb_tpcc.
#
# Outputs under OUT_DIR:
#   tpcc.json, tpcc.txt, tpcc_stderr.log, server_tail.log, server.der
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

RUSTDB_IMAGE="${RUSTDB_IMAGE:?RUSTDB_IMAGE must be set (e.g. ghcr.io/org/repo:sha-xxxxxxx)}"
OUT_DIR="${OUT_DIR:-$ROOT/tpcc-out}"
UDP_PORT="${UDP_PORT:-15432}"
CONTAINER_NAME="${CONTAINER_NAME:-rustdb-tpcc-server}"
VOL_NAME="${VOL_NAME:-rustdb_tpcc_ci_data}"

CONCURRENCY="${CONCURRENCY:-64}"
DURATION_SECS="${DURATION_SECS:-300}"
WARMUP_SECS="${WARMUP_SECS:-10}"
WAREHOUSES="${WAREHOUSES:-4}"
ITEMS="${ITEMS:-1000}"
CUSTOMERS_PER_DISTRICT="${CUSTOMERS_PER_DISTRICT:-100}"
SEED="${SEED:-0}"
STMT_TIMEOUT_MS="${STMT_TIMEOUT_MS:-120000}"

mkdir -p "$OUT_DIR"
OUT_DIR_ABS="$(cd "$OUT_DIR" && pwd)"
rm -f "$OUT_DIR_ABS/tpcc.json" "$OUT_DIR_ABS/tpcc.txt" "$OUT_DIR_ABS/tpcc_stderr.log" || true

echo "==> pull image: $RUSTDB_IMAGE"
docker pull "$RUSTDB_IMAGE" >/dev/null

echo "==> prepare volume (empty; workload loads schema)"
docker volume rm -f "$VOL_NAME" >/dev/null 2>&1 || true
docker volume create "$VOL_NAME" >/dev/null

echo "==> start QUIC server (UDP host :$UDP_PORT -> container :5432)"
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
docker run -d --name "$CONTAINER_NAME" \
  -p "${UDP_PORT}:5432/udp" \
  -v "$VOL_NAME:/app/data" \
  -v "$ROOT/config.toml:/app/config/config.toml:ro" \
  "$RUSTDB_IMAGE" \
  sh -c 'rustdb --config /app/config/config.toml server --host 0.0.0.0 --port 5432 --cert-out /tmp/server.der' >/dev/null

for _ in $(seq 1 120); do
  if docker exec "$CONTAINER_NAME" sh -c "test -s /tmp/server.der" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

CERT="$OUT_DIR_ABS/server.der"
docker cp "$CONTAINER_NAME:/tmp/server.der" "$CERT"
test -s "$CERT"

echo "==> build rustdb_tpcc (host, release)"
cargo build -q --release --bin rustdb_tpcc

echo "==> run rustdb_tpcc (measurement; errors recorded in JSON — non-zero workload errors do not fail CI)"
set +e
./target/release/rustdb_tpcc \
  --addr "127.0.0.1:${UDP_PORT}" \
  --cert "$CERT" \
  --server-name localhost \
  --concurrency "$CONCURRENCY" \
  --warmup-secs "$WARMUP_SECS" \
  --duration-secs "$DURATION_SECS" \
  --warehouses "$WAREHOUSES" \
  --items "$ITEMS" \
  --customers-per-district "$CUSTOMERS_PER_DISTRICT" \
  --seed "$SEED" \
  --output-json "$OUT_DIR_ABS/tpcc.json" \
  --output-text "$OUT_DIR_ABS/tpcc.txt" \
  --statement-timeout-ms "$STMT_TIMEOUT_MS" \
  2> "$OUT_DIR_ABS/tpcc_stderr.log"
rc=$?
set -e

echo "==> capture server tail logs"
docker logs "$CONTAINER_NAME" 2>&1 | tail -n 200 > "$OUT_DIR_ABS/server_tail.log" || true

echo "==> stop server + volume"
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
docker volume rm -f "$VOL_NAME" >/dev/null 2>&1 || true

if [[ "$rc" -ne 0 ]]; then
  echo "rustdb_tpcc infrastructure failure (exit $rc). See $OUT_DIR_ABS/tpcc_stderr.log and server_tail.log"
  exit "$rc"
fi

echo "==> wrote $OUT_DIR_ABS/tpcc.json and tpcc.txt"
