#!/usr/bin/env bash
#
# TPC-C-ish throughput benchmark for RustDB in CI.
# Starts server from GHCR image (produced by workflow), seeds a tiny dataset via CLI,
# then runs the QUIC load generator `rustdb_tpcc` from the host build.
#
# Outputs:
#   tpcc-out/tpcc.json   (machine readable)
#   tpcc-out/tpcc.txt    (human readable)
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
TXNS="${TXNS:-5000}"
MIX="${MIX:-new_order=0.45,payment=0.43,order_status=0.04,delivery=0.04,stock_level=0.04}"

mkdir -p "$OUT_DIR"
rm -f "$OUT_DIR"/tpcc.* || true

echo "==> pull image: $RUSTDB_IMAGE"
docker pull "$RUSTDB_IMAGE" >/dev/null

echo "==> prepare volume + seed schema/data"
docker volume rm -f "$VOL_NAME" >/dev/null 2>&1 || true
docker volume create "$VOL_NAME" >/dev/null

# Seed using CLI path (local engine) before server starts.
docker run --rm -i \
  -v "$VOL_NAME:/app/data" \
  -v "$ROOT/scripts/tpcc_seed.sql:/tmp/tpcc_seed.sql:ro" \
  "$RUSTDB_IMAGE" \
  sh -c 'rustdb query --batch-file /tmp/tpcc_seed.sql' >/dev/null

echo "==> start QUIC server (UDP host :$UDP_PORT -> container :5432)"
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
docker run -d --name "$CONTAINER_NAME" \
  -p "${UDP_PORT}:5432/udp" \
  -v "$VOL_NAME:/app/data" \
  -v "$ROOT/config.toml:/app/config/config.toml:ro" \
  "$RUSTDB_IMAGE" \
  sh -c 'rustdb --config /app/config/config.toml server --host 0.0.0.0 --port 5432 --cert-out /tmp/server.der' >/dev/null

for i in $(seq 1 120); do
  if docker exec "$CONTAINER_NAME" sh -c "test -s /tmp/server.der" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

echo "==> copy leaf cert"
CERT="$OUT_DIR/server.der"
docker cp "$CONTAINER_NAME:/tmp/server.der" "$CERT"
test -s "$CERT"

echo "==> build rustdb_tpcc (host)"
cargo build -q --bin rustdb_tpcc

echo "==> run rustdb_tpcc"
set +e
./target/debug/rustdb_tpcc \
  --addr "127.0.0.1:${UDP_PORT}" \
  --cert "$CERT" \
  --server-name localhost \
  --concurrency "$CONCURRENCY" \
  --transactions "$TXNS" \
  --mix "$MIX" \
  --json > "$OUT_DIR/tpcc.json"
rc=$?
set -e

echo "==> capture server tail logs"
docker logs "$CONTAINER_NAME" 2>&1 | tail -n 200 > "$OUT_DIR/server_tail.log" || true

echo "==> stop server + volume"
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
docker volume rm -f "$VOL_NAME" >/dev/null 2>&1 || true

if [[ "$rc" -ne 0 ]]; then
  echo "tpcc benchmark failed (exit $rc). See $OUT_DIR/server_tail.log"
  exit "$rc"
fi

python3 - <<'PY'
import json, sys, pathlib
p = pathlib.Path("tpcc-out/tpcc.json")
data = json.loads(p.read_text())
txt = []
txt.append("== rustdb_tpcc throughput ==")
txt.append(f"concurrency: {data['concurrency']}")
txt.append(f"transactions: {data['transactions']}")
txt.append(f"elapsed_s: {data['elapsed_s']:.3f}")
txt.append(f"txns_per_s: {data['txns_per_s']:.1f}")
txt.append(f"new_orders: {data['new_orders']}")
txt.append(f"tpmC: {data['tpmC']:.1f}")
txt.append(f"p50_ms: {data['p50_ms']:.2f}  p95_ms: {data['p95_ms']:.2f}  p99_ms: {data['p99_ms']:.2f}")
pathlib.Path("tpcc-out/tpcc.txt").write_text("\n".join(txt) + "\n")
print("\n".join(txt))
PY

echo "==> wrote $OUT_DIR/tpcc.json and tpcc.txt"

