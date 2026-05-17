#!/usr/bin/env bash
#
# Short A/B: only order_status transactions (isolates idx_oorder_wdc vs new_order noise).
# Runs postgres_tpcc then rustdb_tpcc with a fresh seed each time.
#
# Outputs under OUT_DIR (default: tpcc-out/order_status_micro):
#   postgres_tpcc.json, postgres_tpcc_txn.log
#   tpcc.json, tpcc_txn.log, server_full.log
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

OUT_DIR="${OUT_DIR:-$ROOT/tpcc-out/order_status_micro}"
CONCURRENCY="${CONCURRENCY:-64}"
DURATION_SECS="${DURATION_SECS:-60}"
MIX="${MIX:-order_status=1.0}"
POSTGRES_HOST_PORT="${POSTGRES_HOST_PORT:-15441}"
UDP_PORT="${UDP_PORT:-15433}"
CONTAINER_NAME="${CONTAINER_NAME:-rustdb-tpcc-os-micro}"
VOL_NAME="${VOL_NAME:-rustdb_tpcc_os_micro_data}"
POSTGRES_CONTAINER_NAME="${POSTGRES_CONTAINER_NAME:-postgres-tpcc-os-micro}"

mkdir -p "$OUT_DIR"
OUT_DIR_ABS="$(cd "$OUT_DIR" && pwd)"

echo "==> order_status microbench (duration=${DURATION_SECS}s, concurrency=${CONCURRENCY}, mix=${MIX})"
echo "    OUT_DIR=$OUT_DIR"

export OUT_DIR CONCURRENCY DURATION_SECS MIX POSTGRES_HOST_PORT
export POSTGRES_CONTAINER_NAME
# Native ExecuteTpcc + slim client/server wire path for order_status micro leg.
export RUSTDB_TPCC_NATIVE=1
export RUSTDB_TPCC_NATIVE_MICRO=1

chmod +x scripts/bench_postgres_tpcc.sh scripts/tpcc_throughput_ci.sh

echo "==> PostgreSQL (postgres_tpcc)"
./scripts/bench_postgres_tpcc.sh

if [[ -z "${RUSTDB_IMAGE:-}" ]]; then
  echo "ERROR: RUSTDB_IMAGE must be set for RustDB leg of microbench"
  exit 1
fi

export UDP_PORT CONTAINER_NAME VOL_NAME
echo "==> RustDB (rustdb_tpcc / QUIC)"
./scripts/tpcc_throughput_ci.sh

echo ""
echo "==> Latency by kind (client txn log)"
python3 scripts/analyze_tpcc_txn_log.py "$OUT_DIR/postgres_tpcc_txn.log" || true
echo ""
python3 scripts/analyze_tpcc_txn_log.py "$OUT_DIR/tpcc_txn.log" || true

python3 - <<PY
import json
import pathlib
import subprocess
import sys

out = pathlib.Path(r"${OUT_DIR_ABS}")

def p50_from_log(path: pathlib.Path) -> float | None:
    if not path.is_file():
        return None
    proc = subprocess.run(
        [sys.executable, "scripts/analyze_tpcc_txn_log.py", str(path)],
        capture_output=True,
        text=True,
        cwd=pathlib.Path("."),
    )
    for line in proc.stdout.splitlines():
        if line.startswith("order_status"):
            parts = line.split()
            if len(parts) >= 3:
                return float(parts[2])
    return None

pg_p50 = p50_from_log(out / "postgres_tpcc_txn.log")
rd_p50 = p50_from_log(out / "tpcc_txn.log")
pg_tps = json.loads((out / "postgres_tpcc.json").read_text()).get("txns_per_s", 0.0) if (out / "postgres_tpcc.json").is_file() else 0.0
rd_tps = json.loads((out / "tpcc.json").read_text()).get("txns_per_s", 0.0) if (out / "tpcc.json").is_file() else 0.0
ratio = (100.0 * rd_tps / pg_tps) if pg_tps > 0 else 0.0
print("")
print("==> order_status micro summary")
print(f"postgres txns_per_s: {pg_tps:.1f}")
print(f"rustdb   txns_per_s: {rd_tps:.1f}")
print(f"rustdb/postgres TPS: {ratio:.1f}%")
if pg_p50 is not None and rd_p50 is not None:
    print(f"order_status p50_ms: postgres={pg_p50:.3f} rustdb={rd_p50:.3f} ratio={100.0*rd_p50/pg_p50:.1f}%")
PY
