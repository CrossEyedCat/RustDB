#!/usr/bin/env bash
# Local fair compare: PostgreSQL baseline then RustDB (same profile as CI bench job).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export TPCC_ROOT="$ROOT"

# CI bench profile (main post #95)
export TPCC_PRESET=bench
export TPCC_CARGO_PROFILE=release
export CONCURRENCY="${CONCURRENCY:-64}"
export DURATION_SECS="${DURATION_SECS:-300}"
export MIX="${MIX:-new_order=0.45,payment=0.43,order_status=0.04,delivery=0.04,stock_level=0.04}"
export POSTGRES_BENCH_TUNING=1
export POSTGRES_TPCC_PREPARED=0
export RUSTDB_TPCC_NATIVE=1
export RUSTDB_TPCC_ORDER_LINE_SHARDS="${RUSTDB_TPCC_ORDER_LINE_SHARDS:-5}"
export RUSTDB_SQL_PHASE_LOG="${RUSTDB_SQL_PHASE_LOG:-0}"
export RUSTDB_TPCC_NATIVE_MICRO="${RUSTDB_TPCC_NATIVE_MICRO:-0}"
export RUSTDB_IMAGE="${RUSTDB_IMAGE:-rustdb:local}"
export OUT_DIR="${OUT_DIR:-$ROOT/tpcc-out/local-fair}"

mkdir -p "$OUT_DIR"

echo "==> Local TPC-C fair compare"
echo "    CONCURRENCY=$CONCURRENCY DURATION_SECS=$DURATION_SECS RUSTDB_IMAGE=$RUSTDB_IMAGE"
echo "    OUT_DIR=$OUT_DIR"
echo ""

echo "==> [1/2] PostgreSQL baseline"
export OUT_DIR
./scripts/bench_postgres_tpcc.sh

echo ""
echo "==> [2/2] RustDB (native bench)"
./scripts/tpcc_throughput_ci.sh

echo ""
echo "==> Summary"
python3 - <<PY
import json
import os
from pathlib import Path

root = Path(os.environ["OUT_DIR"])
pg = json.loads((root / "postgres_tpcc.json").read_text(encoding="utf-8"))
rd = json.loads((root / "tpcc.json").read_text(encoding="utf-8"))
pg_tps = pg["txns_per_s"]
rd_tps = rd["txns_per_s"]
raw = 100.0 * rd_tps / pg_tps
pg_ref = 1380.0
ref_ratio = 100.0 * rd_tps / max(pg_tps, pg_ref)
print(f"PostgreSQL TPS:     {pg_tps:.1f}")
print(f"RustDB TPS:         {rd_tps:.1f}")
print(f"raw ratio:          {raw:.1f}%")
print(f"vs PG ref {pg_ref:.0f}: {ref_ratio:.1f}%")
print(f"claim (>105% ref):  {ref_ratio >= 105.0}")
print(f"PG p50/p95 ms:      {pg.get('p50_ms', 0):.2f} / {pg.get('p95_ms', 0):.2f}")
print(f"RustDB p50/p95 ms:  {rd.get('p50_ms', 0):.2f} / {rd.get('p95_ms', 0):.2f}")
print(f"PG new_orders:      {pg.get('new_orders', 0)}")
print(f"RustDB new_orders:  {rd.get('new_orders', 0)}")
PY
