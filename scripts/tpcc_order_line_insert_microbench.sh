#!/usr/bin/env bash
#
# Native new_order microbench focusing on `order_line` insert cost.
#
# Runs a short RustDB-only leg (docker image server + host loadgen) with:
#   - mix=new_order=1.0
#   - configurable order_line insert loop via RUSTDB_TPCC_NEW_ORDER_OL_CNT
#
# Outputs under OUT_DIR (default: tpcc-out/order_line_insert_micro):
#   tpcc.json, tpcc_txn.log, server_full.log, phases_native_new_order.md
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

OUT_DIR="${OUT_DIR:-$ROOT/tpcc-out/order_line_insert_micro}"
CONCURRENCY="${CONCURRENCY:-64}"
DURATION_SECS="${DURATION_SECS:-60}"
ORDER_LINE_CNT="${ORDER_LINE_CNT:-10}"

mkdir -p "$OUT_DIR"

echo "==> order_line insert microbench (duration=${DURATION_SECS}s, concurrency=${CONCURRENCY}, order_line_cnt=${ORDER_LINE_CNT})"
echo "    OUT_DIR=$OUT_DIR"

export OUT_DIR CONCURRENCY DURATION_SECS
export MIX="new_order=1.0"
export RUSTDB_TPCC_NATIVE=1
export RUSTDB_SQL_PHASE_LOG="${RUSTDB_SQL_PHASE_LOG:-1}"
export RUSTDB_TPCC_NEW_ORDER_OL_CNT="${ORDER_LINE_CNT}"
# Optional: shard native order_line heap by w_id (default 16 for local microbench).
export RUSTDB_TPCC_ORDER_LINE_SHARDS="${RUSTDB_TPCC_ORDER_LINE_SHARDS:-16}"

chmod +x scripts/tpcc_throughput_ci.sh
./scripts/tpcc_throughput_ci.sh

if [[ -s "$OUT_DIR/server_full.log" ]]; then
  python3 scripts/summarize_tpcc_native_new_order_md.py \
    "$OUT_DIR/server_full.log" \
    --out "$OUT_DIR/phases_native_new_order.md"

  echo ""
  echo "==> insert_order_line_us quick stats (from server_full.log)"
  python3 - <<'PY'
import os
import re, statistics
from pathlib import Path

p = Path(os.environ["OUT_DIR"]) / "server_full.log"
us = []
for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
    if "sql.execute_tpcc.new_order" not in line:
        continue
    m = re.search(r"insert_order_line_us=(\d+)", line)
    if m:
        us.append(int(m.group(1)))
if not us:
    print("(no matches)")
else:
    us.sort()
    def q(xs, q):
        idx = (len(xs) - 1) * q
        lo = int(idx)
        hi = min(lo + 1, len(xs) - 1)
        frac = idx - lo
        return xs[lo] * (1.0 - frac) + xs[hi] * frac
    print(f"n={len(us)} p50_ms={q(us,0.50)/1000:.3f} p95_ms={q(us,0.95)/1000:.3f} mean_ms={statistics.fmean(us)/1000:.3f}")
PY
else
  echo "::warning::missing $OUT_DIR/server_full.log; cannot summarize phases"
fi

