#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export RUSTDB_IMAGE="${RUSTDB_IMAGE:-rustdb-local:main}"
export CONCURRENCY="${CONCURRENCY:-64}"
export DURATION_SECS="${DURATION_SECS:-45}"
SWEEP_ROOT="${SWEEP_ROOT:-$ROOT/tpcc-out/order_line_insert_micro/v5}"
mkdir -p "$SWEEP_ROOT"
for n in 1 5 10 20; do
  export ORDER_LINE_CNT="$n"
  export OUT_DIR="$SWEEP_ROOT/ol${n}"
  mkdir -p "$OUT_DIR"
  echo "=== ORDER_LINE_CNT=$n ===" | tee "$OUT_DIR/run.log"
  bash "$ROOT/scripts/tpcc_order_line_insert_microbench.sh" >>"$OUT_DIR/run.log" 2>&1
done
echo "==> sweep done: $SWEEP_ROOT"
python3 "$ROOT/scripts/summarize_order_line_sweep.py" "$SWEEP_ROOT" \
  >"$SWEEP_ROOT/SWEEP_SUMMARY.md"
echo "    wrote $SWEEP_ROOT/SWEEP_SUMMARY.md"
