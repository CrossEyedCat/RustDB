#!/usr/bin/env bash
#
# One fair-compare iteration: strict then bench, each with paired PG + RustDB legs.
#
# Requires: RUSTDB_IMAGE, docker, host cargo (rustdb_tpcc / postgres_tpcc).
#
# Env:
#   FAIR_RUN_ID          — run folder suffix (default 1)
#   CONCURRENCY          — default 64
#   DURATION_SECS        — default 300
#   MIX                  — same as CI bench job
#   RUSTDB_TPCC_NATIVE   — default 1
#
# Output:
#   tpcc-out/fair_compare/run-$FAIR_RUN_ID/{strict,bench}/...
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

: "${RUSTDB_IMAGE:?RUSTDB_IMAGE must be set}"

FAIR_RUN_ID="${FAIR_RUN_ID:-1}"
CONCURRENCY="${CONCURRENCY:-64}"
DURATION_SECS="${DURATION_SECS:-300}"
MIX="${MIX:-new_order=0.45,payment=0.43,order_status=0.04,delivery=0.04,stock_level=0.04}"
RUSTDB_TPCC_NATIVE="${RUSTDB_TPCC_NATIVE:-1}"

COMPARE_ROOT="$ROOT/tpcc-out/fair_compare/run-${FAIR_RUN_ID}"
mkdir -p "$COMPARE_ROOT"

# shellcheck source=scripts/tpcc_env_presets.sh
source "$ROOT/scripts/tpcc_env_presets.sh"

run_mode() {
  local mode="$1"
  local out_dir="$COMPARE_ROOT/$mode"
  mkdir -p "$out_dir"
  local port_off=0
  if [[ "$mode" == "bench" ]]; then
    port_off=1
  fi
  local pg_port=$((15450 + FAIR_RUN_ID * 10 + port_off))
  local udp_port=$((15460 + FAIR_RUN_ID * 10 + port_off))

  echo "==> fair compare run=${FAIR_RUN_ID} mode=${mode} out=${out_dir}"
  tpcc_apply_env_preset "$mode"

  echo "==> PostgreSQL leg (${mode})"
  OUT_DIR="$out_dir" \
    CONCURRENCY="$CONCURRENCY" \
    DURATION_SECS="$DURATION_SECS" \
    MIX="$MIX" \
    POSTGRES_CONTAINER_NAME="postgres-fair-${FAIR_RUN_ID}-${mode}" \
    POSTGRES_HOST_PORT="$pg_port" \
    bash "$ROOT/scripts/bench_postgres_tpcc.sh"

  echo "==> RustDB leg (${mode})"
  OUT_DIR="$out_dir" \
    CONCURRENCY="$CONCURRENCY" \
    DURATION_SECS="$DURATION_SECS" \
    MIX="$MIX" \
    RUSTDB_TPCC_NATIVE="$RUSTDB_TPCC_NATIVE" \
    UDP_PORT="$udp_port" \
    CONTAINER_NAME="rustdb-fair-${FAIR_RUN_ID}-${mode}" \
    VOL_NAME="rustdb_fair_${FAIR_RUN_ID}_${mode}" \
    bash "$ROOT/scripts/tpcc_throughput_ci.sh"

  echo "==> validate (${mode})"
  set +e
  python3 "$ROOT/scripts/validate_tpcc_run.py" --mode "$mode" "$out_dir"
  val_rc=$?
  set -e
  if [[ "$val_rc" -ne 0 ]]; then
    echo "::warning::validation failed for run=${FAIR_RUN_ID} mode=${mode} (exit $val_rc)"
  fi

  if [[ -s "$out_dir/server_full.log" ]]; then
    echo "==> SQL phase summary (${mode})"
    python3 "$ROOT/scripts/summarize_sql_phase_log.py" \
      --warn-lock-p99-ms 50 \
      "$out_dir/server_full.log" > "$out_dir/phases.txt" 2>&1 || true
  else
    echo "(no server_full.log for phases.txt)" > "$out_dir/phases.txt"
  fi

  echo "==> done mode=${mode} valid_rc=${val_rc}"
}

chmod +x "$ROOT/scripts/bench_postgres_tpcc.sh" "$ROOT/scripts/tpcc_throughput_ci.sh"

for mode in strict bench; do
  run_mode "$mode"
done

echo "==> fair iteration complete: $COMPARE_ROOT"
