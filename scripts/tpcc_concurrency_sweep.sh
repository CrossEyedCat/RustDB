#!/usr/bin/env bash
#
# TPC-C throughput sweep across concurrency levels (RustDB + PostgreSQL).
#
# For each concurrency: paired bench run, validate, then plot via tpcc_concurrency_plot.py.
#
# Env:
#   RUSTDB_IMAGE          — required (GHCR tag)
#   SWEEP_ROOT            — default tpcc-out/concurrency_sweep
#   CONCURRENCY_STEPS     — default "8,16,32,64" (comma-separated)
#   DURATION_SECS         — default 90 (shorter than CI 300s; override for serious runs)
#   TPCC_PRESET           — bench|strict (default bench)
#   SKIP_PLOT             — set 1 to only collect data
#
# Example:
#   export RUSTDB_IMAGE=ghcr.io/crosseyedcat/rustdb:main
#   DURATION_SECS=120 CONCURRENCY_STEPS=1,4,8,16,32,64 ./scripts/tpcc_concurrency_sweep.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

: "${RUSTDB_IMAGE:?RUSTDB_IMAGE must be set}"

SWEEP_ROOT="${SWEEP_ROOT:-$ROOT/tpcc-out/concurrency_sweep}"
CONCURRENCY_STEPS="${CONCURRENCY_STEPS:-8,16,32,64}"
DURATION_SECS="${DURATION_SECS:-90}"
MIX="${MIX:-new_order=0.45,payment=0.43,order_status=0.04,delivery=0.04,stock_level=0.04}"

if [[ -n "${TPCC_COMPARE_PROFILE:-}" ]]; then
  # shellcheck source=scripts/tpcc_compare_profiles.sh
  source "$ROOT/scripts/tpcc_compare_profiles.sh"
  tpcc_apply_compare_profile "$TPCC_COMPARE_PROFILE"
fi

TPCC_PRESET="${TPCC_PRESET:-bench}"
RUSTDB_TPCC_NATIVE="${RUSTDB_TPCC_NATIVE:-1}"
POSTGRES_TPCC_PREPARED="${POSTGRES_TPCC_PREPARED:-0}"
export CONCURRENCY_STEPS DURATION_SECS TPCC_PRESET MIX RUSTDB_IMAGE RUSTDB_TPCC_NATIVE POSTGRES_TPCC_PREPARED TPCC_COMPARE_PROFILE

# shellcheck source=scripts/tpcc_env_presets.sh
source "$ROOT/scripts/tpcc_env_presets.sh"
tpcc_apply_env_preset "$TPCC_PRESET"

mkdir -p "$SWEEP_ROOT"
SWEEP_ABS="$(cd "$SWEEP_ROOT" && pwd)"
if command -v cygpath >/dev/null 2>&1; then
  SWEEP_ABS="$(cygpath -m "$SWEEP_ABS")"
fi

python3 - <<PY
import json, os
from pathlib import Path
_default_mix = "new_order=0.45,payment=0.43,order_status=0.04,delivery=0.04,stock_level=0.04"
cfg = {
    "concurrency_steps": [int(x) for x in os.environ.get("CONCURRENCY_STEPS", "8,16,32,64").split(",") if x.strip()],
    "duration_secs": int(os.environ.get("DURATION_SECS", "90")),
    "preset": os.environ.get("TPCC_PRESET", "bench"),
    "compare_profile": os.environ.get("TPCC_COMPARE_PROFILE") or None,
    "rustdb_tpcc_native": os.environ.get("RUSTDB_TPCC_NATIVE", "1") == "1",
    "postgres_prepared": os.environ.get("POSTGRES_TPCC_PREPARED", "0") == "1",
    "mix": os.environ.get("MIX", _default_mix),
    "rustdb_image": os.environ["RUSTDB_IMAGE"],
}
Path(r"${SWEEP_ABS}").joinpath("sweep_config.json").write_text(
    json.dumps(cfg, indent=2) + "\n", encoding="utf-8"
)
print("sweep_config:", cfg)
PY

IFS=',' read -ra STEPS <<< "$CONCURRENCY_STEPS"
for c in "${STEPS[@]}"; do
  c="$(echo "$c" | tr -d ' ')"
  [[ -n "$c" ]] || continue
  step_dir="$SWEEP_ABS/c${c}"
  mkdir -p "$step_dir"

  pg_port=$((15450 + c))
  rd_port=$((16450 + c))

  echo "==> sweep concurrency=$c out=$step_dir (duration=${DURATION_SECS}s)"

  echo "==> PostgreSQL @ c=$c"
  OUT_DIR="$step_dir" \
    CONCURRENCY="$c" \
    DURATION_SECS="$DURATION_SECS" \
    MIX="$MIX" \
    POSTGRES_TPCC_PREPARED="$POSTGRES_TPCC_PREPARED" \
    POSTGRES_CONTAINER_NAME="postgres-tpcc-sweep-${c}" \
    POSTGRES_HOST_PORT="$pg_port" \
    bash "$ROOT/scripts/bench_postgres_tpcc.sh"

  echo "==> RustDB @ c=$c"
  OUT_DIR="$step_dir" \
    CONCURRENCY="$c" \
    DURATION_SECS="$DURATION_SECS" \
    MIX="$MIX" \
    RUSTDB_TPCC_NATIVE="$RUSTDB_TPCC_NATIVE" \
    UDP_PORT="$rd_port" \
    CONTAINER_NAME="rustdb-tpcc-sweep-${c}" \
    VOL_NAME="rustdb_tpcc_sweep_${c}" \
    bash "$ROOT/scripts/tpcc_throughput_ci.sh"

  echo "==> validate c=$c"
  set +e
  python3 "$ROOT/scripts/validate_tpcc_run.py" --mode "$TPCC_PRESET" "$step_dir"
  val_rc=$?
  set -e
  if [[ "$val_rc" -ne 0 ]]; then
    echo "::warning::validation failed for concurrency=$c (exit $val_rc)"
  fi
done

if [[ "${SKIP_PLOT:-0}" != "1" ]]; then
  echo "==> plots"
  python3 "$ROOT/scripts/tpcc_concurrency_plot.py" "$SWEEP_ABS"
fi

echo "==> done: $SWEEP_ABS"
