#!/usr/bin/env bash
#
# Concurrency sweep with the fair_sql compare profile:
#   - RustDB: SQL/ExecuteScript path (RUSTDB_TPCC_NATIVE=0), strict durability
#   - PostgreSQL: prepared statements (POSTGRES_TPCC_PREPARED=1)
#
# Example:
#   export RUSTDB_IMAGE=rustdb-local:main
#   DURATION_SECS=120 ./scripts/tpcc_fair_sql_sweep.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=scripts/tpcc_compare_profiles.sh
source "$ROOT/scripts/tpcc_compare_profiles.sh"
tpcc_apply_compare_profile fair_sql

export SWEEP_ROOT="${SWEEP_ROOT:-$ROOT/tpcc-out/concurrency_sweep_fair_sql}"
export CONCURRENCY_STEPS="${CONCURRENCY_STEPS:-8,16,32,64}"
export DURATION_SECS="${DURATION_SECS:-90}"

exec bash "$ROOT/scripts/tpcc_concurrency_sweep.sh"
