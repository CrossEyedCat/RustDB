#!/usr/bin/env bash
#
# Named TPC-C compare profiles (RustDB vs PostgreSQL harness tuning).
#
# Usage:
#   source scripts/tpcc_compare_profiles.sh
#   tpcc_apply_compare_profile fair_sql
#
# Profiles:
#   native_bench     — CI/sweep default: native ExecuteTpcc, bench durability, PG simple_query
#   sql_path         — SQL on both sides + PG prepared stmts (preset unchanged; use in fair_tpcc_compare)
#   fair_sql         — sql_path + strict durability (concurrency sweep)
#   fair_sql_bench   — sql_path + bench durability (faster local A/B)
#
set -euo pipefail

tpcc_apply_sql_path_profile() {
  export RUSTDB_TPCC_NATIVE=0
  export RUSTDB_TPCC_NATIVE_MICRO=0
  export POSTGRES_TPCC_PREPARED=1
}

tpcc_apply_compare_profile() {
  local profile="${1:?profile required: native_bench|sql_path|fair_sql|fair_sql_bench}"
  export TPCC_COMPARE_PROFILE="$profile"
  case "$profile" in
    native_bench)
      export RUSTDB_TPCC_NATIVE="${RUSTDB_TPCC_NATIVE:-1}"
      export RUSTDB_TPCC_NATIVE_MICRO="${RUSTDB_TPCC_NATIVE_MICRO:-0}"
      export TPCC_PRESET="${TPCC_PRESET:-bench}"
      export POSTGRES_TPCC_PREPARED="${POSTGRES_TPCC_PREPARED:-0}"
      ;;
    sql_path)
      tpcc_apply_sql_path_profile
      ;;
    fair_sql)
      tpcc_apply_sql_path_profile
      export TPCC_PRESET=strict
      ;;
    fair_sql_bench)
      tpcc_apply_sql_path_profile
      export TPCC_PRESET=bench
      ;;
    *)
      echo "tpcc_apply_compare_profile: unknown profile '$profile'" >&2
      return 1
      ;;
  esac
}
