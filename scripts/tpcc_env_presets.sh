#!/usr/bin/env bash
#
# TPC-C durability env presets for fair RustDB vs PostgreSQL comparison.
# Source from fair_tpcc_compare.sh or CI wrappers:
#   source scripts/tpcc_env_presets.sh
#   tpcc_apply_env_preset bench   # or strict
#
# Group commit vars are read at WAL open (see src/network/sql_engine_wal.rs /
# src/logging/log_writer.rs). Bench preset tunes batching for throughput CI; strict leaves
# conservative defaults unless overridden in the environment.
#
set -euo pipefail

tpcc_apply_env_preset() {
  local preset="${1:?preset required: bench|strict}"
  case "$preset" in
    bench)
      export RUSTDB_DEFER_HEAP_FLUSH_ON_COMMIT=1
      export RUSTDB_DEFER_HEAP_FLUSH_AFTER_DML=1
      export RUSTDB_BENCH_DEFER_HEAP_FSYNC=1
      export RUSTDB_GROUP_COMMIT_ENABLED=1
      export RUSTDB_GROUP_COMMIT_INTERVAL_MS="${RUSTDB_GROUP_COMMIT_INTERVAL_MS:-2}"
      export RUSTDB_GROUP_COMMIT_MAX_BATCH="${RUSTDB_GROUP_COMMIT_MAX_BATCH:-64}"
      fi
      ;;
    strict)
      export RUSTDB_DEFER_HEAP_FLUSH_ON_COMMIT=0
      export RUSTDB_DEFER_HEAP_FLUSH_AFTER_DML=0
      export RUSTDB_BENCH_DEFER_HEAP_FSYNC=0
      export RUSTDB_GROUP_COMMIT_ENABLED="${RUSTDB_GROUP_COMMIT_ENABLED:-1}"
      export RUSTDB_GROUP_COMMIT_INTERVAL_MS="${RUSTDB_GROUP_COMMIT_INTERVAL_MS:-1}"
      export RUSTDB_GROUP_COMMIT_MAX_BATCH="${RUSTDB_GROUP_COMMIT_MAX_BATCH:-10}"
      ;;
    *)
      echo "tpcc_apply_env_preset: unknown preset '$preset' (expected bench|strict)" >&2
      return 1
      ;;
  esac
}
