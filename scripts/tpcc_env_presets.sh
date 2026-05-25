#!/usr/bin/env bash
#
# TPC-C durability env presets for fair RustDB vs PostgreSQL comparison.
# Source from fair_tpcc_compare.sh or CI wrappers:
#   source scripts/tpcc_env_presets.sh
#   tpcc_apply_env_preset bench   # or strict
#
# Default QUIC SQL worker threads for bench when RUSTDB_SQL_WORKER_COUNT is unset:
# min(CONCURRENCY, host logical CPUs, 64). Matches load generator concurrency without
# oversubscribing tiny VMs. Explicit RUSTDB_SQL_WORKER_COUNT in the environment wins.
tpcc_bench_sql_worker_count_default() {
  local conc="${CONCURRENCY:-64}"
  local cpus
  cpus="$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)"
  python3 -c "print(min(64, int('${conc}'), int('${cpus}')))"
}
set -euo pipefail

tpcc_apply_env_preset() {
  local preset="${1:?preset required: bench|strict}"
  case "$preset" in
    bench)
      export RUSTDB_DEFER_HEAP_FLUSH_ON_COMMIT=1
      export RUSTDB_DEFER_HEAP_FLUSH_AFTER_DML=1
      export RUSTDB_BENCH_DEFER_HEAP_FSYNC=1
        export RUSTDB_SQL_WORKER_COUNT="$(tpcc_bench_sql_worker_count_default)"
      fi
      ;;
      ;;
    strict)
      export RUSTDB_DEFER_HEAP_FLUSH_ON_COMMIT=0
      export RUSTDB_DEFER_HEAP_FLUSH_AFTER_DML=0
      export RUSTDB_BENCH_DEFER_HEAP_FSYNC=0
      ;;
    *)
      echo "tpcc_apply_env_preset: unknown preset '$preset' (expected bench|strict)" >&2
      return 1
      ;;
  esac
}
