#!/usr/bin/env bash
# PostgreSQL TPC-C-ish baseline via postgres_tpcc (Docker + same schema as RustDB CI).
#
# RustDB does NOT speak the PostgreSQL wire protocol; use `rustdb_tpcc` / `tpcc_throughput_ci.sh`
# against RustDB. This script only targets PostgreSQL for a comparable tpmC / txns/s baseline.

set -euo pipefail

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker CLI not found in PATH."
  echo "Hint (Windows/WSL): enable Docker Desktop WSL integration, or run:"
  echo "  scripts/pgbench_compare.ps1"
  exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export OUT_DIR="${PGBENCH_OUT_DIR:-tpcc-out}"
export POSTGRES_CONTAINER_NAME="${PGBENCH_CONTAINER_NAME:-postgres-tpcc-bench}"
export POSTGRES_HOST_PORT="${PGBENCH_POSTGRES_PORT:-15440}"
export POSTGRES_DB="${PGBENCH_DB:-tpcc_bench}"
export POSTGRES_USER="${PGBENCH_POSTGRES_USER:-postgres}"
export POSTGRES_PASSWORD="${PGBENCH_POSTGRES_PASSWORD:-postgres}"
export CONCURRENCY="${PGBENCH_CLIENTS:-64}"
export DURATION_SECS="${PGBENCH_DURATION:-300}"
export MIX="${TPCC_MIX:-new_order=0.45,payment=0.43,order_status=0.04,delivery=0.04,stock_level=0.04}"

chmod +x scripts/bench_postgres_tpcc.sh
./scripts/bench_postgres_tpcc.sh

echo ""
echo "=== Next: compare with RustDB ==="
echo "Run RustDB side with the same CONCURRENCY / DURATION_SECS / MIX, e.g.:"
echo "  scripts/tpcc_throughput_ci.sh (sets RUSTDB_IMAGE, writes tpcc-out/tpcc.json)"
