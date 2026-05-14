#!/usr/bin/env bash
# Deprecated name: forwards to `bench_postgres_tpcc.sh` (TPC-C-ish baseline, not pgbench).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
exec "$ROOT/scripts/bench_postgres_tpcc.sh" "$@"
