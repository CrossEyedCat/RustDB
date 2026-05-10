#!/usr/bin/env bash
#
# Back-compat wrapper: CI uses scripts/bench_rustdb_tpcc.sh directly.
#
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
exec "$ROOT/scripts/bench_rustdb_tpcc.sh"
