#!/usr/bin/env bash
# Record CPU profile for a PID (Linux perf). Requires `perf` in PATH.
#
# Usage:
#   ./scripts/profile_cpu_linux.sh <PID> 30
#   perf script -i perf.data | ...
#
set -euo pipefail
PID="${1:?PID required}"
DUR="${2:-15}"
if ! command -v perf >/dev/null 2>&1; then
  echo "perf not found; install linux-tools-common or use WSL2 with perf."
  exit 0
fi
OUT="${PERF_DATA:-perf.data}"
echo "Recording $OUT for ${DUR}s on pid $PID ..."
perf record -g -F 997 -p "$PID" -o "$OUT" -- sleep "$DUR"
echo "Done. Report: perf report -i $OUT --no-call-graph"
