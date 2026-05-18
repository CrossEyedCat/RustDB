#!/usr/bin/env bash
# Phase 42 helper: bisect run-23 TPC-C flush stack (PR #65–#69).
# Does not run benchmarks itself — prints checkout steps and artifact paths.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> Phase 42 bisect — run-23 flush stack"
echo "Doc: docs/tpcc-phase42-bisect-run23.md"
echo "Open PR #69: https://github.com/CrossEyedCat/RustDB/pull/69"
echo ""

PRS=(60 61 62 64 65 66 67 69)
for n in "${PRS[@]}"; do
  state=$(gh pr view "$n" --json state,title -q '.state + " | " + .title' 2>/dev/null || echo "missing")
  echo "  PR #${n}: ${state}"
done

echo ""
echo "==> Suggested workflow"
cat <<'EOF'
1. git fetch origin && git checkout main && git pull
2. Run bench (or CI artifact) on main → tpcc-artifacts-run23/main-post-59
3. gh pr checkout 69 && build image / run scripts/tpcc_throughput_ci.sh
4. Compare tpcc.json: ratio_vs_postgres, new_order commit_*, payment commit_flush_us
5. If #69 regresses payment: bisect between pr66 and pr67 artifacts (presort without dirty)
6. Phase 40 (#70) / 41 (#71) on main — re-run after merge to see if #69 still needed
EOF

ART_ROOT="${TPCC_ARTIFACTS_ROOT:-$ROOT/tpcc-artifacts-run23}"
if [[ -d "$ART_ROOT" ]]; then
  echo ""
  echo "==> Local artifact dirs under $ART_ROOT"
  ls -1d "$ART_ROOT"/*/ 2>/dev/null | sed 's|.*/||;s|/$||' || true
fi
