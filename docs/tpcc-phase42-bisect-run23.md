# Phase 42 — bisect run-23 perf stack (PR #65–#69)

**Status:** investigation only — do **not** merge until full-mix `ratio_vs_postgres` regression (&lt;58%) is explained.

## Context

| PR | Branch (typical) | Theme | Outcome |
|----|------------------|-------|---------|
| #60 | run-23 PR1 | commit flush v3 (fix run-22 regression) | closed — superseded |
| #61 | run-23 PR2 | touched-only dirty PM flush | closed |
| #62 | run-23 PR3 | touched dirty flush, parallel@2 | closed |
| #64 | run-23 PR4 | single-pass serial commit flush | closed |
| #65 | run-23 PR5 | restore dirty cached flush + file_id sort | closed |
| #66 | run-23 PR6 | touched cached flush + file_id sort | closed |
| #67 | run-23 PR7 | presorted commit flush, one PM lock | closed — **payment `commit_flush` regression** (12ms→66ms) |
| **#69** | `tpcc-run23-pr8-single-pass-dirty-presorted` | single-pass touched dirty presorted flush | **OPEN** — supersedes #66+#67 intent |

Artifacts: `tpcc-artifacts-run23/pr65` … `pr69`, `main-post-59`.

## Hypotheses

1. **Full-cache COMMIT flush** — flushing all `txn_pm_cache` entries (not only `touched_tables`) inflates `commit_flush_us` on payment/new_order mix.
2. **Presorted flush without dirty check (#67)** — serial flush of clean PMs caused payment regression; #69 reintroduces dirty check in one pass.
3. **`file_id` sort via extra PM locks** — sorting PMs on commit by locking each PM for `file_id` adds `commit_pm_lock_wait_us`.
4. **Stack interaction with #58 DML** — deferred index + row buffer on main may mask or amplify flush changes.

## Bisect procedure

```bash
# Baseline after observability (#59) on main
git fetch origin
git checkout main && git pull

# Compare artifact dirs (ratio, commit_us, insert_order_line_us)
# tpcc-artifacts-run23/main-post-59 vs pr65 … pr69

# Per-PR replay (rebuild image from branch head, run scripts/tpcc_throughput_ci.sh)
gh pr checkout 65   # or 66, 67 — closed but branches may exist
# PR #69 (open):
gh pr checkout 69
```

Use `scripts/tpcc_bisect_run23.sh` for a scripted checklist and env parity with CI.

## Acceptance before merging any flush stack to main

- [ ] `ratio_vs_postgres` ≥ **60%** (stretch **62%** after phase 40)
- [ ] `new_order` `commit_pm_lock_wait_us` p50 &lt; **20ms**
- [ ] `new_order` `commit_us` p50 &lt; **90ms** (phase 40 target)
- [ ] `insert_order_line_us` p50 ≤ **35ms** (phase 41)
- [ ] Payment `commit_flush_us` does not regress vs main-post-59 (watch #67 failure mode)

## Relation to run-24 (phase 40–41)

- **Phase 40** (#70): minimal flush/PM-lock fixes on `main` (no #69 `txn_pm_cache` struct change).
- **Phase 41** (#71): DML `order_line` buffer — orthogonal.
- **Phase 42** (this doc): decide whether to land #69, cherry-pick subsets, or close #69 after phase 40+41 CI.

## First revert experiment (optional)

If #69 is merged locally for A/B, revert only presorted path and keep touched-only filter:

```bash
git checkout -b experiment/revert-pr69-presort-only
# Revert commit 21d3fbed… on top of pr69 branch, or apply inverse of flush_touched_dirty_presorted
```

Record results in this PR description checklist.
