# PR checklist: native TPC-C `new_order` optimizations

Copy into PR description when landing a branch from the [new_order optimization plan](https://github.com/CrossEyedCat/RustDB) (hypotheses A–F). One hypothesis per PR — do not mix unrelated engine changes.

Baseline reference: [`tpcc-new-order-baseline.md`](tpcc-new-order-baseline.md).

---

## Before merge

- [ ] **CI bench** (`bench_tpcc_throughput` on `tpcc*` branch or `workflow_dispatch`): `validation.json` → **`valid: true`**
- [ ] **Client `new_order` p50** (full mix, c=64, 300 s): **&lt; 110 ms** (target: stable **&lt; 100 ms**); no new `::warning::` vs baseline ~152 ms on [run 26398151075](https://github.com/CrossEyedCat/RustDB/actions/runs/26398151075)
- [ ] **Full-mix TPS ratio** vs PostgreSQL: no regression vs baseline **~101%** (~898 / ~888 TPS) without documented PG degradation
- [ ] **Server breakdown** (artifact `rustdb-tpcc-throughput`): `python3 scripts/summarize_sql_phase_log.py tpcc-out/server_full.log` — record `district_us`, `pre_commit_us`, `commit_us` (tpcc_kind=0), `commit_index_batch_us`, `row_storage_lock lock_wait_us` in PR
- [ ] **Concurrency sweep**: `DURATION_SECS=90 CONCURRENCY_STEPS=8,16,32,64 ./scripts/tpcc_concurrency_sweep.sh` (or CI sweep artifact)
  - [ ] At **c=64**, RustDB `new_order` p50 improves **≥ 15–25%** vs `main` **or** TPS knee shifts right with lower p50 at c=32/64
  - [ ] **`payment` p95** does not regress materially on sweep steps
- [ ] **Strict smoke** (optional but recommended): `TPCC_PRESET=strict FAIR_RUN_ID=1 ./scripts/fair_tpcc_compare.sh` then `python3 scripts/validate_tpcc_run.py --mode strict tpcc-out/fair_compare/run-1/strict` — gates still pass
- [ ] **order_status micro** (CI leg): ratio **≥ 70%** — if micro fails, do not claim an engine `new_order` fix (transport/index path)
- [ ] **Do not claim** “faster than PostgreSQL” from sweep **ratio &gt; 105%** at c=32/64 alone — PG payment tail degrades on sweep VMs ([`tpcc-fair-compare.md`](tpcc-fair-compare.md))

---

## PR body template

```markdown
## Hypothesis
<!-- e.g. D — SQL worker queue -->

## Baseline vs branch

| Run | RustDB TPS | PG TPS | ratio % | RD new_order p50 |
|-----|----------:|-------:|--------:|-----------------:|
| main (26398151075 class) | 898 | 888 | 101 | 152 ms |
| this PR CI | | | | |
| sweep c=8 | | | | |
| sweep c=64 | | | | |

## Server phases (kind=0)
<!-- paste summarize_sql_phase_log snippet or phases.txt -->

## Notes
<!-- env flags, risks, correctness tests -->
```

---

## Out of scope for these PRs

- Bench schema / warehouse scale changes
- Treating PG `new_order` ~3 ms (TCP/SQL) as the RustDB native target
- Merging prior reverted PRs (#78/#82) without `server_full.log` + sweep evidence
