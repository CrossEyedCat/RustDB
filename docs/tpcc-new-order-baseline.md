# TPC-C `new_order` baseline (main)

Reference numbers for native TPC-C `new_order` optimization work. Use these tables when comparing PR branches (district locks, commit batch, SQL workers, WAL tuning).

**Profile:** bench preset (`RUSTDB_DEFER_HEAP_*`, `RUSTDB_BENCH_DEFER_HEAP_FSYNC`), `concurrency=64` for full-mix CI unless noted, native `ExecuteTpcc` (`RUSTDB_TPCC_NATIVE=1`).

## Validation thresholds (informational)

From [`.github/workflows/ci-cd.yml`](../.github/workflows/ci-cd.yml) step summary and [`scripts/validate_tpcc_run.py`](../scripts/validate_tpcc_run.py):

| Metric | Threshold | Notes |
|--------|------------:|-------|
| Client `new_order` p50 (`tpcc_txn.log`) | **≤ 110 ms** | CI emits `::warning::` above this |
| Server `commit_us` p50 (`tpcc_kind=0`) | **≤ 50 ms** | informational |
| Server `commit_us` p50 (`tpcc_kind=0`) | **≤ 70 ms** | run-22 stretch target |
| Full-mix TPS ratio (300 s, c=64) | ~**101%** on healthy PG | baseline run below; do not regress without cause |
| `claim_faster_than_pg` | ratio > **105%** + valid gates | see [`tpcc-fair-compare.md`](tpcc-fair-compare.md) |

Sweep ratios above 105% at high **c** are **not** proof RustDB beats PostgreSQL on `new_order` — PG payment p95 degrades on the sweep VM ([`tpcc-fair-compare.md`](tpcc-fair-compare.md)).

---

## CI full mix — [run 26398151075](https://github.com/CrossEyedCat/RustDB/actions/runs/26398151075)

Commit `e624ea7`, **c=64**, **300 s**, bench defer-heap, `RUSTDB_SQL_WORKER_COUNT=24` (workflow default).

| Leg | TPS | Ratio (RD/PG) | `new_order` p50 (client) | `new_order` p95 |
|-----|----:|----------------:|-------------------------:|----------------:|
| RustDB | **898.3** | — | **152.1 ms** | 234.5 ms |
| PostgreSQL | **888.4** | **101.1%** | 3.6 ms | 7.4 ms |

Other RustDB per-kind client p50 (same run): payment **3.9 ms**, order_status **2.2 ms**, delivery **6.7 ms**, stock_level **2.3 ms**.

Server-side (`server_full.log`, kind=0): `commit_us` p50 **~0.55 ms** (median of `sql.execute_tpcc.commit` lines). Client `new_order` p50 is dominated by queue + district/contention + pre-commit, not commit wall alone.

Artifact copy: `tpcc-ci-26398151075/validation.json`.

**Takeaway:** TPS plateaus near ~900 at c=64 while `new_order` client p50 is **well above** the 110 ms soft threshold → optimize hot-path contention/queue, not aggregate TPS alone.

---

## Concurrency sweep — [run 26402376146](https://github.com/CrossEyedCat/RustDB/actions/runs/26402376146)

Bench preset, **90 s** per step, steps **8 / 16 / 32 / 64**. Source: `tpcc-ci-26402376146/sweep.csv`, `sweep_report.md`.

| c | valid | RustDB TPS | PG TPS | ratio % | RD `new_order` p50 | PG `new_order` p50 |
|--:|:-----:|-----------:|-------:|--------:|---------------------:|-------------------:|
| 8 | yes | 1348.5 | 1429.8 | 94.3 | **8.2 ms** | 2.8 ms |
| 16 | yes | 1420.4 | 1379.3 | 103.0 | **19.9 ms** | 2.8 ms |
| 32 | yes | 1382.6 | 1199.9 | 115.2 | **45.5 ms** | 2.9 ms |
| 64 | yes | 1384.4 | 1007.0 | 137.5 | **99.1 ms** | 3.3 ms |

Saturation (98% of peak RustDB TPS): knee ≈ **c=16**. RustDB `new_order` p50 grows roughly linearly from **8 ms → 99 ms** as **c** increases while TPS stays flat → queue/lock contention, not payment-bound.

Reproduce locally:

```bash
export RUSTDB_IMAGE=ghcr.io/crosseyedcat/rustdb:main
DURATION_SECS=90 CONCURRENCY_STEPS=8,16,32,64 ./scripts/tpcc_concurrency_sweep.sh
```

---

## How to refresh baselines

1. Download CI artifact `rustdb-tpcc-throughput` into `tpcc-ci-<run_id>/`.
2. Run `python3 scripts/validate_tpcc_run.py --mode bench tpcc-ci-<run_id>` (or use uploaded `validation.json`).
3. For sweep: copy `tpcc-out/concurrency_sweep` or artifact `concurrency-sweep` into `tpcc-ci-<run_id>/`.

See also [`tpcc-new-order-pr-checklist.md`](tpcc-new-order-pr-checklist.md) for merge gates.
