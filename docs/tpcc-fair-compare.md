# Fair TPC-C compare harness

Reproducible RustDB vs PostgreSQL comparison with **bench** and **strict** durability presets, automatic validity gates on the PostgreSQL baseline, and multi-run aggregation so a single ratio cannot be read as “RustDB is faster” without context.

## Modes

| Preset | `RUSTDB_DEFER_HEAP_FLUSH_ON_COMMIT` | `RUSTDB_DEFER_HEAP_FLUSH_AFTER_DML` | `RUSTDB_BENCH_DEFER_HEAP_FSYNC` | Meaning |
|--------|-------------------------------------|-------------------------------------|----------------------------------|---------|
| **bench** | `1` | `1` | `1` | Same profile as the main CI `bench_tpcc_throughput` job (defer heap flush on commit; WAL + commits.log remain durable). |
| **strict** | `0` | `0` | `0` | Synchronous heap flush on commit — closer to PostgreSQL page durability at commit time. |

Presets are defined in `scripts/tpcc_env_presets.sh` (`tpcc_apply_env_preset bench|strict`).

Other env (group commit, `RUSTDB_TPCC_DEFER_INDEX_SYNC`, worker count) matches `scripts/tpcc_throughput_ci.sh` in both modes.

## What you can claim

- **bench**: throughput regression signal for the CI profile only. A ratio above 105% does **not** mean RustDB is “production faster” than PostgreSQL — native TPC-C over QUIC is not the same path as `postgres_tpcc` over TCP/SQL.
- **strict**: stricter durability; median ratio is usually **lower** than bench (often ~60–70% when commit heap flush is the bottleneck). That is expected and is **not** a bench-job regression.
- **`claim_faster_than_pg`** (in `validation.json` / `fair_compare/report.json`): only when the run is **valid** (gates below) **and** median ratio > 105% across ≥ 2 of 3 nightly iterations.

## Validity gates (`scripts/validate_tpcc_run.py`)

A run is `valid: false` when any gate fails:

| Gate | Threshold |
|------|-----------|
| PostgreSQL `txns_per_s` | ≥ 800 |
| PostgreSQL `payment` p95 (txn log) | < 700 ms |
| Success rate (PG + RustDB) | 100%, `err == 0` |
| `new_order` share (txn count) | 0.42 – 0.48 |
| **bench** mode consistency | `commit_flush_us` p50 ≈ 0; ≥ 90% commits with `commit_heap_flush_skipped=1` in `server_full.log` |
| **strict** mode consistency | `commit_flush_us` p50 > 0 **or** skipped < 10% |

Invalid PostgreSQL runs (slow VM, noisy neighbor) inflate RustDB/PostgreSQL ratio — the validator blocks “faster than PG” claims in that case.

## Commands (local)

Prerequisites: Docker, `RUSTDB_IMAGE` (GHCR tag), host Rust toolchain for `rustdb_tpcc` / `postgres_tpcc`.

```bash
# One iteration (strict then bench, ~2× 300s full mix)
export RUSTDB_IMAGE=ghcr.io/org/repo:sha-abcdef0
FAIR_RUN_ID=1 ./scripts/fair_tpcc_compare.sh

# Validate an existing artifact dir
python3 scripts/validate_tpcc_run.py --mode bench tpcc-out
python3 scripts/validate_tpcc_run.py --mode strict tpcc-out/fair_compare/run-1/strict

# Three iterations + aggregate (as nightly workflow)
for i in 1 2 3; do FAIR_RUN_ID=$i ./scripts/fair_tpcc_compare.sh; done
python3 scripts/aggregate_fair_tpcc.py tpcc-out/fair_compare --runs 3
```

Compare several single CI artifact dirs (median TPS + per-kind `new_order` p50):

```bash
python3 scripts/aggregate_ci_tpcc_validations.py \
  tpcc-ci-26165808773/validation.json \
  tpcc-ci-26130427214/validation.json \
  tpcc-ci-26394263210/validation.json
```

Read **`validation.json` → `metrics.rustdb_txn_log.per_kind`**, not only `txns_per_s`. Roughly 45% of the mix is `new_order`; payment/order_status are already fast — regressions often show up as higher `new_order.p50_ms` while aggregate TPS moves a few percent.

Outputs:

- Per mode: `tpcc-out/fair_compare/run-<id>/{strict,bench}/` — JSON, txn logs, `validation.json`, `phases.txt`
- Aggregate: `tpcc-out/fair_compare/report.json`, `report.md` (includes `per_kind_medians` and `new_order` focus)

## CI

- **PR / main bench** (`bench_tpcc_throughput` in `.github/workflows/ci-cd.yml`): bench preset only; runs `validate_tpcc_run.py --mode bench`; uploads `validation.json`; step summary warns on invalid runs and does not imply a “faster than PG” claim.
- **Nightly** (`.github/workflows/fair_tpcc_compare.yml`): `workflow_dispatch` or weekly schedule; three sequential iterations; `continue-on-error: true` initially; artifact `fair_compare`.

## Reference runs

Post–PR #80 bench profile on healthy PG baseline: PG ~835+ TPS, ratio ~113% ([run 26130427214](https://github.com/CrossEyedCat/RustDB/actions/runs/26130427214)-class metrics). Degraded PG (~420 TPS) must **not** be used for ratio claims.

See also [Durability & recovery](durability-and-recovery.md) for defer-on-commit vs strict flush semantics.

## Concurrency sweep (full TPC-C mix)

Unlike `scripts/bench_saturation_rustdb_postgres.py` (single SQL, `rustdb_load` / psycopg), the TPC-C sweep runs the **same native mix** as CI at several worker counts and builds comparison charts.

```bash
export RUSTDB_IMAGE=ghcr.io/org/repo:sha-abcdef0
# default: c=8,16,32,64 × 90s; bench preset
./scripts/tpcc_concurrency_sweep.sh

# Wider sweep (local, ~6× longer)
DURATION_SECS=120 CONCURRENCY_STEPS=1,4,8,16,32,64 ./scripts/tpcc_concurrency_sweep.sh

# Re-plot after manual edits / partial runs
python3 scripts/tpcc_concurrency_plot.py tpcc-out/concurrency_sweep
```

Per step: `tpcc-out/concurrency_sweep/c{N}/` — `tpcc.json`, `postgres_tpcc.json`, txn logs, `validation.json`, server log.

Aggregate outputs under the sweep root:

| File | Content |
|------|---------|
| `sweep_config.json` | Steps, duration, preset |
| `sweep.csv` | TPS, ratio, per-kind p50 |
| `sweep_report.md` | Table + saturation knee (98% of peak TPS) |
| `plots/throughput_vs_concurrency.png` | RustDB vs PG TPS |
| `plots/ratio_vs_concurrency.png` | Ratio % + 100% / 105% lines |
| `plots/per_kind_p50.png` | new_order, payment, order_status, delivery |
| `plots/new_order_p50.png` | Dominant mix component |
| `plots/rustdb_overall_latency.png` | p50 / p95 / p99 vs c |

Unit tests: `python3 scripts/test_tpcc_concurrency_plot.py`

**SELECT 1 saturation** (lighter, no Docker TPC-C): `scripts/bench_saturation_rustdb_postgres.py` → `saturation.csv`, `saturation.png`.

## `new_order` optimization branches

Baseline metrics and PR gates: [`tpcc-new-order-baseline.md`](tpcc-new-order-baseline.md), [`tpcc-new-order-pr-checklist.md`](tpcc-new-order-pr-checklist.md). Implement one hypothesis per branch (`opt/no-district-contention`, `opt/no-commit-index-batch`, etc.).
