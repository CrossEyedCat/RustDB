# Отчёт о производительности: split-PR `new_order` (#86–#90)

Дата сбора: 2026-05-25. Источник: артефакты CI `rustdb-tpcc-throughput` (full mix, c=64, 300 s, bench preset). Базовая линия: [`tpcc-new-order-baseline.md`](tpcc-new-order-baseline.md) — [run 26398151075](https://github.com/CrossEyedCat/RustDB/actions/runs/26398151075).

---

## Краткое резюме (executive)

| Критерий | Лидер | Комментарий |
|----------|-------|-------------|
| **Лучший `new_order` p50 (RustDB)** | **#87** (114.6 ms, −24.7% к baseline) | CI `validation.json`: **`valid: false`** — PG деградировал (587 TPS, payment p95 917 ms). Сравнение ratio **нечестное**. |
| **Лучший среди `valid: true`** | **#86** (129.3 ms, −15.0%; ratio **113.7%**) | Единственный PR с заметным выигрышем по latency при прохождении PG-гейтов. |
| **Ближайший к baseline (docs-only)** | **#90** (148.6 ms; ratio 100.7%) | Ожидаемо: без изменений движка. |
| **Порог чеклиста p50 ≤ 110 ms** | **Ни один PR** | Все прогоны **FAIL** по client `new_order` p50. |

**Рекомендуемый порядок merge** (стек A→B→C→D, затем docs):

1. **#90** — baseline + checklist (можно сразу).
2. **#86** (Hypothesis **A**) — единственный «зелёный» bench с улучшением p50 и ratio при `valid: true`.
3. **#88** (Hypothesis **C**) — нейтрально по p50; ratio ~102.7%, без регрессии PG.
4. **#89** (Hypothesis **D**) — **регрессия ratio** (~95.9% vs ~101% baseline); p50 почти как main — merge после C или с повторным CI.
5. **#87** (Hypothesis **B**) — **перезапуск bench** до `valid: true`; иначе не мержить по perf-данным этого прогона.

---

## Базовая линия (main)

| Метрика | Значение | Run |
|---------|----------|-----|
| RustDB TPS | **898.3** | [26398151075](https://github.com/CrossEyedCat/RustDB/actions/runs/26398151075) |
| PostgreSQL TPS | **888.4** | |
| Ratio RD/PG | **101.1%** | |
| RustDB `new_order` p50 (client) | **152.1 ms** | |
| PG `payment` p95 | ~**518 ms** (класс bench VM) | из соседних valid-прогонов |
| `valid` | **true** | |

Sweep (c=64, RD `new_order` p50 **99.1 ms**): [26402376146](https://github.com/CrossEyedCat/RustDB/actions/runs/26402376146) — в артефактах PR #86–#90 **не загружался** (только full-mix leg).

---

## Сводная таблица (full mix CI)

| PR | Гипотеза | Branch | CI run | Bench job | RustDB TPS | PG TPS | Ratio % | Δ ratio vs BL | RD `new_order` p50 | Δ p50 vs BL | PG pay p95 | `valid` | p50 ≤110 | Verdict |
|----|:--------:|--------|--------|-----------|----------:|-------:|--------:|--------------:|-------------------:|------------:|-----------:|:-------:|:--------:|:-------:|
| — | baseline | `main` | [26398151075](https://github.com/CrossEyedCat/RustDB/actions/runs/26398151075) | — | 898.3 | 888.4 | 101.1 | — | 152.1 ms | — | ~518 | true | **FAIL** | ref |
| [#86](https://github.com/CrossEyedCat/RustDB/pull/86) | **A** district / locks | `opt/no-district-contention` | [26408647770](https://github.com/CrossEyedCat/RustDB/actions/runs/26408647770) | [job](https://github.com/CrossEyedCat/RustDB/actions/runs/26408647770/job/77738868453) | **1031.9** | 907.4 | **113.7** | **+12.5%** | **129.3 ms** | **−15.0%** | 518.8 | true | FAIL | **PASS** valid / **FAIL** p50 |
| [#87](https://github.com/CrossEyedCat/RustDB/pull/87) | **B** commit index batch | `opt/no-commit-index-batch` | [26408663776](https://github.com/CrossEyedCat/RustDB/actions/runs/26408663776) | [job](https://github.com/CrossEyedCat/RustDB/actions/runs/26408663776/job/77738873555) | 1182.8 | 587.2 | 201.4 | +99.2%* | **114.6 ms** | **−24.7%** | 916.7 | **false** | FAIL | **FAIL** (PG gates) |
| [#88](https://github.com/CrossEyedCat/RustDB/pull/88) | **C** WAL group commit | `opt/no-wal-group-commit` | [26411971455](https://github.com/CrossEyedCat/RustDB/actions/runs/26411971455) | [job](https://github.com/CrossEyedCat/RustDB/actions/runs/26411971455/job/77748518194) | 907.6 | 884.0 | 102.7 | +1.6% | 149.9 ms | −1.5% | 533.2 | true | FAIL | **PASS** valid / **FAIL** p50 |
| [#89](https://github.com/CrossEyedCat/RustDB/pull/89) | **D** SQL workers | `opt/no-sql-workers` | [26408672413](https://github.com/CrossEyedCat/RustDB/actions/runs/26408672413) | [job](https://github.com/CrossEyedCat/RustDB/actions/runs/26408672413/job/77739070686) | 905.6 | 944.8 | **95.9** | **−5.2%** | 149.5 ms | −1.7% | 499.1 | true | FAIL | **FAIL** ratio / **FAIL** p50 |
| [#90](https://github.com/CrossEyedCat/RustDB/pull/90) | docs | `docs/tpcc-new-order-validation` | [26416983399](https://github.com/CrossEyedCat/RustDB/actions/runs/26416983399) | [job](https://github.com/CrossEyedCat/RustDB/actions/runs/26416983399/job/77763810588) | 910.2 | 903.6 | 100.7 | −0.4% | 148.6 ms | −2.3% | 515.0 | true | FAIL | baseline-like |

\* Ratio #87 завышен из-за падения PG TPS (< 800) и payment p95 (> 700 ms) — см. [`tpcc-fair-compare.md`](tpcc-fair-compare.md).

**Пороги чеклиста** ([`tpcc-new-order-pr-checklist.md`](tpcc-new-order-pr-checklist.md)): `valid: true`; client `new_order` p50 **≤ 110 ms**; ratio без регрессии vs **~101%** без документированной деградации PG.

Локальные копии артефактов: `tpcc-ci-pr86/` … `tpcc-ci-pr90/` (`validation.json`, `tpcc.json`, `postgres_tpcc.json`).

---

## Детали по PR

### [#86](https://github.com/CrossEyedCat/RustDB/pull/86) — Hypothesis **A** (district contention)

- **Гипотеза:** шардированные row locks, fast path `d_next_o_id`, без batch index на commit.
- **Статус CI:** Benchmark TPC-C — **pass** ([26408647770](https://github.com/CrossEyedCat/RustDB/actions/runs/26408647770)).
- **Метрики:** RD 1031.9 TPS (+14.9% vs BL), ratio 113.7%, `new_order` p50 **129.3 ms** (лучший среди valid).
- **Вердикт:** `valid=true` — **PASS**; p50 **FAIL** (129 > 110); ratio — **PASS** (рост vs 101%).
- **Вывод:** единственный PR с устойчивым улучшением latency при здоровом PG; первый кандидат на merge в perf-стеке.

### [#87](https://github.com/CrossEyedCat/RustDB/pull/87) — Hypothesis **B** (commit-time index batch)

- **Гипотеза:** пакетная вставка отложенных secondary-index ops на `COMMIT` для native `new_order`.
- **Статус CI:** job **pass**, но `validation.json` → **`valid: false`**.
- **Причины:** `PG txns_per_s 587.2 < 800.0`; `PG payment p95 916.7ms >= 700.0ms`.
- **Метрики:** RD 1182.8 TPS, `new_order` p50 **114.6 ms** (лучший абсолютный, но всё ещё > 110 ms).
- **Вердикт:** **FAIL** по чеклисту (PG + p50); нужен **повторный** fair bench run.
- **Вывод:** latency-эффект возможен, но этот прогон **не пригоден** для merge-решения по ratio/TPS.

### [#88](https://github.com/CrossEyedCat/RustDB/pull/88) — Hypothesis **C** (WAL group commit preset)

- **Гипотеза:** bench/strict presets для `RUSTDB_GROUP_COMMIT_*`, `tpcc_throughput_ci.sh` → preset `bench`.
- **Статус CI:** **pass** ([26411971455](https://github.com/CrossEyedCat/RustDB/actions/runs/26411971455)).
- **Метрики:** RD 907.6 TPS (~+1% BL), ratio 102.7%, p50 149.9 ms (~как main).
- **Вердикт:** `valid=true` — **PASS**; p50 **FAIL**; perf **нейтральный**.
- **Вывод:** безопасный merge для инфраструктуры bench; не даёт выигрыша p50 в этом run.

### [#89](https://github.com/CrossEyedCat/RustDB/pull/89) — Hypothesis **D** (SQL worker count)

- **Гипотеза:** выше default `RUSTDB_SQL_WORKER_COUNT` на bench + `query_stream` worker default.
- **Статус CI:** **pass** ([26408672413](https://github.com/CrossEyedCat/RustDB/actions/runs/26408672413)).
- **Метрики:** RD 905.6 TPS, ratio **95.9%** (−5.2% vs BL), p50 149.5 ms.
- **Вердикт:** `valid=true`, но **FAIL** по ratio (регрессия vs ~101%); p50 **FAIL**.
- **Вывод:** merge только после **#88** (полный bench preset в CI) и/или повторного sweep; иначе отложить.

### [#90](https://github.com/CrossEyedCat/RustDB/pull/90) — docs + CI validation

- **Содержание:** `tpcc-new-order-baseline.md`, `tpcc-new-order-pr-checklist.md`; **без** изменений движка.
- **Статус CI:** **pass** ([26416983399](https://github.com/CrossEyedCat/RustDB/actions/runs/26416983399)).
- **Метрики:** RD 910.2 TPS, ratio 100.7%, p50 148.6 ms — в пределах шума vs main (898/152).
- **Вердикт:** контрольный прогон; p50 **FAIL** (как baseline).
- **Вывод:** merge **в любой момент**; не ожидается perf-эффекта.

---

## Сравнение с порогами (итог)

| PR | `valid` | `new_order` p50 ≤ 110 ms | ratio ≥ ~101% (no PG collapse) | Рекомендация merge |
|----|:-------:|:------------------------:|:--------------------------------:|:-------------------|
| #86 | ✅ | ❌ (129 ms) | ✅ | **Да** (первый perf) |
| #87 | ❌ | ❌ (115 ms) | ❌* | **Нет** до re-run |
| #88 | ✅ | ❌ (150 ms) | ✅ | **Да** (нейтрально) |
| #89 | ✅ | ❌ (150 ms) | ❌ (96%) | **Осторожно** / re-bench |
| #90 | ✅ | ❌ (149 ms) | ✅ | **Да** (docs) |

---

## Пропущенные / отсутствующие данные

| Элемент | Статус |
|---------|--------|
| Concurrency sweep (c=8…64) | Не в артефактах PR-прогонов; отдельно — baseline [26402376146](https://github.com/CrossEyedCat/RustDB/actions/runs/26402376146) |
| Server phase breakdown (`summarize_sql_phase_log.py`) | Не извлекался; см. `server_full.log` в `tpcc-ci-pr*/` |
| Failed/skipped bench | **Нет** — все пять PR: последний Benchmark TPC-C **pass** |

---

## Как воспроизвести

```bash
gh run download <run_id> -n rustdb-tpcc-throughput -D tpcc-ci-prXX
python3 scripts/validate_tpcc_run.py --mode bench tpcc-ci-prXX
```

Run IDs: #86 → 26408647770; #87 → 26408663776; #88 → 26411971455; #89 → 26408672413; #90 → 26416983399.
