# Сравнение бенчмарков RustDB и PostgreSQL

## Обзор

Документ содержит результаты бенчмарков RustDB и типичные показатели PostgreSQL для ориентировочного сравнения. Учитывайте, что тесты различаются по методике и окружению.

**Дата последнего запуска**: 2025-03-22 (benchmark-results v3)

### Режимы E2E INSERT (план Reduce PostgreSQL Gap)

| Режим                         | RustDB (median) | PostgreSQL (ориентир) | Сравнение |
|-------------------------------|-----------------|------------------------|-----------|
| insert_with_tx_wal            | ~1.9 ms/опер.   | ~1–10 ms/опер.         | ✓ в диапазоне PG |
| insert_with_tx_wal_group_commit | ~1.8 ms/опер. | ~1–10 ms/опер.         | ✓ в диапазоне PG |
| insert_with_tx_wal_async_commit | **~0.44 ms/опер.** | ~100–500 µs       | ✓ близко к PG |
| insert_without_wal           | **~0.35 ms/опер.** | ~100–500 µs        | ✓ в диапазоне PG |
| insert_batch_flush (50)      | **~0.12 ms/insert** | —                 | лучший режим по latency |

---

## Сводная таблица RustDB vs PostgreSQL

| Метрика | RustDB | PostgreSQL | Соотношение |
|---------|--------|------------|-------------|
| **Создание БД** | ~39 ms | ~50–200 ms* | RustDB быстрее или сопоставимо |
| **Открытие БД** | ~19.5 ms | ~20–100 ms* | сопоставимо |
| **Парсинг SQL** | ~5–7 µs | ~1–5 µs | один порядок величины |
| **E2E INSERT (fsync)** | ~1.9 ms/опер. | ~1–10 ms/опер. | сопоставимо |
| **E2E INSERT (async_commit)** | **~0.44 ms** | ~100–500 µs | близко к PG |
| **E2E INSERT (batch 50)** | **~0.12 ms** | — | очень низкая latency |
| **Чтение страницы** | ~282 µs | ~0.1–1 ms | сопоставимо |
| **Запись страницы** | ~277 µs (median) | ~0.1–1 ms | сопоставимо |
| **SELECT 100 rows** | ~112 µs | ~30–100 µs | сопоставимо |
| **begin_insert_commit** | ~1.5 ms | ~0.5–2 ms | сопоставимо |
| **TPS (single thread, async)** | **~2 300** | — | оценка |
| **TPS (batch 50)** | **~8 300** | — | оценка |
| **TPS (pgbench, multi-client)** | — | 2,900–300,000 | PG масштабируется лучше |

\* Время инициализации зависит от каталога и диска.

---

## Результаты RustDB (текущие)

### Базовые операции (benchmarks)

| Операция           | Mean   | Median | PostgreSQL (ориентир) |
|--------------------|--------|--------|------------------------|
| database_creation  | ~39 ms | ~39 ms | ~50–200 ms             |
| database_open      | ~19.5 ms | ~19.4 ms | ~20–100 ms          |

### SQL парсинг (sql_benchmarks)

| Операция           | parse_only | parse_and_plan | PostgreSQL |
|--------------------|------------|----------------|------------|
| SELECT simple      | ~4.1 µs    | ~4.7 µs        | ~1–5 µs    |
| SELECT WHERE       | ~4.2 µs    | ~5.3 µs        | ~1–5 µs    |
| INSERT             | ~5.3 µs    | ~6.8 µs        | ~1–5 µs    |
| UPDATE             | ~4.3 µs    | ~4.5 µs        | ~1–5 µs    |

### E2E (полный цикл: парсинг → планирование → выполнение → WAL)

| Тест                         | RustDB        | PostgreSQL (ориентир) |
|------------------------------|---------------|------------------------|
| insert_with_tx_wal           | ~1.9 ms/опер. | ~1–10 ms/опер.         |
| insert_with_tx_wal_group_commit | ~1.8 ms/опер.| ~1–10 ms/опер.         |
| insert_with_tx_wal_async_commit | **~0.44 ms/опер.** | ~100–500 µs   |
| insert_without_wal           | **~0.35 ms/опер.** | ~100–500 µs      |
| insert_batch_flush (50)      | ~6 ms/батч (~0.12 ms/insert) | —        |
| inserts_with_tx_wal (5)      | ~2.7 ms/батч  | —                      |
| begin_insert_commit          | ~1.5 ms/цикл  | ~0.5–2 ms              |
| select_full_scan_100         | ~112 µs       | ~30–100 µs             |

### I/O (io_benchmarks)

| Операция            | RustDB (mean) | RustDB (median) | PostgreSQL | Пропускная способность |
|---------------------|---------------|-----------------|------------|------------------------|
| read_page (1000)    | ~283 µs       | ~282 µs         | ~0.1–1 ms  | ~5 MiB/s               |
| write_page (1000)   | ~1.1 ms*      | ~277 µs         | ~0.1–1 ms  | ~5 MiB/s               |

\* Mean выше из‑за периодических fsync/flush; median отражает типичный случай.

---

## Результаты PostgreSQL (ориентир)

> **На данной системе pgbench не установлен.** Значения — из документации и публичных бенчмарков (2024).

### pgbench TPS (TPC-B‑подобный)

| Конфигурация | TPS | Комментарий |
|--------------|-----|-------------|
| 16 clients, 4 workers | ~2,900–3,000 | Типичная нагрузка |
| Intel i9-14900K, 800 clients | ~57,900 | Mid-range |
| AMD EPYC, 800 clients | до ~125,000 | High-end |
| 1536 clients (PG 15.6+) | до ~300,000 | Масштабируемость |

### Латентность операций

| Операция | Время | Примечание |
|----------|-------|------------|
| **fsync** | ~1 ms | pg_test_fsync, типичное железо |
| INSERT с WAL (без fsync) | ~100–500 µs | Group commit |
| INSERT с fsync | ~1–10 ms | Каждый commit = fsync |
| Простой запрос (в памяти) | ~30–100 µs | — |

---

## Запуск бенчмарков

### RustDB

```bash
# Все бенчмарки
cargo bench

# Отдельные наборы
cargo bench --bench benchmarks
cargo bench --bench sql_benchmarks
cargo bench --bench e2e_benchmarks
cargo bench --bench io_benchmarks
cargo bench --bench performance_benchmarks
```

### PostgreSQL (pgbench)

```bash
# Инициализация
pgbench -i -s 10 pgbench_db

# Простой TPC-B‑подобный тест (60 s)
pgbench -c 16 -j 4 -T 60 pgbench_db

# С кастомным скриптом
pgbench -c 16 -j 4 -T 60 -f custom_script.sql pgbench_db
```

Пример `custom_script.sql` для сравнения с RustDB:

```sql
INSERT INTO bench_table (id, name, age) VALUES (1, 'Alice', 30);
```

---

## Анализ разрывов

### Где RustDB сопоставим с PostgreSQL или лучше

- **Парсинг SQL** — тот же порядок (5–7 µs vs 1–5 µs)
- **E2E INSERT с fsync** — ~1.9 ms vs 1–10 ms ✓
- **E2E INSERT без fsync** — **~0.35–0.44 ms** vs 100–500 µs ✓ (близко к PG)
- **I/O страницы** — 0.28–0.28 ms в том же диапазоне (0.1–1 ms)
- **Операции в памяти** (блоки: 49–67 ns) — быстрее за счёт простой модели
- **Batch INSERT** — ~0.12 ms/insert при батче 50 — очень эффективно

### Где PostgreSQL всё ещё заметно быстрее

- **Многопоточный TPS**: PG 2,900–300,000 (pgbench) vs RustDB single-thread ~2,300
- **Масштабируемость**: PG использует пул соединений, shared buffers, параллельные воркеры
- **Латентность async commit**: PG 100–500 µs, RustDB 350–440 µs (разница ~2×)

### Причины разрыва (митигировано)

1. ~~Синхронный fsync на каждый commit~~ — group commit включён
2. ~~Нет group commit~~ — реализован
3. ~~INSERT без fsync значительно медленнее~~ — synchronous_commit=off даёт ~0.44 ms
4. **Остаётся:** нет пула соединений и параллельных воркеров

### Выводы

- **RustDB** достиг латентности, сопоставимой с PostgreSQL для одиночных операций (fsync, async commit, batch).
- **Разрыв по TPS** (~×1.3–×130) сохраняется из‑за отсутствия параллелизма и пула соединений.
- **По единичным операциям** RustDB и PostgreSQL дают сопоставимые результаты.

---

## Рекомендации по улучшению RustDB

- [ ] Оптимизация сериализации страниц (CompactPage → бинарный формат)
- [ ] Увеличение размера PAGE_SIZE или более компактный layout
- [x] Асинхронный I/O и буферизация WAL
- [x] Group commit для WAL
- [x] synchronous_commit=off (high_throughput preset) — снижает латентность ~1.4×
- [x] Отложенный flush данных (defer_data_flush, flush_interval_ms)
- [ ] Параллельное выполнение запросов
- [ ] Индексы для ускорения SELECT
