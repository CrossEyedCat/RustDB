# Сравнение бенчмарков RustDB и PostgreSQL

## Обзор

Документ содержит результаты бенчмарков RustDB и типичные показатели PostgreSQL для ориентировочного сравнения. Учитывайте, что тесты различаются по методике и окружению.

**Дата последнего запуска**: 2025-03-22

### Новые режимы (план Reduce PostgreSQL Gap)

| Режим                         | Описание                                      | Латентность     |
|-------------------------------|-----------------------------------------------|-----------------|
| insert_with_tx_wal            | Полный sync, force_flush по умолчанию         | ~15 ms/опер.    |
| insert_with_tx_wal_group_commit | Group commit (force_flush_immediately=false) | ~14 ms/опер.    |
| insert_with_tx_wal_async_commit | synchronous_commit=off (high_throughput)      | ~10 ms/опер.    |
| insert_batch_flush (50)       | 50 inserts в одной tx + один flush            | ~32 ms/батч (~0.64 ms/insert) |

---

## Сводная таблица сравнения RustDB vs PostgreSQL

| Метрика | RustDB | PostgreSQL | Разница |
|---------|--------|------------|---------|
| **Создание БД** | ~39 ms | ~50–200 ms* | сопоставимо |
| **Открытие БД** | ~19.5 ms | ~20–100 ms* | сопоставимо |
| **Парсинг SQL** | ~4–5 µs | ~1–5 µs | одинаковый порядок |
| **E2E INSERT (с fsync)** | ~7.9 ms/опер. | ~1–10 ms/опер. | сопоставимо (fsync ≈ 1 ms) |
| **Чтение страницы** | ~271 µs (кеш) | ~0.1–1 ms | сопоставимо |
| **Запись страницы** | ~272 µs–1.1 ms | ~0.1–1 ms | сопоставимо |
| **TPS** | ~126 | 2,900–300,000 | **×20–×2400** в пользу PG |

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

| Тест                         | RustDB        | PostgreSQL (fsync=on) |
|------------------------------|---------------|------------------------|
| insert_with_tx_wal           | ~15 ms/опер.  | ~1–10 ms/опер.         |
| insert_with_tx_wal_group_commit | ~14 ms/опер.| ~1–10 ms/опер.         |
| insert_with_tx_wal_async_commit | ~10 ms/опер. | ~100–500 µs            |
| insert_without_wal           | ~11 ms/опер.  | ~100–500 µs            |
| insert_batch_flush (50)      | ~32 ms/батч   | —                      |
| inserts_with_tx_wal (5)      | ~21 ms/батч   | —                      |
| begin_insert_commit          | ~2.0 ms/цикл  | ~0.5–2 ms              |
| select_full_scan_100         | ~69 µs        | ~30–100 µs             |

### I/O (io_benchmarks)

| Операция            | RustDB (mean) | RustDB (median) | PostgreSQL | Пропускная способность |
|---------------------|---------------|-----------------|------------|------------------------|
| read_page (1000)    | ~271 µs       | ~271 µs         | ~0.1–1 ms  | ~5 MiB/s               |
| write_page (1000)   | ~1.1 ms*      | ~272 µs         | ~0.1–1 ms  | ~5 MiB/s               |
| read_page (10000)   | ~272 µs       | ~272 µs         | ~0.1–1 ms  | ~6 MiB/s               |
| write_page (10000)  | ~1.1 ms*      | ~272 µs         | ~0.1–1 ms  | ~6 MiB/s               |

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

### Где RustDB сопоставим с PostgreSQL

- **Парсинг SQL** — тот же порядок (микросекунды)
- **E2E INSERT с fsync** — ~8 ms vs 1–10 ms (ограничение fsync ~1 ms)
- **I/O страницы** — 0.3–1 ms в том же диапазоне
- **Операции в памяти** (блоки: 49–67 ns) — быстрее за счёт простой модели

### Где PostgreSQL значительно быстрее

- **TPS** (×20–×2400): пул соединений, group commit, WAL, shared buffers
- **INSERT без fsync** — ~100–500 µs vs ~9 ms: group commit и буферизация WAL

### Причины разрыва по TPS в RustDB (митигировано)

1. ~~Синхронный fsync на каждый commit~~ — group commit включён (force_flush_immediately=false)
2. ~~Нет group commit~~ — реализован в log_writer
3. Нет пула соединений и параллельных воркеров
4. Упрощённая реализация страниц и WAL

### Выводы

- **RustDB** — учебная/экспериментальная СУБД с фокусом на архитектуре и SQL-пайплайне.
- **PostgreSQL** — промышленная СУБД с оптимизациями под высокую нагрузку.
- По единичным операциям (парсинг, I/O) порядки величин сопоставимы.
- Главный разрыв — пропускная способность (TPS) из‑за отсутствия group commit и параллелизма.

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
