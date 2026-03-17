# Анализ узких мест RustDB

## Критический путь E2E INSERT (~26 ms/операция)

```
parse → plan → optimize → begin_tx → [insert → log_insert] → commit_transaction
```

---

## 1. Дисковый I/O (страницы данных)

**Местоположение:** `CachedFileManager` → `AdvancedFileManager` → `DatabaseFile`

**Проблема:**
- Каждый INSERT выполняет **read_page** (загрузка страницы) и **write_page** (запись)
- Даже при кэше: при промахе — чтение с диска; запись всегда идёт на диск
- `io_benchmarks`: read ~0.8 ms, write ~1.0 ms → **~2 ms на операцию**

**Код:**
```rust
// page_manager.rs:189-210
let page_data = self.file_manager.read_page(self.file_id, page_id)?;
// ...
self.file_manager.write_page(self.file_id, page_id, &serialized)?;
```

**Рекомендации:**
- [ ] Write-ahead: не писать страницу до commit, держать в буфере
- [ ] Batch flush: группировать несколько страниц перед записью
- [ ] Проверить, что `sync_file` не вызывается на каждый write (сейчас только в Drop — ок)

---

## 2. WAL: ожидание group commit

**Местоположение:** `wal.rs:370` → `log_writer.write_log_sync` → `log_writer.rs:504`

**Проблема:**
- `commit_transaction` вызывает `write_log_sync(commit_record)` — блокирующий вызов
- При `group_commit_enabled: true` транзакция ждёт следующего тика group commit
- Интервал: `group_commit_interval_ms = 5` → ожидание **до 5 ms**
- Лог не пишется в реальный файл — используется симуляция `sleep(50µs * N)`, но ожидание тика остаётся

**Код:**
```rust
// wal.rs:367-370
let commit_record = LogRecord::new_transaction_commit(...);
let commit_lsn = self.log_writer.write_log_sync(commit_record).await?;
```

```rust
// log_writer.rs:348-361 — group commit ждёт интервал
let mut interval = tokio::time::interval(gc_interval);
loop {
    interval.tick().await;  // Блокирует до следующего тика
    if should_flush { ... }
}
```

**Рекомендации:**
- [ ] Уменьшить `group_commit_interval_ms` (например, до 1 ms) для меньшей задержки
- [ ] Реализовать реальную запись WAL в файл вместо симуляции
- [ ] Опция `force_flush_immediately` для транзакций, требующих минимальной задержки

---

## 3. Сериализация страниц (bincode)

**Местоположение:** `page.rs:232` — `Page::to_bytes()`

**Проблема:**
- Сериализация через bincode: `CompactPage { header, slots, data }`
- При ~100+ записях на странице сериализация превышает `PAGE_SIZE` (4096) → split
- Накладные расходы: ~17 байт на слот + ~10 байт данных на запись

**Код:**
```rust
// page.rs:254-268
let compact_page = CompactPage { header, slots, data: used_data };
let serialized = bincode::serialize(&compact_page)?;
```

**Рекомендации:**
- [ ] Фиксированный бинарный формат страницы (slotted page) вместо bincode
- [ ] Более компактное представление слотов (например, u16 offset/size)
- [ ] Увеличение `PAGE_SIZE` до 8192 при необходимости

---

## 4. Глобальная блокировка PageManager

**Местоположение:** `e2e_benchmarks.rs:54` — `page_manager.lock()`

**Проблема:**
- `PageManager` обёрнут в `Mutex` → только один поток может выполнять insert
- Нет параллелизма при множестве клиентов

**Код:**
```rust
let (result, file_id) = {
    let mut pm = page_manager.lock().unwrap();
    let result = pm.insert(&data)?;
    ...
};
```

**Рекомендации:**
- [ ] Fine-grained locking: блокировать только страницу, а не весь менеджер
- [ ] Sharded page managers по file_id
- [ ] Lock-free структуры для hot path (например, для кэша)

---

## 5. Двойная запись: данные + WAL

**Проблема:**
- INSERT пишет в PageManager (данные) и в WAL (логирование)
- Два независимых потока записи на одну логическую операцию

**Рекомендации:**
- [ ] Сначала только WAL, данные — при checkpoint или lazy flush
- [ ] Group commit для данных (аналогично WAL)

---

## 6. Парсинг и планирование (относительно быстро)

**Местоположение:** `SqlParser`, `QueryPlanner`, `QueryOptimizer`

**Текущее состояние:**
- Парсинг: ~7–10 µs
- Планирование: ~8–14 µs
- На фоне ~26 ms E2E это **< 0.1%** — не узкое место

---

## Сводка приоритетов

| # | Узкое место              | Влияние (оценка) | Сложность исправления |
|---|---------------------------|------------------|------------------------|
| 1 | Дисковый I/O страниц     | Высокое (~2 ms)  | Средняя                |
| 2 | WAL group commit wait     | Среднее (до 5 ms)| Низкая                  |
| 3 | Сериализация bincode     | Среднее (split)  | Средняя                |
| 4 | Mutex PageManager        | Высокое (масштаб)| Высокая                 |
| 5 | Двойная запись           | Среднее          | Высокая                 |

---

## Быстрые улучшения

1. **Уменьшить `group_commit_interval_ms`** до 1–2 ms в `WalConfig`
2. **Увеличить buffer pool** в `PageManagerConfig` для большего числа cache hit
3. **Отключить WAL** в бенчмарках для оценки чистого I/O (если допустимо по тестам)
