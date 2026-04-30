## Durability & crash recovery (SqlEngine WAL)

Этот документ фиксирует **текущее** поведение долговечности (durability) и восстановления (crash-recovery) для сетевого движка `SqlEngine` и его WAL-интеграции.

Цель — убрать неоднозначность вокруг «что именно считается коммитом» и какие гарантии ожидаются при рестарте после падения процесса/ОС.

### Где это реализовано

- **SQL-движок**: `src/network/sql_engine/mod.rs`
- **WAL интеграция** (BEGIN/COMMIT/ABORT + DML records + replay): `src/network/sql_engine_wal.rs`
- **Формат log records**: `src/logging/log_record.rs`
- **RecoveryManager** (общая реализация, используется на `SqlEngine::open`): `src/logging/recovery.rs`
- **CheckpointManager**: `src/logging/checkpoint.rs`
- **Commit log (минимальный durability hook)**: `src/network/sql_commit_log.rs`

### Термины

- **WAL**: append-only журнальные записи изменений, хранятся в `data_dir/.rustdb/wal/*.log`.
- **LSN**: log sequence number (`u64`), монотонно возрастающий идентификатор записи WAL.
- **Txn id**: `transaction_id` (`u64`) внутри WAL.
- **Commit point**: момент, после которого транзакция должна считаться «зафиксированной» и должна пережить рестарт (при выбранной политике fsync).

### Политика включения WAL / checkpoints

По умолчанию WAL **включён**. Переменные окружения:

- `RUSTDB_DISABLE_WAL=1`: отключить WAL целиком (не будет recovery по WAL; поведение после crash может отражать незакоммиченные изменения, т.к. heap writes происходят напрямую).
- `RUSTDB_DISABLE_CHECKPOINT=1`: отключить checkpoints, даже если WAL включён.
- `RUSTDB_AUTO_CHECKPOINT=1`: включить авто-checkpoint (иначе только ручной `SqlEngine::checkpoint()`).
- `RUSTDB_CHECKPOINT_INTERVAL_SECS=<n>`: интервал авто-checkpoint (секунды).
- `RUSTDB_FSYNC_COMMIT=1`: включить «синхронный commit» на уровне WAL writer и `commits.log` (см. ниже).

### Что пишется в WAL

Транзакционные записи (все имеют `transaction_id`):

- `TransactionBegin`
- `DataInsert`
- `DataUpdate`
- `DataDelete`
- `TransactionCommit`
- `TransactionAbort`

Системные/метаданные записи:

- `Checkpoint` / `CheckpointEnd` (создаёт `CheckpointManager`)
- `MetadataUpdate` (используется как marker после успешной записи `catalog.json`)

Примечание: marker `MetadataUpdate(kind=catalog_json)` **не привязан** к user transaction (нет `transaction_id`) и в текущем поведении **не влияет** на WAL replay; он нужен для наблюдаемости/диагностики порядка сохранения каталога.

### Текущее поведение DML внутри explicit transaction

`SqlEngine` реализует минимальные транзакции на уровне сессии:

- `BEGIN`: создаёт `SqlTransaction` в `SessionContext` и пишет WAL `TransactionBegin` (если WAL включён).
- DML (`INSERT/UPDATE/DELETE`) **сразу** модифицирует heap (`PageManager`) и пишет соответствующие WAL records (если WAL включён и есть активная транзакция).
- После DML движок вызывает `flush_dirty_pages()` для page manager (видимость на диске повышается даже для незакоммиченных данных).
- `ROLLBACK`: пишет WAL `TransactionAbort`, применяет in-memory undo log и затем принудительно `flush_all_page_managers()` (чтобы “откатанные” вставки/апдейты не остались видимыми после рестарта).
- `COMMIT`: пишет WAL `TransactionCommit` и добавляет строку в `data_dir/.rustdb/commits.log`.

### Commit point (зафиксированная спецификация для текущей реализации)

Для explicit transaction `BEGIN … COMMIT` commit point определяется так:

1. В WAL записан `TransactionCommit` для `transaction_id` этой транзакции.\n+2. Если `RUSTDB_FSYNC_COMMIT=1`, запись WAL commit выполнена в режиме `synchronous_commit` (fsync/эквивалент на уровне log writer) и строка в `commits.log` записана с `sync_all()`.

Следствия:

- При **WAL включён**: при рестарте выполняется анализ WAL, и операции транзакций с `TransactionCommit` попадают в REDO (см. `analyze_wal_for_replay`), а транзакции без `Commit/Abort` — в UNDO.
- При **WAL выключен** (`RUSTDB_DISABLE_WAL=1`): commit point в строгом смысле отсутствует; устойчивость зависит от того, что и когда успело попасть на диск через `flush_dirty_pages()` и OS buffers.

### Recovery / replay на `SqlEngine::open`

`SqlEngine::open` (при включённом WAL) выполняет:

1. `RecoveryManager::recover_database(wal_dir)` — общий recovery-проход по WAL директории (в режиме `quiet=true`, `enable_validation=false`).
2. `replay_wal_into_engine(state, wal_dir)`:\n+   - анализирует WAL и получает:\n+     - `redo`: все `Data*` операций для транзакций, которые `committed && !aborted`\n+     - `undo_per_tx`: операции `Data*` для “активных” транзакций (нет commit и нет abort), в обратном LSN порядке\n+   - применяет REDO по LSN возрастанию\n+   - применяет UNDO по каждой активной транзакции в обратном порядке (по сути «компенсация» на уровне PageManager)\n+
Важно: текущее replay — **страница-ориентированное** (см. `PageManager::apply_log_record_recovery`) и предполагает, что `record_offset` (`u16`) адресует слот внутри страницы.

### Гарантии и ограничения (v1)

- **Гарантия (при WAL ON)**: после рестарта транзакции без `COMMIT` не должны “просачиваться” в итоговое состояние: их DML изменения компенсируются UNDO при `open()`.
- **Гарантия (при `RUSTDB_FSYNC_COMMIT=1`)**: commit marker в WAL и commit line в `commits.log` должны быть устойчивы к падению после возврата из `COMMIT`.
- **Ограничение**: DML изменения физически записываются в heap ещё до `COMMIT` (и даже могут быть сброшены на диск после statement-level flush). Корректность после рестарта достигается **за счёт UNDO**.\n+  Это отличается от классического “WAL-first + no-force” дизайна и должно учитываться при дальнейшем развитии.\n+- **Ограничение**: DDL запрещён внутри explicit transaction (см. `README.md`), поэтому WAL для DDL пока не формализован как часть user transactions.\n+
### Инварианты (для тестов / regression gates)

Рекомендуемые инварианты, которые должны быть покрыты тестами:\n+
1. **Crash before commit**: после `BEGIN; INSERT …; <crash>` и последующего `open()` результат запроса не включает вставленную строку.\n+2. **Crash after commit**: после `BEGIN; INSERT …; COMMIT; <crash>` и `open()` вставленная строка присутствует.\n+3. **Rollback durability**: после `BEGIN; INSERT …; ROLLBACK; <crash>` и `open()` вставленная строка отсутствует.\n+4. **Idempotent replay**: повторный `open()` без новых операций не меняет содержимое (повторный replay не «дублирует» строки).\n+
