# Implementation roadmap

Short, prioritized plan for RustDB beyond the current **SQL engine** (`SqlEngine`: parse → plan → execute, shared by CLI and QUIC). Context on **what is done vs in flight** stays in [README.md](../README.md) (sections *SQL-92 compatibility*, *Implemented*, *What’s still evolving*).

## Priorities (ordered)

1. **Library surface:** optional wrapper API (e.g. `Database` + owned `SqlEngine` or `Connection`) so embedders do not depend on wiring details; keep `SqlEngine` as the low-level primitive.
   - *Done (minimal):* `SqlSession` (`src/sql_session.rs`), `Database::into_sql_engine`, and `open_sql_engine` (`src/lib.rs`).
2. **Durability:** define a commit point: append **WAL records** for DML/DDL (or checkpointed equivalents), **`fsync` policy**, and **replay** on open; align `COMMIT` with log sequence and page state.
   - *Done (minimal):* each explicit `COMMIT` appends a line to `data_dir/.rustdb/commits.log`; set **`RUSTDB_FSYNC_COMMIT=1`** to `fsync` that append. Structured WAL logs explicit transactions plus DML `Data*` records; **`SqlEngine::checkpoint`** flushes heaps and appends a checkpoint record (same `LogWriter`).
3. **Recovery:** integrate existing **checkpoint/recovery** modules in `src/logging/` with the **SqlEngine** data directory lifecycle; tests for crash-after-append, crash-after-commit.
   - *Done (minimal):* `SqlEngine::open` runs `RecoveryManager::recover_database` on `data_dir/.rustdb/wal` (quiet, validation off) before opening `LogWriter`. Log analysis reads length-prefixed records. DML REDO/UNDO replays into `PageManager` on open; **`CheckpointManager`** is attached when WAL is enabled (disable with **`RUSTDB_DISABLE_CHECKPOINT=1`**; periodic checkpoints with **`RUSTDB_AUTO_CHECKPOINT=1`** and optional **`RUSTDB_CHECKPOINT_INTERVAL_SECS`**).
4. **Isolation (later):** stronger guarantees if needed (locking upgrades, snapshot isolation), building on current `RwLock` + MVCC direction in `src/core/`.
   - *Done (minimal):* `SqlIsolationLevel` adds `RepeatableRead` and `Serializable`. **`RUSTDB_DEFAULT_ISOLATION`** (`read_committed` \| `repeatable_read` \| `serializable`) selects the level at `BEGIN`. RR/SER use a process-global `parking_lot::Mutex` so at most one such transaction runs at a time. True snapshot isolation / MVCC for SQL scans is still future work.
5. **DDL / catalog:** serialize **catalog** consistently with heap files; expand **`ALTER`** (column add/drop/rewrite) and document unsupported forms; stress tests for FK/PK under concurrency.
   - *Done (engine v1):* v1 JSON catalog snapshot and WAL **`MetadataUpdate`** marker on each successful save (see durability/recovery items above). **`ALTER TABLE`:** `ADD COLUMN` (optional column constraints), `DROP COLUMN` (guarded by PK/unique/FK/parent references), `RENAME COLUMN` / `RENAME TO`, `MODIFY COLUMN` for common type changes and `NOT NULL` / `DEFAULT` (constraints on `MODIFY` are limited — use `ADD CONSTRAINT` separately). Heap rows are rewritten when needed; **`SchemaManager::rename_table`** keeps FK metadata aligned. Stress: concurrent child inserts under mutex (`engine_alter_fk_many_inserts_under_contention`). Full logical DDL redo in WAL remains future work.
6. **Operational clarity:** extend docs and smoke tests as behavior stabilizes (Docker stateful SQL smoke already covers constraints and session transactions).
   - *Partial:* engine tests cover catalog reopen and commit-log append; extend docs/smoke as behavior stabilizes.

This is a living list; adjust order as durability and recovery become blocking for real workloads.
