## Durability & crash recovery (SqlEngine WAL)

This document specifies the **current** durability and crash-recovery behavior of `SqlEngine` and
its write-ahead log (WAL) integration.

The goal is to remove ambiguity around:

- what exactly counts as a “commit”
- when changes are considered durable
- what to expect after a process/OS crash and subsequent restart

### Where this is implemented

- **SQL engine**: `src/network/sql_engine/mod.rs`
- **WAL integration** (BEGIN/COMMIT/ABORT + DML records + replay): `src/network/sql_engine_wal.rs`
- **Log record format**: `src/logging/log_record.rs`
- **Recovery manager** (used from `SqlEngine::open`): `src/logging/recovery.rs`
- **Checkpoint manager**: `src/logging/checkpoint.rs`
- **Commit log (minimal durability hook)**: `src/network/sql_commit_log.rs`

### Terms

- **WAL**: append-only log records stored under `data_dir/.rustdb/wal/*.log`.
- **LSN**: log sequence number (`u64`), monotonically increasing WAL record id.
- **Txn id**: `transaction_id` (`u64`) carried by transactional WAL records.
- **Commit point**: the moment after which a transaction must be treated as committed and must
  survive a restart (subject to the chosen fsync policy / durability mode).

### WAL/checkpoints and durability policy

WAL is **enabled by default**. Environment variables:

- `RUSTDB_DISABLE_WAL=1`: disable WAL entirely (no WAL recovery; crash behavior may reflect
  uncommitted changes because heap writes happen directly).
- `RUSTDB_DISABLE_CHECKPOINT=1`: disable checkpoints even if WAL is enabled.
- `RUSTDB_AUTO_CHECKPOINT=1`: enable automatic checkpoints (otherwise only manual
  `SqlEngine::checkpoint()`).
- `RUSTDB_CHECKPOINT_INTERVAL_SECS=<n>`: automatic checkpoint interval in seconds.
- `RUSTDB_FSYNC_COMMIT=1`: enable “synchronous commit” (commit points wait for fsync / `sync_all`
  where applicable). This switches `SqlEngineConfig::default()` to `DurabilityMode::Safe`; without
  it, the default for server/CLI is `Fast`.

### What is written to the WAL

Transactional records (all have a `transaction_id`):

- `TransactionBegin`
- `DataInsert`
- `DataUpdate`
- `DataDelete`
- `TransactionCommit`
- `TransactionAbort`

System/metadata records:

- `Checkpoint` / `CheckpointEnd` (emitted by `CheckpointManager`)
- `MetadataUpdate` (used as a marker after a successful `catalog.json` write)

Note: `MetadataUpdate(kind=catalog_json)` is **not** tied to a user transaction (no
`transaction_id`) and currently does **not** affect WAL replay; it exists primarily for
observability/diagnostics around catalog persistence ordering.

### DML inside an explicit transaction (BEGIN … COMMIT/ROLLBACK)

`SqlEngine` implements minimal session-level transactions:

- `BEGIN`: creates an `SqlTransaction` in `SessionContext` and writes WAL `TransactionBegin`
  (when WAL is enabled).
- DML (`INSERT/UPDATE/DELETE`) immediately modifies the heap (`PageManager`) and writes the
  corresponding WAL `Data*` record (when WAL is enabled and the session has an active
  transaction).
- After each DML statement, the engine calls `flush_dirty_pages()` on the page manager (this
  increases on-disk visibility even for uncommitted data).
- `ROLLBACK`: writes WAL `TransactionAbort`, applies the in-memory undo log, then forces
  `flush_all_page_managers()` so that “rolled back” heap writes do not remain visible after a
  restart.
- `COMMIT`: writes WAL `TransactionCommit` and appends a line to `data_dir/.rustdb/commits.log`.

### DML outside an explicit transaction (implicit auto-commit)

When the session is **not** inside an explicit transaction, each DML statement runs as an
**implicit short transaction**:

1. The engine creates an internal transaction id and writes `TransactionBegin`.
2. The statement executes and emits one or more `DataInsert` / `DataUpdate` / `DataDelete` WAL
   records under that transaction id.
3. The engine writes `TransactionCommit` for that transaction id and appends a commit marker to
   `commits.log`.

This makes standalone DML **WAL-logged and crash-recoverable**, even without an explicit
`BEGIN/COMMIT`.

### Commit point (current spec)

For both explicit transactions and implicit auto-commit DML, the commit point is:

1. `TransactionCommit` for the transaction id is appended to the WAL.
2. If `RUSTDB_FSYNC_COMMIT=1` / `DurabilityMode::Safe` is active, the WAL writer performs the
   synchronous commit (fsync / equivalent) and the commit marker line in `commits.log` is written
   with `sync_all()`.

Statement-level commit point for implicit auto-commit DML:

- The commit point is reached **after the statement finishes successfully**, when the engine has
  written the WAL commit record (and performed fsync in `Safe` mode). If the statement errors, the
  implicit transaction is aborted and must not become visible after restart.

Durability expectations by mode:

- **`DurabilityMode::Fast` (default for server/CLI)**: commits are recorded, but the engine does
  not wait for fsync at the commit point. A power loss / kernel crash can lose the most recent
  committed transactions that were still in OS buffers, even though they were “committed” from the
  client’s perspective.
- **`DurabilityMode::Safe` (`RUSTDB_FSYNC_COMMIT=1`)**: commit points wait for durability of the
  WAL commit record (and the `commits.log` marker). After `COMMIT` returns (or an implicit DML
  statement returns successfully), the committed effects should survive a crash consistent with the
  platform’s fsync semantics.

### Recovery / replay on `SqlEngine::open`

When WAL is enabled, `SqlEngine::open` performs:

1. `RecoveryManager::recover_database(wal_dir)` — a generic WAL recovery pass over the WAL
   directory (currently `quiet=true`, `enable_validation=false`).
2. `replay_wal_into_engine(state, wal_dir)`:
   - analyzes WAL and computes:
     - `redo`: all `Data*` operations for transactions that are `committed && !aborted`
     - `undo_per_tx`: `Data*` operations for “active” transactions (no commit and no abort), in
       reverse LSN order
   - applies REDO in increasing LSN order
   - applies UNDO per active transaction in reverse order (page-manager compensation)

Important: current replay is **page-oriented** (see `PageManager::apply_log_record_recovery`) and
assumes `record_offset` (`u16`) addresses a slot within a page.

### Guarantees and limitations (v1)

- **Guarantee (WAL ON)**: after restart, transactions without a commit record must not “leak” into
  the final state; their DML effects are compensated by UNDO during `open()`.
- **Guarantee (`RUSTDB_FSYNC_COMMIT=1`)**: the WAL commit marker and the commit line in
  `commits.log` are expected to be durable after returning from `COMMIT` / successful auto-commit
  DML.
- **Limitation**: DML heap writes may be persisted before `COMMIT` (and can be flushed after each
  statement). Correctness after restart is achieved **via UNDO**. This differs from a classic
  “WAL-first + no-force” design and should be kept in mind as the storage layer evolves.
- **Limitation**: DDL is currently forbidden inside an explicit transaction (see `README.md`), so
  DDL WAL semantics are not yet formalized as part of user transactions.

### Bench defer vs strict commit flush (TPC-C harness)

Throughput CI and the fair-compare harness use env presets in `scripts/tpcc_env_presets.sh`:

- **bench**: `RUSTDB_DEFER_HEAP_FLUSH_ON_COMMIT=1` (and related defer flags) — skips synchronous heap flush on explicit `COMMIT` when WAL is on; WAL + `commits.log` remain the durability path for the job.
- **strict**: all defer flags `0` — heap pages are flushed on commit (closer to PostgreSQL data-page persistence at commit time).

See [Fair TPC-C compare](tpcc-fair-compare.md) for validity gates, multi-run aggregation, and what ratio claims are allowed.

### Invariants (for tests / regression gates)

Recommended invariants to cover with tests:

1. **Crash before commit**: after `BEGIN; INSERT …; <crash>` and then `open()`, the inserted row is
   not visible.
2. **Crash after commit**: after `BEGIN; INSERT …; COMMIT; <crash>` and then `open()`, the inserted
   row is visible.
3. **Rollback durability**: after `BEGIN; INSERT …; ROLLBACK; <crash>` and then `open()`, the
   inserted row is not visible.
4. **Auto-commit crash behavior**: after `INSERT …; <crash>` and then `open()`, the inserted row is
   visible iff the implicit transaction reached its commit point.
5. **Idempotent replay**: reopening without new operations does not change contents (replay does
   not duplicate rows).
