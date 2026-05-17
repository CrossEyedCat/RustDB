//! Real SQL execution for the network [`crate::network::engine::EngineHandle`] boundary.
//!
//! Pipeline: parse → plan ([`QueryPlanner`]) → optimize ([`QueryOptimizer`]) → execute ([`QueryExecutor`]).
//! `SELECT` without `FROM` is evaluated from literal projections only.
//! `INSERT` / `UPDATE` / `DELETE` run against the heap file (`default.tbl`) using serialized [`crate::storage::tuple::Tuple`] rows.
//!
//! **Phase 6 — transactions & concurrency (minimal)**:
//! - `BEGIN` / `COMMIT` / `ROLLBACK` with an undo log for DML in the current [`SessionContext`].
//! - **Per-table storage locks**: each physical table name has a lazily allocated [`RwLock`] in
//!   `SqlEngineState::table_storage_locks`. `SELECT` / set operations take shared (`read`) locks on
//!   every referenced base table (sorted name order) for plan + optimize + execute; `INSERT ... VALUES`,
//!   `UPDATE`, and `DELETE` take an exclusive (`write`) lock on the target table only.
//! - A **global** `SqlEngineState::storage_access` still serializes DDL, `ROLLBACK`, and
//!   `INSERT ... SELECT` (mixed read/write on the same statement) against storage-wide invariants.
//! - Read committed baseline: each statement sees data committed before that statement began,
//!   excluding the current session’s own uncommitted writes which are already on the heap.
//! - Stronger isolation ([`crate::network::engine::SqlIsolationLevel::RepeatableRead`] /
//!   [`crate::network::engine::SqlIsolationLevel::Serializable`]) uses a global lock so at most one
//!   such transaction runs at a time (see `RUSTDB_DEFAULT_ISOLATION`).
//! - Optional structured WAL under `data_dir/.rustdb/wal` (`RUSTDB_DISABLE_WAL=1` to skip); recovery
//!   runs via [`crate::logging::recovery::RecoveryManager`] on open. When WAL is on, a
//!   [`crate::logging::checkpoint::CheckpointManager`] is attached (unless `RUSTDB_DISABLE_CHECKPOINT=1`);
//!   call [`SqlEngine::checkpoint`] for a manual checkpoint (flushes heaps + writes a checkpoint record).
//!   After each successful write of `catalog.json` (DDL), the WAL records a `MetadataUpdate` marker when WAL is on.
//! - With WAL enabled, you may set **`RUSTDB_DEFER_HEAP_FLUSH_AFTER_DML=1`** to skip `flush_dirty_pages`
//!   after successful implicit auto-commit DML (higher throughput; heap catches up at checkpoint /
//!   `COMMIT`). Explicit `BEGIN … COMMIT` transactions defer per-statement heap flush and flush only
//!   heap page managers for tables touched by DML on `COMMIT` / `ROLLBACK` (see
//!   `SqlTransaction::touched_tables`).
//! - WAL group commit knobs: `RUSTDB_GROUP_COMMIT_ENABLED`, `RUSTDB_GROUP_COMMIT_INTERVAL_MS`,
//!   `RUSTDB_GROUP_COMMIT_MAX_BATCH`, `RUSTDB_FORCE_FLUSH_IMMEDIATELY` (see `crate::network::sql_engine_wal`).
//! - Implicit auto-commit DML still flushes after each statement by default so standalone heap files
//!   stay coherent for tests and tooling that reopen without relying on WAL replay ordering.
//! - DDL (`CREATE` / `DROP` / `ALTER`) is rejected while a transaction is open.
//!
//! **SQL plan cache:** normalized SQL text maps to a validated, optimized [`ExecutionPlan`] for the
//! current catalog/index epoch (LRU, shared by `ExecuteScript` / TPC-C and single-statement DML).
//! Cleared on catalog persist and optimizer rebuild.
//!
//! **Index-backed DML:** `UPDATE` / `DELETE` with an index lookup acquire per-row write locks
//! instead of a whole-table storage lock when possible (see [`RowLockManager`]).
//!
//! **Index-only SELECT:** single-table `SELECT` with a full index equality seek skips the table
//! storage read lock (order-status style). Range predicates (e.g. stock-level `s_qty < 20`) still
//! take the table read lock.
//!
//! **Profiling:** set `RUSTDB_SQL_PHASE_LOG=1` to emit `tracing` events on target `rustdb::sql_phases`
//! (parse latency, per-table `lock_wait_us` on storage lock acquire, `UPDATE`/`DELETE` scan vs row loop).
//! Use `RUST_LOG=rustdb::sql_phases=info` to filter.

use crate::catalog::schema::{
    CheckConstraint, ForeignKeyConstraintDef, SchemaManager, TableSchema, UniqueConstraintDef,
};
use crate::common::types::{ColumnValue, DataType, RecordId};
use crate::common::DurabilityMode;
use crate::common::Error as DbError;
use crate::executor::operators::{
    eval_predicate_expression, eval_scalar_expression, ScanOperatorFactory,
};
use crate::executor::QueryExecutor;
use crate::network::engine::{
    engine_error_code, EngineError, EngineHandle, EngineOutput, PendingIndexInsert, SessionContext,
    SqlIsolationLevel, SqlTransaction, UndoEntry,
};
use crate::network::sql_commit_log;
use crate::network::sql_constraints::{self, ConstraintRuntime};
use crate::parser::ast::{
    AlterTableOperation, AlterTableStatement, BinaryOperator, ColumnConstraint,
    CreateIndexStatement, CreateTableStatement, DataType as SqlDataType, DeleteStatement,
    DropTableStatement, Expression, FromClause, InList, InsertStatement, InsertValues, Literal,
    SelectItem, SelectStatement, TableConstraint, TableReference, UpdateStatement,
};
use crate::parser::{SqlParser, SqlStatement};
use crate::planner::planner::IndexScanNode;
use crate::planner::{ExecutionPlan, PlanNode, QueryOptimizer, QueryPlanner};
use crate::storage::index_registry::IndexRegistry;
use crate::storage::page_manager::{PageManager, PageManagerConfig};
use crate::storage::row_locks::RowLockManager;
use crate::storage::tuple::Tuple;
use crate::Row;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;
use tracing::{info, info_span};

mod alter_table_ops;
mod tpcc_native;

/// Global lock: at most one [`SqlIsolationLevel::RepeatableRead`] or [`SqlIsolationLevel::Serializable`]
/// engine transaction across all sessions.
static STRONG_ISO_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

// Test-only crash injection (enabled via env vars) to deterministically simulate
// "process crash mid-statement" while keeping the test runner alive.
//
// - `RUSTDB_SIMULATE_CRASH_AFTER_DML_OPS=<n>`: panic on the n-th DML row-level op
//   (insert/update/delete tuple operation) in the current process.
// - `RUSTDB_SIMULATE_CRASH_POINT=<point>` (optional): only crash at a matching point
//   (e.g. `insert_row`, `update_row`, `delete_row`).
//
// Note: env vars are read dynamically so integration tests can toggle them per-test.
static SIM_CRASH_DML_OP_COUNTER: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn maybe_simulate_dml_crash(point: &'static str) {
    let target = match std::env::var("RUSTDB_SIMULATE_CRASH_AFTER_DML_OPS") {
        Ok(v) => v.parse::<u64>().ok().filter(|n| *n > 0),
        Err(_) => {
            SIM_CRASH_DML_OP_COUNTER.store(0, Ordering::SeqCst);
            None
        }
    };
    let Some(target) = target else {
        return;
    };
    if let Ok(p) = std::env::var("RUSTDB_SIMULATE_CRASH_POINT") {
        if p != point {
            return;
        }
    }
    let now = SIM_CRASH_DML_OP_COUNTER.fetch_add(1, Ordering::SeqCst) + 1;
    if now == target {
        panic!("simulated crash at {point} (op #{now})");
    }
}

/// Engine backed by the in-process planner, optimizer, and executor (single default table file per data directory).
#[derive(Clone)]
pub struct SqlEngine {
    state: Arc<SqlEngineState>,
}

/// Configuration for `SqlEngine` opening and durability behavior.
///
/// Prefer struct-update construction (`SqlEngineConfig { wal_enabled: false, ..Default::default() }`)
/// so future fields don't break callers.
#[derive(Debug, Clone)]
pub struct SqlEngineConfig {
    /// Enable structured WAL + recovery.
    pub wal_enabled: bool,
    /// Durability policy (fsync on commit points, etc.).
    pub durability: DurabilityMode,
    /// Wire a [`crate::logging::checkpoint::CheckpointManager`] to the engine when WAL is enabled.
    ///
    /// Disabling skips checkpoint setup entirely (manual `SqlEngine::checkpoint` will then error).
    /// The legacy env var `RUSTDB_DISABLE_CHECKPOINT` still wins over an enabled value.
    pub checkpoints_enabled: bool,
}

impl Default for SqlEngineConfig {
    fn default() -> Self {
        // Server / CLI default: favor throughput unless explicitly overridden.
        //
        // Set `RUSTDB_FSYNC_COMMIT=1` to opt into safe durability (commit points wait for fsync).
        let durability = if std::env::var_os("RUSTDB_FSYNC_COMMIT").is_some() {
            DurabilityMode::Safe
        } else {
            DurabilityMode::Fast
        };
        Self {
            wal_enabled: true,
            durability,
            checkpoints_enabled: true,
        }
    }
}

pub(crate) struct SqlEngineState {
    data_dir: PathBuf,
    durability: DurabilityMode,
    pub(crate) default_page_manager: Arc<Mutex<PageManager>>,
    pub(crate) table_page_managers: Arc<Mutex<HashMap<String, Arc<Mutex<PageManager>>>>>,
    /// Monotonic id assigned to inserted [`Tuple`] rows (persisted in tuple bytes).
    next_tuple_id: AtomicU64,
    planner: QueryPlanner,
    optimizer: Mutex<QueryOptimizer>,
    executor: QueryExecutor,
    /// Secondary indexes (definitions persisted in catalog; btree rebuilt on open).
    index_registry: Arc<RwLock<IndexRegistry>>,
    /// Cache for deterministic `SELECT` queries without `FROM` (literal projections only).
    ///
    /// These queries are common in benchmarks (`SELECT 1`) and are safe to memoize.
    select_no_from_cache: Mutex<HashMap<String, EngineOutput>>,
    /// LRU of normalized SQL → optimized plans (DML validation + read path; invalidated on DDL).
    dml_plan_validation_cache: Mutex<DmlPlanValidationCache>,
    pub(crate) catalog: Mutex<SchemaManager>,
    constraint_runtime: Mutex<ConstraintRuntime>,
    /// Serializes storage-mutating statements vs table scans (`SELECT` with `FROM`).
    storage_access: RwLock<()>,
    /// Per physical table: coordinates concurrent `SELECT` (shared) vs DML writers (exclusive).
    table_storage_locks: Mutex<HashMap<String, Arc<RwLock<()>>>>,
    /// Per-row locks for index-backed single-row UPDATE/DELETE.
    row_locks: RowLockManager,
    /// Structured WAL (`src/logging`); disabled when `RUSTDB_DISABLE_WAL` is set.
    wal: Option<crate::network::sql_engine_wal::SqlEngineWal>,
    /// Secondary-index column names per table (refreshed on `CREATE INDEX` / open).
    index_columns_by_table: Mutex<HashMap<String, Arc<Vec<String>>>>,
}

impl SqlEngine {
    /// Opens or creates storage under `data_dir` (directory is created if missing).
    /// Uses one heap file `default.tbl` for table scans (see [`ScanOperatorFactory`]).
    pub fn open(data_dir: PathBuf) -> Result<Self, DbError> {
        Self::open_with_config(data_dir, SqlEngineConfig::default())
    }

    /// Open with an explicit config (embedded-friendly).
    pub fn open_with_config(data_dir: PathBuf, config: SqlEngineConfig) -> Result<Self, DbError> {
        std::fs::create_dir_all(&data_dir)?;
        let wal_dir = data_dir.join(".rustdb").join("wal");
        // `open_with_config` is intended to be deterministic (embedded/tests). If callers
        // enable WAL explicitly, do not let global env vars silently disable it.
        let wal = if config.wal_enabled {
            std::fs::create_dir_all(&wal_dir)?;
            Some(crate::network::sql_engine_wal::SqlEngineWal::open(
                &wal_dir,
                config.durability.fsync_on_commit(),
            )?)
        } else {
            None
        };
        let catalog = match SchemaManager::try_load_catalog_from_data_dir(&data_dir)? {
            Some(c) => c,
            None => SchemaManager::new()?,
        };
        let pm = match PageManager::open(data_dir.clone(), "default", PageManagerConfig::default())
        {
            Ok(pm) => pm,
            Err(_) => PageManager::new(data_dir.clone(), "default", PageManagerConfig::default())?,
        };
        let pm = Arc::new(Mutex::new(pm));
        let table_pms: Arc<Mutex<HashMap<String, Arc<Mutex<PageManager>>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let index_registry = Arc::new(RwLock::new(IndexRegistry::new()));
        let factory = Arc::new(ScanOperatorFactory::with_tables(
            pm.clone(),
            table_pms.clone(),
            data_dir.clone(),
            Some(index_registry.clone()),
        ));
        let executor = QueryExecutor::new(factory)?;
        let state = Arc::new(SqlEngineState {
            data_dir,
            durability: config.durability,
            default_page_manager: pm,
            table_page_managers: table_pms,
            next_tuple_id: AtomicU64::new(1),
            planner: QueryPlanner::new()?,
            optimizer: Mutex::new(QueryOptimizer::new()?),
            executor,
            index_registry,
            select_no_from_cache: Mutex::new(HashMap::new()),
            dml_plan_validation_cache: Mutex::new(DmlPlanValidationCache::default()),
            catalog: Mutex::new(catalog),
            constraint_runtime: Mutex::new(ConstraintRuntime::new()),
            storage_access: RwLock::new(()),
            table_storage_locks: Mutex::new(HashMap::new()),
            row_locks: RowLockManager::new(),
            wal,
            index_columns_by_table: Mutex::new(HashMap::new()),
        });
        if state.wal.is_some() && wal_dir.is_dir() {
            crate::network::sql_engine_wal::replay_wal_into_engine(
                state.as_ref(),
                &wal_dir,
                state.wal.as_ref(),
            )
            .map_err(|e| DbError::database(format!("WAL replay on open: {e}")))?;
        }
        if let Some(ref wal) = state.wal {
            if config.checkpoints_enabled {
                wal.setup_checkpoint(state.clone())
                    .map_err(|e| DbError::database(format!("checkpoint setup on open: {e}")))?;
            }
        }
        rebuild_all_constraint_runtime(state.as_ref()).map_err(|e| {
            DbError::database(format!("catalog/constraint rebuild on open: {}", e.message))
        })?;
        rebuild_secondary_indexes_from_catalog(state.as_ref()).map_err(|e| {
            DbError::database(format!("secondary index rebuild on open: {}", e.message))
        })?;
        rebuild_index_columns_cache(state.as_ref()).map_err(|e| {
            DbError::database(format!("index columns cache on open: {}", e.message))
        })?;
        Ok(Self { state })
    }

    /// Runs a manual checkpoint: flush all heap page managers and append a checkpoint record to the WAL.
    pub fn checkpoint(&self) -> Result<(), DbError> {
        let wal = self
            .state
            .wal
            .as_ref()
            .ok_or_else(|| DbError::database("WAL disabled; checkpoint unavailable"))?;
        wal.checkpoint()
    }

    /// Active durability policy for this engine instance.
    pub fn durability(&self) -> DurabilityMode {
        self.state.durability
    }

    /// Whether structured WAL + recovery is wired for this engine.
    pub fn wal_enabled(&self) -> bool {
        self.state.wal.is_some()
    }

    /// Root data directory backing this engine.
    pub fn data_dir(&self) -> &std::path::Path {
        &self.state.data_dir
    }

    #[cfg(test)]
    pub(crate) fn state_for_test(&self) -> &SqlEngineState {
        self.state.as_ref()
    }

    /// Checkpoint statistics (returns `None` when WAL or checkpoints are disabled).
    pub fn checkpoint_statistics(
        &self,
    ) -> Option<crate::logging::checkpoint::CheckpointStatistics> {
        let wal = self.state.wal.as_ref()?;
        wal.checkpoint_statistics()
    }

    pub(crate) fn execute_sql_inner(
        state: &SqlEngineState,
        sql: &str,
        ctx: &mut SessionContext,
    ) -> Result<EngineOutput, EngineError> {
        let span = info_span!(
            "sql.execute",
            sql_len = sql.len(),
            sql = %summarize_sql(sql)
        );
        let _g = span.enter();

        let parse_clock = sql_phase_log_enabled().then(Instant::now);

        // Hot path: deterministic "SELECT <literals>" queries without FROM can be memoized by SQL.
        // This avoids repeated parse/AST construction in tight loops (e.g. select_literal bench).
        if likely_select_without_from(sql) {
            if let Ok(g) = state.select_no_from_cache.lock() {
                if let Some(cached) = g.get(sql) {
                    return Ok(cached.clone());
                }
            }
        }

        let mut parser = SqlParser::new(sql).map_err(map_db_err)?;
        let stmts = {
            let s = info_span!("sql.parse");
            let _sg = s.enter();
            parser.parse_multiple().map_err(map_db_err)?
        };
        if let Some(t0) = parse_clock {
            info!(
                target: "rustdb::sql_phases",
                parse_us = t0.elapsed().as_micros(),
                sql = %summarize_sql(sql),
                "sql_parse"
            );
        }
        if stmts.is_empty() {
            return Err(EngineError::new(
                engine_error_code::PROTOCOL,
                "empty SQL statement",
            ));
        }
        if stmts.len() > 1 {
            return Err(EngineError::new(
                engine_error_code::PROTOCOL,
                "only one SQL statement per request is supported",
            ));
        }
        let stmt = &stmts[0];
        match stmt {
            SqlStatement::Select(sel) if sel.from.is_none() => {
                let out = eval_select_without_from(sel)?;
                if likely_select_without_from(sql) {
                    if let Ok(mut g) = state.select_no_from_cache.lock() {
                        g.insert(sql.to_string(), out.clone());
                    }
                }
                Ok(out)
            }
            SqlStatement::Select(_) | SqlStatement::SetOperation(_) => {
                let table_names = collect_physical_tables_for_read_stmt(stmt);
                let optimized_plan = {
                    let s = info_span!("sql.plan");
                    let _sg = s.enter();
                    plan_and_optimize_read(state, sql, stmt)?
                };
                let skip_read =
                    select_skip_table_read_lock_tables(state, &table_names, &optimized_plan.root);
                if table_names.is_empty() {
                    let _storage = state
                        .storage_access
                        .read()
                        .map_err(|_| lock_poisoned_engine())?;
                    let rows = {
                        let s = info_span!("sql.exec_plan");
                        let _sg = s.enter();
                        state
                            .executor
                            .execute(&optimized_plan)
                            .map_err(map_db_err)?
                    };
                    {
                        let s = info_span!("sql.encode_rows", row_count = rows.len());
                        let _eg = s.enter();
                        rows_to_engine_output(rows)
                    }
                } else {
                    let locks: Vec<Arc<RwLock<()>>> = table_names
                        .iter()
                        .filter(|n| !skip_read.contains(*n))
                        .map(|n| table_storage_lock_arc(state, n))
                        .collect::<Result<_, _>>()?;
                    let locked_tables: Vec<&String> = table_names
                        .iter()
                        .filter(|n| !skip_read.contains(*n))
                        .collect();
                    let _table_reads: Vec<std::sync::RwLockReadGuard<'_, ()>> = locks
                        .iter()
                        .zip(locked_tables.iter())
                        .map(|(l, table)| acquire_table_storage_read_lock(l, table))
                        .collect::<Result<_, _>>()?;
                    let rows = {
                        let s = info_span!("sql.exec_plan");
                        let _sg = s.enter();
                        state
                            .executor
                            .execute(&optimized_plan)
                            .map_err(map_db_err)?
                    };
                    {
                        let s = info_span!("sql.encode_rows", row_count = rows.len());
                        let _eg = s.enter();
                        rows_to_engine_output(rows)
                    }
                }
            }
            SqlStatement::Insert(ins) => {
                let s = info_span!("sql.insert", table = %ins.table);
                let _sg = s.enter();
                match &ins.values {
                    InsertValues::Select(_) => {
                        let _storage = state
                            .storage_access
                            .write()
                            .map_err(|_| lock_poisoned_engine())?;
                        execute_dml_autocommit(state, ctx, |state, ctx| {
                            execute_insert(state, ctx, sql, stmt, ins)
                        })
                    }
                    InsertValues::Values(_) => {
                        // Heap writes use per-page latches; tables with a PRIMARY KEY still take a
                        // table write lock so concurrent inserts cannot race on the PK map.
                        let table_lock = if table_has_primary_key(state, &ins.table) {
                            Some((
                                table_storage_lock_arc(state, &ins.table)?,
                                ins.table.clone(),
                            ))
                        } else {
                            None
                        };
                        execute_dml_autocommit(state, ctx, |state, ctx| {
                            let _guard = table_lock
                                .as_ref()
                                .map(|(l, t)| acquire_table_storage_write_lock(l, t))
                                .transpose()?;
                            execute_insert(state, ctx, sql, stmt, ins)
                        })
                    }
                }
            }
            SqlStatement::Update(upd) => {
                let s = info_span!("sql.update", table = %upd.table);
                let _sg = s.enter();
                with_dml_write_lock(
                    state,
                    &upd.table,
                    upd.where_clause.as_ref(),
                    ctx.skip_dml_storage_lock,
                    || {
                        execute_dml_autocommit(state, ctx, |state, ctx| {
                            execute_update(state, ctx, sql, stmt, upd)
                        })
                    },
                )
            }
            SqlStatement::Delete(del) => {
                let s = info_span!("sql.delete", table = %del.table);
                let _sg = s.enter();
                with_dml_write_lock(
                    state,
                    &del.table,
                    del.where_clause.as_ref(),
                    ctx.skip_dml_storage_lock,
                    || {
                        execute_dml_autocommit(state, ctx, |state, ctx| {
                            execute_delete(state, ctx, sql, stmt, del)
                        })
                    },
                )
            }
            SqlStatement::CreateIndex(ci) => {
                let s = info_span!(
                    "sql.create_index",
                    table = %ci.table_name,
                    index = %ci.index_name
                );
                let _sg = s.enter();
                let _storage = state
                    .storage_access
                    .write()
                    .map_err(|_| lock_poisoned_engine())?;
                execute_create_index(state, ctx, ci)
            }
            SqlStatement::CreateTable(ct) => {
                let s = info_span!("sql.create_table", table = %ct.table_name);
                let _sg = s.enter();
                let _storage = state
                    .storage_access
                    .write()
                    .map_err(|_| lock_poisoned_engine())?;
                execute_create_table(state, ctx, ct)
            }
            SqlStatement::DropTable(dt) => {
                let s = info_span!("sql.drop_table", table = %dt.table_name);
                let _sg = s.enter();
                let _storage = state
                    .storage_access
                    .write()
                    .map_err(|_| lock_poisoned_engine())?;
                execute_drop_table(state, ctx, dt)
            }
            SqlStatement::AlterTable(alt) => {
                let s = info_span!("sql.alter_table", table = %alt.table_name);
                let _sg = s.enter();
                let _storage = state
                    .storage_access
                    .write()
                    .map_err(|_| lock_poisoned_engine())?;
                execute_alter_table(state, ctx, alt)
            }
            SqlStatement::BeginTransaction => begin_transaction(state, ctx),
            SqlStatement::CommitTransaction => commit_transaction(state, ctx),
            SqlStatement::RollbackTransaction => {
                let _storage = state
                    .storage_access
                    .write()
                    .map_err(|_| lock_poisoned_engine())?;
                rollback_transaction(state, ctx)
            }
            _ => Err(EngineError::new(
                engine_error_code::UNSUPPORTED_SQL,
                "this SQL statement type is not supported by the server engine yet",
            )),
        }
    }
}

fn likely_select_without_from(sql: &str) -> bool {
    // Cheap heuristic: `SELECT` prefix and no obvious `FROM`, `;`, or DML keywords.
    // If this returns false we just skip caching; correctness never depends on it.
    let s = sql.trim_start();
    if s.len() < 6 {
        return false;
    }
    let upper = s.get(..s.len().min(64)).unwrap_or(s).to_ascii_uppercase();
    if !upper.starts_with("SELECT") {
        return false;
    }
    // We don't try to memoize multi-statement or complex queries.
    if s.contains(';') {
        return false;
    }
    // FROM anywhere means it's not our ultra-fast literal-only target.
    if upper.contains(" FROM ") {
        return false;
    }
    true
}

fn summarize_sql(sql: &str) -> String {
    let s = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    const MAX: usize = 160;
    if s.len() <= MAX {
        return s;
    }
    format!("{}…", &s[..MAX])
}

impl EngineHandle for SqlEngine {
    fn execute_sql(
        &self,
        sql: &str,
        ctx: &mut SessionContext,
    ) -> Result<EngineOutput, EngineError> {
        Self::execute_sql_inner(self.state.as_ref(), sql, ctx)
    }

    fn execute_tpcc(
        &self,
        kind: u8,
        seed: u64,
        global_txn_id: u64,
        ctx: &mut SessionContext,
    ) -> Result<EngineOutput, EngineError> {
        tpcc_native::execute_tpcc(self.state.as_ref(), kind, seed, global_txn_id, ctx)
    }

    fn supports_select_no_from_wire_cache(&self) -> bool {
        true
    }
}

fn db_err_is_record_not_found(e: &DbError) -> bool {
    e.to_string().contains("Record not found")
}

fn map_db_err(e: DbError) -> EngineError {
    EngineError::new(engine_error_code::INTERNAL, e.to_string())
}

fn heap_delete_idempotent(pm: &mut PageManager, rid: RecordId) -> Result<bool, EngineError> {
    match pm.delete(rid) {
        Ok(_) => Ok(true),
        Err(e) if db_err_is_record_not_found(&e) => Ok(false),
        Err(e) => Err(map_db_err(e)),
    }
}

/// Microseconds spent in WAL `log_data_insert` for a TPC-C heap insert.
pub(crate) struct TpccInsertTimings {
    pub wal_us: u64,
    pub index_us: u64,
}

/// When true (default), native TPC-C heap inserts defer secondary-index sync until `COMMIT`.
pub(crate) fn tpcc_defer_index_sync_enabled() -> bool {
    match std::env::var("RUSTDB_TPCC_DEFER_INDEX_SYNC") {
        Ok(s) if s == "0" || s.eq_ignore_ascii_case("false") => false,
        Ok(_) => true,
        Err(_) => true,
    }
}

/// TPC-C heap insert without constraint-runtime work (seed tables have no PK/FK).
pub(crate) fn insert_row_tuple_tpcc(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
    table: &str,
    tuple: Tuple,
) -> Result<(), EngineError> {
    if table_has_primary_key(state, table) {
        return insert_row_tuple(state, ctx, table, tuple);
    }
    if tpcc_defer_index_sync_enabled() {
        let _ = insert_row_tuple_tpcc_deferred(state, ctx, table, tuple)?;
        return Ok(());
    }
    let timings = insert_row_tuple_tpcc_immediate(state, ctx, table, tuple)?;
    let _ = timings;
    Ok(())
}

/// Heap + WAL + undo; secondary indexes applied at `COMMIT` (see [`apply_pending_index_inserts`]).
pub(crate) fn insert_row_tuple_tpcc_deferred(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
    table: &str,
    tuple: Tuple,
) -> Result<TpccInsertTimings, EngineError> {
    if table_has_primary_key(state, table) {
        insert_row_tuple(state, ctx, table, tuple)?;
        return Ok(TpccInsertTimings {
            wal_us: 0,
            index_us: 0,
        });
    }
    record_touched_table(ctx, table);
    ctx.tpcc_row_bytes_buf = tuple.to_bytes().map_err(map_db_err)?;
    let pm_for_table = table_page_manager_cached(state, ctx, table)?;
    let mut pm = pm_for_table.lock().map_err(|_| lock_poisoned_engine())?;
    let ins = pm.insert(&ctx.tpcc_row_bytes_buf).map_err(map_db_err)?;
    maybe_simulate_dml_crash("insert_row");
    let mut payload = std::mem::take(&mut ctx.tpcc_row_bytes_buf);
    let wal_clock = state.wal.is_some().then(Instant::now);
    if let Some(tx) = ctx.transaction.as_mut() {
        if let Some(ref wal) = state.wal {
            let page_id = (ins.record_id >> 32) as u64;
            let off = (ins.record_id & 0xffff_ffff) as u32;
            let record_offset: u16 = off.try_into().map_err(|_| {
                EngineError::new(
                    engine_error_code::INTERNAL,
                    "record offset too large for WAL",
                )
            })?;
            wal.log_data_insert(
                tx,
                pm.file_id(),
                page_id,
                record_offset,
                &payload,
            )?;
        }
    }
    let column_map = tpcc_pending_index_column_map(state, ctx, table, &tuple)?;
    if let Some(tx) = ctx.transaction.as_mut() {
        tx.pending_index_inserts.push(PendingIndexInsert {
            table: table.to_string(),
            rid: ins.record_id,
            column_map,
        });
    } else {
        sync_index_after_insert(state, table, ins.record_id, &tuple)?;
    }
    let wal_us = wal_clock
        .map(|t0| t0.elapsed().as_micros() as u64)
        .unwrap_or(0);
    push_undo(
        ctx,
        UndoEntry::Insert {
            table: table.to_string(),
            rid: ins.record_id,
            payload,
        },
    );
    Ok(TpccInsertTimings {
        wal_us,
        index_us: 0,
    })
}

fn insert_row_tuple_tpcc_immediate(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
    table: &str,
    tuple: Tuple,
) -> Result<TpccInsertTimings, EngineError> {
    record_touched_table(ctx, table);
    let bytes = tuple.to_bytes().map_err(map_db_err)?;
    let pm_for_table = table_page_manager(state, table)?;
    let mut pm = pm_for_table.lock().map_err(|_| lock_poisoned_engine())?;
    let ins = pm.insert(&bytes).map_err(map_db_err)?;
    maybe_simulate_dml_crash("insert_row");
    let wal_clock = state.wal.is_some().then(Instant::now);
    if let Some(tx) = ctx.transaction.as_mut() {
        if let Some(ref wal) = state.wal {
            let page_id = (ins.record_id >> 32) as u64;
            let off = (ins.record_id & 0xffff_ffff) as u32;
            let record_offset: u16 = off.try_into().map_err(|_| {
                EngineError::new(
                    engine_error_code::INTERNAL,
                    "record offset too large for WAL",
                )
            })?;
            wal.log_data_insert(tx, pm.file_id(), page_id, record_offset, &bytes)?;
        }
    }
    let wal_us = wal_clock
        .map(|t0| t0.elapsed().as_micros() as u64)
        .unwrap_or(0);
    let idx_clock = Instant::now();
    sync_index_after_insert(state, table, ins.record_id, &tuple)?;
    let index_us = idx_clock.elapsed().as_micros() as u64;
    push_undo(
        ctx,
        UndoEntry::Insert {
            table: table.to_string(),
            rid: ins.record_id,
            payload: bytes,
        },
    );
    Ok(TpccInsertTimings { wal_us, index_us })
}

fn apply_pending_index_inserts(
    state: &SqlEngineState,
    pending: &mut Vec<PendingIndexInsert>,
) -> Result<(), EngineError> {
    if pending.is_empty() {
        return Ok(());
    }
    let ops = std::mem::take(pending);
    let mut ir = state
        .index_registry
        .write()
        .map_err(|_| lock_poisoned_engine())?;
    for op in &ops {
        ir.insert_into_indexes(&op.table, op.rid, &op.column_map)
            .map_err(|e| {
                EngineError::new(engine_error_code::INTERNAL, format!("index insert: {e}"))
            })?;
    }
    Ok(())
}

pub(crate) fn log_execute_tpcc_phase(kind: u8, phase: &'static str, elapsed_us: u64) {
    if !sql_phase_log_enabled() {
        return;
    }
    info!(
        target: "rustdb::sql_phases",
        kind,
        phase,
        us = elapsed_us,
        "sql.execute_tpcc_phase"
    );
}

fn lock_poisoned_engine() -> EngineError {
    EngineError::new(engine_error_code::INTERNAL, "storage lock poisoned")
}

/// When set (non-empty, not `0`/`false`), emit `tracing` on target `rustdb::sql_phases` for statement timing.
pub(crate) fn sql_phase_log_enabled() -> bool {
    match std::env::var("RUSTDB_SQL_PHASE_LOG") {
        Ok(s) if s == "0" || s.eq_ignore_ascii_case("false") => false,
        Ok(_) => true,
        Err(_) => false,
    }
}

/// Flush dirty heap pages after successful DML.
///
/// Skips per-statement flush inside an explicit `BEGIN … COMMIT` transaction (heap is flushed on
/// `COMMIT` via [`crate::network::sql_engine_wal::flush_page_managers_for_tables`]). Implicit auto-commit
/// DML still flushes after each statement unless `RUSTDB_DEFER_HEAP_FLUSH_AFTER_DML` is set (WAL on).
fn flush_heap_after_dml_success(
    state: &SqlEngineState,
    ctx: &SessionContext,
    pm: &mut PageManager,
) -> Result<(), EngineError> {
    let in_explicit_txn = ctx
        .transaction
        .as_ref()
        .is_some_and(|tx| !tx.implicit_autocommit);
    if in_explicit_txn {
        return Ok(());
    }
    let defer = state.wal.is_some()
        && std::env::var_os("RUSTDB_DEFER_HEAP_FLUSH_AFTER_DML").is_some_and(|v| v != "0");
    if !defer {
        pm.flush_dirty_pages().map_err(map_db_err)?;
    }
    Ok(())
}

/// `expr` must use only shapes supported by [`match_where_tuple`] (for pushdown into [`PageManager::select`]).
fn validate_dml_where_structure(expr: &Expression) -> Result<(), EngineError> {
    match expr {
        Expression::Literal(Literal::Boolean(_)) => Ok(()),
        Expression::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            validate_dml_where_structure(left)?;
            validate_dml_where_structure(right)
        }
        Expression::BinaryOp {
            left,
            op: BinaryOperator::Equal,
            right,
        } => {
            let ok = (column_name_expr(left).is_some() && value_expr(right).is_some())
                || (column_name_expr(right).is_some() && value_expr(left).is_some());
            if ok {
                Ok(())
            } else {
                Err(EngineError::new(
                    engine_error_code::UNSUPPORTED_SQL,
                    "WHERE for UPDATE/DELETE supports only column = literal (and AND)",
                ))
            }
        }
        _ => Err(EngineError::new(
            engine_error_code::UNSUPPORTED_SQL,
            "WHERE for UPDATE/DELETE supports only column = literal (and AND)",
        )),
    }
}

fn table_storage_lock_arc(
    state: &SqlEngineState,
    table: &str,
) -> Result<Arc<RwLock<()>>, EngineError> {
    let mut map = state
        .table_storage_locks
        .lock()
        .map_err(|_| lock_poisoned_engine())?;
    Ok(map
        .entry(table.to_string())
        .or_insert_with(|| Arc::new(RwLock::new(())))
        .clone())
}

fn acquire_table_storage_read_lock<'a>(
    lock: &'a RwLock<()>,
    table: &str,
) -> Result<std::sync::RwLockReadGuard<'a, ()>, EngineError> {
    let wait_clock = sql_phase_log_enabled().then(Instant::now);
    let guard = lock.read().map_err(|_| lock_poisoned_engine())?;
    if let Some(t0) = wait_clock {
        info!(
            target: "rustdb::sql_phases",
            table = %table,
            mode = "read",
            lock_wait_us = t0.elapsed().as_micros() as u64,
            "table_storage_lock"
        );
    }
    Ok(guard)
}

fn acquire_table_storage_write_lock<'a>(
    lock: &'a RwLock<()>,
    table: &str,
) -> Result<std::sync::RwLockWriteGuard<'a, ()>, EngineError> {
    let wait_clock = sql_phase_log_enabled().then(Instant::now);
    let guard = lock.write().map_err(|_| lock_poisoned_engine())?;
    if let Some(t0) = wait_clock {
        info!(
            target: "rustdb::sql_phases",
            table = %table,
            mode = "write",
            lock_wait_us = t0.elapsed().as_micros() as u64,
            "table_storage_lock"
        );
    }
    Ok(guard)
}

/// Resolves target row ids via index before DML locking (UPDATE/DELETE fast path).
///
/// Returns `(rids, index_exact)` where `index_exact` is true when the `WHERE` equalities
/// form a full key on a registered index (see [`IndexRegistry::lookup_record_ids_by_equalities`]).
fn resolve_dml_row_lock_rids(
    state: &SqlEngineState,
    table: &str,
    where_expr: &Expression,
) -> Result<Option<(Vec<RecordId>, bool)>, EngineError> {
    let lit_eq = extract_dml_where_equalities(where_expr);
    if lit_eq.is_empty() {
        return Ok(None);
    }
    let equalities: HashMap<String, String> = lit_eq
        .iter()
        .map(|(col, lit)| (col.clone(), literal_to_index_key_string(lit)))
        .collect();
    let ir = state
        .index_registry
        .read()
        .map_err(|_| lock_poisoned_engine())?;
    let Some((rids, index_exact)) = ir
        .lookup_record_ids_by_equalities(table, &equalities)
        .map_err(|e| EngineError::new(engine_error_code::INTERNAL, format!("index lookup: {e}")))?
    else {
        return Ok(None);
    };
    Ok(Some((rids, index_exact)))
}

fn log_dml_lock_path(
    table: &str,
    lock_path: &'static str,
    reason: Option<&str>,
    row_count: Option<usize>,
) {
    if !sql_phase_log_enabled() {
        return;
    }
    match (reason, row_count) {
        (Some(reason), Some(n)) => info!(
            target: "rustdb::sql_phases",
            table = %table,
            lock_path,
            reason,
            row_count = n,
            "sql.dml.lock_path"
        ),
        (Some(reason), None) => info!(
            target: "rustdb::sql_phases",
            table = %table,
            lock_path,
            reason,
            "sql.dml.lock_path"
        ),
        (None, Some(n)) => info!(
            target: "rustdb::sql_phases",
            table = %table,
            lock_path,
            row_count = n,
            "sql.dml.lock_path"
        ),
        (None, None) => info!(
            target: "rustdb::sql_phases",
            table = %table,
            lock_path,
            "sql.dml.lock_path"
        ),
    }
}

fn with_dml_write_lock<T>(
    state: &SqlEngineState,
    table: &str,
    where_clause: Option<&Expression>,
    skip_storage_lock: bool,
    f: impl FnOnce() -> Result<T, EngineError>,
) -> Result<T, EngineError> {
    if skip_storage_lock {
        log_dml_lock_path(table, "skip", Some("skip_dml_storage_lock"), None);
        return f();
    }
    if let Some(expr) = where_clause {
        if let Some((rids, index_exact)) = resolve_dml_row_lock_rids(state, table, expr)? {
            if index_exact {
                if rids.is_empty() {
                    log_dml_lock_path(table, "skip", Some("index_exact_no_rows"), None);
                    return f();
                }
                log_dml_lock_path(table, "row", None, Some(rids.len()));
                return state.row_locks.with_write_locks(table, rids, f);
            }
            log_dml_lock_path(
                table,
                "table",
                Some("index_prefix_not_exact"),
                Some(rids.len()),
            );
        } else {
            let reason = if extract_dml_where_equalities(expr).is_empty() {
                "where_not_literal_equalities"
            } else {
                "no_matching_index"
            };
            log_dml_lock_path(table, "table", Some(reason), None);
        }
    } else {
        log_dml_lock_path(table, "table", Some("no_where_clause"), None);
    }
    let lock = table_storage_lock_arc(state, table)?;
    let _guard = acquire_table_storage_write_lock(&lock, table)?;
    f()
}

/// Physical base table names from [`TableReference::Table`] only (not `QualifiedIdentifier`).
fn collect_tables_expr(out: &mut HashSet<String>, expr: &Expression) {
    match expr {
        Expression::Literal(_) | Expression::Identifier(_) => {}
        Expression::QualifiedIdentifier { .. } => {}
        Expression::BinaryOp { left, right, .. } => {
            collect_tables_expr(out, left);
            collect_tables_expr(out, right);
        }
        Expression::UnaryOp { expr, .. } => collect_tables_expr(out, expr),
        Expression::Function { args, .. } => {
            for a in args {
                collect_tables_expr(out, a);
            }
        }
        Expression::Case {
            expr,
            when_clauses,
            else_clause,
        } => {
            if let Some(e) = expr {
                collect_tables_expr(out, e);
            }
            for w in when_clauses {
                collect_tables_expr(out, &w.condition);
                collect_tables_expr(out, &w.result);
            }
            if let Some(e) = else_clause {
                collect_tables_expr(out, e);
            }
        }
        Expression::Exists(s) => collect_tables_for_select(out, s),
        Expression::In { expr, list } => {
            collect_tables_expr(out, expr);
            match list {
                InList::Values(vals) => {
                    for v in vals {
                        collect_tables_expr(out, v);
                    }
                }
                InList::Subquery(s) => collect_tables_for_select(out, s),
            }
        }
        Expression::Between { expr, low, high } => {
            collect_tables_expr(out, expr);
            collect_tables_expr(out, low);
            collect_tables_expr(out, high);
        }
        Expression::IsNull { expr, .. } => collect_tables_expr(out, expr),
        Expression::Like { expr, pattern, .. } => {
            collect_tables_expr(out, expr);
            collect_tables_expr(out, pattern);
        }
    }
}

fn collect_tables_table_reference(out: &mut HashSet<String>, tr: &TableReference) {
    match tr {
        TableReference::Table { name, .. } => {
            out.insert(name.clone());
        }
        TableReference::Subquery { query, .. } => {
            collect_tables_for_select(out, query);
        }
    }
}

fn collect_tables_from_clause(out: &mut HashSet<String>, from: &FromClause) {
    collect_tables_table_reference(out, &from.table);
    for j in &from.joins {
        collect_tables_table_reference(out, &j.table);
        if let Some(cond) = &j.condition {
            collect_tables_expr(out, cond);
        }
    }
}

fn collect_tables_for_select(out: &mut HashSet<String>, sel: &SelectStatement) {
    if let Some(ref from) = sel.from {
        collect_tables_from_clause(out, from);
    }
    if let Some(ref w) = sel.where_clause {
        collect_tables_expr(out, w);
    }
    for e in &sel.group_by {
        collect_tables_expr(out, e);
    }
    if let Some(ref h) = sel.having {
        collect_tables_expr(out, h);
    }
    for ob in &sel.order_by {
        collect_tables_expr(out, &ob.expr);
    }
    for item in &sel.select_list {
        match item {
            SelectItem::Wildcard => {}
            SelectItem::Expression { expr, .. } => collect_tables_expr(out, expr),
        }
    }
}

/// Sorted, deduplicated physical table names referenced by a `SELECT` or set-operation statement.
fn collect_physical_tables_for_read_stmt(stmt: &SqlStatement) -> Vec<String> {
    let mut set = HashSet::new();
    match stmt {
        SqlStatement::Select(s) => collect_tables_for_select(&mut set, s),
        SqlStatement::SetOperation(b) => {
            collect_tables_for_select(&mut set, &b.left);
            collect_tables_for_select(&mut set, &b.right);
        }
        _ => {}
    }
    let mut names: Vec<String> = set.into_iter().collect();
    names.sort();
    names.dedup();
    names
}

fn execute_dml_autocommit<T>(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
    f: impl FnOnce(&SqlEngineState, &mut SessionContext) -> Result<T, EngineError>,
) -> Result<T, EngineError> {
    if ctx.transaction.is_some() {
        return f(state, ctx);
    }

    // Statement-level implicit transaction (auto-commit) for DML.
    // Use a predictable baseline isolation: ReadCommitted without the strong isolation lock.
    let iso = SqlIsolationLevel::ReadCommitted;
    let strong_iso = None;
    let mut tx = SqlTransaction::new(iso, strong_iso);
    tx.implicit_autocommit = true;
    if let Some(ref wal) = state.wal {
        wal.log_begin(&mut tx, iso)?;
    }
    ctx.transaction = Some(tx);

    match f(state, ctx) {
        Ok(out) => {
            commit_transaction(state, ctx)?;
            Ok(out)
        }
        Err(e) => match rollback_transaction(state, ctx) {
            Ok(_) => Err(e),
            Err(rb) => Err(rb),
        },
    }
}

fn default_session_isolation() -> SqlIsolationLevel {
    match std::env::var("RUSTDB_DEFAULT_ISOLATION").as_deref() {
        Ok("repeatable_read") | Ok("REPEATABLE_READ") => SqlIsolationLevel::RepeatableRead,
        Ok("serializable") | Ok("SERIALIZABLE") => SqlIsolationLevel::Serializable,
        _ => SqlIsolationLevel::ReadCommitted,
    }
}

fn begin_transaction(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
) -> Result<EngineOutput, EngineError> {
    ctx.txn_pm_cache.clear();
    ctx.tpcc_index_column_map_buf.clear();
    if ctx.transaction.is_some() {
        return Err(EngineError::new(
            engine_error_code::ALREADY_IN_TRANSACTION,
            "already in a transaction",
        ));
    }
    let iso = default_session_isolation();
    let strong_iso = if matches!(
        iso,
        SqlIsolationLevel::RepeatableRead | SqlIsolationLevel::Serializable
    ) {
        Some(STRONG_ISO_LOCK.lock())
    } else {
        None
    };
    let mut tx = SqlTransaction::new(iso, strong_iso);
    if let Some(ref wal) = state.wal {
        wal.log_begin(&mut tx, iso)?;
    }
    ctx.transaction = Some(tx);
    Ok(EngineOutput::ExecutionOk { rows_affected: 0 })
}

fn commit_transaction(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
) -> Result<EngineOutput, EngineError> {
    let mut tx = ctx.transaction.take().ok_or_else(|| {
        EngineError::new(
            engine_error_code::NO_ACTIVE_TRANSACTION,
            "no active transaction",
        )
    })?;
    let touched_table_count = tx.touched_tables.len();
    let pending_index_count = tx.pending_index_inserts.len();
    let commit_log_fsync = state.durability.fsync_on_commit();
    let span = info_span!(
        "sql.commit",
        flush_tables_count = tracing::field::Empty,
        flush_us = tracing::field::Empty,
        wal_us = tracing::field::Empty,
        commit_wal_us = tracing::field::Empty,
        commit_flush_us = tracing::field::Empty,
        commit_index_batch_us = tracing::field::Empty,
        commit_log_append_us = tracing::field::Empty,
        commit_log_commit_wait_us = tracing::field::Empty,
        pending_index_count,
        commit_log_fsync,
    );
    let _g = span.enter();
    let mut commit_wal_us = 0u64;
    let mut commit_log_commit_wait_us = 0u64;
    if let Some(ref wal) = state.wal {
        let t0 = Instant::now();
        wal.log_commit(&mut tx)?;
        commit_log_commit_wait_us = t0.elapsed().as_micros() as u64;
        commit_wal_us = commit_log_commit_wait_us;
    }
    span.record("wal_us", commit_wal_us);
    span.record("commit_wal_us", commit_wal_us);
    span.record("commit_log_commit_wait_us", commit_log_commit_wait_us);

    let index_batch_clock = (!tx.pending_index_inserts.is_empty()).then(Instant::now);
    apply_pending_index_inserts(state, &mut tx.pending_index_inserts)?;
    let commit_index_batch_us = index_batch_clock
        .map(|t0| t0.elapsed().as_micros() as u64)
        .unwrap_or(0);
    span.record("commit_index_batch_us", commit_index_batch_us);

    let touched: HashSet<String> = std::mem::take(&mut tx.touched_tables);
    let flush_tables_count = touched.len();
    span.record("flush_tables_count", flush_tables_count);
    let flush_clock = Instant::now();
    let (_flushed_pages, flush_phases) = if !ctx.txn_pm_cache.is_empty() {
        let mut pms: Vec<Arc<Mutex<PageManager>>> = ctx.txn_pm_cache.values().cloned().collect();
        pms.sort_by_key(|pm| pm.lock().map(|g| g.file_id()).unwrap_or(u32::MAX));
        crate::network::sql_engine_wal::flush_page_managers_cached(&pms).map_err(map_db_err)?
    } else if touched.is_empty() {
        (
            0,
            crate::network::sql_engine_wal::CommitFlushPhaseUs::default(),
        )
    } else {
        crate::network::sql_engine_wal::flush_page_managers_for_tables(state, &touched)
            .map_err(map_db_err)?
    };
    let commit_flush_us = flush_clock.elapsed().as_micros() as u64;
    span.record("flush_us", commit_flush_us);
    span.record("commit_flush_us", commit_flush_us);

    let log_clock = Instant::now();
    sql_commit_log::append_commit_log_line(&state.data_dir, commit_log_fsync)
        .map_err(map_db_err)?;
    let commit_log_append_us = log_clock.elapsed().as_micros() as u64;
    span.record("commit_log_append_us", commit_log_append_us);

    let tpcc_kind = ctx.tpcc_kind.unwrap_or(255);
    if sql_phase_log_enabled() {
        info!(
            target: "rustdb::sql_phases",
            tpcc_kind,
            flush_us = commit_flush_us,
            wal_us = commit_wal_us,
            commit_wal_us,
            commit_flush_us,
            commit_index_batch_us,
            commit_log_append_us,
            commit_log_commit_wait_us,
            commit_table_map_lock_us = flush_phases.table_map_lock_us,
            commit_pm_lock_wait_us = flush_phases.pm_lock_wait_us,
            commit_heap_fsync_us = flush_phases.heap_fsync_us,
            flush_tables_count,
            touched_table_count,
            pending_index_count,
            commit_log_fsync,
            "sql.commit"
        );
    }
    ctx.last_commit_flush_phases = Some(flush_phases);
    ctx.txn_pm_cache.clear();
    drop(tx);
    Ok(EngineOutput::ExecutionOk { rows_affected: 0 })
}

fn persist_catalog(state: &SqlEngineState) -> Result<(), EngineError> {
    let cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
    cat.save_catalog_to_data_dir_with_options(&state.data_dir, state.durability.fsync_on_commit())
        .map_err(map_db_err)?;
    drop(cat);
    if let Some(ref wal) = state.wal {
        wal.log_catalog_snapshot()?;
    }
    invalidate_dml_plan_validation_cache(state);
    Ok(())
}

fn rollback_transaction(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
) -> Result<EngineOutput, EngineError> {
    let mut tx = ctx.transaction.take().ok_or_else(|| {
        EngineError::new(
            engine_error_code::NO_ACTIVE_TRANSACTION,
            "no active transaction",
        )
    })?;
    let _pending_index = std::mem::take(&mut tx.pending_index_inserts);
    let mut flush_tables = std::mem::take(&mut tx.touched_tables);
    let undo = std::mem::take(&mut tx.undo);
    for op in undo.iter() {
        flush_tables.insert(undo_entry_table(op));
    }
    let had_undo = !undo.is_empty();
    for op in undo.into_iter().rev() {
        apply_undo(state, op)?;
    }
    // Rebuilding from heap is expensive and races with concurrent DML that already
    // updated the in-memory maps (e.g. another thread's committed INSERT). Only rescan
    // after we applied undo entries that may have left maps inconsistent with the heap.
    if had_undo {
        rebuild_all_constraint_runtime(state)?;
    }
    // Persist rollback: otherwise the next process sees heap pages from disk that still contain
    // undone inserts (WAL replay does not redo aborted txs, so durability must match).
    let _ = if !ctx.txn_pm_cache.is_empty() && !flush_tables.is_empty() {
        let mut pms: Vec<Arc<Mutex<PageManager>>> = flush_tables
            .iter()
            .filter_map(|t| ctx.txn_pm_cache.get(t).cloned())
            .collect();
        pms.sort_by_key(|pm| pm.lock().map(|g| g.file_id()).unwrap_or(u32::MAX));
        crate::network::sql_engine_wal::flush_page_managers_cached(&pms).map(|(n, _)| n)
    } else {
        crate::network::sql_engine_wal::flush_page_managers_for_tables(state, &flush_tables)
            .map(|(n, _)| n)
    }
    .map_err(map_db_err)?;
    // Only append an ABORT marker after the UNDO is applied *and* persisted.
    //
    // If we mark the transaction as aborted in WAL first and then crash before the UNDO is flushed,
    // recovery would skip UNDO (seeing ABORT) while the heap still contains uncommitted changes.
    if let Some(ref wal) = state.wal {
        wal.log_abort(&mut tx)?;
    }
    ctx.txn_pm_cache.clear();
    Ok(EngineOutput::ExecutionOk { rows_affected: 0 })
}

fn ensure_no_active_transaction(ctx: &SessionContext) -> Result<(), EngineError> {
    if ctx.transaction.is_some() {
        return Err(EngineError::new(
            engine_error_code::DDL_IN_TRANSACTION,
            "DDL is not supported inside an explicit transaction",
        ));
    }
    Ok(())
}

fn push_undo(ctx: &mut SessionContext, entry: UndoEntry) {
    if let Some(tx) = ctx.transaction.as_mut() {
        tx.undo.push(entry);
    }
}

fn undo_entry_table(entry: &UndoEntry) -> String {
    match entry {
        UndoEntry::Insert { table, .. }
        | UndoEntry::Delete { table, .. }
        | UndoEntry::Update { table, .. } => table.clone(),
    }
}

fn record_touched_table(ctx: &mut SessionContext, table: &str) {
    if let Some(tx) = ctx.transaction.as_mut() {
        tx.touched_tables.insert(table.to_string());
    }
}

fn apply_undo(state: &SqlEngineState, op: UndoEntry) -> Result<(), EngineError> {
    match op {
        UndoEntry::Insert {
            table,
            rid,
            payload,
        } => {
            let pm = table_page_manager(state, &table)?;
            let tuple = Tuple::from_bytes(&payload).map_err(map_db_err)?;
            let cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
            let sch = cat.schema(&table).cloned();
            let cat_clone = cat.clone();
            drop(cat);
            if let Some(ref s) = sch {
                let mut rt = state
                    .constraint_runtime
                    .lock()
                    .map_err(|_| lock_poisoned_engine())?;
                sql_constraints::unregister_row(&mut rt, &table, rid, &tuple, s, &cat_clone)?;
            }
            let mut g = pm.lock().map_err(|_| lock_poisoned_engine())?;
            match g.delete(rid) {
                Ok(_) => {}
                Err(e) => {
                    // During error paths (e.g. failed INSERT that already performed a compensating
                    // delete), the row might already be gone by the time we roll back.
                    // Treat "not found" as idempotent for rollback/recovery.
                    let msg = e.to_string();
                    if !msg.contains("Record not found") {
                        return Err(map_db_err(e));
                    }

                    // RecordIds encode a byte offset within the page; page split/merge/defrag can
                    // move records and invalidate stored `rid`s within the same transaction.
                    // As a fallback, locate and delete the row by its payload bytes.
                    if let Ok(snapshot) = g.select(None) {
                        if let Some((found_rid, _)) =
                            snapshot.into_iter().find(|(_rid, bytes)| *bytes == payload)
                        {
                            let _ = g.delete(found_rid);
                        }
                    }
                }
            }
        }
        UndoEntry::Delete {
            table,
            rid: _,
            payload,
        } => {
            let pm = table_page_manager(state, &table)?;
            let mut g = pm.lock().map_err(|_| lock_poisoned_engine())?;
            let _ins = g.insert(&payload).map_err(map_db_err)?;
        }
        UndoEntry::Update {
            table,
            rid,
            old_payload,
        } => {
            let pm = table_page_manager(state, &table)?;
            let mut g = pm.lock().map_err(|_| lock_poisoned_engine())?;
            g.update(rid, &old_payload).map_err(map_db_err)?;
        }
    }
    Ok(())
}

pub(crate) fn table_has_dirty_heap_pages(state: &SqlEngineState, table: &str) -> bool {
    let Ok(pm) = table_page_manager(state, table) else {
        return false;
    };
    let Ok(guard) = pm.lock() else {
        return false;
    };
    guard.dirty_page_count() > 0
}

pub(crate) fn table_page_manager(
    state: &SqlEngineState,
    table: &str,
) -> Result<Arc<Mutex<PageManager>>, EngineError> {
    {
        let g = state
            .table_page_managers
            .lock()
            .map_err(|_| lock_poisoned_engine())?;
        if let Some(pm) = g.get(table) {
            return Ok(pm.clone());
        }
    }
    let pm = match PageManager::open(state.data_dir.clone(), table, PageManagerConfig::default()) {
        Ok(pm) => pm,
        Err(_) => PageManager::new(state.data_dir.clone(), table, PageManagerConfig::default())
            .map_err(map_db_err)?,
    };
    let pm = Arc::new(Mutex::new(pm));
    {
        let mut g = state
            .table_page_managers
            .lock()
            .map_err(|_| lock_poisoned_engine())?;
        g.insert(table.to_string(), pm.clone());
    }
    Ok(pm)
}

/// Returns the page manager for `table`, caching in [`SessionContext::txn_pm_cache`] for the txn.
pub(crate) fn table_page_manager_cached(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
    table: &str,
) -> Result<Arc<Mutex<PageManager>>, EngineError> {
    if let Some(pm) = ctx.txn_pm_cache.get(table) {
        return Ok(pm.clone());
    }
    let pm = table_page_manager(state, table)?;
    ctx.txn_pm_cache.insert(table.to_string(), pm.clone());
    Ok(pm)
}

fn index_columns_for_table(state: &SqlEngineState, table: &str) -> Arc<Vec<String>> {
    if let Ok(cache) = state.index_columns_by_table.lock() {
        if let Some(cols) = cache.get(table) {
            return Arc::clone(cols);
        }
    }
    refresh_index_columns_cache_for_table(state, table);
    state
        .index_columns_by_table
        .lock()
        .ok()
        .and_then(|c| c.get(table).cloned())
        .unwrap_or_else(|| Arc::new(Vec::new()))
}

fn refresh_index_columns_cache_for_table(state: &SqlEngineState, table: &str) {
    let mut cols: Vec<String> = Vec::new();
    if let Ok(ir) = state.index_registry.read() {
        for (_idx_name, col_list) in ir.list_indexes_for_table(table) {
            for c in col_list {
                if !cols.iter().any(|x| x == &c) {
                    cols.push(c.clone());
                }
            }
        }
    }
    if let Ok(mut cache) = state.index_columns_by_table.lock() {
        cache.insert(table.to_string(), Arc::new(cols));
    }
}

fn rebuild_index_columns_cache(state: &SqlEngineState) -> Result<(), EngineError> {
    let tables: Vec<String> = {
        let cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
        cat.table_names()
    };
    for table in tables {
        refresh_index_columns_cache_for_table(state, &table);
    }
    Ok(())
}

fn tpcc_pending_index_column_map(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
    table: &str,
    tuple: &Tuple,
) -> Result<HashMap<String, String>, EngineError> {
    let cols = index_columns_for_table(state, table);
    let mut column_map = std::mem::take(&mut ctx.tpcc_index_column_map_buf);
    column_map.clear();
    for c in cols.iter() {
        if let Some(cv) = tuple.values.get(c) {
            column_map.insert(c.clone(), column_value_to_index_string(cv));
        }
    }
    Ok(column_map)
}

fn execute_create_table(
    state: &SqlEngineState,
    ctx: &SessionContext,
    ct: &CreateTableStatement,
) -> Result<EngineOutput, EngineError> {
    ensure_no_active_transaction(ctx)?;
    let _ = table_page_manager(state, &ct.table_name)?;
    let schema = table_schema_from_create_table(ct)?;
    {
        let cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
        validate_new_table_fks(&cat, &schema)?;
    }
    {
        let mut cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
        cat.register_schema(schema);
    }
    rebuild_all_constraint_runtime(state)?;
    persist_catalog(state)?;
    Ok(EngineOutput::ExecutionOk { rows_affected: 0 })
}

fn execute_drop_table(
    state: &SqlEngineState,
    ctx: &SessionContext,
    dt: &DropTableStatement,
) -> Result<EngineOutput, EngineError> {
    ensure_no_active_transaction(ctx)?;
    let exists = state
        .catalog
        .lock()
        .map_err(|_| lock_poisoned_engine())?
        .schema(&dt.table_name)
        .is_some();

    if !exists {
        if dt.if_exists {
            return Ok(EngineOutput::ExecutionOk { rows_affected: 0 });
        }
        return Err(EngineError::new(
            engine_error_code::CONSTRAINT_VIOLATION,
            format!("table {} does not exist", dt.table_name),
        ));
    }

    if !dt.cascade {
        let deps = state
            .catalog
            .lock()
            .map_err(|_| lock_poisoned_engine())?
            .tables_with_fk_to(&dt.table_name);
        if !deps.is_empty() {
            return Err(EngineError::new(
                engine_error_code::CONSTRAINT_VIOLATION,
                format!(
                    "cannot DROP TABLE {}: referenced by foreign key from {:?} (use CASCADE)",
                    dt.table_name, deps
                ),
            ));
        }
    }

    let mut visited = HashSet::new();
    drop_table_cascade(state, &dt.table_name, &mut visited)?;
    persist_catalog(state)?;
    Ok(EngineOutput::ExecutionOk { rows_affected: 0 })
}

fn drop_table_cascade(
    state: &SqlEngineState,
    table: &str,
    visited: &mut HashSet<String>,
) -> Result<(), EngineError> {
    if !visited.insert(table.to_string()) {
        return Ok(());
    }
    let deps = state
        .catalog
        .lock()
        .map_err(|_| lock_poisoned_engine())?
        .tables_with_fk_to(table);
    for dep in deps {
        drop_table_cascade(state, &dep, visited)?;
    }
    physical_drop_table(state, table)
}

fn physical_drop_table(state: &SqlEngineState, table: &str) -> Result<(), EngineError> {
    let schema = {
        let cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
        cat.schema(table).cloned()
    };
    let cat_snapshot = {
        let cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
        cat.clone()
    };
    if let Some(ref sch) = schema {
        let pm = table_page_manager(state, table)?;
        let snapshot = pm
            .lock()
            .map_err(|_| lock_poisoned_engine())?
            .select(None)
            .map_err(map_db_err)?;
        let mut rt = state
            .constraint_runtime
            .lock()
            .map_err(|_| lock_poisoned_engine())?;
        for (rid, data) in snapshot {
            let tuple = Tuple::from_bytes(&data).map_err(map_db_err)?;
            sql_constraints::unregister_row(&mut rt, table, rid, &tuple, sch, &cat_snapshot)?;
        }
        rt.clear_table_maps(table);
        rt.clear_fk_refs_to_parent(table);
    }

    {
        let mut ir = state
            .index_registry
            .write()
            .map_err(|_| lock_poisoned_engine())?;
        ir.remove_all_indexes_for_table(table);
    }

    {
        let mut g = state
            .table_page_managers
            .lock()
            .map_err(|_| lock_poisoned_engine())?;
        g.remove(table);
    }
    let path = state.data_dir.join(format!("{table}.tbl"));
    let _ = std::fs::remove_file(path);
    let mut cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
    cat.drop_table(table);
    rebuild_optimizer_with_indexes(state)?;
    Ok(())
}

fn execute_alter_table(
    state: &SqlEngineState,
    ctx: &SessionContext,
    stmt: &AlterTableStatement,
) -> Result<EngineOutput, EngineError> {
    ensure_no_active_transaction(ctx)?;
    match &stmt.operation {
        AlterTableOperation::AddConstraint { name, definition } => {
            {
                let mut cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
                let schema = cat.schema_mut(&stmt.table_name).ok_or_else(|| {
                    EngineError::new(
                        engine_error_code::CONSTRAINT_VIOLATION,
                        format!("table {} does not exist", stmt.table_name),
                    )
                })?;
                apply_table_constraint_to_schema(schema, name.as_deref(), definition)?;
            }
            {
                let cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
                let schema = cat.schema(&stmt.table_name).ok_or_else(|| {
                    EngineError::new(engine_error_code::INTERNAL, "table disappeared after ALTER")
                })?;
                validate_new_table_fks(&cat, schema)?;
            }
            rebuild_all_constraint_runtime(state)?;
            persist_catalog(state)?;
            Ok(EngineOutput::ExecutionOk { rows_affected: 0 })
        }
        AlterTableOperation::DropConstraint(name) => {
            {
                let mut cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
                let schema = cat.schema_mut(&stmt.table_name).ok_or_else(|| {
                    EngineError::new(
                        engine_error_code::CONSTRAINT_VIOLATION,
                        format!("table {} does not exist", stmt.table_name),
                    )
                })?;
                drop_constraint_by_name(schema, name)?;
            }
            rebuild_all_constraint_runtime(state)?;
            persist_catalog(state)?;
            Ok(EngineOutput::ExecutionOk { rows_affected: 0 })
        }
        AlterTableOperation::AddColumn(cd) => {
            alter_table_ops::add_column(state, &stmt.table_name, cd)?;
            Ok(EngineOutput::ExecutionOk { rows_affected: 0 })
        }
        AlterTableOperation::DropColumn(col) => {
            alter_table_ops::drop_column(state, &stmt.table_name, col)?;
            Ok(EngineOutput::ExecutionOk { rows_affected: 0 })
        }
        AlterTableOperation::RenameColumn { old_name, new_name } => {
            alter_table_ops::rename_column(state, &stmt.table_name, old_name, new_name)?;
            Ok(EngineOutput::ExecutionOk { rows_affected: 0 })
        }
        AlterTableOperation::RenameTable(new_name) => {
            alter_table_ops::rename_table(state, &stmt.table_name, new_name)?;
            Ok(EngineOutput::ExecutionOk { rows_affected: 0 })
        }
        AlterTableOperation::ModifyColumn(cd) => {
            alter_table_ops::modify_column(state, &stmt.table_name, cd)?;
            Ok(EngineOutput::ExecutionOk { rows_affected: 0 })
        }
    }
}

fn column_value_to_index_string(cv: &ColumnValue) -> String {
    if cv.is_null {
        return String::new();
    }
    match &cv.data_type {
        DataType::Null => String::new(),
        DataType::Boolean(b) => b.to_string(),
        DataType::TinyInt(v) => v.to_string(),
        DataType::SmallInt(v) => v.to_string(),
        DataType::Integer(v) => v.to_string(),
        DataType::BigInt(v) => v.to_string(),
        DataType::Float(v) => v.to_string(),
        DataType::Double(v) => v.to_string(),
        DataType::Char(s) | DataType::Varchar(s) | DataType::Text(s) => s.clone(),
        DataType::Date(s) | DataType::Time(s) | DataType::Timestamp(s) => s.clone(),
        DataType::Blob(_) => String::new(),
    }
}

fn tuple_to_index_column_map(tuple: &Tuple) -> HashMap<String, String> {
    let mut m = HashMap::new();
    for (name, cv) in &tuple.values {
        m.insert(name.clone(), column_value_to_index_string(cv));
    }
    m
}

/// Index column map for deferred TPC-C inserts (only columns referenced by secondary indexes).
fn tuple_to_index_column_map_for_table(
    state: &SqlEngineState,
    table: &str,
    tuple: &Tuple,
) -> Result<HashMap<String, String>, EngineError> {
    let cols = index_columns_for_table(state, table);
    if cols.is_empty() {
        return Ok(HashMap::new());
    }
    let mut m = HashMap::with_capacity(cols.len());
    for c in cols.iter() {
        if let Some(cv) = tuple.values.get(c) {
            m.insert(c.clone(), column_value_to_index_string(cv));
        }
    }
    Ok(m)
}

/// Collects `column = literal` pairs from a DML `WHERE` (`AND` only).
fn extract_dml_where_equalities(expr: &Expression) -> HashMap<String, Literal> {
    match expr {
        Expression::Literal(Literal::Boolean(_)) => HashMap::new(),
        Expression::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut m = extract_dml_where_equalities(left);
            for (k, v) in extract_dml_where_equalities(right) {
                m.insert(k, v);
            }
            m
        }
        Expression::BinaryOp {
            left,
            op: BinaryOperator::Equal,
            right,
        } => {
            let mut m = HashMap::new();
            if let (Some(c), Some(l)) = (column_name_expr(left), value_expr(right)) {
                m.insert(c, l.clone());
            } else if let (Some(c), Some(l)) = (column_name_expr(right), value_expr(left)) {
                m.insert(c, l.clone());
            }
            m
        }
        _ => HashMap::new(),
    }
}

fn literal_to_index_key_string(lit: &Literal) -> String {
    column_value_to_index_string(&literal_to_column_value(lit))
}

/// Emits `sql.dml.index_lookup` tracing span for DML index path observability.
fn record_dml_index_lookup(
    table: &str,
    hit: bool,
    exact_key: bool,
    rids: usize,
    reason: Option<&'static str>,
) {
    let _span = info_span!(
        "sql.dml.index_lookup",
        table = %table,
        hit,
        exact_key,
        rids,
        reason = reason.unwrap_or(""),
    )
    .entered();
}

/// Index-backed row fetch for UPDATE/DELETE when a matching index exists.
///
/// Returns `(rows, skip_heap_where)` when an index applies; `skip_heap_where` is true when the
/// `WHERE` clause is a conjunction of equalities fully covered by an exact index key lookup.
fn try_dml_rows_via_index(
    state: &SqlEngineState,
    table: &str,
    where_expr: &Expression,
    pm: &mut PageManager,
) -> Result<Option<(Vec<(RecordId, Vec<u8>)>, bool)>, EngineError> {
    let lit_eq = extract_dml_where_equalities(where_expr);
    if lit_eq.is_empty() {
        record_dml_index_lookup(table, false, false, 0, Some("parse_where_failed"));
        return Ok(None);
    }
    let equalities: HashMap<String, String> = lit_eq
        .iter()
        .map(|(col, lit)| (col.clone(), literal_to_index_key_string(lit)))
        .collect();
    let ir = state
        .index_registry
        .read()
        .map_err(|_| lock_poisoned_engine())?;
    let lookup = ir
        .lookup_record_ids_by_equalities(table, &equalities)
        .map_err(|e| EngineError::new(engine_error_code::INTERNAL, format!("index lookup: {e}")))?;
    let Some((rids, index_exact)) = lookup else {
        record_dml_index_lookup(table, false, false, 0, Some("no_index"));
        return Ok(None);
    };
    let where_eq_cols: HashSet<String> = extract_dml_where_equalities(where_expr)
        .into_keys()
        .collect();
    let lit_eq_cols: HashSet<String> = lit_eq.keys().cloned().collect();
    let skip_heap_where = index_exact && where_eq_cols == lit_eq_cols && !where_eq_cols.is_empty();
    let mut rows = Vec::with_capacity(rids.len());
    for rid in rids {
        if let Some(data) = pm.get_record(rid).map_err(map_db_err)? {
            rows.push((rid, data));
        }
    }
    record_dml_index_lookup(table, true, index_exact, rows.len(), None);
    Ok(Some((rows, skip_heap_where)))
}

fn sync_index_after_insert(
    state: &SqlEngineState,
    table: &str,
    rid: RecordId,
    tuple: &Tuple,
) -> Result<(), EngineError> {
    let m = tuple_to_index_column_map(tuple);
    let mut ir = state
        .index_registry
        .write()
        .map_err(|_| lock_poisoned_engine())?;
    ir.insert_into_indexes(table, rid, &m)
        .map_err(|e| EngineError::new(engine_error_code::INTERNAL, format!("index insert: {e}")))?;
    Ok(())
}

fn sync_index_after_update(
    state: &SqlEngineState,
    table: &str,
    rid: RecordId,
    old_tuple: &Tuple,
    new_tuple: &Tuple,
) -> Result<(), EngineError> {
    let old_m = tuple_to_index_column_map(old_tuple);
    let new_m = tuple_to_index_column_map(new_tuple);
    let mut ir = state
        .index_registry
        .write()
        .map_err(|_| lock_poisoned_engine())?;
    ir.update_indexes(table, rid, &old_m, &new_m)
        .map_err(|e| EngineError::new(engine_error_code::INTERNAL, format!("index update: {e}")))?;
    Ok(())
}

fn sync_index_after_delete(
    state: &SqlEngineState,
    table: &str,
    rid: RecordId,
    tuple: &Tuple,
) -> Result<(), EngineError> {
    let m = tuple_to_index_column_map(tuple);
    let mut ir = state
        .index_registry
        .write()
        .map_err(|_| lock_poisoned_engine())?;
    ir.delete_from_indexes(table, rid, &m)
        .map_err(|e| EngineError::new(engine_error_code::INTERNAL, format!("index delete: {e}")))?;
    Ok(())
}

fn rebuild_optimizer_with_indexes(state: &SqlEngineState) -> Result<(), EngineError> {
    let snapshot = state
        .index_registry
        .read()
        .map_err(|_| lock_poisoned_engine())?
        .clone();
    let mut opt = state.optimizer.lock().map_err(|_| lock_poisoned_engine())?;
    *opt = if snapshot.is_empty() {
        QueryOptimizer::new().map_err(map_db_err)?
    } else {
        QueryOptimizer::new()
            .map_err(map_db_err)?
            .with_index_registry(Arc::new(snapshot))
    };
    invalidate_dml_plan_validation_cache(state);
    Ok(())
}

/// Max distinct normalized SQL texts remembered per catalog/index epoch (TPC-C / ExecuteScript).
const DML_PLAN_VALIDATION_CACHE_CAP: usize = 256;

#[derive(Default)]
struct DmlPlanValidationCache {
    schema_epoch: u64,
    order: VecDeque<String>,
    plans: HashMap<String, Arc<ExecutionPlan>>,
}

fn invalidate_dml_plan_validation_cache(state: &SqlEngineState) {
    if let Ok(mut cache) = state.dml_plan_validation_cache.lock() {
        cache.schema_epoch = cache.schema_epoch.wrapping_add(1);
        cache.order.clear();
        cache.plans.clear();
    }
}

fn normalize_sql_for_plan_cache(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn cached_sql_plan(cache: &DmlPlanValidationCache, cache_key: &str) -> Option<Arc<ExecutionPlan>> {
    cache.plans.get(cache_key).cloned()
}

fn record_sql_plan_cache(
    cache: &mut DmlPlanValidationCache,
    cache_key: String,
    plan: Arc<ExecutionPlan>,
    epoch: u64,
) {
    if cache.schema_epoch != epoch {
        return;
    }
    if cache.plans.contains_key(&cache_key) {
        return;
    }
    cache.plans.insert(cache_key.clone(), plan);
    cache.order.push_back(cache_key);
    while cache.order.len() > DML_PLAN_VALIDATION_CACHE_CAP {
        if let Some(victim) = cache.order.pop_front() {
            cache.plans.remove(&victim);
        }
    }
}

fn plan_and_optimize_read(
    state: &SqlEngineState,
    sql: &str,
    stmt: &SqlStatement,
) -> Result<ExecutionPlan, EngineError> {
    let cache_key = normalize_sql_for_plan_cache(sql);
    let epoch = {
        let cache = state
            .dml_plan_validation_cache
            .lock()
            .map_err(|_| lock_poisoned_engine())?;
        if let Some(plan) = cached_sql_plan(&cache, &cache_key) {
            return Ok((*plan).clone());
        }
        cache.schema_epoch
    };
    let plan = state.planner.create_plan(stmt).map_err(map_db_err)?;
    let optimized = state
        .optimizer
        .lock()
        .map_err(|_| lock_poisoned_engine())?
        .optimize(plan)
        .map_err(map_db_err)?;
    if let Ok(mut cache) = state.dml_plan_validation_cache.lock() {
        record_sql_plan_cache(
            &mut cache,
            cache_key,
            Arc::new(optimized.optimized_plan.clone()),
            epoch,
        );
    }
    Ok(optimized.optimized_plan)
}

fn index_columns_for_scan(
    registry: &IndexRegistry,
    table: &str,
    index_name: &str,
) -> Option<Vec<String>> {
    registry
        .list_indexes_for_table(table)
        .into_iter()
        .find(|(name, _)| name == index_name)
        .map(|(_, cols)| cols)
}

fn index_scan_is_exact_key(registry: &IndexRegistry, idx: &IndexScanNode) -> bool {
    let Some(cols) = index_columns_for_scan(registry, &idx.table_name, &idx.index_name) else {
        return false;
    };
    if cols.is_empty() || idx.conditions.len() != cols.len() {
        return false;
    }
    idx.conditions.iter().all(|c| c.operator == "=")
}

fn find_index_scan_node(node: &PlanNode) -> Option<&IndexScanNode> {
    match node {
        PlanNode::IndexScan(i) => Some(i),
        PlanNode::Filter(f) => find_index_scan_node(&f.input),
        PlanNode::Projection(p) => find_index_scan_node(&p.input),
        PlanNode::Limit(l) => find_index_scan_node(&l.input),
        PlanNode::Sort(s) => find_index_scan_node(&s.input),
        PlanNode::Offset(o) => find_index_scan_node(&o.input),
        PlanNode::Aggregate(a) => find_index_scan_node(&a.input),
        PlanNode::GroupBy(g) => find_index_scan_node(&g.input),
        PlanNode::Distinct(d) => find_index_scan_node(&d.input),
        _ => None,
    }
}

/// Single-table `SELECT` with a full index equality seek can skip the table storage read lock.
fn select_skip_table_read_lock_tables(
    state: &SqlEngineState,
    table_names: &[String],
    plan_root: &PlanNode,
) -> HashSet<String> {
    if table_names.len() != 1 {
        return HashSet::new();
    }
    let registry = match state.index_registry.read() {
        Ok(r) => r,
        Err(_) => return HashSet::new(),
    };
    let Some(idx) = find_index_scan_node(plan_root) else {
        return HashSet::new();
    };
    if idx.table_name != table_names[0] || !index_scan_is_exact_key(&registry, idx) {
        return HashSet::new();
    }
    let mut out = HashSet::new();
    out.insert(table_names[0].clone());
    out
}

fn execute_create_index(
    state: &SqlEngineState,
    ctx: &SessionContext,
    ci: &CreateIndexStatement,
) -> Result<EngineOutput, EngineError> {
    ensure_no_active_transaction(ctx)?;
    let table = ci.table_name.as_str();
    let cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
    let Some(schema) = cat.schema(table) else {
        return Err(EngineError::new(
            engine_error_code::CONSTRAINT_VIOLATION,
            format!("table {table} does not exist"),
        ));
    };
    for col in &ci.columns {
        if !schema.columns.iter().any(|c| c.name == *col) {
            return Err(EngineError::new(
                engine_error_code::CONSTRAINT_VIOLATION,
                format!("unknown column {table}.{col}"),
            ));
        }
    }
    drop(cat);
    {
        let mut reg = state
            .index_registry
            .write()
            .map_err(|_| lock_poisoned_engine())?;
        reg.create_index(table, &ci.index_name, ci.columns.clone())
            .map_err(|e| {
                EngineError::new(engine_error_code::CONSTRAINT_VIOLATION, e.to_string())
            })?;
    }
    backfill_index_from_heap(state, table, &ci.index_name)?;
    {
        use crate::catalog::schema::SecondaryIndexDef;
        let mut cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
        let Some(sch) = cat.schema_mut(table) else {
            return Err(EngineError::new(
                engine_error_code::CONSTRAINT_VIOLATION,
                format!("table {table} does not exist"),
            ));
        };
        if !sch
            .secondary_indexes
            .iter()
            .any(|i| i.name == ci.index_name)
        {
            sch.secondary_indexes.push(SecondaryIndexDef {
                name: ci.index_name.clone(),
                columns: ci.columns.clone(),
            });
        }
    }
    persist_catalog(state)?;
    rebuild_optimizer_with_indexes(state)?;
    refresh_index_columns_cache_for_table(state, table);
    Ok(EngineOutput::ExecutionOk { rows_affected: 0 })
}

/// Rebuilds in-memory secondary indexes from catalog metadata after reopen (CI seed path).
fn rebuild_secondary_indexes_from_catalog(state: &SqlEngineState) -> Result<(), EngineError> {
    let index_defs: Vec<(String, String, Vec<String>)> = {
        let cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
        let mut out = Vec::new();
        for table in cat.table_names() {
            let Some(sch) = cat.schema(&table) else {
                continue;
            };
            for idx in &sch.secondary_indexes {
                out.push((table.clone(), idx.name.clone(), idx.columns.clone()));
            }
        }
        out
    };
    if index_defs.is_empty() {
        return Ok(());
    }
    {
        let mut reg = state
            .index_registry
            .write()
            .map_err(|_| lock_poisoned_engine())?;
        for (table, index_name, columns) in &index_defs {
            if reg.get_index_entry(table, index_name).is_some() {
                continue;
            }
            reg.create_index(table, index_name, columns.clone())
                .map_err(|e| {
                    EngineError::new(engine_error_code::INTERNAL, format!("index rebuild: {e}"))
                })?;
        }
    }
    for (table, index_name, _) in index_defs {
        backfill_index_from_heap(state, &table, &index_name)?;
    }
    rebuild_optimizer_with_indexes(state)?;
    Ok(())
}

fn backfill_index_from_heap(
    state: &SqlEngineState,
    table: &str,
    index_name: &str,
) -> Result<(), EngineError> {
    let pm = table_page_manager(state, table)?;
    let snapshot = pm
        .lock()
        .map_err(|_| lock_poisoned_engine())?
        .select(None)
        .map_err(map_db_err)?;
    let mut ir = state
        .index_registry
        .write()
        .map_err(|_| lock_poisoned_engine())?;
    for (rid, data) in snapshot {
        let tuple = Tuple::from_bytes(&data).map_err(map_db_err)?;
        let m = tuple_to_index_column_map(&tuple);
        ir.insert_into_named_index(table, index_name, rid, &m)
            .map_err(|e| {
                EngineError::new(engine_error_code::INTERNAL, format!("index backfill: {e}"))
            })?;
    }
    Ok(())
}

fn validate_plan(
    state: &SqlEngineState,
    sql: &str,
    stmt: &SqlStatement,
) -> Result<(), EngineError> {
    let _ = plan_and_optimize_read(state, sql, stmt)?;
    Ok(())
}

fn execute_insert(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
    sql: &str,
    stmt: &SqlStatement,
    insert: &InsertStatement,
) -> Result<EngineOutput, EngineError> {
    validate_plan(state, sql, stmt)?;
    record_touched_table(ctx, &insert.table);
    match &insert.values {
        InsertValues::Select(sel) => {
            // Plan/execute the SELECT subquery and insert its resulting rows.
            let select_stmt = SqlStatement::Select((**sel).clone());
            let plan = state
                .planner
                .create_plan(&select_stmt)
                .map_err(map_db_err)?;
            let optimized = state
                .optimizer
                .lock()
                .map_err(|_| lock_poisoned_engine())?
                .optimize(plan)
                .map_err(map_db_err)?;
            let rows = state
                .executor
                .execute(&optimized.optimized_plan)
                .map_err(map_db_err)?;

            let pm_for_table = table_page_manager(state, &insert.table)?;
            let mut rows_affected = 0u64;
            for r in rows {
                let tuple =
                    build_insert_tuple_from_row(state, &insert.table, insert.columns.as_ref(), &r)?;
                rows_affected += insert_heap_row_in_execute_insert(
                    state,
                    ctx,
                    &insert.table,
                    &tuple,
                    &pm_for_table,
                )?;
            }
            {
                let mut pm = pm_for_table.lock().map_err(|_| lock_poisoned_engine())?;
                flush_heap_after_dml_success(state, ctx, &mut pm)?;
            }
            Ok(EngineOutput::ExecutionOk { rows_affected })
        }
        InsertValues::Values(rows) => {
            let mut rows_affected = 0u64;
            let pm_for_table = table_page_manager(state, &insert.table)?;
            for row in rows {
                let tuple = build_insert_tuple(state, &insert.table, insert.columns.as_ref(), row)?;
                rows_affected += insert_heap_row_in_execute_insert(
                    state,
                    ctx,
                    &insert.table,
                    &tuple,
                    &pm_for_table,
                )?;
            }
            {
                let mut pm = pm_for_table.lock().map_err(|_| lock_poisoned_engine())?;
                flush_heap_after_dml_success(state, ctx, &mut pm)?;
            }
            Ok(EngineOutput::ExecutionOk { rows_affected })
        }
    }
}

fn build_insert_tuple(
    state: &SqlEngineState,
    table: &str,
    columns: Option<&Vec<String>>,
    row: &[Expression],
) -> Result<Tuple, EngineError> {
    let id = state.next_tuple_id.fetch_add(1, Ordering::Relaxed);
    let mut tuple = Tuple::new(id);
    let col_names: Vec<String> = match columns {
        Some(cols) => {
            if cols.len() != row.len() {
                return Err(EngineError::new(
                    engine_error_code::UNSUPPORTED_SQL,
                    "INSERT column count does not match VALUES row length",
                ));
            }
            cols.clone()
        }
        None => (0..row.len()).map(|i| format!("col{}", i + 1)).collect(),
    };
    for (name, expr) in col_names.iter().zip(row.iter()) {
        let cv = expr_to_column_value(expr)?;
        tuple.set_value(name, cv);
    }
    apply_defaults_and_validate(state, table, &mut tuple)?;
    Ok(tuple)
}

fn build_insert_tuple_from_row(
    state: &SqlEngineState,
    table: &str,
    columns: Option<&Vec<String>>,
    row: &Row,
) -> Result<Tuple, EngineError> {
    let id = state.next_tuple_id.fetch_add(1, Ordering::Relaxed);
    let mut tuple = Tuple::new(id);

    let col_names: Vec<String> = match columns {
        Some(cols) => cols.clone(),
        None => {
            let mut keys: Vec<String> = row.values.keys().cloned().collect();
            keys.sort();
            keys
        }
    };
    for name in col_names {
        let Some(cv) = row.values.get(&name) else {
            continue;
        };
        tuple.set_value(&name, cv.clone());
    }
    apply_defaults_and_validate(state, table, &mut tuple)?;
    Ok(tuple)
}

fn tuple_as_eval_row(tuple: &Tuple) -> Row {
    let mut row = Row::with_capacity(tuple.values.len());
    for (k, v) in &tuple.values {
        row.set_value_fast(k, v.clone());
    }
    row
}

fn execute_update(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
    sql: &str,
    stmt: &SqlStatement,
    update: &UpdateStatement,
) -> Result<EngineOutput, EngineError> {
    validate_plan(state, sql, stmt)?;
    record_touched_table(ctx, &update.table);
    let pm_for_table = table_page_manager(state, &update.table)?;
    let mut pm = pm_for_table.lock().map_err(|_| lock_poisoned_engine())?;
    let scan_clock = sql_phase_log_enabled().then(Instant::now);
    let (snapshot, where_pre_filtered) = match &update.where_clause {
        None => (pm.select(None).map_err(map_db_err)?, false),
        Some(expr) => {
            validate_dml_where_structure(expr)?;
            if let Some((rows, skip_where)) =
                try_dml_rows_via_index(state, &update.table, expr, &mut pm)?
            {
                (rows, skip_where)
            } else {
                let expr = expr.clone();
                let pred = Box::new(move |data: &[u8]| {
                    let tuple = Tuple::from_bytes(data).expect("heap tuple must deserialize");
                    match_where_tuple(&expr, &tuple).expect("WHERE validated for heap predicate")
                });
                (pm.select(Some(pred)).map_err(map_db_err)?, true)
            }
        }
    };
    let scan_us = scan_clock.map(|t| t.elapsed().as_micros() as u64);
    let snapshot_len = snapshot.len();
    let row_clock = sql_phase_log_enabled().then(Instant::now);
    let mut rows_affected = 0u64;
    for (rid, data) in snapshot {
        let mut tuple = Tuple::from_bytes(&data).map_err(map_db_err)?;
        let keep = if where_pre_filtered {
            true
        } else {
            match &update.where_clause {
                None => true,
                Some(expr) => match_where_tuple(expr, &tuple)?,
            }
        };
        if !keep {
            continue;
        }
        let old_tuple = tuple.clone();
        let cat_snapshot = {
            let cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
            cat.clone()
        };
        let schema = cat_snapshot.schema(&update.table).cloned();
        if let Some(ref sch) = schema {
            let mut rt = state
                .constraint_runtime
                .lock()
                .map_err(|_| lock_poisoned_engine())?;
            sql_constraints::unregister_row(
                &mut rt,
                &update.table,
                rid,
                &old_tuple,
                sch,
                &cat_snapshot,
            )?;
        }
        let eval_row = tuple_as_eval_row(&tuple);
        for a in &update.assignments {
            let cv = eval_scalar_expression(&eval_row, &a.value);
            tuple.set_value(&a.column, cv);
        }
        if let Err(e) = apply_defaults_and_validate(state, &update.table, &mut tuple) {
            if let Some(ref sch) = schema {
                let mut rt = state
                    .constraint_runtime
                    .lock()
                    .map_err(|_| lock_poisoned_engine())?;
                let _ = sql_constraints::register_row(
                    &mut rt,
                    &update.table,
                    rid,
                    &old_tuple,
                    sch,
                    &cat_snapshot,
                );
            }
            return Err(e);
        }
        if let Some(ref sch) = schema {
            let mut rt = state
                .constraint_runtime
                .lock()
                .map_err(|_| lock_poisoned_engine())?;
            if let Err(e) = sql_constraints::register_row(
                &mut rt,
                &update.table,
                rid,
                &tuple,
                sch,
                &cat_snapshot,
            ) {
                let _ = sql_constraints::register_row(
                    &mut rt,
                    &update.table,
                    rid,
                    &old_tuple,
                    sch,
                    &cat_snapshot,
                );
                return Err(e);
            }
        }
        let new_bytes = tuple.to_bytes().map_err(map_db_err)?;
        if let Some(tx) = ctx.transaction.as_mut() {
            if let Some(ref wal) = state.wal {
                let page_id = (rid >> 32) as u64;
                let off = (rid & 0xffff_ffff) as u32;
                let record_offset: u16 = off.try_into().map_err(|_| {
                    EngineError::new(
                        engine_error_code::INTERNAL,
                        "record offset too large for WAL",
                    )
                })?;
                wal.log_data_update(
                    tx,
                    pm.file_id(),
                    page_id,
                    record_offset,
                    data.clone(),
                    new_bytes.clone(),
                )?;
            }
        }
        pm.update(rid, &new_bytes).map_err(map_db_err)?;
        sync_index_after_update(state, &update.table, rid, &old_tuple, &tuple)?;
        maybe_simulate_dml_crash("update_row");
        push_undo(
            ctx,
            UndoEntry::Update {
                table: update.table.clone(),
                rid,
                old_payload: data.clone(),
            },
        );
        rows_affected += 1;
    }
    if let Some(t0) = row_clock {
        info!(
            target: "rustdb::sql_phases",
            table = %update.table,
            scan_us = scan_us.unwrap_or(0),
            row_loop_us = t0.elapsed().as_micros() as u64,
            snapshot_rows = snapshot_len,
            rows_affected,
            "update"
        );
    }
    flush_heap_after_dml_success(state, ctx, &mut pm)?;
    Ok(EngineOutput::ExecutionOk { rows_affected })
}

fn execute_delete(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
    sql: &str,
    stmt: &SqlStatement,
    delete: &DeleteStatement,
) -> Result<EngineOutput, EngineError> {
    validate_plan(state, sql, stmt)?;
    record_touched_table(ctx, &delete.table);
    let pm_for_table = table_page_manager(state, &delete.table)?;
    let mut pm = pm_for_table.lock().map_err(|_| lock_poisoned_engine())?;
    let scan_clock = sql_phase_log_enabled().then(Instant::now);
    let (snapshot, where_pre_filtered) = match &delete.where_clause {
        None => (pm.select(None).map_err(map_db_err)?, false),
        Some(expr) => {
            validate_dml_where_structure(expr)?;
            if let Some((rows, skip_where)) =
                try_dml_rows_via_index(state, &delete.table, expr, &mut pm)?
            {
                (rows, skip_where)
            } else {
                let expr = expr.clone();
                let pred = Box::new(move |data: &[u8]| {
                    let tuple = Tuple::from_bytes(data).expect("heap tuple must deserialize");
                    match_where_tuple(&expr, &tuple).expect("WHERE validated for heap predicate")
                });
                (pm.select(Some(pred)).map_err(map_db_err)?, true)
            }
        }
    };
    let scan_us = scan_clock.map(|t| t.elapsed().as_micros() as u64);
    let snapshot_len = snapshot.len();
    let row_clock = sql_phase_log_enabled().then(Instant::now);
    let cat_snapshot = {
        let cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
        cat.clone()
    };
    let schema = cat_snapshot.schema(&delete.table).cloned();
    let mut to_delete: Vec<(RecordId, Vec<u8>)> = Vec::new();
    for (rid, data) in snapshot {
        let tuple = Tuple::from_bytes(&data).map_err(map_db_err)?;
        let keep = if where_pre_filtered {
            true
        } else {
            match &delete.where_clause {
                None => true,
                Some(expr) => match_where_tuple(expr, &tuple)?,
            }
        };
        if keep {
            if let Some(ref sch) = schema {
                let rt = state
                    .constraint_runtime
                    .lock()
                    .map_err(|_| lock_poisoned_engine())?;
                if sql_constraints::fk_blocks_parent_delete(&rt, &delete.table, &tuple, sch)? {
                    return Err(EngineError::new(
                        engine_error_code::CONSTRAINT_VIOLATION,
                        format!(
                            "cannot delete row: foreign key references exist for {}",
                            delete.table
                        ),
                    ));
                }
            }
            to_delete.push((rid, data));
        }
    }
    let mut rows_affected = 0u64;
    for (rid, data) in to_delete {
        let tuple = Tuple::from_bytes(&data).map_err(map_db_err)?;
        if let Some(ref sch) = schema {
            let mut rt = state
                .constraint_runtime
                .lock()
                .map_err(|_| lock_poisoned_engine())?;
            sql_constraints::unregister_row(
                &mut rt,
                &delete.table,
                rid,
                &tuple,
                sch,
                &cat_snapshot,
            )?;
        }
        push_undo(
            ctx,
            UndoEntry::Delete {
                table: delete.table.clone(),
                rid,
                payload: data.clone(),
            },
        );
        sync_index_after_delete(state, &delete.table, rid, &tuple)?;
        pm.delete(rid).map_err(map_db_err)?;
        maybe_simulate_dml_crash("delete_row");
        if let Some(tx) = ctx.transaction.as_mut() {
            if let Some(ref wal) = state.wal {
                let page_id = (rid >> 32) as u64;
                let off = (rid & 0xffff_ffff) as u32;
                let record_offset: u16 = off.try_into().map_err(|_| {
                    EngineError::new(
                        engine_error_code::INTERNAL,
                        "record offset too large for WAL",
                    )
                })?;
                wal.log_data_delete(tx, pm.file_id(), page_id, record_offset, data.clone())?;
            }
        }
        rows_affected += 1;
    }
    if let Some(t0) = row_clock {
        info!(
            target: "rustdb::sql_phases",
            table = %delete.table,
            scan_us = scan_us.unwrap_or(0),
            row_loop_us = t0.elapsed().as_micros() as u64,
            snapshot_rows = snapshot_len,
            rows_affected,
            "delete"
        );
    }
    flush_heap_after_dml_success(state, ctx, &mut pm)?;
    Ok(EngineOutput::ExecutionOk { rows_affected })
}

/// WHERE for DML: boolean literal, `=` (and `AND`), or column = literal.
fn match_where_tuple(expr: &Expression, tuple: &Tuple) -> Result<bool, EngineError> {
    match expr {
        Expression::Literal(Literal::Boolean(b)) => Ok(*b),
        Expression::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => Ok(match_where_tuple(left, tuple)? && match_where_tuple(right, tuple)?),
        Expression::BinaryOp {
            left,
            op: BinaryOperator::Equal,
            right,
        } => {
            let (col, lit) = match (column_name_expr(left), value_expr(right)) {
                (Some(c), Some(l)) => (c, l),
                (None, _) | (_, None) => match (column_name_expr(right), value_expr(left)) {
                    (Some(c), Some(l)) => (c, l),
                    _ => {
                        return Err(EngineError::new(
                            engine_error_code::UNSUPPORTED_SQL,
                            "WHERE for UPDATE/DELETE supports only column = literal (and AND)",
                        ));
                    }
                },
            };
            let expected = literal_to_column_value(lit);
            let Some(cv) = tuple.values.get(&col) else {
                return Ok(false);
            };
            Ok(cv.data_type == expected.data_type && cv.is_null == expected.is_null)
        }
        _ => Err(EngineError::new(
            engine_error_code::UNSUPPORTED_SQL,
            "WHERE for UPDATE/DELETE supports only column = literal (and AND)",
        )),
    }
}

fn sql_type_to_column_datatype(t: &SqlDataType) -> DataType {
    match t {
        SqlDataType::Integer => DataType::Integer(0),
        SqlDataType::BigInt => DataType::BigInt(0),
        SqlDataType::Real => DataType::Float(0.0),
        SqlDataType::Double => DataType::Double(0.0),
        SqlDataType::Decimal { .. } => DataType::Double(0.0),
        SqlDataType::Text => DataType::Text(String::new()),
        SqlDataType::Varchar { .. } => DataType::Varchar(String::new()),
        SqlDataType::Boolean => DataType::Boolean(false),
        SqlDataType::Date => DataType::Date(String::new()),
        SqlDataType::Time => DataType::Time(String::new()),
        SqlDataType::Timestamp => DataType::Timestamp(String::new()),
        SqlDataType::Blob => DataType::Blob(Vec::new()),
    }
}

fn table_schema_from_create_table(ct: &CreateTableStatement) -> Result<TableSchema, EngineError> {
    use crate::common::types::Column;
    let mut columns: Vec<Column> = Vec::new();
    let mut checks: Vec<CheckConstraint> = Vec::new();
    let mut primary_key: Option<(String, Vec<String>)> = None;
    let mut unique_constraints: Vec<UniqueConstraintDef> = Vec::new();
    let mut foreign_keys: Vec<ForeignKeyConstraintDef> = Vec::new();

    for c in &ct.columns {
        let mut col = Column::new(c.name.clone(), sql_type_to_column_datatype(&c.data_type));
        for cc in &c.constraints {
            match cc {
                ColumnConstraint::NotNull => col.not_null = true,
                ColumnConstraint::Default(expr) => {
                    let cv = expr_to_column_value(expr)?;
                    col.default_value = Some(cv);
                }
                ColumnConstraint::Check(expr) => {
                    checks.push(CheckConstraint {
                        name: format!("chk_{}_{}", ct.table_name, c.name),
                        expr: expr.clone(),
                    });
                }
                ColumnConstraint::Unique => {
                    unique_constraints.push(UniqueConstraintDef {
                        name: format!("uq_{}_{}", ct.table_name, c.name),
                        columns: vec![c.name.clone()],
                    });
                }
                ColumnConstraint::PrimaryKey => {
                    if primary_key.is_some() {
                        return Err(EngineError::new(
                            engine_error_code::CONSTRAINT_VIOLATION,
                            "multiple PRIMARY KEY definitions",
                        ));
                    }
                    primary_key = Some((format!("pk_{}", ct.table_name), vec![c.name.clone()]));
                }
                ColumnConstraint::References { table, column } => {
                    let ref_cols = match column {
                        Some(rc) => vec![rc.clone()],
                        None => Vec::new(),
                    };
                    foreign_keys.push(ForeignKeyConstraintDef {
                        name: format!("fk_{}_{}", ct.table_name, c.name),
                        columns: vec![c.name.clone()],
                        referenced_table: table.clone(),
                        referenced_columns: ref_cols,
                    });
                }
            }
        }
        columns.push(col);
    }

    for tc in &ct.constraints {
        match tc {
            TableConstraint::PrimaryKey(cols) => {
                if primary_key.is_some() {
                    return Err(EngineError::new(
                        engine_error_code::CONSTRAINT_VIOLATION,
                        "multiple PRIMARY KEY definitions",
                    ));
                }
                primary_key = Some((format!("pk_{}", ct.table_name), cols.clone()));
            }
            TableConstraint::Unique(cols) => {
                unique_constraints.push(UniqueConstraintDef {
                    name: format!("uq_{}_{}", ct.table_name, unique_constraints.len()),
                    columns: cols.clone(),
                });
            }
            TableConstraint::ForeignKey {
                columns,
                referenced_table,
                referenced_columns,
            } => {
                foreign_keys.push(ForeignKeyConstraintDef {
                    name: format!("fk_{}_{}", ct.table_name, foreign_keys.len()),
                    columns: columns.clone(),
                    referenced_table: referenced_table.clone(),
                    referenced_columns: referenced_columns.clone(),
                });
            }
            TableConstraint::Check(expr) => {
                checks.push(CheckConstraint {
                    name: format!("chk_{}_{}", ct.table_name, checks.len()),
                    expr: expr.clone(),
                });
            }
        }
    }

    Ok(TableSchema {
        table_name: ct.table_name.clone(),
        columns,
        primary_key,
        unique_constraints,
        foreign_keys,
        check_constraints: checks,
        secondary_indexes: Vec::new(),
    })
}

fn validate_new_table_fks(cat: &SchemaManager, schema: &TableSchema) -> Result<(), EngineError> {
    for fk in &schema.foreign_keys {
        let parent = cat.schema(&fk.referenced_table).ok_or_else(|| {
            EngineError::new(
                engine_error_code::CONSTRAINT_VIOLATION,
                format!(
                    "foreign key {}: referenced table {} does not exist",
                    fk.name, fk.referenced_table
                ),
            )
        })?;
        let parent_cols = if fk.referenced_columns.is_empty() {
            parent
                .primary_key
                .as_ref()
                .map(|(_, c)| c.as_slice())
                .ok_or_else(|| {
                    EngineError::new(
                        engine_error_code::CONSTRAINT_VIOLATION,
                        format!(
                            "foreign key {}: referenced table {} has no PRIMARY KEY",
                            fk.name, fk.referenced_table
                        ),
                    )
                })?
        } else {
            fk.referenced_columns.as_slice()
        };
        if fk.columns.len() != parent_cols.len() {
            return Err(EngineError::new(
                engine_error_code::CONSTRAINT_VIOLATION,
                format!(
                    "foreign key {}: column count does not match referenced key",
                    fk.name
                ),
            ));
        }
    }
    Ok(())
}

fn apply_table_constraint_to_schema(
    schema: &mut TableSchema,
    user_name: Option<&str>,
    tc: &TableConstraint,
) -> Result<(), EngineError> {
    match tc {
        TableConstraint::PrimaryKey(cols) => {
            if schema.primary_key.is_some() {
                return Err(EngineError::new(
                    engine_error_code::CONSTRAINT_VIOLATION,
                    "multiple PRIMARY KEY definitions",
                ));
            }
            let pk_name = user_name.unwrap_or("PRIMARY").to_string();
            schema.primary_key = Some((pk_name, cols.clone()));
            Ok(())
        }
        TableConstraint::Unique(cols) => {
            let name = user_name.map(|s| s.to_string()).unwrap_or_else(|| {
                format!(
                    "uq_{}_{}",
                    schema.table_name,
                    schema.unique_constraints.len()
                )
            });
            schema.unique_constraints.push(UniqueConstraintDef {
                name,
                columns: cols.clone(),
            });
            Ok(())
        }
        TableConstraint::ForeignKey {
            columns,
            referenced_table,
            referenced_columns,
        } => {
            let name = user_name.map(|s| s.to_string()).unwrap_or_else(|| {
                format!("fk_{}_{}", schema.table_name, schema.foreign_keys.len())
            });
            schema.foreign_keys.push(ForeignKeyConstraintDef {
                name,
                columns: columns.clone(),
                referenced_table: referenced_table.clone(),
                referenced_columns: referenced_columns.clone(),
            });
            Ok(())
        }
        TableConstraint::Check(expr) => {
            let name = user_name.map(|s| s.to_string()).unwrap_or_else(|| {
                format!(
                    "chk_{}_{}",
                    schema.table_name,
                    schema.check_constraints.len()
                )
            });
            schema.check_constraints.push(CheckConstraint {
                name,
                expr: expr.clone(),
            });
            Ok(())
        }
    }
}

fn drop_constraint_by_name(schema: &mut TableSchema, name: &str) -> Result<(), EngineError> {
    if let Some((pk_name, _)) = &schema.primary_key {
        if pk_name == name {
            schema.primary_key = None;
            return Ok(());
        }
    }
    let before = schema.unique_constraints.len();
    schema.unique_constraints.retain(|u| u.name != name);
    if schema.unique_constraints.len() != before {
        return Ok(());
    }
    let before_fk = schema.foreign_keys.len();
    schema.foreign_keys.retain(|f| f.name != name);
    if schema.foreign_keys.len() != before_fk {
        return Ok(());
    }
    let before_chk = schema.check_constraints.len();
    schema.check_constraints.retain(|c| c.name != name);
    if schema.check_constraints.len() != before_chk {
        return Ok(());
    }
    Err(EngineError::new(
        engine_error_code::CONSTRAINT_VIOLATION,
        format!("constraint {name} not found"),
    ))
}

fn table_registration_order(cat: &SchemaManager) -> Result<Vec<String>, EngineError> {
    let mut remaining: HashSet<String> = cat.table_names().into_iter().collect();
    let mut result: Vec<String> = Vec::new();
    while !remaining.is_empty() {
        let mut batch: Vec<String> = Vec::new();
        for t in &remaining {
            let sch = cat.schema(t).expect("schema");
            let mut pending = false;
            for fk in &sch.foreign_keys {
                if fk.referenced_table == *t {
                    continue;
                }
                if remaining.contains(&fk.referenced_table) {
                    pending = true;
                    break;
                }
            }
            if !pending {
                batch.push(t.clone());
            }
        }
        if batch.is_empty() {
            return Err(EngineError::new(
                engine_error_code::CONSTRAINT_VIOLATION,
                "foreign key dependency cycle",
            ));
        }
        batch.sort();
        for t in batch {
            remaining.remove(&t);
            result.push(t);
        }
    }
    Ok(result)
}

fn rebuild_all_constraint_runtime(state: &SqlEngineState) -> Result<(), EngineError> {
    let cat = state
        .catalog
        .lock()
        .map_err(|_| lock_poisoned_engine())?
        .clone();
    {
        let mut rt = state
            .constraint_runtime
            .lock()
            .map_err(|_| lock_poisoned_engine())?;
        *rt = ConstraintRuntime::new();
    }
    let order = table_registration_order(&cat)?;
    for t in order {
        let schema = cat.schema(&t).expect("schema").clone();
        let pm = table_page_manager(state, &t)?;
        let snapshot = pm
            .lock()
            .map_err(|_| lock_poisoned_engine())?
            .select(None)
            .map_err(map_db_err)?;
        let mut rt = state
            .constraint_runtime
            .lock()
            .map_err(|_| lock_poisoned_engine())?;
        for (rid, data) in snapshot {
            let tuple = Tuple::from_bytes(&data).map_err(map_db_err)?;
            if let Err(e) = sql_constraints::register_row(&mut rt, &t, rid, &tuple, &schema, &cat) {
                // On open, we rebuild runtime PK/UNIQUE/FK maps from persisted heap rows.
                // If a row is missing a key column (or has NULL in a key), the heap is already
                // inconsistent with the catalog. Prefer keeping the database open and skipping
                // the bad row, so later statements can surface proper constraint errors.
                //
                // This path is intentionally narrow: we only swallow the specific "missing key"
                // / "NULL key" failures that otherwise brick the database on startup.
                let msg = e.message.to_ascii_lowercase();
                let is_missing_key = msg.contains("missing value for key column");
                let is_null_key = msg.contains("null key column value");
                if e.code == engine_error_code::CONSTRAINT_VIOLATION
                    && (is_missing_key || is_null_key)
                {
                    tracing::warn!(
                        table = %t,
                        rid = rid,
                        err = %e,
                        "skipping heap row during constraint rebuild (invalid key)"
                    );
                    continue;
                }
                return Err(e);
            }
        }
    }
    Ok(())
}

/// Runs a TPC-C native transaction: `BEGIN`, `f`, `COMMIT` (or `ROLLBACK` on error).
pub(crate) fn tpcc_run_in_transaction(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
    kind: u8,
    f: impl FnOnce(&SqlEngineState, &mut SessionContext) -> Result<u64, EngineError>,
) -> Result<EngineOutput, EngineError> {
    ctx.tpcc_kind = Some(kind);
    ctx.tpcc_dml_done_at = None;
    ctx.last_commit_flush_phases = None;
    ctx.tpcc_row_bytes_buf.clear();
    ctx.txn_pm_cache.clear();
    ctx.tpcc_index_column_map_buf.clear();
    begin_transaction(state, ctx)?;
    match f(state, ctx) {
        Ok(rows) => {
            let pre_commit_us = ctx
                .tpcc_dml_done_at
                .take()
                .map_or(0, |t0| t0.elapsed().as_micros() as u64);
            if sql_phase_log_enabled() {
                let t0 = Instant::now();
                commit_transaction(state, ctx)?;
                let commit_transaction_wall_us = t0.elapsed().as_micros() as u64;
                let flush_phases = ctx.last_commit_flush_phases.take();
                if let Some(p) = flush_phases {
                    let accounted_flush_us =
                        p.pm_lock_wait_us.saturating_add(p.heap_fsync_us);
                    info!(
                        target: "rustdb::sql_phases",
                        tpcc_kind = kind,
                        commit_us = commit_transaction_wall_us,
                        commit_transaction_wall_us,
                        pre_commit_us,
                        commit_pm_lock_wait_us = p.pm_lock_wait_us,
                        commit_heap_fsync_us = p.heap_fsync_us,
                        commit_gap_us = commit_transaction_wall_us
                            .saturating_sub(accounted_flush_us),
                        "sql.execute_tpcc.commit"
                    );
                } else {
                    info!(
                        target: "rustdb::sql_phases",
                        tpcc_kind = kind,
                        commit_us = commit_transaction_wall_us,
                        commit_transaction_wall_us,
                        pre_commit_us,
                        commit_gap_us = commit_transaction_wall_us,
                        "sql.execute_tpcc.commit"
                    );
                }
                if kind == 0 && pre_commit_us > 0 {
                    info!(
                        target: "rustdb::sql_phases",
                        pre_commit_us,
                        "sql.execute_tpcc.new_order_pre_commit"
                    );
                }
            } else {
                commit_transaction(state, ctx)?;
            }
            ctx.tpcc_kind = None;
            Ok(EngineOutput::ExecutionOk {
                rows_affected: rows,
            })
        }
        Err(e) => {
            ctx.tpcc_kind = None;
            ctx.tpcc_dml_done_at = None;
            let _ = rollback_transaction(state, ctx);
            Err(e)
        }
    }
}

pub(crate) fn int_column_value(n: i32) -> ColumnValue {
    ColumnValue::new(DataType::Integer(n))
}

pub(crate) fn tuple_i32_field(tuple: &Tuple, col: &str) -> Result<i32, EngineError> {
    let cv = tuple.values.get(col).ok_or_else(|| {
        EngineError::new(engine_error_code::INTERNAL, format!("missing column {col}"))
    })?;
    match &cv.data_type {
        DataType::Integer(v) => Ok(*v),
        DataType::BigInt(v) => i32::try_from(*v).map_err(|_| {
            EngineError::new(
                engine_error_code::INTERNAL,
                format!("{col} out of i32 range"),
            )
        }),
        other => Err(EngineError::new(
            engine_error_code::INTERNAL,
            format!("{col} is not integer: {other:?}"),
        )),
    }
}

pub(crate) fn equalities_map_i32(pairs: &[(&str, i32)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(c, v)| (c.to_string(), v.to_string()))
        .collect()
}

/// Synthetic row-lock key from primary-key column values (not a heap [`RecordId`]).
fn pk_row_lock_rid(table: &str, pk_values: &[String]) -> RecordId {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    table.hash(&mut h);
    for v in pk_values {
        v.hash(&mut h);
    }
    h.finish() | (1u64 << 63)
}

fn table_has_primary_key(state: &SqlEngineState, table: &str) -> bool {
    state
        .catalog
        .lock()
        .ok()
        .and_then(|cat| cat.schema(table).map(|s| s.primary_key.is_some()))
        .unwrap_or(false)
}

fn pk_row_lock_rid_for_tuple(
    state: &SqlEngineState,
    table: &str,
    tuple: &Tuple,
) -> Option<RecordId> {
    let cat = state.catalog.lock().ok()?;
    let schema = cat.schema(table)?.clone();
    let (_, cols) = schema.primary_key.as_ref()?;
    let key = sql_constraints::composite_key_from_tuple_with_schema(tuple, cols, &schema).ok()?;
    Some(pk_row_lock_rid(table, &[key]))
}

fn insert_row_with_optional_pk_lock<R>(
    state: &SqlEngineState,
    table: &str,
    tuple: &Tuple,
    f: impl FnOnce() -> Result<R, EngineError>,
) -> Result<R, EngineError> {
    if let Some(rid) = pk_row_lock_rid_for_tuple(state, table, tuple) {
        state.row_locks.with_write_locks(table, vec![rid], f)
    } else {
        f()
    }
}

/// Inserts one heap row (page latch only). Registers constraints/indexes and WAL/undo when in a txn.
pub(crate) fn insert_row_tuple(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
    table: &str,
    tuple: Tuple,
) -> Result<(), EngineError> {
    record_touched_table(ctx, table);
    let pm_for_table = table_page_manager(state, table)?;
    if table_has_primary_key(state, table) {
        insert_heap_row_pk_serialized(state, ctx, table, &tuple, &pm_for_table)?;
        return Ok(());
    }
    let pk_lock = pk_row_lock_rid_for_tuple(state, table, &tuple);
    if let Some(rid) = pk_lock {
        state.row_locks.with_write_locks(table, vec![rid], || {
            insert_row_tuple_inner(state, ctx, table, tuple)
        })
    } else {
        insert_row_tuple_inner(state, ctx, table, tuple)
    }
}

fn insert_row_tuple_inner(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
    table: &str,
    tuple: Tuple,
) -> Result<(), EngineError> {
    record_touched_table(ctx, table);
    let bytes = tuple.to_bytes().map_err(map_db_err)?;
    let pm_for_table = table_page_manager(state, table)?;
    let mut pm = pm_for_table.lock().map_err(|_| lock_poisoned_engine())?;
    let ins = pm.insert(&bytes).map_err(map_db_err)?;
    maybe_simulate_dml_crash("insert_row");
    if let Some(tx) = ctx.transaction.as_mut() {
        if let Some(ref wal) = state.wal {
            let page_id = (ins.record_id >> 32) as u64;
            let off = (ins.record_id & 0xffff_ffff) as u32;
            let record_offset: u16 = off.try_into().map_err(|_| {
                EngineError::new(
                    engine_error_code::INTERNAL,
                    "record offset too large for WAL",
                )
            })?;
            wal.log_data_insert(tx, pm.file_id(), page_id, record_offset, &bytes)?;
        }
    }
    if let Err(e) = register_row_for_insert(state, table, ins.record_id, &tuple) {
        if let Some(tx) = ctx.transaction.as_mut() {
            if let Some(ref wal) = state.wal {
                let page_id = (ins.record_id >> 32) as u64;
                let off = (ins.record_id & 0xffff_ffff) as u32;
                let record_offset: u16 = off.try_into().map_err(|_| {
                    EngineError::new(
                        engine_error_code::INTERNAL,
                        "record offset too large for WAL",
                    )
                })?;
                wal.log_data_delete(tx, pm.file_id(), page_id, record_offset, bytes.clone())?;
            }
        }
        pm.delete(ins.record_id).map_err(map_db_err)?;
        pm.flush_dirty_pages().map_err(map_db_err)?;
        return Err(e);
    }
    push_undo(
        ctx,
        UndoEntry::Insert {
            table: table.to_string(),
            rid: ins.record_id,
            payload: bytes,
        },
    );
    Ok(())
}

fn insert_heap_row_after_bytes(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
    table: &str,
    tuple: &Tuple,
    pm: &mut PageManager,
    bytes: Vec<u8>,
    ins: crate::storage::page_manager::InsertResult,
) -> Result<(), EngineError> {
    maybe_simulate_dml_crash("insert_row");
    if let Some(tx) = ctx.transaction.as_mut() {
        if let Some(ref wal) = state.wal {
            let page_id = (ins.record_id >> 32) as u64;
            let off = (ins.record_id & 0xffff_ffff) as u32;
            let record_offset: u16 = off.try_into().map_err(|_| {
                EngineError::new(
                    engine_error_code::INTERNAL,
                    "record offset too large for WAL",
                )
            })?;
            wal.log_data_insert(tx, pm.file_id(), page_id, record_offset, &bytes)?;
        }
    }
    if let Err(e) = register_row_for_insert(state, table, ins.record_id, tuple) {
        if let Some(tx) = ctx.transaction.as_mut() {
            if let Some(ref wal) = state.wal {
                let page_id = (ins.record_id >> 32) as u64;
                let off = (ins.record_id & 0xffff_ffff) as u32;
                let record_offset: u16 = off.try_into().map_err(|_| {
                    EngineError::new(
                        engine_error_code::INTERNAL,
                        "record offset too large for WAL",
                    )
                })?;
                wal.log_data_delete(tx, pm.file_id(), page_id, record_offset, bytes.clone())?;
            }
        }
        pm.delete(ins.record_id).map_err(map_db_err)?;
        pm.flush_dirty_pages().map_err(map_db_err)?;
        return Err(e);
    }
    push_undo(
        ctx,
        UndoEntry::Insert {
            table: table.to_string(),
            rid: ins.record_id,
            payload: bytes,
        },
    );
    Ok(())
}

/// PK/UNIQUE tables: hold `constraint_runtime` across validate → heap insert → map commit.
fn insert_heap_row_pk_serialized(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
    table: &str,
    tuple: &Tuple,
    pm_for_table: &Arc<Mutex<PageManager>>,
) -> Result<u64, EngineError> {
    let mut rt = state
        .constraint_runtime
        .lock()
        .map_err(|_| lock_poisoned_engine())?;
    let cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
    let Some(schema) = cat.schema(table).cloned() else {
        drop(cat);
        drop(rt);
        let bytes = tuple.to_bytes().map_err(map_db_err)?;
        let mut pm = pm_for_table.lock().map_err(|_| lock_poisoned_engine())?;
        let ins = pm.insert(&bytes).map_err(map_db_err)?;
        insert_heap_row_after_bytes(state, ctx, table, tuple, &mut pm, bytes, ins)?;
        return Ok(1);
    };
    let snapshot = cat.clone();
    drop(cat);

    sql_constraints::validate_new_row_for_insert(&rt, table, tuple, &schema, &snapshot)?;

    let bytes = tuple.to_bytes().map_err(map_db_err)?;
    let mut pm = pm_for_table.lock().map_err(|_| lock_poisoned_engine())?;
    let ins = pm.insert(&bytes).map_err(map_db_err)?;
    maybe_simulate_dml_crash("insert_row");
    if let Some(tx) = ctx.transaction.as_mut() {
        if let Some(ref wal) = state.wal {
            let page_id = (ins.record_id >> 32) as u64;
            let off = (ins.record_id & 0xffff_ffff) as u32;
            let record_offset: u16 = off.try_into().map_err(|_| {
                EngineError::new(
                    engine_error_code::INTERNAL,
                    "record offset too large for WAL",
                )
            })?;
            wal.log_data_insert(tx, pm.file_id(), page_id, record_offset, &bytes)?;
        }
    }
    if let Err(e) = sql_constraints::commit_row_for_insert(
        &mut rt,
        table,
        ins.record_id,
        tuple,
        &schema,
        &snapshot,
    ) {
        if let Some(tx) = ctx.transaction.as_mut() {
            if let Some(ref wal) = state.wal {
                let page_id = (ins.record_id >> 32) as u64;
                let off = (ins.record_id & 0xffff_ffff) as u32;
                let record_offset: u16 = off.try_into().map_err(|_| {
                    EngineError::new(
                        engine_error_code::INTERNAL,
                        "record offset too large for WAL",
                    )
                })?;
                wal.log_data_delete(tx, pm.file_id(), page_id, record_offset, bytes.clone())?;
            }
        }
        pm.delete(ins.record_id).map_err(map_db_err)?;
        pm.flush_dirty_pages().map_err(map_db_err)?;
        return Err(e);
    }
    drop(rt);
    sync_index_after_insert(state, table, ins.record_id, tuple)?;
    push_undo(
        ctx,
        UndoEntry::Insert {
            table: table.to_string(),
            rid: ins.record_id,
            payload: bytes,
        },
    );
    Ok(1)
}

fn insert_heap_row_in_execute_insert(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
    table: &str,
    tuple: &Tuple,
    pm_for_table: &Arc<Mutex<PageManager>>,
) -> Result<u64, EngineError> {
    if table_has_primary_key(state, table) {
        return insert_heap_row_pk_serialized(state, ctx, table, tuple, pm_for_table);
    }
    insert_row_with_optional_pk_lock(state, table, tuple, || {
        let bytes = tuple.to_bytes().map_err(map_db_err)?;
        let mut pm = pm_for_table.lock().map_err(|_| lock_poisoned_engine())?;
        let ins = pm.insert(&bytes).map_err(map_db_err)?;
        insert_heap_row_after_bytes(state, ctx, table, tuple, &mut pm, bytes, ins)?;
        Ok(1u64)
    })
}

/// Optional lock vs row-update timing for native TPC-C phase logs.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RowUpdatePhaseUs {
    pub lock_us: u64,
    pub update_us: u64,
}

/// Index-backed UPDATE with per-row write locks when the predicate is an exact index key.
pub(crate) fn update_rows_by_equalities<F>(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
    table: &str,
    equalities: &HashMap<String, String>,
    mut update_fn: F,
    phase_us: Option<&mut RowUpdatePhaseUs>,
) -> Result<u64, EngineError>
where
    F: FnMut(&mut Tuple) -> Result<(), EngineError>,
{
    record_touched_table(ctx, table);
    let pm_for_table = table_page_manager_cached(state, ctx, table)?;
    let mut pm = pm_for_table.lock().map_err(|_| lock_poisoned_engine())?;
    let (rows, exact_key) = {
        let ir = state
            .index_registry
            .read()
            .map_err(|_| lock_poisoned_engine())?;
        let Some((rids, exact_key)) = ir
            .lookup_record_ids_by_equalities(table, equalities)
            .map_err(|e| {
                EngineError::new(engine_error_code::INTERNAL, format!("index lookup: {e}"))
            })?
        else {
            return Err(EngineError::new(
                engine_error_code::INTERNAL,
                format!("no index for UPDATE on {table}"),
            ));
        };
        let mut rows = Vec::with_capacity(rids.len());
        for rid in rids {
            if let Some(data) = pm.get_record(rid).map_err(map_db_err)? {
                rows.push((rid, data));
            }
        }
        (rows, exact_key)
    };
    drop(pm);

    if exact_key && rows.is_empty() {
        return Ok(0);
    }
    let row_lock_rids: Vec<RecordId> = if exact_key {
        rows.iter().map(|(rid, _)| *rid).collect()
    } else {
        Vec::new()
    };

    let apply = move || -> Result<u64, EngineError> {
        let mut pm = pm_for_table.lock().map_err(|_| lock_poisoned_engine())?;
        let mut rows_affected = 0u64;
        for (rid, data) in rows {
            let mut tuple = Tuple::from_bytes(&data).map_err(map_db_err)?;
            let old_tuple = tuple.clone();
            let cat_snapshot = {
                let cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
                cat.clone()
            };
            let schema = cat_snapshot.schema(table).cloned();
            let tracks = schema.as_ref().is_some_and(table_tracks_constraints);
            if tracks {
                if let Some(ref sch) = schema {
                    let mut rt = state
                        .constraint_runtime
                        .lock()
                        .map_err(|_| lock_poisoned_engine())?;
                    sql_constraints::unregister_row(
                        &mut rt,
                        table,
                        rid,
                        &old_tuple,
                        sch,
                        &cat_snapshot,
                    )?;
                }
            }
            update_fn(&mut tuple)?;
            apply_defaults_and_validate(state, table, &mut tuple)?;
            if tracks {
                if let Some(ref sch) = schema {
                    let mut rt = state
                        .constraint_runtime
                        .lock()
                        .map_err(|_| lock_poisoned_engine())?;
                    sql_constraints::register_row(&mut rt, table, rid, &tuple, sch, &cat_snapshot)?;
                }
            }
            let new_bytes = tuple.to_bytes().map_err(map_db_err)?;
            if let Some(tx) = ctx.transaction.as_mut() {
                if let Some(ref wal) = state.wal {
                    let page_id = (rid >> 32) as u64;
                    let off = (rid & 0xffff_ffff) as u32;
                    let record_offset: u16 = off.try_into().map_err(|_| {
                        EngineError::new(
                            engine_error_code::INTERNAL,
                            "record offset too large for WAL",
                        )
                    })?;
                    wal.log_data_update(
                        tx,
                        pm.file_id(),
                        page_id,
                        record_offset,
                        data.clone(),
                        new_bytes.clone(),
                    )?;
                }
            }
            pm.update(rid, &new_bytes).map_err(map_db_err)?;
            sync_index_after_update(state, table, rid, &old_tuple, &tuple)?;
            push_undo(
                ctx,
                UndoEntry::Update {
                    table: table.to_string(),
                    rid,
                    old_payload: data,
                },
            );
            rows_affected += 1;
        }
        flush_heap_after_dml_success(state, ctx, &mut pm)?;
        Ok(rows_affected)
    };

    let profile_phases = phase_us.is_some() && sql_phase_log_enabled();
    let mut update_us_acc = 0u64;
    let timed_apply = || {
        let update_t0 = profile_phases.then(Instant::now);
        let rows = apply()?;
        if profile_phases {
            update_us_acc = update_t0
                .map(|t0| t0.elapsed().as_micros() as u64)
                .unwrap_or(0);
        }
        Ok(rows)
    };

    if exact_key {
        let lock_t0 = profile_phases.then(Instant::now);
        let rows = state
            .row_locks
            .with_write_locks(table, row_lock_rids, timed_apply)?;
        if let Some(out) = phase_us {
            out.lock_us = lock_t0
                .map(|t0| t0.elapsed().as_micros() as u64)
                .unwrap_or(0);
            out.update_us = update_us_acc;
        }
        return Ok(rows);
    }
    let lock_t0 = profile_phases.then(Instant::now);
    let lock = table_storage_lock_arc(state, table)?;
    let _guard = acquire_table_storage_write_lock(&lock, table)?;
    let rows = timed_apply()?;
    if let Some(out) = phase_us {
        out.lock_us = lock_t0
            .map(|t0| t0.elapsed().as_micros() as u64)
            .unwrap_or(0);
        out.update_us = update_us_acc;
    }
    Ok(rows)
}

/// Index-backed DELETE; uses row locks on exact single-key lookups.
pub(crate) fn delete_rows_by_equalities(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
    table: &str,
    equalities: &HashMap<String, String>,
) -> Result<u64, EngineError> {
    record_touched_table(ctx, table);
    let pm_for_table = table_page_manager_cached(state, ctx, table)?;
    let mut pm = pm_for_table.lock().map_err(|_| lock_poisoned_engine())?;
    let (rows, exact_key) = {
        let ir = state
            .index_registry
            .read()
            .map_err(|_| lock_poisoned_engine())?;
        let Some((rids, exact_key)) = ir
            .lookup_record_ids_by_equalities(table, equalities)
            .map_err(|e| {
                EngineError::new(engine_error_code::INTERNAL, format!("index lookup: {e}"))
            })?
        else {
            return Err(EngineError::new(
                engine_error_code::INTERNAL,
                format!("no index for DELETE on {table}"),
            ));
        };
        let mut rows = Vec::with_capacity(rids.len());
        for rid in rids {
            if let Some(data) = pm.get_record(rid).map_err(map_db_err)? {
                rows.push((rid, data));
            }
        }
        (rows, exact_key)
    };
    drop(pm);

    if exact_key && rows.is_empty() {
        return Ok(0);
    }
    let row_lock_rids: Vec<RecordId> = if exact_key {
        rows.iter().map(|(rid, _)| *rid).collect()
    } else {
        Vec::new()
    };

    let apply = move || -> Result<u64, EngineError> {
        let mut pm = pm_for_table.lock().map_err(|_| lock_poisoned_engine())?;
        let mut rows_affected = 0u64;
        for (rid, data) in rows {
            let tuple = Tuple::from_bytes(&data).map_err(map_db_err)?;
            let cat_snapshot = {
                let cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
                cat.clone()
            };
            if let Some(sch) = cat_snapshot.schema(table) {
                if table_tracks_constraints(sch) {
                    let mut rt = state
                        .constraint_runtime
                        .lock()
                        .map_err(|_| lock_poisoned_engine())?;
                    sql_constraints::unregister_row(
                        &mut rt,
                        table,
                        rid,
                        &tuple,
                        sch,
                        &cat_snapshot,
                    )?;
                }
            }
            if let Some(tx) = ctx.transaction.as_mut() {
                if let Some(ref wal) = state.wal {
                    let page_id = (rid >> 32) as u64;
                    let off = (rid & 0xffff_ffff) as u32;
                    let record_offset: u16 = off.try_into().map_err(|_| {
                        EngineError::new(
                            engine_error_code::INTERNAL,
                            "record offset too large for WAL",
                        )
                    })?;
                    wal.log_data_delete(tx, pm.file_id(), page_id, record_offset, data.clone())?;
                }
            }
            match pm.delete(rid) {
                Ok(_) => {}
                Err(e) if db_err_is_record_not_found(&e) => continue,
                Err(e) => return Err(map_db_err(e)),
            }
            sync_index_after_delete(state, table, rid, &tuple)?;
            push_undo(
                ctx,
                UndoEntry::Delete {
                    table: table.to_string(),
                    rid,
                    payload: data,
                },
            );
            rows_affected += 1;
        }
        flush_heap_after_dml_success(state, ctx, &mut pm)?;
        Ok(rows_affected)
    };

    if exact_key {
        return state
            .row_locks
            .with_write_locks(table, row_lock_rids, apply);
    }
    let lock = table_storage_lock_arc(state, table)?;
    let _guard = acquire_table_storage_write_lock(&lock, table)?;
    apply()
}

/// Order-status read via exact index key (no table storage read lock when index is exact).
pub(crate) fn tpcc_order_status_row_count(
    state: &SqlEngineState,
    w_id: i32,
    d_id: i32,
    c_id: i32,
) -> Result<u64, EngineError> {
    let equalities = equalities_map_i32(&[("o_w_id", w_id), ("o_d_id", d_id), ("o_c_id", c_id)]);
    let ir = state
        .index_registry
        .read()
        .map_err(|_| lock_poisoned_engine())?;
    let Some((rids, exact_key)) = ir
        .lookup_record_ids_by_equalities("oorder", &equalities)
        .map_err(|e| EngineError::new(engine_error_code::INTERNAL, format!("index lookup: {e}")))?
    else {
        return Ok(0);
    };
    if exact_key {
        return Ok(rids.len() as u64);
    }
    drop(ir);
    let pm = table_page_manager(state, "oorder")?;
    let mut pm = pm.lock().map_err(|_| lock_poisoned_engine())?;
    let mut count = 0u64;
    for rid in rids {
        if pm.get_record(rid).map_err(map_db_err)?.is_some() {
            count += 1;
        }
    }
    Ok(count)
}

/// Stock-level read: index prefix on `s_w_id`, filter `s_qty < threshold` on heap (no table read lock).
pub(crate) fn tpcc_stock_level_row_count(
    state: &SqlEngineState,
    w_id: i32,
    threshold: i32,
) -> Result<u64, EngineError> {
    let equalities = equalities_map_i32(&[("s_w_id", w_id)]);
    let ir = state
        .index_registry
        .read()
        .map_err(|_| lock_poisoned_engine())?;
    let Some((rids, _)) = ir
        .lookup_record_ids_by_equalities("stock", &equalities)
        .map_err(|e| EngineError::new(engine_error_code::INTERNAL, format!("index lookup: {e}")))?
    else {
        return Ok(0);
    };
    drop(ir);
    let pm = table_page_manager(state, "stock")?;
    let mut pm = pm.lock().map_err(|_| lock_poisoned_engine())?;
    let mut count = 0u64;
    for rid in rids {
        let Some(data) = pm.get_record(rid).map_err(map_db_err)? else {
            continue;
        };
        let tuple = Tuple::from_bytes(&data).map_err(map_db_err)?;
        if tuple_i32_field(&tuple, "s_qty")? < threshold {
            count += 1;
        }
    }
    Ok(count)
}

fn table_tracks_constraints(schema: &TableSchema) -> bool {
    schema.primary_key.is_some()
        || !schema.unique_constraints.is_empty()
        || !schema.foreign_keys.is_empty()
}

fn register_row_for_insert(
    state: &SqlEngineState,
    table: &str,
    rid: RecordId,
    tuple: &Tuple,
) -> Result<(), EngineError> {
    let cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
    let Some(schema) = cat.schema(table).cloned() else {
        return Ok(());
    };
    if !table_tracks_constraints(&schema) {
        drop(cat);
        return sync_index_after_insert(state, table, rid, tuple);
    }
    let snapshot = cat.clone();
    drop(cat);
    let mut rt = state
        .constraint_runtime
        .lock()
        .map_err(|_| lock_poisoned_engine())?;
    sql_constraints::register_row(&mut rt, table, rid, tuple, &schema, &snapshot)?;
    drop(rt);
    sync_index_after_insert(state, table, rid, tuple)?;
    Ok(())
}

fn apply_defaults_and_validate(
    state: &SqlEngineState,
    table: &str,
    tuple: &mut Tuple,
) -> Result<(), EngineError> {
    let schema = {
        let cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
        cat.schema(table).cloned()
    };
    let Some(schema) = schema else {
        return Ok(());
    };

    // Apply DEFAULT for missing columns.
    for c in &schema.columns {
        if !tuple.values.contains_key(&c.name) {
            if let Some(def) = &c.default_value {
                tuple.set_value(&c.name, def.clone());
            }
        }
    }

    // Enforce NOT NULL
    for c in &schema.columns {
        if c.not_null {
            match tuple.values.get(&c.name) {
                Some(v) if !v.is_null() => {}
                _ => {
                    return Err(EngineError::new(
                        engine_error_code::PROTOCOL,
                        format!("NOT NULL constraint failed: {}.{}", table, c.name),
                    ))
                }
            }
        }
    }

    // Enforce CHECK
    if !schema.check_constraints.is_empty() {
        let mut row = Row::new();
        row.values = tuple.values.clone();
        for chk in &schema.check_constraints {
            if !eval_predicate_expression(&row, &chk.expr) {
                return Err(EngineError::new(
                    engine_error_code::PROTOCOL,
                    format!("CHECK constraint failed: {}", chk.name),
                ));
            }
        }
    }
    Ok(())
}

fn column_name_expr(expr: &Expression) -> Option<String> {
    match expr {
        Expression::Identifier(s) => Some(s.clone()),
        Expression::QualifiedIdentifier { column, .. } => Some(column.clone()),
        _ => None,
    }
}

fn value_expr(expr: &Expression) -> Option<&Literal> {
    match expr {
        Expression::Literal(l) => Some(l),
        _ => None,
    }
}

fn expr_to_column_value(expr: &Expression) -> Result<ColumnValue, EngineError> {
    let row = Row::with_capacity(0);
    Ok(eval_scalar_expression(&row, expr))
}

fn literal_to_column_value(l: &Literal) -> ColumnValue {
    ColumnValue::new(literal_to_data_type(l))
}

fn literal_to_data_type(l: &Literal) -> DataType {
    match l {
        Literal::Null => DataType::Null,
        Literal::Boolean(b) => DataType::Boolean(*b),
        Literal::Integer(n) => {
            if *n >= i32::MIN as i64 && *n <= i32::MAX as i64 {
                DataType::Integer(*n as i32)
            } else {
                DataType::BigInt(*n)
            }
        }
        Literal::Float(f) => DataType::Double(*f),
        Literal::String(s) => DataType::Varchar(s.clone()),
    }
}

fn eval_select_without_from(sel: &SelectStatement) -> Result<EngineOutput, EngineError> {
    let mut columns = Vec::new();
    let mut row = Vec::new();
    for (i, item) in sel.select_list.iter().enumerate() {
        match item {
            SelectItem::Wildcard => {
                return Err(EngineError::new(
                    engine_error_code::UNSUPPORTED_SQL,
                    "SELECT * requires a FROM clause",
                ));
            }
            SelectItem::Expression { expr, alias } => {
                let name = alias.clone().unwrap_or_else(|| format!("col{}", i + 1));
                columns.push(name);
                row.push(expr_to_string(expr)?);
            }
        }
    }
    Ok(EngineOutput::ResultSet {
        columns,
        rows: vec![row],
    })
}

fn expr_to_string(expr: &Expression) -> Result<String, EngineError> {
    match expr {
        Expression::Literal(l) => Ok(literal_to_string(l)),
        _ => Err(EngineError::new(
            engine_error_code::UNSUPPORTED_SQL,
            "only literal expressions are supported in SELECT without FROM",
        )),
    }
}

fn literal_to_string(l: &Literal) -> String {
    match l {
        Literal::Null => "NULL".to_string(),
        Literal::Boolean(b) => b.to_string(),
        Literal::Integer(n) => n.to_string(),
        Literal::Float(f) => f.to_string(),
        Literal::String(s) => s.clone(),
    }
}

fn rows_to_engine_output(rows: Vec<Row>) -> Result<EngineOutput, EngineError> {
    if rows.is_empty() {
        return Ok(EngineOutput::ResultSet {
            columns: vec![],
            rows: vec![],
        });
    }
    let mut columns: Vec<String> = rows[0].values.keys().cloned().collect();
    columns.sort();
    let data: Vec<Vec<String>> = rows
        .iter()
        .map(|r| {
            columns
                .iter()
                .map(|c| {
                    r.values
                        .get(c)
                        .map(|cv| format!("{:?}", cv.data_type))
                        .unwrap_or_else(|| "NULL".to_string())
                })
                .collect()
        })
        .collect();
    Ok(EngineOutput::ResultSet {
        columns,
        rows: data,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn sql_engine_select_without_from() {
        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        let out = eng.execute_sql("SELECT 1, 2", &mut ctx).expect("ok");
        match out {
            EngineOutput::ResultSet { columns, rows } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0], vec!["1", "2"]);
                assert_eq!(columns.len(), 2);
            }
            _ => panic!("expected result set"),
        }
    }

    #[test]
    fn sql_engine_insert_execution_ok() {
        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        let out = eng
            .execute_sql("INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y')", &mut ctx)
            .expect("insert");
        assert_eq!(out, EngineOutput::ExecutionOk { rows_affected: 2 });
    }

    #[test]
    fn sql_engine_oorder_order_status_select_uses_index() {
        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        eng.execute_sql(
            "CREATE TABLE oorder (o_id INTEGER, o_d_id INTEGER, o_w_id INTEGER, o_c_id INTEGER, o_ol_cnt INTEGER)",
            &mut ctx,
        )
        .unwrap();
        eng.execute_sql(
            "CREATE INDEX idx_oorder_wdc ON oorder (o_w_id, o_d_id, o_c_id)",
            &mut ctx,
        )
        .unwrap();
        eng.execute_sql(
            "INSERT INTO oorder (o_id, o_d_id, o_w_id, o_c_id, o_ol_cnt) VALUES (1, 2, 1, 3, 1), (2, 2, 1, 4, 1), (3, 3, 1, 3, 1)",
            &mut ctx,
        )
        .unwrap();
        let out = eng
            .execute_sql(
                "SELECT o_id FROM oorder WHERE o_w_id = 1 AND o_d_id = 2 AND o_c_id = 3",
                &mut ctx,
            )
            .unwrap();
        match out {
            EngineOutput::ResultSet { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert!(rows[0][0].contains('1'));
            }
            _ => panic!("expected result set"),
        }
    }

    #[test]
    fn order_status_skips_oorder_table_read_lock() {
        use crate::parser::{SqlParser, SqlStatement};

        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        eng.execute_sql(
            "CREATE TABLE oorder (o_id INTEGER, o_d_id INTEGER, o_w_id INTEGER, o_c_id INTEGER, o_ol_cnt INTEGER)",
            &mut ctx,
        )
        .unwrap();
        eng.execute_sql(
            "CREATE INDEX idx_oorder_wdc ON oorder (o_w_id, o_d_id, o_c_id)",
            &mut ctx,
        )
        .unwrap();

        let sql = "SELECT * FROM oorder WHERE o_w_id = 1 AND o_d_id = 2 AND o_c_id = 3";
        let mut parser = SqlParser::new(sql).unwrap();
        let stmt = parser.parse_multiple().unwrap().remove(0);
        let SqlStatement::Select(sel) = stmt else {
            panic!("expected SELECT");
        };
        let state = eng.state_for_test();
        let table_names = collect_physical_tables_for_read_stmt(&SqlStatement::Select(sel.clone()));
        let plan = state
            .planner
            .create_plan(&SqlStatement::Select(sel))
            .unwrap();
        let optimized = state.optimizer.lock().unwrap().optimize(plan).unwrap();
        let skip =
            select_skip_table_read_lock_tables(state, &table_names, &optimized.optimized_plan.root);
        assert!(
            skip.contains("oorder"),
            "expected oorder in skip-read set, tables={table_names:?} plan={:?}",
            optimized.optimized_plan.root
        );
    }

    #[test]
    fn order_status_plan_uses_idx_oorder_wdc_index_scan() {
        use crate::parser::{SqlParser, SqlStatement};
        use crate::planner::{PlanNode, QueryPlanner};

        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        eng.execute_sql(
            "CREATE TABLE oorder (o_id INTEGER, o_d_id INTEGER, o_w_id INTEGER, o_c_id INTEGER, o_ol_cnt INTEGER)",
            &mut ctx,
        )
        .unwrap();
        eng.execute_sql(
            "CREATE INDEX idx_oorder_wdc ON oorder (o_w_id, o_d_id, o_c_id)",
            &mut ctx,
        )
        .unwrap();

        let sql = "SELECT o_id FROM oorder WHERE o_w_id = 1 AND o_d_id = 2 AND o_c_id = 3";
        let mut parser = SqlParser::new(sql).unwrap();
        let stmt = parser.parse_multiple().unwrap().remove(0);
        let SqlStatement::Select(sel) = stmt else {
            panic!("expected SELECT");
        };
        let state = eng.state_for_test();
        let plan = state
            .planner
            .create_plan(&SqlStatement::Select(sel))
            .unwrap();
        let optimized = state.optimizer.lock().unwrap().optimize(plan).unwrap();
        fn plan_has_oorder_index_scan(node: &PlanNode) -> bool {
            match node {
                PlanNode::IndexScan(idx) => {
                    idx.table_name == "oorder" && idx.index_name == "idx_oorder_wdc"
                }
                PlanNode::Filter(f) => plan_has_oorder_index_scan(&f.input),
                PlanNode::Projection(p) => plan_has_oorder_index_scan(&p.input),
                _ => false,
            }
        }
        assert!(
            plan_has_oorder_index_scan(&optimized.optimized_plan.root),
            "expected IndexScan on oorder.idx_oorder_wdc, got {:?}",
            optimized.optimized_plan.root
        );
    }

    #[test]
    fn concurrent_payment_updates_on_hot_warehouse_row() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::{Arc, Barrier};
        use std::thread;
        use std::time::{Duration, Instant};

        const HOT_WORKERS: usize = 8;
        const OTHER_WORKERS: usize = 4;
        const ITERS: u32 = 32;

        let dir = TempDir::new().unwrap();
        let eng = Arc::new(SqlEngine::open(dir.path().to_path_buf()).unwrap());
        let mut ctx = SessionContext::default();
        eng.execute_sql(
            "CREATE TABLE warehouse (w_id INTEGER, w_ytd INTEGER)",
            &mut ctx,
        )
        .unwrap();
        eng.execute_sql("CREATE INDEX idx_wh_id ON warehouse (w_id)", &mut ctx)
            .unwrap();
        eng.execute_sql(
            "INSERT INTO warehouse (w_id, w_ytd) VALUES (1, 0), (2, 0)",
            &mut ctx,
        )
        .unwrap();

        let progress = Arc::new(AtomicU64::new(0));
        let other_done = Arc::new(AtomicU64::new(0));
        let barrier = Arc::new(Barrier::new(HOT_WORKERS + OTHER_WORKERS + 1));
        let mut handles = Vec::new();

        for _ in 0..HOT_WORKERS {
            let eng = Arc::clone(&eng);
            let barrier = Arc::clone(&barrier);
            let progress = Arc::clone(&progress);
            handles.push(thread::spawn(move || {
                barrier.wait();
                for _ in 0..ITERS {
                    let mut ctx = SessionContext::default();
                    eng.execute_sql(
                        "UPDATE warehouse SET w_ytd = w_ytd + 1 WHERE w_id = 1",
                        &mut ctx,
                    )
                    .expect("concurrent payment update");
                    progress.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        for _ in 0..OTHER_WORKERS {
            let eng = Arc::clone(&eng);
            let barrier = Arc::clone(&barrier);
            let other_done = Arc::clone(&other_done);
            handles.push(thread::spawn(move || {
                barrier.wait();
                for _ in 0..ITERS {
                    let mut ctx = SessionContext::default();
                    eng.execute_sql(
                        "UPDATE warehouse SET w_ytd = w_ytd + 1 WHERE w_id = 2",
                        &mut ctx,
                    )
                    .expect("concurrent other-row update");
                }
                other_done.fetch_add(1, Ordering::Relaxed);
            }));
        }

        barrier.wait();
        let t0 = Instant::now();
        let deadline = Duration::from_secs(5);
        while other_done.load(Ordering::Relaxed) < OTHER_WORKERS as u64 {
            assert!(
                t0.elapsed() < deadline,
                "w_id=2 workers did not finish (table-wide lock would stall here)"
            );
            if progress.load(Ordering::Relaxed) > 0 {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        assert!(
            progress.load(Ordering::Relaxed) > 0,
            "hot-row updates made no progress while other rows finished"
        );

        for h in handles {
            h.join().expect("worker join");
        }

        let mut ctx = SessionContext::default();
        let out = eng
            .execute_sql("SELECT w_id, w_ytd FROM warehouse ORDER BY w_id", &mut ctx)
            .unwrap();
        match out {
            EngineOutput::ResultSet { rows, .. } => {
                assert_eq!(rows.len(), 2);
                let hot_ytd: i64 = rows[0][1]
                    .trim_start_matches("Integer(")
                    .trim_end_matches(')')
                    .parse()
                    .unwrap_or_else(|_| rows[0][1].parse().expect("numeric w_ytd"));
                let other_ytd: i64 = rows[1][1]
                    .trim_start_matches("Integer(")
                    .trim_end_matches(')')
                    .parse()
                    .unwrap_or_else(|_| rows[1][1].parse().expect("numeric w_ytd"));
                assert_eq!(
                    hot_ytd,
                    (HOT_WORKERS as i64) * (ITERS as i64),
                    "expected w_ytd for w_id=1"
                );
                assert_eq!(
                    other_ytd,
                    (OTHER_WORKERS as i64) * (ITERS as i64),
                    "expected w_ytd for w_id=2"
                );
            }
            _ => panic!("expected result set"),
        }
    }

    #[test]
    fn sql_engine_dml_index_lookup_exact_key() {
        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        eng.execute_sql(
            "CREATE TABLE warehouse (w_id INTEGER, w_ytd INTEGER)",
            &mut ctx,
        )
        .unwrap();
        eng.execute_sql("CREATE INDEX idx_wh_id ON warehouse (w_id)", &mut ctx)
            .unwrap();
        eng.execute_sql(
            "INSERT INTO warehouse (w_id, w_ytd) VALUES (1, 100), (2, 200)",
            &mut ctx,
        )
        .unwrap();
        let upd = eng
            .execute_sql("UPDATE warehouse SET w_ytd = 150 WHERE w_id = 1", &mut ctx)
            .unwrap();
        assert_eq!(upd, EngineOutput::ExecutionOk { rows_affected: 1 });
        let mut eq = HashMap::new();
        eq.insert("w_id".to_string(), "1".to_string());
        let indexed = eng
            .state_for_test()
            .index_registry
            .read()
            .expect("index registry read")
            .lookup_record_ids_by_equalities("warehouse", &eq)
            .expect("lookup")
            .expect("index hit");
        assert_eq!(indexed.0.len(), 1, "warehouse w_id=1 must be in index");
        let sel = eng
            .execute_sql("SELECT w_ytd FROM warehouse WHERE w_id = 1", &mut ctx)
            .unwrap();
        match sel {
            EngineOutput::ResultSet { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert!(rows[0][0].contains("150"));
            }
            _ => panic!("expected result set"),
        }
    }

    fn index_lookup_count(state: &SqlEngineState, table: &str, col: &str, val: &str) -> usize {
        let mut eq = HashMap::new();
        eq.insert(col.to_string(), val.to_string());
        state
            .index_registry
            .read()
            .expect("index read")
            .lookup_record_ids_by_equalities(table, &eq)
            .expect("lookup")
            .map(|(rids, _)| rids.len())
            .unwrap_or(0)
    }

    #[test]
    fn tpcc_deferred_index_visible_after_commit() {
        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let state = eng.state_for_test();
        let mut ctx = SessionContext::default();
        eng.execute_sql("CREATE TABLE bench_row (k INTEGER, v INTEGER)", &mut ctx)
            .unwrap();
        eng.execute_sql("CREATE INDEX idx_bench_k ON bench_row (k)", &mut ctx)
            .unwrap();
        eng.execute_sql("BEGIN TRANSACTION", &mut ctx).unwrap();
        insert_row_tuple_tpcc_deferred(state, &mut ctx, "bench_row", {
            let mut t = Tuple::new(1);
            t.set_value("k", int_column_value(10));
            t.set_value("v", int_column_value(1));
            t
        })
        .unwrap();
        insert_row_tuple_tpcc_deferred(state, &mut ctx, "bench_row", {
            let mut t = Tuple::new(2);
            t.set_value("k", int_column_value(20));
            t.set_value("v", int_column_value(2));
            t
        })
        .unwrap();
        assert_eq!(index_lookup_count(state, "bench_row", "k", "10"), 0);
        assert_eq!(index_lookup_count(state, "bench_row", "k", "20"), 0);
        eng.execute_sql("COMMIT", &mut ctx).unwrap();
        assert_eq!(index_lookup_count(state, "bench_row", "k", "10"), 1);
        assert_eq!(index_lookup_count(state, "bench_row", "k", "20"), 1);
    }

    #[test]
    fn tpcc_deferred_index_discarded_on_rollback() {
        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let state = eng.state_for_test();
        let mut ctx = SessionContext::default();
        eng.execute_sql("CREATE TABLE bench_row (k INTEGER, v INTEGER)", &mut ctx)
            .unwrap();
        eng.execute_sql("CREATE INDEX idx_bench_k ON bench_row (k)", &mut ctx)
            .unwrap();
        eng.execute_sql("BEGIN TRANSACTION", &mut ctx).unwrap();
        insert_row_tuple_tpcc_deferred(state, &mut ctx, "bench_row", {
            let mut t = Tuple::new(1);
            t.set_value("k", int_column_value(99));
            t.set_value("v", int_column_value(1));
            t
        })
        .unwrap();
        eng.execute_sql("ROLLBACK", &mut ctx).unwrap();
        assert_eq!(index_lookup_count(state, "bench_row", "k", "99"), 0);
        let left = eng
            .execute_sql("SELECT k FROM bench_row", &mut ctx)
            .unwrap();
        match left {
            EngineOutput::ResultSet { rows, .. } => assert!(rows.is_empty()),
            _ => panic!("expected empty result set"),
        }
    }

    #[test]
    fn native_delivery_empty_new_order_returns_execution_ok() {
        use crate::network::engine::EngineHandle;
        use crate::tpcc_workload::{txn_kind_as_u8, TxnKind};

        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        eng.execute_sql(
            "CREATE TABLE new_order (no_o_id INTEGER, no_d_id INTEGER, no_w_id INTEGER)",
            &mut ctx,
        )
        .unwrap();
        eng.execute_sql(
            "CREATE INDEX idx_new_order_wd ON new_order (no_w_id, no_d_id)",
            &mut ctx,
        )
        .unwrap();
        let out = eng
            .execute_tpcc(txn_kind_as_u8(TxnKind::Delivery), 42, 1, &mut ctx)
            .unwrap();
        assert_eq!(out, EngineOutput::ExecutionOk { rows_affected: 0 });
    }

    #[test]
    fn create_index_backfills_heap_for_delivery_style_delete() {
        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        eng.execute_sql(
            "CREATE TABLE new_order (no_o_id INTEGER, no_d_id INTEGER, no_w_id INTEGER)",
            &mut ctx,
        )
        .unwrap();
        eng.execute_sql(
            "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (1, 4, 1), (2, 4, 1), (3, 5, 1)",
            &mut ctx,
        )
        .unwrap();
        eng.execute_sql(
            "CREATE INDEX idx_new_order_wd ON new_order (no_w_id, no_d_id)",
            &mut ctx,
        )
        .unwrap();
        let del = eng
            .execute_sql(
                "DELETE FROM new_order WHERE no_w_id = 1 AND no_d_id = 4",
                &mut ctx,
            )
            .unwrap();
        assert_eq!(del, EngineOutput::ExecutionOk { rows_affected: 2 });
        let left = eng
            .execute_sql("SELECT no_o_id FROM new_order", &mut ctx)
            .unwrap();
        match left {
            EngineOutput::ResultSet { rows, .. } => assert_eq!(rows.len(), 1),
            _ => panic!("expected result set"),
        }
    }

    #[test]
    fn sql_engine_update_delete_use_composite_index() {
        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        eng.execute_sql(
            "CREATE TABLE stock (s_w_id INTEGER, s_i_id INTEGER, qty INTEGER)",
            &mut ctx,
        )
        .unwrap();
        eng.execute_sql(
            "CREATE INDEX idx_stock_wid_iid ON stock (s_w_id, s_i_id)",
            &mut ctx,
        )
        .unwrap();
        eng.execute_sql(
            "INSERT INTO stock (s_w_id, s_i_id, qty) VALUES (1, 4, 10), (1, 5, 20), (2, 4, 30)",
            &mut ctx,
        )
        .unwrap();

        let upd = eng
            .execute_sql(
                "UPDATE stock SET qty = 99 WHERE s_w_id = 1 AND s_i_id = 4",
                &mut ctx,
            )
            .unwrap();
        assert_eq!(upd, EngineOutput::ExecutionOk { rows_affected: 1 });

        let sel = eng.execute_sql("SELECT qty FROM stock", &mut ctx).unwrap();
        match sel {
            EngineOutput::ResultSet { rows, .. } => {
                assert_eq!(
                    rows.iter().filter(|r| r[0].contains("99")).count(),
                    1,
                    "expected one row with qty=99, got {:?}",
                    rows
                );
            }
            _ => panic!("expected result set"),
        }

        let del = eng
            .execute_sql(
                "DELETE FROM stock WHERE s_w_id = 1 AND s_i_id = 5",
                &mut ctx,
            )
            .unwrap();
        assert_eq!(del, EngineOutput::ExecutionOk { rows_affected: 1 });

        let left = eng
            .execute_sql("SELECT s_w_id, s_i_id FROM stock", &mut ctx)
            .unwrap();
        match left {
            EngineOutput::ResultSet { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("expected result set"),
        }
    }

    #[test]
    fn sql_engine_update_delete_use_single_column_index() {
        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        eng.execute_sql("CREATE TABLE t (a INTEGER, b INTEGER)", &mut ctx)
            .unwrap();
        eng.execute_sql("CREATE INDEX idx_t_a ON t (a)", &mut ctx)
            .unwrap();
        eng.execute_sql(
            "INSERT INTO t (a, b) VALUES (1, 10), (1, 11), (2, 20)",
            &mut ctx,
        )
        .unwrap();

        let upd = eng
            .execute_sql("UPDATE t SET b = 0 WHERE a = 1", &mut ctx)
            .unwrap();
        assert_eq!(upd, EngineOutput::ExecutionOk { rows_affected: 2 });

        let del = eng
            .execute_sql("DELETE FROM t WHERE a = 2", &mut ctx)
            .unwrap();
        assert_eq!(del, EngineOutput::ExecutionOk { rows_affected: 1 });

        let left = eng.execute_sql("SELECT a FROM t", &mut ctx).unwrap();
        match left {
            EngineOutput::ResultSet { rows, .. } => assert_eq!(rows.len(), 2),
            _ => panic!("expected result set"),
        }
    }

    #[test]
    fn sql_engine_update_delete_roundtrip_heap() {
        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        eng.execute_sql("INSERT INTO users (rid, nm) VALUES (10, 1)", &mut ctx)
            .unwrap();
        let out = eng
            .execute_sql("UPDATE users SET nm = 2 WHERE true", &mut ctx)
            .unwrap();
        assert_eq!(out, EngineOutput::ExecutionOk { rows_affected: 1 });
        let del = eng
            .execute_sql("DELETE FROM users WHERE true", &mut ctx)
            .unwrap();
        assert_eq!(del, EngineOutput::ExecutionOk { rows_affected: 1 });
    }

    #[test]
    fn sql_engine_update_arithmetic_set_rhs() {
        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        eng.execute_sql("CREATE TABLE cnt (id INT, v INT)", &mut ctx)
            .unwrap();
        eng.execute_sql("INSERT INTO cnt (id, v) VALUES (1, 10)", &mut ctx)
            .unwrap();
        let out = eng
            .execute_sql("UPDATE cnt SET v = v + 5 WHERE id = 1", &mut ctx)
            .unwrap();
        assert_eq!(out, EngineOutput::ExecutionOk { rows_affected: 1 });
        let sel = eng
            .execute_sql("SELECT v FROM cnt WHERE id = 1", &mut ctx)
            .unwrap();
        match sel {
            EngineOutput::ResultSet { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert!(
                    rows[0][0].contains("15"),
                    "expected v=15, got {:?}",
                    rows[0][0]
                );
            }
            _ => panic!("expected result set"),
        }
    }

    #[test]
    fn sql_engine_insert_values_arithmetic_literal() {
        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        eng.execute_sql("CREATE TABLE tmul (a INT)", &mut ctx)
            .unwrap();
        eng.execute_sql("INSERT INTO tmul (a) VALUES (3 * 10)", &mut ctx)
            .unwrap();
        let sel = eng.execute_sql("SELECT a FROM tmul", &mut ctx).unwrap();
        match sel {
            EngineOutput::ResultSet { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert!(rows[0][0].contains("30"), "got {:?}", rows[0][0]);
            }
            _ => panic!("expected result set"),
        }
    }

    #[test]
    fn sql_engine_select_returns_heap_rows_after_insert() {
        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        eng.execute_sql("INSERT INTO t (a, b) VALUES (42, 'hi')", &mut ctx)
            .unwrap();
        let out = eng
            .execute_sql("SELECT a, b FROM t", &mut ctx)
            .expect("select");
        match out {
            EngineOutput::ResultSet { columns, rows } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(columns, vec!["a", "b"]);
                assert_eq!(rows[0][0], "Integer(42)");
                assert!(
                    rows[0][1].contains("hi"),
                    "expected varchar cell to contain hi, got {:?}",
                    rows[0][1]
                );
            }
            _ => panic!("expected result set"),
        }
    }

    #[test]
    fn sql_engine_autocommit_dml_rolls_back_on_error() {
        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();

        eng.execute_sql("CREATE TABLE tuniq (a INTEGER UNIQUE)", &mut ctx)
            .unwrap();
        eng.execute_sql("INSERT INTO tuniq (a) VALUES (1)", &mut ctx)
            .unwrap();

        // In auto-commit mode, the whole statement must behave atomically: if it errors mid-way,
        // it must not leave partial changes behind.
        assert!(eng
            .execute_sql("INSERT INTO tuniq (a) VALUES (2), (1)", &mut ctx)
            .is_err());

        let out = eng
            .execute_sql("SELECT a FROM tuniq WHERE a = 2", &mut ctx)
            .unwrap();
        match out {
            EngineOutput::ResultSet { rows, .. } => assert_eq!(rows.len(), 0),
            _ => panic!("expected result set"),
        }
    }

    #[test]
    fn sql_engine_autocommit_pk_violation_does_not_corrupt_on_reopen() {
        let dir = TempDir::new().unwrap();
        {
            let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
            let mut ctx = SessionContext::default();
            eng.execute_sql("CREATE TABLE tpk (id INT PRIMARY KEY)", &mut ctx)
                .unwrap();
            eng.execute_sql("INSERT INTO tpk (id) VALUES (1)", &mut ctx)
                .unwrap();
            assert!(eng
                .execute_sql("INSERT INTO tpk (id) VALUES (1)", &mut ctx)
                .is_err());
        }

        // After reopen, constraints must rebuild cleanly and the duplicate row must not persist.
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        let out = eng
            .execute_sql("SELECT id FROM tpk WHERE id = 1", &mut ctx)
            .unwrap();
        match out {
            EngineOutput::ResultSet { rows, .. } => assert_eq!(rows.len(), 1),
            _ => panic!("expected result set"),
        }
    }

    #[test]
    fn district_update_exact_index_enables_row_lock_path() {
        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        eng.execute_sql(
            "CREATE TABLE district (d_w_id INTEGER, d_id INTEGER, d_ytd INTEGER)",
            &mut ctx,
        )
        .unwrap();
        eng.execute_sql(
            "CREATE INDEX idx_district_wd ON district (d_w_id, d_id)",
            &mut ctx,
        )
        .unwrap();
        eng.execute_sql(
            "INSERT INTO district (d_w_id, d_id, d_ytd) VALUES (1, 1, 0)",
            &mut ctx,
        )
        .unwrap();
        let upd = eng
            .execute_sql(
                "UPDATE district SET d_ytd = 1 WHERE d_w_id = 1 AND d_id = 1",
                &mut ctx,
            )
            .unwrap();
        assert_eq!(upd, EngineOutput::ExecutionOk { rows_affected: 1 });
        let mut eq = HashMap::new();
        eq.insert("d_w_id".to_string(), "1".to_string());
        eq.insert("d_id".to_string(), "1".to_string());
        let indexed = eng
            .state_for_test()
            .index_registry
            .read()
            .expect("index registry read")
            .lookup_record_ids_by_equalities("district", &eq)
            .expect("lookup")
            .expect("idx_district_wd must cover d_w_id + d_id");
        assert!(
            indexed.1,
            "composite equality on idx_district_wd must be index_exact for row locks"
        );
        assert_eq!(indexed.0.len(), 1);
    }

    /// CI seeds via CLI then starts the server in a new process; secondary indexes must reload from catalog.
    #[test]
    fn reopen_restores_secondary_indexes_for_district_row_lock() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();
        {
            let eng = SqlEngine::open(path.clone()).unwrap();
            let mut ctx = SessionContext::default();
            eng.execute_sql(
                "CREATE TABLE district (d_w_id INTEGER, d_id INTEGER, d_ytd INTEGER)",
                &mut ctx,
            )
            .unwrap();
            eng.execute_sql(
                "CREATE INDEX idx_district_wd ON district (d_w_id, d_id)",
                &mut ctx,
            )
            .unwrap();
            eng.execute_sql(
                "INSERT INTO district (d_w_id, d_id, d_ytd) VALUES (1, 1, 0)",
                &mut ctx,
            )
            .unwrap();
        }
        let eng = SqlEngine::open(path).unwrap();
        let mut ctx = SessionContext::default();
        let mut eq = HashMap::new();
        eq.insert("d_w_id".to_string(), "1".to_string());
        eq.insert("d_id".to_string(), "1".to_string());
        let indexed = eng
            .state_for_test()
            .index_registry
            .read()
            .expect("index registry read")
            .lookup_record_ids_by_equalities("district", &eq)
            .expect("lookup")
            .expect("indexes must be rebuilt from catalog on open");
        assert!(indexed.1, "expected exact index key after reopen");
        assert_eq!(indexed.0.len(), 1);
        eng.execute_sql(
            "UPDATE district SET d_ytd = 5 WHERE d_w_id = 1 AND d_id = 1",
            &mut ctx,
        )
        .expect("update after reopen");
    }

    /// Concurrent district updates use per-row locks when `idx_district_wd` matches
    /// `WHERE d_w_id = ? AND d_id = ?` (full index key); otherwise DML falls back to
    /// `table_storage_lock` and phase logs show `sql.dml.lock_path=table`.
    #[test]
    fn concurrent_update_district_row_locks() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let dir = TempDir::new().unwrap();
        let eng = Arc::new(SqlEngine::open(dir.path().to_path_buf()).unwrap());
        let mut setup = SessionContext::default();
        eng.execute_sql(
            "CREATE TABLE district (d_w_id INTEGER, d_id INTEGER, d_ytd INTEGER)",
            &mut setup,
        )
        .unwrap();
        eng.execute_sql(
            "CREATE INDEX idx_district_wd ON district (d_w_id, d_id)",
            &mut setup,
        )
        .unwrap();
        for d_id in 1..=4 {
            eng.execute_sql(
                &format!("INSERT INTO district (d_w_id, d_id, d_ytd) VALUES (1, {d_id}, 0)"),
                &mut setup,
            )
            .unwrap();
        }

        let barrier = Arc::new(Barrier::new(5));
        let mut handles = Vec::new();
        for d_id in 1..=4 {
            let eng = Arc::clone(&eng);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                let mut ctx = SessionContext::default();
                for _ in 0..25 {
                    eng.execute_sql(
                        &format!(
                            "UPDATE district SET d_ytd = d_ytd + 1 WHERE d_w_id = 1 AND d_id = {d_id}"
                        ),
                        &mut ctx,
                    )
                    .expect("update");
                }
            }));
        }
        barrier.wait();
        for h in handles {
            h.join().unwrap();
        }

        let mut ctx = SessionContext::default();
        let out = eng
            .execute_sql("SELECT d_id, d_ytd FROM district ORDER BY d_id", &mut ctx)
            .unwrap();
        match out {
            EngineOutput::ResultSet { rows, .. } => {
                assert_eq!(rows.len(), 4);
                for row in rows {
                    let ytd: i64 = row[1]
                        .trim_start_matches("Integer(")
                        .trim_end_matches(')')
                        .parse()
                        .unwrap_or_else(|_| row[1].parse().expect("numeric d_ytd"));
                    assert_eq!(ytd, 25, "expected d_ytd=25 per district, got {:?}", row);
                }
            }
            _ => panic!("expected result set"),
        }
    }

    #[test]
    fn explicit_transaction_selective_flush_only_touched_tables() {
        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        eng.execute_sql("CREATE TABLE ta (k INT PRIMARY KEY)", &mut ctx)
            .unwrap();
        eng.execute_sql("CREATE TABLE tb (k INT PRIMARY KEY)", &mut ctx)
            .unwrap();
        eng.execute_sql("INSERT INTO tb (k) VALUES (0)", &mut ctx)
            .unwrap();
        let pm_b = table_page_manager(eng.state_for_test(), "tb").unwrap();
        assert_eq!(pm_b.lock().unwrap().dirty_page_count(), 0);

        eng.execute_sql("BEGIN TRANSACTION", &mut ctx).unwrap();
        eng.execute_sql("INSERT INTO ta (k) VALUES (1)", &mut ctx)
            .unwrap();
        let pm_a = table_page_manager(eng.state_for_test(), "ta").unwrap();
        assert!(
            pm_a.lock().unwrap().dirty_page_count() > 0,
            "touched table ta should have dirty pages before COMMIT"
        );
        assert_eq!(
            pm_b.lock().unwrap().dirty_page_count(),
            0,
            "untouched table tb should not be flushed mid-txn"
        );
        eng.execute_sql("COMMIT", &mut ctx).unwrap();
        assert_eq!(pm_a.lock().unwrap().dirty_page_count(), 0);
        assert_eq!(pm_b.lock().unwrap().dirty_page_count(), 0);
    }

    #[test]
    fn explicit_transaction_defers_per_statement_heap_flush() {
        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        eng.execute_sql("CREATE TABLE defer_flush (k INT PRIMARY KEY)", &mut ctx)
            .unwrap();
        eng.execute_sql("BEGIN TRANSACTION", &mut ctx).unwrap();
        eng.execute_sql("INSERT INTO defer_flush (k) VALUES (1)", &mut ctx)
            .unwrap();
        eng.execute_sql("INSERT INTO defer_flush (k) VALUES (2)", &mut ctx)
            .unwrap();
        let pm = table_page_manager(eng.state_for_test(), "defer_flush").unwrap();
        let dirty_mid_txn = pm.lock().expect("page manager lock").dirty_page_count();
        assert!(
            dirty_mid_txn > 0,
            "expected dirty heap pages before COMMIT, got {dirty_mid_txn}"
        );
        eng.execute_sql("COMMIT", &mut ctx).unwrap();
        let dirty_after_commit = pm.lock().unwrap().dirty_page_count();
        assert_eq!(
            dirty_after_commit, 0,
            "COMMIT should flush dirty heap pages"
        );
    }

    #[test]
    fn table_page_manager_cached_reuses_arc_in_transaction() {
        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let state = eng.state_for_test();
        let mut ctx = SessionContext::default();
        eng.execute_sql("CREATE TABLE pm_cache (k INT)", &mut ctx)
            .unwrap();
        eng.execute_sql("BEGIN TRANSACTION", &mut ctx).unwrap();
        let pm1 = table_page_manager_cached(state, &mut ctx, "pm_cache").unwrap();
        let pm2 = table_page_manager_cached(state, &mut ctx, "pm_cache").unwrap();
        assert!(Arc::ptr_eq(&pm1, &pm2));
        assert_eq!(ctx.txn_pm_cache.len(), 1);
        eng.execute_sql("COMMIT", &mut ctx).unwrap();
        assert!(ctx.txn_pm_cache.is_empty());
    }

    #[test]
    fn explicit_transaction_multi_insert_commit_survives_reopen() {
        let dir = TempDir::new().unwrap();
        {
            let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
            let mut ctx = SessionContext::default();
            eng.execute_sql("CREATE TABLE txn_persist (k INT PRIMARY KEY)", &mut ctx)
                .unwrap();
            eng.execute_sql("BEGIN TRANSACTION", &mut ctx).unwrap();
            for k in 1..=3 {
                let sql = format!("INSERT INTO txn_persist (k) VALUES ({k})");
                eng.execute_sql(&sql, &mut ctx).unwrap();
            }
            eng.execute_sql("COMMIT", &mut ctx).unwrap();
        }
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        let out = eng
            .execute_sql("SELECT k FROM txn_persist ORDER BY k", &mut ctx)
            .unwrap();
        match out {
            EngineOutput::ResultSet { rows, .. } => assert_eq!(rows.len(), 3),
            _ => panic!("expected result set"),
        }
    }

    #[test]
    fn sql_engine_repeated_dml_plan_validation_cache() {
        // Regression: repeated INSERT/UPDATE/DELETE must stay correct when plan validation is cached.
        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        eng.execute_sql("CREATE TABLE tc (id INT, n INT)", &mut ctx)
            .unwrap();
        for i in 0..32 {
            let ins = format!("INSERT INTO tc (id, n) VALUES ({}, {})", i, i);
            let out = eng.execute_sql(&ins, &mut ctx).unwrap();
            assert_eq!(out, EngineOutput::ExecutionOk { rows_affected: 1 });
        }
        for _ in 0..8 {
            let out = eng
                .execute_sql("UPDATE tc SET n = n + 1 WHERE id = 0", &mut ctx)
                .unwrap();
            assert_eq!(out, EngineOutput::ExecutionOk { rows_affected: 1 });
        }
        let del = eng
            .execute_sql("DELETE FROM tc WHERE id = 0", &mut ctx)
            .unwrap();
        assert_eq!(del, EngineOutput::ExecutionOk { rows_affected: 1 });
    }

    #[test]
    fn sql_engine_dml_plan_cache_invalidated_after_ddl() {
        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        eng.execute_sql("CREATE TABLE tddl (a INT)", &mut ctx)
            .unwrap();
        eng.execute_sql("INSERT INTO tddl (a) VALUES (1)", &mut ctx)
            .unwrap();
        eng.execute_sql("ALTER TABLE tddl ADD COLUMN b INT", &mut ctx)
            .unwrap();
        let out = eng
            .execute_sql("INSERT INTO tddl (a, b) VALUES (2, 3)", &mut ctx)
            .unwrap();
        assert_eq!(out, EngineOutput::ExecutionOk { rows_affected: 1 });
    }

    #[test]
    fn sql_engine_rollback_survives_page_splits() {
        // Regression: record ids encode byte offsets, so page split/merge can invalidate them.
        // Rollback must still remove all uncommitted rows, even if their offsets moved.
        let dir = TempDir::new().unwrap();
        {
            let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
            let mut ctx = SessionContext::default();
            eng.execute_sql("CREATE TABLE t (a INTEGER)", &mut ctx)
                .unwrap();
            eng.execute_sql("BEGIN TRANSACTION", &mut ctx).unwrap();
            // Insert enough rows to force at least one page split (MAX_RECORDS_PER_PAGE=100).
            for i in 0..180 {
                let sql = format!("INSERT INTO t (a) VALUES ({})", i);
                eng.execute_sql(&sql, &mut ctx).unwrap();
            }
            eng.execute_sql("ROLLBACK", &mut ctx).unwrap();
        }
        // After reopen, no rows should remain.
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        let out = eng.execute_sql("SELECT a FROM t", &mut ctx).unwrap();
        match out {
            EngineOutput::ResultSet { rows, .. } => assert_eq!(rows.len(), 0),
            _ => panic!("expected result set"),
        }
    }
}
