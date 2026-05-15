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
//!   after successful DML (higher throughput; heap catches up at checkpoint / process exit). Default
//!   remains **flush after each successful DML** so standalone heap files stay coherent for tests and
//!   tooling that reopen without relying on WAL replay ordering.
//! - DDL (`CREATE` / `DROP` / `ALTER`) is rejected while a transaction is open.
//!
//! **Profiling:** set `RUSTDB_SQL_PHASE_LOG=1` to emit `tracing` events on target `rustdb::sql_phases`
//! (parse latency, `UPDATE`/`DELETE` scan vs row loop). Use `RUST_LOG=rustdb::sql_phases=info` to filter.

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
    engine_error_code, EngineError, EngineHandle, EngineOutput, SessionContext, SqlIsolationLevel,
    SqlTransaction, UndoEntry,
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
use crate::planner::{QueryOptimizer, QueryPlanner};
use crate::storage::index_registry::IndexRegistry;
use crate::storage::page_manager::{PageManager, PageManagerConfig};
use crate::storage::tuple::Tuple;
use crate::Row;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;
use tracing::{info, info_span};

mod alter_table_ops;

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
    /// Secondary indexes maintained for `CREATE INDEX` (not persisted across bare-metal restarts).
    index_registry: Arc<Mutex<IndexRegistry>>,
    /// Cache for deterministic `SELECT` queries without `FROM` (literal projections only).
    ///
    /// These queries are common in benchmarks (`SELECT 1`) and are safe to memoize.
    select_no_from_cache: Mutex<HashMap<String, EngineOutput>>,
    pub(crate) catalog: Mutex<SchemaManager>,
    constraint_runtime: Mutex<ConstraintRuntime>,
    /// Serializes storage-mutating statements vs table scans (`SELECT` with `FROM`).
    storage_access: RwLock<()>,
    /// Per physical table: coordinates concurrent `SELECT` (shared) vs DML writers (exclusive).
    table_storage_locks: Mutex<HashMap<String, Arc<RwLock<()>>>>,
    /// Structured WAL (`src/logging`); disabled when `RUSTDB_DISABLE_WAL` is set.
    wal: Option<crate::network::sql_engine_wal::SqlEngineWal>,
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
        let index_registry = Arc::new(Mutex::new(IndexRegistry::new()));
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
            catalog: Mutex::new(catalog),
            constraint_runtime: Mutex::new(ConstraintRuntime::new()),
            storage_access: RwLock::new(()),
            table_storage_locks: Mutex::new(HashMap::new()),
            wal,
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

    /// Checkpoint statistics (returns `None` when WAL or checkpoints are disabled).
    pub fn checkpoint_statistics(
        &self,
    ) -> Option<crate::logging::checkpoint::CheckpointStatistics> {
        let wal = self.state.wal.as_ref()?;
        wal.checkpoint_statistics()
    }

    fn execute_sql_inner(
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
                if table_names.is_empty() {
                    let _storage = state
                        .storage_access
                        .read()
                        .map_err(|_| lock_poisoned_engine())?;
                    let plan = {
                        let s = info_span!("sql.plan");
                        let _sg = s.enter();
                        state.planner.create_plan(stmt).map_err(map_db_err)?
                    };
                    let optimized = {
                        let s = info_span!("sql.optimize");
                        let _sg = s.enter();
                        state
                            .optimizer
                            .lock()
                            .map_err(|_| lock_poisoned_engine())?
                            .optimize(plan)
                            .map_err(map_db_err)?
                    };
                    let rows = {
                        let s = info_span!("sql.exec_plan");
                        let _sg = s.enter();
                        state
                            .executor
                            .execute(&optimized.optimized_plan)
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
                        .map(|n| table_storage_lock_arc(state, n))
                        .collect::<Result<_, _>>()?;
                    let _table_reads: Vec<std::sync::RwLockReadGuard<'_, ()>> = locks
                        .iter()
                        .map(|l| l.read().map_err(|_| lock_poisoned_engine()))
                        .collect::<Result<_, _>>()?;
                    let plan = {
                        let s = info_span!("sql.plan");
                        let _sg = s.enter();
                        state.planner.create_plan(stmt).map_err(map_db_err)?
                    };
                    let optimized = {
                        let s = info_span!("sql.optimize");
                        let _sg = s.enter();
                        state
                            .optimizer
                            .lock()
                            .map_err(|_| lock_poisoned_engine())?
                            .optimize(plan)
                            .map_err(map_db_err)?
                    };
                    let rows = {
                        let s = info_span!("sql.exec_plan");
                        let _sg = s.enter();
                        state
                            .executor
                            .execute(&optimized.optimized_plan)
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
                            execute_insert(state, ctx, stmt, ins)
                        })
                    }
                    InsertValues::Values(_) => {
                        let lock = table_storage_lock_arc(state, &ins.table)?;
                        let _table_write = lock.write().map_err(|_| lock_poisoned_engine())?;
                        execute_dml_autocommit(state, ctx, |state, ctx| {
                            execute_insert(state, ctx, stmt, ins)
                        })
                    }
                }
            }
            SqlStatement::Update(upd) => {
                let s = info_span!("sql.update", table = %upd.table);
                let _sg = s.enter();
                let lock = table_storage_lock_arc(state, &upd.table)?;
                let _table_write = lock.write().map_err(|_| lock_poisoned_engine())?;
                execute_dml_autocommit(state, ctx, |state, ctx| {
                    execute_update(state, ctx, stmt, upd)
                })
            }
            SqlStatement::Delete(del) => {
                let s = info_span!("sql.delete", table = %del.table);
                let _sg = s.enter();
                let lock = table_storage_lock_arc(state, &del.table)?;
                let _table_write = lock.write().map_err(|_| lock_poisoned_engine())?;
                execute_dml_autocommit(state, ctx, |state, ctx| {
                    execute_delete(state, ctx, stmt, del)
                })
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

    fn supports_select_no_from_wire_cache(&self) -> bool {
        true
    }
}

fn map_db_err(e: DbError) -> EngineError {
    EngineError::new(engine_error_code::INTERNAL, e.to_string())
}

fn lock_poisoned_engine() -> EngineError {
    EngineError::new(engine_error_code::INTERNAL, "storage lock poisoned")
}

/// When set (non-empty, not `0`/`false`), emit `tracing` on target `rustdb::sql_phases` for statement timing.
fn sql_phase_log_enabled() -> bool {
    match std::env::var("RUSTDB_SQL_PHASE_LOG") {
        Ok(s) if s == "0" || s.eq_ignore_ascii_case("false") => false,
        Ok(_) => true,
        Err(_) => false,
    }
}

/// Flush dirty heap pages after successful DML. When WAL is on, flushing may be skipped if
/// `RUSTDB_DEFER_HEAP_FLUSH_AFTER_DML` is set (benchmark throughput); otherwise always flush so heap
/// files remain coherent across process restarts without depending on replay timing.
fn flush_heap_after_dml_success(
    state: &SqlEngineState,
    pm: &mut PageManager,
) -> Result<(), EngineError> {
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
    if let Some(ref wal) = state.wal {
        wal.log_commit(&mut tx)?;
    }
    sql_commit_log::append_commit_log_line(&state.data_dir, state.durability.fsync_on_commit())
        .map_err(map_db_err)?;
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
    let undo = std::mem::take(&mut tx.undo);
    for op in undo.into_iter().rev() {
        apply_undo(state, op)?;
    }
    rebuild_all_constraint_runtime(state)?;
    // Persist rollback: otherwise the next process sees heap pages from disk that still contain
    // undone inserts (WAL replay does not redo aborted txs, so durability must match).
    crate::network::sql_engine_wal::flush_all_page_managers(state).map_err(map_db_err)?;
    // Only append an ABORT marker after the UNDO is applied *and* persisted.
    //
    // If we mark the transaction as aborted in WAL first and then crash before the UNDO is flushed,
    // recovery would skip UNDO (seeing ABORT) while the heap still contains uncommitted changes.
    if let Some(ref wal) = state.wal {
        wal.log_abort(&mut tx)?;
    }
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
            .lock()
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

fn tuple_to_index_column_map(tuple: &Tuple) -> HashMap<String, String> {
    let mut m = HashMap::new();
    for (name, cv) in &tuple.values {
        if cv.is_null {
            m.insert(name.clone(), String::new());
            continue;
        }
        let s = match &cv.data_type {
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
        };
        m.insert(name.clone(), s);
    }
    m
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
        .lock()
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
        .lock()
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
        .lock()
        .map_err(|_| lock_poisoned_engine())?;
    ir.delete_from_indexes(table, rid, &m)
        .map_err(|e| EngineError::new(engine_error_code::INTERNAL, format!("index delete: {e}")))?;
    Ok(())
}

fn rebuild_optimizer_with_indexes(state: &SqlEngineState) -> Result<(), EngineError> {
    let snapshot = state
        .index_registry
        .lock()
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
    Ok(())
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
            .lock()
            .map_err(|_| lock_poisoned_engine())?;
        reg.create_index(table, &ci.index_name, ci.columns.clone())
            .map_err(|e| {
                EngineError::new(engine_error_code::CONSTRAINT_VIOLATION, e.to_string())
            })?;
    }
    rebuild_optimizer_with_indexes(state)?;
    Ok(EngineOutput::ExecutionOk { rows_affected: 0 })
}

fn validate_plan(state: &SqlEngineState, stmt: &SqlStatement) -> Result<(), EngineError> {
    let plan = state.planner.create_plan(stmt).map_err(map_db_err)?;
    let _ = state
        .optimizer
        .lock()
        .map_err(|_| lock_poisoned_engine())?
        .optimize(plan)
        .map_err(map_db_err)?;
    Ok(())
}

fn execute_insert(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
    stmt: &SqlStatement,
    insert: &InsertStatement,
) -> Result<EngineOutput, EngineError> {
    validate_plan(state, stmt)?;
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
                        wal.log_data_insert(
                            tx,
                            pm.file_id(),
                            page_id,
                            record_offset,
                            bytes.clone(),
                        )?;
                    }
                }
                if let Err(e) = register_row_for_insert(state, &insert.table, ins.record_id, &tuple)
                {
                    // We already logged the INSERT to WAL (for crash recovery). If the statement
                    // fails after that point (e.g. PK/UNIQUE violation), we must also log the
                    // compensating DELETE in the *same* transaction so a later COMMIT cannot
                    // resurrect the rejected row on recovery (redo would replay insert+delete).
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
                            wal.log_data_delete(
                                tx,
                                pm.file_id(),
                                page_id,
                                record_offset,
                                bytes.clone(),
                            )?;
                        }
                    }
                    pm.delete(ins.record_id).map_err(map_db_err)?;
                    // Persist the compensating delete: otherwise an erroring insert could leave the
                    // row on disk (insert flushed earlier by the page manager), corrupting
                    // constraints on the next open.
                    pm.flush_dirty_pages().map_err(map_db_err)?;
                    return Err(e);
                }
                push_undo(
                    ctx,
                    UndoEntry::Insert {
                        table: insert.table.clone(),
                        rid: ins.record_id,
                        payload: bytes.clone(),
                    },
                );
                rows_affected += 1;
            }
            {
                let mut pm = pm_for_table.lock().map_err(|_| lock_poisoned_engine())?;
                flush_heap_after_dml_success(state, &mut pm)?;
            }
            Ok(EngineOutput::ExecutionOk { rows_affected })
        }
        InsertValues::Values(rows) => {
            let mut rows_affected = 0u64;
            let pm_for_table = table_page_manager(state, &insert.table)?;
            for row in rows {
                let tuple = build_insert_tuple(state, &insert.table, insert.columns.as_ref(), row)?;
                let bytes = tuple.to_bytes().map_err(map_db_err)?;
                let mut pm = state
                    .default_page_manager
                    .lock()
                    .map_err(|_| lock_poisoned_engine())?;
                // insert into per-table heap
                drop(pm);
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
                        wal.log_data_insert(
                            tx,
                            pm.file_id(),
                            page_id,
                            record_offset,
                            bytes.clone(),
                        )?;
                    }
                }
                if let Err(e) = register_row_for_insert(state, &insert.table, ins.record_id, &tuple)
                {
                    // Mirror the WAL-visible INSERT with a WAL-visible compensating DELETE so
                    // a later COMMIT cannot replay an otherwise rejected row on recovery.
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
                            wal.log_data_delete(
                                tx,
                                pm.file_id(),
                                page_id,
                                record_offset,
                                bytes.clone(),
                            )?;
                        }
                    }
                    pm.delete(ins.record_id).map_err(map_db_err)?;
                    // Persist the compensating delete: otherwise a failed insert (e.g. PK/UNIQUE)
                    // can leave the row visible after restart.
                    pm.flush_dirty_pages().map_err(map_db_err)?;
                    return Err(e);
                }
                push_undo(
                    ctx,
                    UndoEntry::Insert {
                        table: insert.table.clone(),
                        rid: ins.record_id,
                        payload: bytes.clone(),
                    },
                );
                rows_affected += 1;
            }
            {
                let mut pm = pm_for_table.lock().map_err(|_| lock_poisoned_engine())?;
                flush_heap_after_dml_success(state, &mut pm)?;
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
    stmt: &SqlStatement,
    update: &UpdateStatement,
) -> Result<EngineOutput, EngineError> {
    validate_plan(state, stmt)?;
    let pm_for_table = table_page_manager(state, &update.table)?;
    let mut pm = pm_for_table.lock().map_err(|_| lock_poisoned_engine())?;
    let scan_clock = sql_phase_log_enabled().then(Instant::now);
    let (snapshot, where_pre_filtered) = match &update.where_clause {
        None => (pm.select(None).map_err(map_db_err)?, false),
        Some(expr) => {
            validate_dml_where_structure(expr)?;
            let expr = expr.clone();
            let pred = Box::new(move |data: &[u8]| {
                let tuple = Tuple::from_bytes(data).expect("heap tuple must deserialize");
                match_where_tuple(&expr, &tuple).expect("WHERE validated for heap predicate")
            });
            (pm.select(Some(pred)).map_err(map_db_err)?, true)
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
    flush_heap_after_dml_success(state, &mut pm)?;
    Ok(EngineOutput::ExecutionOk { rows_affected })
}

fn execute_delete(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
    stmt: &SqlStatement,
    delete: &DeleteStatement,
) -> Result<EngineOutput, EngineError> {
    validate_plan(state, stmt)?;
    let pm_for_table = table_page_manager(state, &delete.table)?;
    let mut pm = pm_for_table.lock().map_err(|_| lock_poisoned_engine())?;
    let scan_clock = sql_phase_log_enabled().then(Instant::now);
    let (snapshot, where_pre_filtered) = match &delete.where_clause {
        None => (pm.select(None).map_err(map_db_err)?, false),
        Some(expr) => {
            validate_dml_where_structure(expr)?;
            let expr = expr.clone();
            let pred = Box::new(move |data: &[u8]| {
                let tuple = Tuple::from_bytes(data).expect("heap tuple must deserialize");
                match_where_tuple(&expr, &tuple).expect("WHERE validated for heap predicate")
            });
            (pm.select(Some(pred)).map_err(map_db_err)?, true)
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
    flush_heap_after_dml_success(state, &mut pm)?;
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
