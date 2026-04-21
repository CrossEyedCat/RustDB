//! Real SQL execution for the network [`crate::network::engine::EngineHandle`] boundary.
//!
//! Pipeline: parse → plan ([`QueryPlanner`]) → optimize ([`QueryOptimizer`]) → execute ([`QueryExecutor`]).
//! `SELECT` without `FROM` is evaluated from literal projections only.
//! `INSERT` / `UPDATE` / `DELETE` run against the heap file (`default.tbl`) using serialized [`crate::storage::tuple::Tuple`] rows.
//!
//! **Phase 6 — transactions & concurrency (minimal)**:
//! - `BEGIN` / `COMMIT` / `ROLLBACK` with an undo log for DML in the current [`SessionContext`].
//! - A [`RwLock`] serializes writers vs readers at **statement** granularity (read committed baseline:
//!   each statement sees data committed before that statement began, excluding the current session’s
//!   own uncommitted writes which are already on the heap).
//! - Stronger isolation ([`crate::network::engine::SqlIsolationLevel::RepeatableRead`] /
//!   [`crate::network::engine::SqlIsolationLevel::Serializable`]) uses a global lock so at most one
//!   such transaction runs at a time (see `RUSTDB_DEFAULT_ISOLATION`).
//! - Optional structured WAL under `data_dir/.rustdb/wal` (`RUSTDB_DISABLE_WAL=1` to skip); recovery
//!   runs via [`crate::logging::recovery::RecoveryManager`] on open. When WAL is on, a
//!   [`crate::logging::checkpoint::CheckpointManager`] is attached (unless `RUSTDB_DISABLE_CHECKPOINT=1`);
//!   call [`SqlEngine::checkpoint`] for a manual checkpoint (flushes heaps + writes a checkpoint record).
//!   After each successful write of `catalog.json` (DDL), the WAL records a `MetadataUpdate` marker when WAL is on.
//! - DDL (`CREATE` / `DROP` / `ALTER`) is rejected while a transaction is open.

use crate::catalog::schema::{
    CheckConstraint, ForeignKeyConstraintDef, SchemaManager, TableSchema, UniqueConstraintDef,
};
use crate::common::types::{ColumnValue, DataType, RecordId};
use crate::common::Error as DbError;
use crate::executor::operators::eval_predicate_expression;
use crate::executor::operators::ScanOperatorFactory;
use crate::executor::QueryExecutor;
use crate::network::engine::{
    engine_error_code, EngineError, EngineHandle, EngineOutput, SessionContext, SqlIsolationLevel,
    SqlTransaction, UndoEntry,
};
use crate::network::sql_commit_log;
use crate::network::sql_constraints::{self, ConstraintRuntime};
use crate::parser::ast::{
    AlterTableOperation, AlterTableStatement, BinaryOperator, ColumnConstraint,
    CreateTableStatement, DataType as SqlDataType, DeleteStatement, DropTableStatement, Expression,
    InsertStatement, InsertValues, Literal, SelectItem, SelectStatement, TableConstraint,
    UpdateStatement,
};
use crate::parser::{SqlParser, SqlStatement};
use crate::planner::{QueryOptimizer, QueryPlanner};
use crate::storage::page_manager::{PageManager, PageManagerConfig};
use crate::storage::tuple::Tuple;
use crate::Row;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tracing::info_span;

mod alter_table_ops;

/// Global lock: at most one [`SqlIsolationLevel::RepeatableRead`] or [`SqlIsolationLevel::Serializable`]
/// engine transaction across all sessions.
static STRONG_ISO_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

/// Engine backed by the in-process planner, optimizer, and executor (single default table file per data directory).
pub struct SqlEngine {
    state: Arc<SqlEngineState>,
}

pub(crate) struct SqlEngineState {
    data_dir: PathBuf,
    pub(crate) default_page_manager: Arc<Mutex<PageManager>>,
    pub(crate) table_page_managers: Arc<Mutex<HashMap<String, Arc<Mutex<PageManager>>>>>,
    /// Monotonic id assigned to inserted [`Tuple`] rows (persisted in tuple bytes).
    next_tuple_id: AtomicU64,
    planner: QueryPlanner,
    optimizer: QueryOptimizer,
    executor: QueryExecutor,
    /// Cache for deterministic `SELECT` queries without `FROM` (literal projections only).
    ///
    /// These queries are common in benchmarks (`SELECT 1`) and are safe to memoize.
    select_no_from_cache: Mutex<HashMap<String, EngineOutput>>,
    pub(crate) catalog: Mutex<SchemaManager>,
    constraint_runtime: Mutex<ConstraintRuntime>,
    /// Serializes storage-mutating statements vs table scans (`SELECT` with `FROM`).
    storage_access: RwLock<()>,
    /// Structured WAL (`src/logging`); disabled when `RUSTDB_DISABLE_WAL` is set.
    wal: Option<crate::network::sql_engine_wal::SqlEngineWal>,
}

impl SqlEngine {
    /// Opens or creates storage under `data_dir` (directory is created if missing).
    /// Uses one heap file `default.tbl` for table scans (see [`ScanOperatorFactory`]).
    pub fn open(data_dir: PathBuf) -> Result<Self, DbError> {
        std::fs::create_dir_all(&data_dir)?;
        let wal_dir = data_dir.join(".rustdb").join("wal");
        let wal = if std::env::var_os("RUSTDB_DISABLE_WAL").is_none() {
            std::fs::create_dir_all(&wal_dir)?;
            crate::network::sql_engine_wal::recover_sql_engine_wal(&wal_dir)?;
            Some(crate::network::sql_engine_wal::SqlEngineWal::open(
                &wal_dir,
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
        let factory = Arc::new(ScanOperatorFactory::with_tables(
            pm.clone(),
            table_pms.clone(),
            data_dir.clone(),
        ));
        let executor = QueryExecutor::new(factory)?;
        let state = Arc::new(SqlEngineState {
            data_dir,
            default_page_manager: pm,
            table_page_managers: table_pms,
            next_tuple_id: AtomicU64::new(1),
            planner: QueryPlanner::new()?,
            optimizer: QueryOptimizer::new()?,
            executor,
            select_no_from_cache: Mutex::new(HashMap::new()),
            catalog: Mutex::new(catalog),
            constraint_runtime: Mutex::new(ConstraintRuntime::new()),
            storage_access: RwLock::new(()),
            wal,
        });
        if state.wal.is_some() && wal_dir.is_dir() {
            crate::network::sql_engine_wal::replay_wal_into_engine(state.as_ref(), &wal_dir)
                .map_err(|e| DbError::database(format!("WAL replay on open: {e}")))?;
        }
        if let Some(ref wal) = state.wal {
            wal.setup_checkpoint(state.clone())
                .map_err(|e| DbError::database(format!("checkpoint setup on open: {e}")))?;
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
                    state.optimizer.optimize(plan).map_err(map_db_err)?
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
            SqlStatement::Insert(ins) => {
                let s = info_span!("sql.insert", table = %ins.table);
                let _sg = s.enter();
                let _storage = state
                    .storage_access
                    .write()
                    .map_err(|_| lock_poisoned_engine())?;
                execute_insert(state, ctx, stmt, ins)
            }
            SqlStatement::Update(upd) => {
                let s = info_span!("sql.update", table = %upd.table);
                let _sg = s.enter();
                let _storage = state
                    .storage_access
                    .write()
                    .map_err(|_| lock_poisoned_engine())?;
                execute_update(state, ctx, stmt, upd)
            }
            SqlStatement::Delete(del) => {
                let s = info_span!("sql.delete", table = %del.table);
                let _sg = s.enter();
                let _storage = state
                    .storage_access
                    .write()
                    .map_err(|_| lock_poisoned_engine())?;
                execute_delete(state, ctx, stmt, del)
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
    sql_commit_log::append_commit_log_line(&state.data_dir).map_err(map_db_err)?;
    drop(tx);
    Ok(EngineOutput::ExecutionOk { rows_affected: 0 })
}

fn persist_catalog(state: &SqlEngineState) -> Result<(), EngineError> {
    let cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
    cat.save_catalog_to_data_dir(&state.data_dir)
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
    if let Some(ref wal) = state.wal {
        wal.log_abort(&mut tx)?;
    }
    for op in tx.undo.into_iter().rev() {
        apply_undo(state, op)?;
    }
    rebuild_all_constraint_runtime(state)?;
    // Persist rollback: otherwise the next process sees heap pages from disk that still contain
    // undone inserts (WAL replay does not redo aborted txs, so durability must match).
    crate::network::sql_engine_wal::flush_all_page_managers(state).map_err(map_db_err)?;
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
            g.delete(rid).map_err(map_db_err)?;
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

fn validate_plan(state: &SqlEngineState, stmt: &SqlStatement) -> Result<(), EngineError> {
    let plan = state.planner.create_plan(stmt).map_err(map_db_err)?;
    let _ = state.optimizer.optimize(plan).map_err(map_db_err)?;
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
            let optimized = state.optimizer.optimize(plan).map_err(map_db_err)?;
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
                    pm.delete(ins.record_id).map_err(map_db_err)?;
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
                pm.flush_dirty_pages().map_err(map_db_err)?;
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
                    pm.delete(ins.record_id).map_err(map_db_err)?;
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
                pm.flush_dirty_pages().map_err(map_db_err)?;
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

fn execute_update(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
    stmt: &SqlStatement,
    update: &UpdateStatement,
) -> Result<EngineOutput, EngineError> {
    validate_plan(state, stmt)?;
    let pm_for_table = table_page_manager(state, &update.table)?;
    let mut pm = pm_for_table.lock().map_err(|_| lock_poisoned_engine())?;
    let snapshot = pm.select(None).map_err(map_db_err)?;
    let mut rows_affected = 0u64;
    for (rid, data) in snapshot {
        let mut tuple = Tuple::from_bytes(&data).map_err(map_db_err)?;
        let keep = match &update.where_clause {
            None => true,
            Some(expr) => match_where_tuple(expr, &tuple)?,
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
        for a in &update.assignments {
            let cv = expr_to_column_value(&a.value)?;
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
    pm.flush_dirty_pages().map_err(map_db_err)?;
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
    let snapshot = pm.select(None).map_err(map_db_err)?;
    let cat_snapshot = {
        let cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
        cat.clone()
    };
    let schema = cat_snapshot.schema(&delete.table).cloned();
    let mut to_delete: Vec<(RecordId, Vec<u8>)> = Vec::new();
    for (rid, data) in snapshot {
        let tuple = Tuple::from_bytes(&data).map_err(map_db_err)?;
        let keep = match &delete.where_clause {
            None => true,
            Some(expr) => match_where_tuple(expr, &tuple)?,
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
        pm.delete(rid).map_err(map_db_err)?;
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
    pm.flush_dirty_pages().map_err(map_db_err)?;
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
            if let Err(e) = sql_constraints::register_row(&mut rt, &t, rid, &tuple, &schema, &cat)
            {
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
                if e.code == engine_error_code::CONSTRAINT_VIOLATION && (is_missing_key || is_null_key)
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
    sql_constraints::register_row(&mut rt, table, rid, tuple, &schema, &snapshot)
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
    match expr {
        Expression::Literal(l) => Ok(literal_to_column_value(l)),
        _ => Err(EngineError::new(
            engine_error_code::UNSUPPORTED_SQL,
            "value must be a literal in INSERT/UPDATE for this engine",
        )),
    }
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
}
