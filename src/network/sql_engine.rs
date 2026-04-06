//! Real SQL execution for the network [`crate::network::engine::EngineHandle`] boundary.
//!
//! Pipeline: parse → plan ([`QueryPlanner`]) → optimize ([`QueryOptimizer`]) → execute ([`QueryExecutor`]).
//! `SELECT` without `FROM` is evaluated from literal projections only.
//! `INSERT` / `UPDATE` / `DELETE` run against the heap file (`default.tbl`) using serialized [`crate::storage::tuple::Tuple`] rows.

use crate::common::types::{ColumnValue, DataType, RecordId};
use crate::common::Error as DbError;
use crate::executor::operators::ScanOperatorFactory;
use crate::executor::QueryExecutor;
use crate::network::engine::{
    engine_error_code, EngineError, EngineHandle, EngineOutput, SessionContext,
};
use crate::parser::ast::{
    BinaryOperator, DeleteStatement, Expression, InsertStatement, InsertValues, Literal,
    SelectItem, SelectStatement, UpdateStatement,
};
use crate::parser::{SqlParser, SqlStatement};
use crate::planner::{QueryOptimizer, QueryPlanner};
use crate::storage::page_manager::{PageManager, PageManagerConfig};
use crate::storage::tuple::Tuple;
use crate::Row;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// Engine backed by the in-process planner, optimizer, and executor (single default table file per data directory).
pub struct SqlEngine {
    inner: Mutex<SqlEngineInner>,
}

struct SqlEngineInner {
    data_dir: PathBuf,
    default_page_manager: Arc<Mutex<PageManager>>,
    table_page_managers: Arc<Mutex<HashMap<String, Arc<Mutex<PageManager>>>>>,
    /// Monotonic id assigned to inserted [`Tuple`] rows (persisted in tuple bytes).
    next_tuple_id: u64,
    planner: QueryPlanner,
    optimizer: QueryOptimizer,
    executor: QueryExecutor,
}

impl SqlEngine {
    /// Opens or creates storage under `data_dir` (directory is created if missing).
    /// Uses one heap file `default.tbl` for table scans (see [`ScanOperatorFactory`]).
    pub fn open(data_dir: PathBuf) -> Result<Self, DbError> {
        std::fs::create_dir_all(&data_dir)?;
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
        Ok(Self {
            inner: Mutex::new(SqlEngineInner {
                data_dir,
                default_page_manager: pm,
                table_page_managers: table_pms,
                next_tuple_id: 1,
                planner: QueryPlanner::new()?,
                optimizer: QueryOptimizer::new()?,
                executor,
            }),
        })
    }

    fn execute_sql_inner(
        inner: &mut SqlEngineInner,
        sql: &str,
    ) -> Result<EngineOutput, EngineError> {
        let mut parser = SqlParser::new(sql).map_err(map_db_err)?;
        let stmts = parser.parse_multiple().map_err(map_db_err)?;
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
            SqlStatement::Select(sel) if sel.from.is_none() => eval_select_without_from(sel),
            SqlStatement::Select(_) => {
                let plan = inner.planner.create_plan(stmt).map_err(map_db_err)?;
                let optimized = inner.optimizer.optimize(plan).map_err(map_db_err)?;
                let rows = inner
                    .executor
                    .execute(&optimized.optimized_plan)
                    .map_err(map_db_err)?;
                rows_to_engine_output(rows)
            }
            SqlStatement::Insert(ins) => execute_insert(inner, stmt, ins),
            SqlStatement::Update(upd) => execute_update(inner, stmt, upd),
            SqlStatement::Delete(del) => execute_delete(inner, stmt, del),
            SqlStatement::CreateTable(ct) => execute_create_table(inner, &ct.table_name),
            SqlStatement::DropTable(dt) => execute_drop_table(inner, &dt.table_name),
            SqlStatement::BeginTransaction
            | SqlStatement::CommitTransaction
            | SqlStatement::RollbackTransaction => {
                Ok(EngineOutput::ExecutionOk { rows_affected: 0 })
            }
            _ => Err(EngineError::new(
                engine_error_code::UNSUPPORTED_SQL,
                "this SQL statement type is not supported by the server engine yet",
            )),
        }
    }
}

impl EngineHandle for SqlEngine {
    fn execute_sql(
        &self,
        sql: &str,
        _ctx: &mut SessionContext,
    ) -> Result<EngineOutput, EngineError> {
        let mut g = self.inner.lock().map_err(|_| {
            EngineError::new(engine_error_code::INTERNAL, "SQL engine lock poisoned")
        })?;
        Self::execute_sql_inner(&mut g, sql)
    }
}

fn map_db_err(e: DbError) -> EngineError {
    EngineError::new(engine_error_code::INTERNAL, e.to_string())
}

fn lock_poisoned_engine() -> EngineError {
    EngineError::new(engine_error_code::INTERNAL, "storage lock poisoned")
}

fn table_page_manager(
    inner: &mut SqlEngineInner,
    table: &str,
) -> Result<Arc<Mutex<PageManager>>, EngineError> {
    {
        let g = inner
            .table_page_managers
            .lock()
            .map_err(|_| lock_poisoned_engine())?;
        if let Some(pm) = g.get(table) {
            return Ok(pm.clone());
        }
    }
    let pm = match PageManager::open(inner.data_dir.clone(), table, PageManagerConfig::default()) {
        Ok(pm) => pm,
        Err(_) => PageManager::new(inner.data_dir.clone(), table, PageManagerConfig::default())
            .map_err(map_db_err)?,
    };
    let pm = Arc::new(Mutex::new(pm));
    {
        let mut g = inner
            .table_page_managers
            .lock()
            .map_err(|_| lock_poisoned_engine())?;
        g.insert(table.to_string(), pm.clone());
    }
    Ok(pm)
}

fn execute_create_table(
    inner: &mut SqlEngineInner,
    table: &str,
) -> Result<EngineOutput, EngineError> {
    let _ = table_page_manager(inner, table)?;
    Ok(EngineOutput::ExecutionOk { rows_affected: 0 })
}

fn execute_drop_table(
    inner: &mut SqlEngineInner,
    table: &str,
) -> Result<EngineOutput, EngineError> {
    {
        let mut g = inner
            .table_page_managers
            .lock()
            .map_err(|_| lock_poisoned_engine())?;
        g.remove(table);
    }
    let path = inner.data_dir.join(format!("{table}.tbl"));
    let _ = std::fs::remove_file(path);
    Ok(EngineOutput::ExecutionOk { rows_affected: 0 })
}

fn validate_plan(inner: &mut SqlEngineInner, stmt: &SqlStatement) -> Result<(), EngineError> {
    let plan = inner.planner.create_plan(stmt).map_err(map_db_err)?;
    let _ = inner.optimizer.optimize(plan).map_err(map_db_err)?;
    Ok(())
}

fn execute_insert(
    inner: &mut SqlEngineInner,
    stmt: &SqlStatement,
    insert: &InsertStatement,
) -> Result<EngineOutput, EngineError> {
    validate_plan(inner, stmt)?;
    match &insert.values {
        InsertValues::Select(sel) => {
            // Plan/execute the SELECT subquery and insert its resulting rows.
            let select_stmt = SqlStatement::Select((**sel).clone());
            let plan = inner
                .planner
                .create_plan(&select_stmt)
                .map_err(map_db_err)?;
            let optimized = inner.optimizer.optimize(plan).map_err(map_db_err)?;
            let rows = inner
                .executor
                .execute(&optimized.optimized_plan)
                .map_err(map_db_err)?;

            let pm_for_table = table_page_manager(inner, &insert.table)?;
            let mut rows_affected = 0u64;
            for r in rows {
                let tuple = build_insert_tuple_from_row(inner, insert.columns.as_ref(), &r)?;
                let bytes = tuple.to_bytes().map_err(map_db_err)?;
                let mut pm = pm_for_table.lock().map_err(|_| lock_poisoned_engine())?;
                pm.insert(&bytes).map_err(map_db_err)?;
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
            let pm_for_table = table_page_manager(inner, &insert.table)?;
            for row in rows {
                let tuple = build_insert_tuple(inner, insert.columns.as_ref(), row)?;
                let bytes = tuple.to_bytes().map_err(map_db_err)?;
                let mut pm = inner
                    .default_page_manager
                    .lock()
                    .map_err(|_| lock_poisoned_engine())?;
                // insert into per-table heap
                drop(pm);
                let mut pm = pm_for_table.lock().map_err(|_| lock_poisoned_engine())?;
                pm.insert(&bytes).map_err(map_db_err)?;
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
    inner: &mut SqlEngineInner,
    columns: Option<&Vec<String>>,
    row: &[Expression],
) -> Result<Tuple, EngineError> {
    let id = inner.next_tuple_id;
    inner.next_tuple_id = inner.next_tuple_id.saturating_add(1);
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
    Ok(tuple)
}

fn build_insert_tuple_from_row(
    inner: &mut SqlEngineInner,
    columns: Option<&Vec<String>>,
    row: &Row,
) -> Result<Tuple, EngineError> {
    let id = inner.next_tuple_id;
    inner.next_tuple_id = inner.next_tuple_id.saturating_add(1);
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
    Ok(tuple)
}

fn execute_update(
    inner: &mut SqlEngineInner,
    stmt: &SqlStatement,
    update: &UpdateStatement,
) -> Result<EngineOutput, EngineError> {
    validate_plan(inner, stmt)?;
    let pm_for_table = table_page_manager(inner, &update.table)?;
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
        for a in &update.assignments {
            let cv = expr_to_column_value(&a.value)?;
            tuple.set_value(&a.column, cv);
        }
        let new_bytes = tuple.to_bytes().map_err(map_db_err)?;
        pm.update(rid, &new_bytes).map_err(map_db_err)?;
        rows_affected += 1;
    }
    pm.flush_dirty_pages().map_err(map_db_err)?;
    Ok(EngineOutput::ExecutionOk { rows_affected })
}

fn execute_delete(
    inner: &mut SqlEngineInner,
    stmt: &SqlStatement,
    delete: &DeleteStatement,
) -> Result<EngineOutput, EngineError> {
    validate_plan(inner, stmt)?;
    let pm_for_table = table_page_manager(inner, &delete.table)?;
    let mut pm = pm_for_table.lock().map_err(|_| lock_poisoned_engine())?;
    let snapshot = pm.select(None).map_err(map_db_err)?;
    let mut to_delete: Vec<RecordId> = Vec::new();
    for (rid, data) in snapshot {
        let tuple = Tuple::from_bytes(&data).map_err(map_db_err)?;
        let keep = match &delete.where_clause {
            None => true,
            Some(expr) => match_where_tuple(expr, &tuple)?,
        };
        if keep {
            to_delete.push(rid);
        }
    }
    let mut rows_affected = 0u64;
    for rid in to_delete {
        pm.delete(rid).map_err(map_db_err)?;
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
