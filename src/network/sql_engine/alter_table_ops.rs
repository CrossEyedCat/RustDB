//! `ALTER TABLE … ADD/DROP/RENAME/MODIFY` for [`super::SqlEngineState`].

use super::{
    expr_to_column_value, lock_poisoned_engine, map_db_err, persist_catalog,
    rebuild_all_constraint_runtime, sql_type_to_column_datatype, table_page_manager,
    validate_new_table_fks, EngineError, SqlEngineState,
};
use crate::catalog::schema::{
    CheckConstraint, ForeignKeyConstraintDef, SchemaManager, TableSchema, UniqueConstraintDef,
};
use crate::common::types::{Column, ColumnValue, DataType};
use crate::network::engine::engine_error_code;
use crate::parser::ast::{
    ColumnConstraint, ColumnDefinition, DataType as SqlDataType, Expression, InList,
};
use crate::storage::tuple::Tuple;

mod expr_walk {
    use super::Expression;
    use crate::parser::ast::InList;

    pub(super) fn mentions_column(expr: &Expression, col: &str) -> bool {
        match expr {
            Expression::Identifier(s) => s == col,
            Expression::QualifiedIdentifier { column, .. } => column == col,
            Expression::Literal(_) => false,
            Expression::BinaryOp { left, right, .. } => {
                mentions_column(left, col) || mentions_column(right, col)
            }
            Expression::UnaryOp { expr, .. } => mentions_column(expr, col),
            Expression::Function { args, .. } => args.iter().any(|a| mentions_column(a, col)),
            Expression::Case {
                expr,
                when_clauses,
                else_clause,
            } => {
                expr.as_ref().is_some_and(|e| mentions_column(e, col))
                    || when_clauses.iter().any(|w| {
                        mentions_column(&w.condition, col) || mentions_column(&w.result, col)
                    })
                    || else_clause
                        .as_ref()
                        .is_some_and(|e| mentions_column(e, col))
            }
            Expression::Exists(_) => false,
            Expression::In { expr, list } => {
                mentions_column(expr, col)
                    || match list {
                        InList::Values(v) => v.iter().any(|e| mentions_column(e, col)),
                        InList::Subquery(_) => false,
                    }
            }
            Expression::Between { expr, low, high } => {
                mentions_column(expr, col)
                    || mentions_column(low, col)
                    || mentions_column(high, col)
            }
            Expression::IsNull { expr, .. } => mentions_column(expr, col),
            Expression::Like { expr, pattern, .. } => {
                mentions_column(expr, col) || mentions_column(pattern, col)
            }
        }
    }
}

fn err(msg: impl Into<String>) -> EngineError {
    EngineError::new(engine_error_code::CONSTRAINT_VIOLATION, msg)
}

fn unsupported(msg: impl Into<String>) -> EngineError {
    EngineError::new(engine_error_code::UNSUPPORTED_SQL, msg)
}

fn table_must_exist<'a>(
    cat: &'a SchemaManager,
    table: &str,
) -> Result<&'a TableSchema, EngineError> {
    cat.schema(table).ok_or_else(|| {
        EngineError::new(
            engine_error_code::CONSTRAINT_VIOLATION,
            format!("table {} does not exist", table),
        )
    })
}

fn rename_column_in_expression(expr: &Expression, table: &str, old: &str, new: &str) -> Expression {
    match expr {
        Expression::Identifier(s) => {
            if s == old {
                Expression::Identifier(new.to_string())
            } else {
                Expression::Identifier(s.clone())
            }
        }
        Expression::QualifiedIdentifier {
            table: t,
            column: c,
        } => {
            if t == table && c == old {
                Expression::QualifiedIdentifier {
                    table: t.clone(),
                    column: new.to_string(),
                }
            } else {
                Expression::QualifiedIdentifier {
                    table: t.clone(),
                    column: c.clone(),
                }
            }
        }
        Expression::Literal(l) => Expression::Literal(l.clone()),
        Expression::BinaryOp { left, op, right } => Expression::BinaryOp {
            left: Box::new(rename_column_in_expression(left, table, old, new)),
            op: op.clone(),
            right: Box::new(rename_column_in_expression(right, table, old, new)),
        },
        Expression::UnaryOp { op, expr: e } => Expression::UnaryOp {
            op: op.clone(),
            expr: Box::new(rename_column_in_expression(e, table, old, new)),
        },
        Expression::Function { name, args } => Expression::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| rename_column_in_expression(a, table, old, new))
                .collect(),
        },
        Expression::Case {
            expr,
            when_clauses,
            else_clause,
        } => Expression::Case {
            expr: expr
                .as_ref()
                .map(|e| Box::new(rename_column_in_expression(e, table, old, new))),
            when_clauses: when_clauses
                .iter()
                .map(|w| crate::parser::ast::WhenClause {
                    condition: rename_column_in_expression(&w.condition, table, old, new),
                    result: rename_column_in_expression(&w.result, table, old, new),
                })
                .collect(),
            else_clause: else_clause
                .as_ref()
                .map(|e| Box::new(rename_column_in_expression(e, table, old, new))),
        },
        Expression::Exists(s) => Expression::Exists(s.clone()),
        Expression::In { expr, list } => {
            let list = match list {
                InList::Values(v) => InList::Values(
                    v.iter()
                        .map(|e| rename_column_in_expression(e, table, old, new))
                        .collect(),
                ),
                InList::Subquery(s) => InList::Subquery(s.clone()),
            };
            Expression::In {
                expr: Box::new(rename_column_in_expression(expr, table, old, new)),
                list,
            }
        }
        Expression::Between { expr, low, high } => Expression::Between {
            expr: Box::new(rename_column_in_expression(expr, table, old, new)),
            low: Box::new(rename_column_in_expression(low, table, old, new)),
            high: Box::new(rename_column_in_expression(high, table, old, new)),
        },
        Expression::IsNull { expr, negated } => Expression::IsNull {
            expr: Box::new(rename_column_in_expression(expr, table, old, new)),
            negated: *negated,
        },
        Expression::Like {
            expr,
            pattern,
            negated,
        } => Expression::Like {
            expr: Box::new(rename_column_in_expression(expr, table, old, new)),
            pattern: Box::new(rename_column_in_expression(pattern, table, old, new)),
            negated: *negated,
        },
    }
}

fn rewrite_tuples<F>(state: &SqlEngineState, table: &str, mut f: F) -> Result<(), EngineError>
where
    F: FnMut(&mut Tuple) -> Result<(), EngineError>,
{
    let pm = table_page_manager(state, table)?;
    let snapshot = pm
        .lock()
        .map_err(|_| lock_poisoned_engine())?
        .select(None)
        .map_err(map_db_err)?;
    for (rid, data) in snapshot {
        let mut t = Tuple::from_bytes(&data).map_err(map_db_err)?;
        f(&mut t)?;
        let bytes = t.to_bytes().map_err(map_db_err)?;
        pm.lock()
            .map_err(|_| lock_poisoned_engine())?
            .update(rid, &bytes)
            .map_err(map_db_err)?;
    }
    pm.lock()
        .map_err(|_| lock_poisoned_engine())?
        .flush_dirty_pages()
        .map_err(map_db_err)?;
    Ok(())
}

fn row_count(state: &SqlEngineState, table: &str) -> Result<usize, EngineError> {
    let pm = table_page_manager(state, table)?;
    let n = pm
        .lock()
        .map_err(|_| lock_poisoned_engine())?
        .select(None)
        .map_err(map_db_err)?
        .len();
    Ok(n)
}

fn merge_column_definition_into_schema(
    schema: &mut TableSchema,
    c: &ColumnDefinition,
) -> Result<(), EngineError> {
    if schema.columns.iter().any(|x| x.name == c.name) {
        return Err(err(format!(
            "column {} already exists in table {}",
            c.name, schema.table_name
        )));
    }
    let mut col = Column::new(c.name.clone(), sql_type_to_column_datatype(&c.data_type));
    for cc in &c.constraints {
        match cc {
            ColumnConstraint::NotNull => col.not_null = true,
            ColumnConstraint::Default(expr) => {
                let cv = expr_to_column_value(expr)?;
                col.default_value = Some(cv);
            }
            ColumnConstraint::Check(expr) => {
                schema.check_constraints.push(CheckConstraint {
                    name: format!("chk_{}_{}", schema.table_name, c.name),
                    expr: expr.clone(),
                });
            }
            ColumnConstraint::Unique => {
                schema.unique_constraints.push(UniqueConstraintDef {
                    name: format!("uq_{}_{}", schema.table_name, c.name),
                    columns: vec![c.name.clone()],
                });
            }
            ColumnConstraint::PrimaryKey => {
                if schema.primary_key.is_some() {
                    return Err(err(
                        "cannot add a second PRIMARY KEY via ADD COLUMN; drop the existing key first",
                    ));
                }
                schema.primary_key =
                    Some((format!("pk_{}", schema.table_name), vec![c.name.clone()]));
                col.not_null = true;
            }
            ColumnConstraint::References { table, column } => {
                let ref_cols = match column {
                    Some(rc) => vec![rc.clone()],
                    None => Vec::new(),
                };
                schema.foreign_keys.push(ForeignKeyConstraintDef {
                    name: format!("fk_{}_{}", schema.table_name, c.name),
                    columns: vec![c.name.clone()],
                    referenced_table: table.clone(),
                    referenced_columns: ref_cols,
                });
            }
        }
    }
    schema.columns.push(col);
    Ok(())
}

pub(super) fn add_column(
    state: &SqlEngineState,
    table: &str,
    c: &ColumnDefinition,
) -> Result<(), EngineError> {
    let _ = table_page_manager(state, table)?;
    let n = row_count(state, table)?;
    let not_null_no_default = {
        let mut col = Column::new(c.name.clone(), sql_type_to_column_datatype(&c.data_type));
        for cc in &c.constraints {
            match cc {
                ColumnConstraint::NotNull => col.not_null = true,
                ColumnConstraint::Default(expr) => {
                    let cv = expr_to_column_value(expr)?;
                    col.default_value = Some(cv);
                }
                _ => {}
            }
        }
        col.not_null && col.default_value.is_none()
    };
    if not_null_no_default && n > 0 {
        return Err(err(
            "ADD COLUMN ... NOT NULL requires a DEFAULT when the table is not empty",
        ));
    }
    {
        let mut cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
        let schema = cat
            .schema_mut(table)
            .ok_or_else(|| err(format!("table {table} does not exist")))?;
        merge_column_definition_into_schema(schema, c)?;
    }
    {
        let cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
        let schema = table_must_exist(&cat, table)?;
        validate_new_table_fks(&cat, schema)?;
    }
    let fill = {
        let cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
        let sch = table_must_exist(&cat, table)?;
        let col_def = sch
            .columns
            .iter()
            .find(|x| x.name == c.name)
            .ok_or_else(|| {
                EngineError::new(engine_error_code::INTERNAL, "column missing after ADD")
            })?;
        col_def
            .default_value
            .clone()
            .unwrap_or_else(ColumnValue::null)
    };
    rewrite_tuples(state, table, |t: &mut Tuple| {
        t.set_value(&c.name, fill.clone());
        Ok(())
    })?;
    rebuild_all_constraint_runtime(state)?;
    persist_catalog(state)?;
    Ok(())
}

fn parent_pk_cols(cat: &SchemaManager, parent: &str) -> Option<Vec<String>> {
    cat.schema(parent)?
        .primary_key
        .as_ref()
        .map(|(_, cols)| cols.clone())
}

fn column_referenced_by_fk(cat: &SchemaManager, table: &str, col: &str) -> bool {
    for tname in cat.table_names() {
        let Some(sch) = cat.schema(&tname) else {
            continue;
        };
        for fk in &sch.foreign_keys {
            if fk.referenced_table != table {
                continue;
            }
            let refcols = if fk.referenced_columns.is_empty() {
                match parent_pk_cols(cat, table) {
                    Some(p) => p,
                    None => continue,
                }
            } else {
                fk.referenced_columns.clone()
            };
            if refcols.iter().any(|c| c == col) {
                return true;
            }
        }
    }
    false
}

fn column_used_in_schema_lists(schema: &TableSchema, col: &str) -> bool {
    if let Some((_, pk)) = &schema.primary_key {
        if pk.iter().any(|c| c == col) {
            return true;
        }
    }
    if schema
        .unique_constraints
        .iter()
        .any(|u| u.columns.contains(&col.to_string()))
    {
        return true;
    }
    if schema
        .foreign_keys
        .iter()
        .any(|fk| fk.columns.contains(&col.to_string()))
    {
        return true;
    }
    false
}

pub(super) fn drop_column(
    state: &SqlEngineState,
    table: &str,
    col: &str,
) -> Result<(), EngineError> {
    let _ = table_page_manager(state, table)?;
    let cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
    let schema = table_must_exist(&cat, table)?;
    if !schema.columns.iter().any(|c| c.name == col) {
        return Err(err(format!("column {col} does not exist")));
    }
    if column_used_in_schema_lists(schema, col) {
        return Err(err(format!(
            "cannot DROP COLUMN {col}: used in PRIMARY KEY, UNIQUE, or FOREIGN KEY; drop constraints first"
        )));
    }
    if column_referenced_by_fk(&cat, table, col) {
        return Err(err(format!(
            "cannot DROP COLUMN {col}: referenced by a FOREIGN KEY from another table"
        )));
    }
    drop(cat);

    {
        let mut cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
        let schema = cat.schema_mut(table).ok_or_else(|| err("table missing"))?;
        schema.columns.retain(|c| c.name != col);
        schema
            .check_constraints
            .retain(|chk| !expr_walk::mentions_column(&chk.expr, col));
    }
    rewrite_tuples(state, table, |t: &mut Tuple| {
        t.values.remove(col);
        Ok(())
    })?;
    rebuild_all_constraint_runtime(state)?;
    persist_catalog(state)?;
    Ok(())
}

pub(super) fn rename_column(
    state: &SqlEngineState,
    table: &str,
    old_name: &str,
    new_name: &str,
) -> Result<(), EngineError> {
    if old_name == new_name {
        return Ok(());
    }
    let _ = table_page_manager(state, table)?;
    {
        let cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
        let schema = table_must_exist(&cat, table)?;
        if !schema.columns.iter().any(|c| c.name == old_name) {
            return Err(err(format!("column {old_name} does not exist")));
        }
        if schema.columns.iter().any(|c| c.name == new_name) {
            return Err(err(format!("column {new_name} already exists")));
        }
    }
    {
        let mut cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
        for tname in cat.table_names() {
            let Some(sch) = cat.schema_mut(&tname) else {
                continue;
            };
            for fk in &mut sch.foreign_keys {
                if fk.referenced_table == table {
                    for rc in &mut fk.referenced_columns {
                        if rc == old_name {
                            *rc = new_name.to_string();
                        }
                    }
                }
            }
        }
        let schema = cat.schema_mut(table).ok_or_else(|| err("table missing"))?;
        for col in &mut schema.columns {
            if col.name == old_name {
                col.name = new_name.to_string();
            }
        }
        if let Some((_, pk)) = schema.primary_key.as_mut() {
            for c in pk.iter_mut() {
                if c == old_name {
                    *c = new_name.to_string();
                }
            }
        }
        for u in &mut schema.unique_constraints {
            for c in &mut u.columns {
                if c == old_name {
                    *c = new_name.to_string();
                }
            }
        }
        for fk in &mut schema.foreign_keys {
            for c in &mut fk.columns {
                if c == old_name {
                    *c = new_name.to_string();
                }
            }
        }
        for chk in &mut schema.check_constraints {
            chk.expr = rename_column_in_expression(&chk.expr, table, old_name, new_name);
        }
    }
    rewrite_tuples(state, table, |t: &mut Tuple| {
        if let Some(v) = t.values.remove(old_name) {
            t.set_value(new_name, v);
        }
        Ok(())
    })?;
    rebuild_all_constraint_runtime(state)?;
    persist_catalog(state)?;
    Ok(())
}

pub(super) fn rename_table(
    state: &SqlEngineState,
    old: &str,
    new: &str,
) -> Result<(), EngineError> {
    if old == new {
        return Ok(());
    }
    {
        let cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
        if cat.schema(new).is_some() {
            return Err(err(format!("table {new} already exists")));
        }
        table_must_exist(&cat, old)?;
    }

    {
        let mut g = state
            .table_page_managers
            .lock()
            .map_err(|_| lock_poisoned_engine())?;
        g.remove(old);
    }

    let old_path = state.data_dir.join(format!("{old}.tbl"));
    let new_path = state.data_dir.join(format!("{new}.tbl"));
    if new_path.exists() {
        return Err(err(format!("cannot rename to {new}: file already exists")));
    }
    if old_path.is_file() {
        std::fs::rename(&old_path, &new_path).map_err(|e| {
            EngineError::new(
                engine_error_code::INTERNAL,
                format!("rename heap file: {e}"),
            )
        })?;
    }

    {
        let mut cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
        cat.rename_table(old, new).map_err(map_db_err)?;
    }

    let _ = table_page_manager(state, new)?;
    rebuild_all_constraint_runtime(state)?;
    persist_catalog(state)?;
    Ok(())
}

fn coerce_value_to_sql_type(
    cv: &ColumnValue,
    sql_t: &SqlDataType,
) -> Result<ColumnValue, EngineError> {
    if cv.is_null {
        return Ok(ColumnValue::null());
    }
    match (&cv.data_type, sql_t) {
        (DataType::Integer(i), SqlDataType::Integer) => Ok(ColumnValue::new(DataType::Integer(*i))),
        (DataType::BigInt(i), SqlDataType::BigInt) => Ok(ColumnValue::new(DataType::BigInt(*i))),
        (DataType::Integer(i), SqlDataType::BigInt) => {
            Ok(ColumnValue::new(DataType::BigInt(*i as i64)))
        }
        (DataType::BigInt(i), SqlDataType::Integer) => {
            if *i >= i32::MIN as i64 && *i <= i32::MAX as i64 {
                Ok(ColumnValue::new(DataType::Integer(*i as i32)))
            } else {
                Err(unsupported(
                    "cannot convert BIGINT value to INTEGER: out of range",
                ))
            }
        }
        (DataType::Float(f), SqlDataType::Real | SqlDataType::Double) => {
            Ok(ColumnValue::new(DataType::Double(*f as f64)))
        }
        (DataType::Double(f), SqlDataType::Real | SqlDataType::Double) => {
            Ok(ColumnValue::new(DataType::Double(*f)))
        }
        (DataType::Varchar(s), SqlDataType::Text) => {
            Ok(ColumnValue::new(DataType::Text(s.clone())))
        }
        (DataType::Text(s), SqlDataType::Varchar { .. }) => {
            Ok(ColumnValue::new(DataType::Varchar(s.clone())))
        }
        (DataType::Varchar(s), SqlDataType::Varchar { .. }) => {
            Ok(ColumnValue::new(DataType::Varchar(s.clone())))
        }
        (DataType::Text(s), SqlDataType::Text) => Ok(ColumnValue::new(DataType::Text(s.clone()))),
        _ => {
            let template = sql_type_to_column_datatype(sql_t);
            if std::mem::discriminant(&cv.data_type) == std::mem::discriminant(&template) {
                Ok(ColumnValue::new(cv.data_type.clone()))
            } else {
                Err(unsupported(format!(
                    "MODIFY COLUMN type change from {:?} to {:?} is not supported yet",
                    cv.data_type, sql_t
                )))
            }
        }
    }
}

pub(super) fn modify_column(
    state: &SqlEngineState,
    table: &str,
    c: &ColumnDefinition,
) -> Result<(), EngineError> {
    let _ = table_page_manager(state, table)?;
    let col_name = c.name.clone();
    let mut new_col = Column::new(col_name.clone(), sql_type_to_column_datatype(&c.data_type));
    for cc in &c.constraints {
        match cc {
            ColumnConstraint::NotNull => new_col.not_null = true,
            ColumnConstraint::Default(expr) => {
                new_col.default_value = Some(expr_to_column_value(expr)?);
            }
            ColumnConstraint::Check(_)
            | ColumnConstraint::Unique
            | ColumnConstraint::PrimaryKey
            | ColumnConstraint::References { .. } => {
                return Err(unsupported(
                    "MODIFY COLUMN supports only type / NOT NULL / DEFAULT changes; add or drop constraints separately",
                ));
            }
        }
    }
    let old_snap = {
        let cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
        let sch = table_must_exist(&cat, table)?;
        sch.columns
            .iter()
            .find(|x| x.name == col_name)
            .cloned()
            .ok_or_else(|| err(format!("column {col_name} does not exist")))?
    };

    if new_col.not_null && !old_snap.not_null {
        let pm = table_page_manager(state, table)?;
        let snapshot = pm
            .lock()
            .map_err(|_| lock_poisoned_engine())?
            .select(None)
            .map_err(map_db_err)?;
        for (_, data) in snapshot {
            let t = Tuple::from_bytes(&data).map_err(map_db_err)?;
            let bad = match t.values.get(&col_name) {
                None => true,
                Some(v) => v.is_null,
            };
            if bad {
                return Err(err(
                    "cannot add NOT NULL: existing rows contain NULL for this column",
                ));
            }
        }
    }

    {
        let mut cat = state.catalog.lock().map_err(|_| lock_poisoned_engine())?;
        let sch = cat.schema_mut(table).ok_or_else(|| err("table missing"))?;
        let idx = sch
            .columns
            .iter()
            .position(|x| x.name == col_name)
            .ok_or_else(|| err("column missing"))?;
        new_col.comment = sch.columns[idx].comment.clone();
        sch.columns[idx] = new_col;
    }

    rewrite_tuples(state, table, |t: &mut Tuple| {
        if let Some(v) = t.values.get_mut(&col_name) {
            *v = coerce_value_to_sql_type(v, &c.data_type)?;
        }
        Ok(())
    })?;
    rebuild_all_constraint_runtime(state)?;
    persist_catalog(state)?;
    Ok(())
}
