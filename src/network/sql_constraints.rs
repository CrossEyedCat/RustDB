//! In-memory enforcement of PRIMARY KEY, UNIQUE, and FOREIGN KEY for [`super::sql_engine::SqlEngine`].

use crate::catalog::schema::{ForeignKeyConstraintDef, SchemaManager, TableSchema};
use crate::common::types::{ColumnValue, DataType, RecordId};
use crate::network::engine::{engine_error_code, EngineError};
use crate::storage::tuple::Tuple;
use std::collections::HashMap;

/// Runtime maps keyed by serialized composite keys (see [`composite_key_from_tuple`]).
#[derive(Debug, Default)]
pub struct ConstraintRuntime {
    /// `table_name -> (key -> record id)`.
    pk: HashMap<String, HashMap<String, RecordId>>,
    /// `(table, unique_constraint_name) -> key -> record id`.
    unique: HashMap<(String, String), HashMap<String, RecordId>>,
    /// `(parent_table, parent_key)` -> number of referencing rows across all child tables.
    fk_refcount: HashMap<(String, String), u32>,
}

impl ConstraintRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    /// Removes PK/UNIQUE indexes for `table` (does not adjust FK refcounts).
    pub fn clear_table_maps(&mut self, table: &str) {
        self.pk.remove(table);
        self.unique.retain(|(t, _), _| t != table);
    }

    /// Removes FK refcount entries where the parent is `parent_table` (used when dropping a parent table).
    pub fn clear_fk_refs_to_parent(&mut self, parent_table: &str) {
        self.fk_refcount.retain(|(p, _), _| p != parent_table);
    }
}

pub fn column_value_key(cv: &ColumnValue) -> String {
    use DataType::*;
    if cv.is_null() {
        return "\x01NULL".to_string();
    }
    match &cv.data_type {
        Null => "\x01NULL".to_string(),
        Boolean(b) => format!("b:{b}"),
        TinyInt(n) => format!("t:{n}"),
        SmallInt(n) => format!("s:{n}"),
        Integer(n) => format!("i:{n}"),
        BigInt(n) => format!("l:{n}"),
        Float(f) => format!("f:{f}"),
        Double(d) => format!("d:{d}"),
        Char(s) | Varchar(s) | Text(s) => format!("str:{s}"),
        Date(s) => format!("date:{s}"),
        Time(s) => format!("time:{s}"),
        Timestamp(s) => format!("ts:{s}"),
        Blob(b) => format!("blob:{}", b.len()),
    }
}

pub fn composite_key_from_tuple(tuple: &Tuple, cols: &[String]) -> Result<String, EngineError> {
    let mut parts = Vec::with_capacity(cols.len());
    for c in cols {
        let Some(cv) = tuple.values.get(c) else {
            return Err(EngineError::new(
                engine_error_code::CONSTRAINT_VIOLATION,
                format!("missing value for key column {c}"),
            ));
        };
        if cv.is_null() {
            return Err(EngineError::new(
                engine_error_code::CONSTRAINT_VIOLATION,
                "NULL key column value in PRIMARY KEY or UNIQUE",
            ));
        }
        parts.push(column_value_key(cv));
    }
    Ok(parts.join("\0"))
}

pub fn resolve_fk_parent_columns(
    cat: &SchemaManager,
    fk: &ForeignKeyConstraintDef,
) -> Result<Vec<String>, EngineError> {
    if !fk.referenced_columns.is_empty() {
        return Ok(fk.referenced_columns.clone());
    }
    let parent = cat.schema(&fk.referenced_table).ok_or_else(|| {
        EngineError::new(
            engine_error_code::CONSTRAINT_VIOLATION,
            format!(
                "foreign key {}: referenced table {} not found",
                fk.name, fk.referenced_table
            ),
        )
    })?;
    let pk_cols = parent
        .primary_key
        .as_ref()
        .map(|(_, c)| c.clone())
        .ok_or_else(|| {
            EngineError::new(
                engine_error_code::CONSTRAINT_VIOLATION,
                format!(
                    "foreign key {}: referenced table {} has no PRIMARY KEY",
                    fk.name, fk.referenced_table
                ),
            )
        })?;
    Ok(pk_cols)
}

fn parent_key_for_fk(
    fk: &ForeignKeyConstraintDef,
    child_tuple: &Tuple,
    cat: &SchemaManager,
) -> Result<(String, String), EngineError> {
    let parent_cols = resolve_fk_parent_columns(cat, fk)?;
    if fk.columns.len() != parent_cols.len() {
        return Err(EngineError::new(
            engine_error_code::CONSTRAINT_VIOLATION,
            format!(
                "foreign key {}: column count does not match referenced key",
                fk.name
            ),
        ));
    }
    let parent_key = composite_key_from_tuple(child_tuple, &fk.columns)?;
    Ok((fk.referenced_table.clone(), parent_key))
}

pub fn register_row(
    runtime: &mut ConstraintRuntime,
    table: &str,
    rid: RecordId,
    tuple: &Tuple,
    schema: &TableSchema,
    cat: &SchemaManager,
) -> Result<(), EngineError> {
    // Validate all constraints before mutating any map (avoid partial state on error).
    if let Some((pk_name, cols)) = &schema.primary_key {
        let key = composite_key_from_tuple(tuple, cols)?;
        if let Some(&existing) = runtime.pk.get(table).and_then(|m| m.get(&key)) {
            if existing != rid {
                return Err(EngineError::new(
                    engine_error_code::CONSTRAINT_VIOLATION,
                    format!("PRIMARY KEY constraint {pk_name} violated"),
                ));
            }
        }
    }

    for uq in &schema.unique_constraints {
        let key = composite_key_from_tuple(tuple, &uq.columns)?;
        if let Some(&existing) = runtime
            .unique
            .get(&(table.to_string(), uq.name.clone()))
            .and_then(|m| m.get(&key))
        {
            if existing != rid {
                return Err(EngineError::new(
                    engine_error_code::CONSTRAINT_VIOLATION,
                    format!("UNIQUE constraint {} violated", uq.name),
                ));
            }
        }
    }

    for fk in &schema.foreign_keys {
        let (parent_table, parent_key) = parent_key_for_fk(fk, tuple, cat)?;
        let parent_has_row = runtime
            .pk
            .get(&parent_table)
            .map(|m| m.contains_key(&parent_key))
            .unwrap_or(false);
        if !parent_has_row {
            return Err(EngineError::new(
                engine_error_code::CONSTRAINT_VIOLATION,
                format!("foreign key {}: missing parent row", fk.name),
            ));
        }
    }

    // Commit
    if let Some((_, cols)) = &schema.primary_key {
        let key = composite_key_from_tuple(tuple, cols)?;
        runtime
            .pk
            .entry(table.to_string())
            .or_default()
            .insert(key, rid);
    }

    for uq in &schema.unique_constraints {
        let key = composite_key_from_tuple(tuple, &uq.columns)?;
        runtime
            .unique
            .entry((table.to_string(), uq.name.clone()))
            .or_default()
            .insert(key, rid);
    }

    for fk in &schema.foreign_keys {
        let (parent_table, parent_key) = parent_key_for_fk(fk, tuple, cat)?;
        let fk_key = (parent_table, parent_key);
        *runtime.fk_refcount.entry(fk_key).or_insert(0) += 1;
    }

    Ok(())
}

pub fn unregister_row(
    runtime: &mut ConstraintRuntime,
    table: &str,
    rid: RecordId,
    tuple: &Tuple,
    schema: &TableSchema,
    cat: &SchemaManager,
) -> Result<(), EngineError> {
    if let Some((_, cols)) = &schema.primary_key {
        let key = composite_key_from_tuple(tuple, cols)?;
        if let Some(map) = runtime.pk.get_mut(table) {
            if map.get(&key).copied() == Some(rid) {
                map.remove(&key);
            }
        }
    }

    for uq in &schema.unique_constraints {
        let key = composite_key_from_tuple(tuple, &uq.columns)?;
        if let Some(map) = runtime
            .unique
            .get_mut(&(table.to_string(), uq.name.clone()))
        {
            if map.get(&key).copied() == Some(rid) {
                map.remove(&key);
            }
        }
    }

    for fk in &schema.foreign_keys {
        let (parent_table, parent_key) = parent_key_for_fk(fk, tuple, cat)?;
        let fk_key = (parent_table, parent_key);
        if let Some(n) = runtime.fk_refcount.get_mut(&fk_key) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                runtime.fk_refcount.remove(&fk_key);
            }
        }
    }

    Ok(())
}

pub fn fk_blocks_parent_delete(
    runtime: &ConstraintRuntime,
    parent_table: &str,
    tuple: &Tuple,
    schema: &TableSchema,
) -> Result<bool, EngineError> {
    let Some((_, cols)) = &schema.primary_key else {
        return Ok(false);
    };
    let key = composite_key_from_tuple(tuple, cols)?;
    let fk_key = (parent_table.to_string(), key);
    Ok(runtime.fk_refcount.get(&fk_key).copied().unwrap_or(0) > 0)
}
