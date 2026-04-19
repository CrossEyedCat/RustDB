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

/// Looks up a column value by logical name, with fallback for legacy rows where
/// `INSERT INTO t VALUES (...)` stored `col1`, `col2`, … in [`Tuple::values`] order.
fn tuple_column_value<'a>(
    tuple: &'a Tuple,
    logical_name: &str,
    schema: &TableSchema,
) -> Option<&'a ColumnValue> {
    if let Some(v) = tuple.values.get(logical_name) {
        return Some(v);
    }
    let idx = schema.columns.iter().position(|c| c.name == logical_name)?;
    tuple.values.get(&format!("col{}", idx + 1))
}

/// Like [`composite_key_from_tuple`], but resolves keys using [`TableSchema`] so legacy
/// `colN` tuple fields match catalog column names (fixes constraint rebuild on open).
pub fn composite_key_from_tuple_with_schema(
    tuple: &Tuple,
    cols: &[String],
    schema: &TableSchema,
) -> Result<String, EngineError> {
    let mut parts = Vec::with_capacity(cols.len());
    for c in cols {
        let Some(cv) = tuple_column_value(tuple, c, schema) else {
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
    child_schema: &TableSchema,
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
    let parent_key = composite_key_from_tuple_with_schema(child_tuple, &fk.columns, child_schema)?;
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
        let key = composite_key_from_tuple_with_schema(tuple, cols, schema)?;
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
        let key = composite_key_from_tuple_with_schema(tuple, &uq.columns, schema)?;
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
        let (parent_table, parent_key) = parent_key_for_fk(fk, tuple, cat, schema)?;
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
        let key = composite_key_from_tuple_with_schema(tuple, cols, schema)?;
        runtime
            .pk
            .entry(table.to_string())
            .or_default()
            .insert(key, rid);
    }

    for uq in &schema.unique_constraints {
        let key = composite_key_from_tuple_with_schema(tuple, &uq.columns, schema)?;
        runtime
            .unique
            .entry((table.to_string(), uq.name.clone()))
            .or_default()
            .insert(key, rid);
    }

    for fk in &schema.foreign_keys {
        let (parent_table, parent_key) = parent_key_for_fk(fk, tuple, cat, schema)?;
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
        let key = composite_key_from_tuple_with_schema(tuple, cols, schema)?;
        if let Some(map) = runtime.pk.get_mut(table) {
            if map.get(&key).copied() == Some(rid) {
                map.remove(&key);
            }
        }
    }

    for uq in &schema.unique_constraints {
        let key = composite_key_from_tuple_with_schema(tuple, &uq.columns, schema)?;
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
        let (parent_table, parent_key) = parent_key_for_fk(fk, tuple, cat, schema)?;
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
    let key = composite_key_from_tuple_with_schema(tuple, cols, schema)?;
    let fk_key = (parent_table.to_string(), key);
    Ok(runtime.fk_refcount.get(&fk_key).copied().unwrap_or(0) > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::schema::{ForeignKeyConstraintDef, TableSchema, UniqueConstraintDef};

    fn schema_pk(table: &str, cols: &[&str]) -> TableSchema {
        TableSchema {
            table_name: table.to_string(),
            columns: vec![],
            primary_key: Some((
                "PRIMARY".to_string(),
                cols.iter().map(|s| s.to_string()).collect(),
            )),
            unique_constraints: vec![],
            foreign_keys: vec![],
            check_constraints: vec![],
        }
    }

    fn tuple_with(cols: &[(&str, ColumnValue)]) -> Tuple {
        let mut t = Tuple::new(1);
        for (k, v) in cols {
            t.values.insert((*k).to_string(), v.clone());
        }
        t
    }

    #[test]
    fn composite_key_with_schema_maps_legacy_coln_to_logical_name() {
        use crate::common::types::Column;
        let sch = TableSchema {
            table_name: "legacy".to_string(),
            columns: vec![Column::new("id".to_string(), DataType::Integer(0))],
            primary_key: Some(("pk_legacy".to_string(), vec!["id".to_string()])),
            unique_constraints: vec![],
            foreign_keys: vec![],
            check_constraints: vec![],
        };
        let mut t = Tuple::new(1);
        t.set_value("col1", ColumnValue::new(DataType::Integer(99)));
        let k_named = composite_key_from_tuple_with_schema(&t, &["id".to_string()], &sch).unwrap();
        let k_direct = composite_key_from_tuple(
            &tuple_with(&[("id", ColumnValue::new(DataType::Integer(99)))]),
            &["id".to_string()],
        )
        .unwrap();
        assert_eq!(k_named, k_direct);
    }

    #[test]
    fn composite_key_missing_or_null_is_error() {
        let t = Tuple::new(1);
        let err = composite_key_from_tuple(&t, &["id".to_string()]).unwrap_err();
        assert_eq!(err.code, engine_error_code::CONSTRAINT_VIOLATION);

        let t = tuple_with(&[("id", ColumnValue::new(DataType::Null))]);
        let err = composite_key_from_tuple(&t, &["id".to_string()]).unwrap_err();
        assert_eq!(err.code, engine_error_code::CONSTRAINT_VIOLATION);
    }

    #[test]
    fn resolve_fk_parent_columns_prefers_explicit_list() {
        let mut cat = SchemaManager::new().unwrap();
        cat.register_schema(schema_pk("p", &["id"]));
        let fk = ForeignKeyConstraintDef {
            name: "fk".to_string(),
            columns: vec!["pid".to_string()],
            referenced_table: "p".to_string(),
            referenced_columns: vec!["id".to_string()],
        };
        let cols = resolve_fk_parent_columns(&cat, &fk).unwrap();
        assert_eq!(cols, vec!["id".to_string()]);
    }

    #[test]
    fn resolve_fk_parent_columns_errors_when_parent_missing_or_no_pk() {
        let cat = SchemaManager::new().unwrap();
        let fk = ForeignKeyConstraintDef {
            name: "fk".to_string(),
            columns: vec!["pid".to_string()],
            referenced_table: "missing".to_string(),
            referenced_columns: vec![],
        };
        assert!(resolve_fk_parent_columns(&cat, &fk).is_err());

        let mut cat = SchemaManager::new().unwrap();
        cat.register_schema(TableSchema {
            table_name: "nopk".to_string(),
            columns: vec![],
            primary_key: None,
            unique_constraints: vec![],
            foreign_keys: vec![],
            check_constraints: vec![],
        });
        let fk = ForeignKeyConstraintDef {
            name: "fk".to_string(),
            columns: vec!["pid".to_string()],
            referenced_table: "nopk".to_string(),
            referenced_columns: vec![],
        };
        assert!(resolve_fk_parent_columns(&cat, &fk).is_err());
    }

    #[test]
    fn register_row_enforces_pk_unique_and_fk() {
        let mut cat = SchemaManager::new().unwrap();
        let parent = schema_pk("p", &["id"]);
        cat.register_schema(parent.clone());

        let child = TableSchema {
            table_name: "c".to_string(),
            columns: vec![],
            primary_key: None,
            unique_constraints: vec![],
            foreign_keys: vec![ForeignKeyConstraintDef {
                name: "fk".to_string(),
                columns: vec!["pid".to_string()],
                referenced_table: "p".to_string(),
                referenced_columns: vec![],
            }],
            check_constraints: vec![],
        };
        cat.register_schema(child.clone());

        let mut rt = ConstraintRuntime::new();
        let p1 = tuple_with(&[("id", ColumnValue::new(DataType::Integer(1)))]);
        register_row(&mut rt, "p", 10, &p1, &parent, &cat).unwrap();

        // PK violation.
        let err = register_row(&mut rt, "p", 11, &p1, &parent, &cat).unwrap_err();
        assert_eq!(err.code, engine_error_code::CONSTRAINT_VIOLATION);

        // UNIQUE violation.
        let mut uq_schema = schema_pk("u", &["id"]);
        uq_schema.unique_constraints.push(UniqueConstraintDef {
            name: "uq".to_string(),
            columns: vec!["a".to_string()],
        });
        let u1 = tuple_with(&[
            ("id", ColumnValue::new(DataType::Integer(1))),
            ("a", ColumnValue::new(DataType::Integer(7))),
        ]);
        let u2 = tuple_with(&[
            ("id", ColumnValue::new(DataType::Integer(2))),
            ("a", ColumnValue::new(DataType::Integer(7))),
        ]);
        register_row(&mut rt, "u", 20, &u1, &uq_schema, &cat).unwrap();
        assert!(register_row(&mut rt, "u", 21, &u2, &uq_schema, &cat).is_err());

        // FK violation (no parent row for pid=2).
        let c_bad = tuple_with(&[("pid", ColumnValue::new(DataType::Integer(2)))]);
        assert!(register_row(&mut rt, "c", 30, &c_bad, &child, &cat).is_err());

        // FK OK for pid=1.
        let c_ok = tuple_with(&[("pid", ColumnValue::new(DataType::Integer(1)))]);
        register_row(&mut rt, "c", 31, &c_ok, &child, &cat).unwrap();
    }

    #[test]
    fn unregister_row_updates_maps_and_fk_refcount() {
        let mut cat = SchemaManager::new().unwrap();
        let parent = schema_pk("p", &["id"]);
        cat.register_schema(parent.clone());
        let child = TableSchema {
            table_name: "c".to_string(),
            columns: vec![],
            primary_key: None,
            unique_constraints: vec![],
            foreign_keys: vec![ForeignKeyConstraintDef {
                name: "fk".to_string(),
                columns: vec!["pid".to_string()],
                referenced_table: "p".to_string(),
                referenced_columns: vec![],
            }],
            check_constraints: vec![],
        };
        cat.register_schema(child.clone());

        let mut rt = ConstraintRuntime::new();
        let p1 = tuple_with(&[("id", ColumnValue::new(DataType::Integer(1)))]);
        register_row(&mut rt, "p", 1, &p1, &parent, &cat).unwrap();
        let c1 = tuple_with(&[("pid", ColumnValue::new(DataType::Integer(1)))]);
        register_row(&mut rt, "c", 2, &c1, &child, &cat).unwrap();

        assert!(fk_blocks_parent_delete(&rt, "p", &p1, &parent).unwrap());
        unregister_row(&mut rt, "c", 2, &c1, &child, &cat).unwrap();
        assert!(!fk_blocks_parent_delete(&rt, "p", &p1, &parent).unwrap());

        // Clear helpers are no-ops/safe.
        rt.clear_fk_refs_to_parent("p");
        rt.clear_table_maps("p");
    }

    #[test]
    fn column_value_key_covers_common_types() {
        assert_eq!(
            column_value_key(&ColumnValue::new(DataType::Boolean(true))),
            "b:true"
        );
        assert_eq!(
            column_value_key(&ColumnValue::new(DataType::Integer(7))),
            "i:7"
        );
        assert_eq!(
            column_value_key(&ColumnValue::new(DataType::Varchar("x".to_string()))),
            "str:x"
        );
        assert_eq!(
            column_value_key(&ColumnValue::new(DataType::Blob(vec![1, 2, 3]))),
            "blob:3"
        );
        assert_eq!(
            column_value_key(&ColumnValue::new(DataType::Null)),
            "\x01NULL".to_string()
        );
    }

    #[test]
    fn composite_key_multiple_columns_is_delimited() {
        let t = tuple_with(&[
            ("a", ColumnValue::new(DataType::Integer(1))),
            ("b", ColumnValue::new(DataType::Integer(2))),
        ]);
        let k = composite_key_from_tuple(&t, &["a".to_string(), "b".to_string()]).unwrap();
        assert!(k.contains('\0'));
    }
}
