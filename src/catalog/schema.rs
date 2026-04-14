//! Schema manager for rustdb

use crate::common::types::Column;
use crate::common::Result;
use crate::parser::ast::Expression;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct CheckConstraint {
    pub name: String,
    pub expr: Expression,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniqueConstraintDef {
    pub name: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyConstraintDef {
    pub name: String,
    pub columns: Vec<String>,
    pub referenced_table: String,
    /// When empty, resolved at runtime to the referenced table's primary key columns.
    pub referenced_columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub table_name: String,
    pub columns: Vec<Column>,
    /// `(constraint_name, column list)` — at most one primary key.
    pub primary_key: Option<(String, Vec<String>)>,
    pub unique_constraints: Vec<UniqueConstraintDef>,
    pub foreign_keys: Vec<ForeignKeyConstraintDef>,
    pub check_constraints: Vec<CheckConstraint>,
}

/// Registered table names and simple ordinal ids (for tests and tooling).
#[derive(Debug, Clone, Default)]
pub struct SchemaManager {
    table_ids: HashMap<String, u32>,
    next_id: u32,
    schemas: HashMap<String, TableSchema>,
}

impl SchemaManager {
    pub fn new() -> Result<Self> {
        Ok(Self {
            table_ids: HashMap::new(),
            next_id: 1,
            schemas: HashMap::new(),
        })
    }

    pub fn register_table(&mut self, name: &str) -> u32 {
        if let Some(&id) = self.table_ids.get(name) {
            return id;
        }
        let id = self.next_id;
        self.next_id += 1;
        self.table_ids.insert(name.to_string(), id);
        id
    }

    pub fn table_id(&self, name: &str) -> Option<u32> {
        self.table_ids.get(name).copied()
    }

    pub fn register_schema(&mut self, schema: TableSchema) -> u32 {
        let id = self.register_table(&schema.table_name);
        self.schemas.insert(schema.table_name.clone(), schema);
        id
    }

    pub fn schema(&self, table: &str) -> Option<&TableSchema> {
        self.schemas.get(table)
    }

    pub fn schema_mut(&mut self, table: &str) -> Option<&mut TableSchema> {
        self.schemas.get_mut(table)
    }

    /// Sorted list of registered table names (for dependency ordering, tests, etc.).
    pub fn table_names(&self) -> Vec<String> {
        let mut v: Vec<String> = self.schemas.keys().cloned().collect();
        v.sort();
        v
    }

    /// Tables that declare a foreign key referencing `parent_table`.
    pub fn tables_with_fk_to(&self, parent_table: &str) -> Vec<String> {
        let mut out = Vec::new();
        for (tname, sch) in &self.schemas {
            if sch
                .foreign_keys
                .iter()
                .any(|fk| fk.referenced_table == parent_table)
            {
                out.push(tname.clone());
            }
        }
        out.sort();
        out
    }

    pub fn drop_table(&mut self, table: &str) {
        self.table_ids.remove(table);
        self.schemas.remove(table);
    }
}
