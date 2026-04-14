//! Schema manager for rustdb

use crate::common::Result;
use crate::common::types::Column;
use crate::parser::ast::Expression;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct CheckConstraint {
    pub name: String,
    pub expr: Expression,
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub table_name: String,
    pub columns: Vec<Column>,
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

    pub fn drop_table(&mut self, table: &str) {
        self.table_ids.remove(table);
        self.schemas.remove(table);
    }
}
