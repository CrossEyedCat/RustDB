//! Schema manager for rustdb

use crate::common::Result;
use std::collections::HashMap;

/// Registered table names and simple ordinal ids (for tests and tooling).
#[derive(Debug, Clone, Default)]
pub struct SchemaManager {
    table_ids: HashMap<String, u32>,
    next_id: u32,
}

impl SchemaManager {
    pub fn new() -> Result<Self> {
        Ok(Self {
            table_ids: HashMap::new(),
            next_id: 1,
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
}
