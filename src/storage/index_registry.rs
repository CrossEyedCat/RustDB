//! Index registry for rustdb
//!
//! Manages B+ tree indexes for tables. Indexes map column values to RecordIds
//! for efficient lookups during SELECT.

use crate::common::types::RecordId;
use crate::common::{Error, Result};
use crate::storage::index::{BPlusTree, Index};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Entry for a single index
#[derive(Debug)]
pub struct IndexEntry {
    /// Indexed column names
    pub columns: Vec<String>,
    /// B+ tree: key = serialized column value, value = list of record IDs
    pub index: Arc<Mutex<BPlusTree<String, Vec<RecordId>>>>,
}

/// Registry of indexes per table
#[derive(Debug, Default)]
pub struct IndexRegistry {
    /// (table_name, index_name) -> IndexEntry
    indexes: HashMap<(String, String), IndexEntry>,
}

impl IndexRegistry {
    /// Creates a new index registry
    pub fn new() -> Self {
        Self {
            indexes: HashMap::new(),
        }
    }

    /// Creates and registers a new index
    pub fn create_index(
        &mut self,
        table_name: &str,
        index_name: &str,
        columns: Vec<String>,
    ) -> Result<()> {
        let key = (table_name.to_string(), index_name.to_string());
        if self.indexes.contains_key(&key) {
            return Err(Error::validation(format!(
                "Index {} already exists on table {}",
                index_name, table_name
            )));
        }

        let index = BPlusTree::new_default();
        self.indexes.insert(
            key,
            IndexEntry {
                columns: columns.clone(),
                index: Arc::new(Mutex::new(index)),
            },
        );
        Ok(())
    }

    /// Drops an index
    pub fn drop_index(&mut self, table_name: &str, index_name: &str) -> Result<()> {
        let key = (table_name.to_string(), index_name.to_string());
        if self.indexes.remove(&key).is_none() {
            return Err(Error::validation(format!(
                "Index {} not found on table {}",
                index_name, table_name
            )));
        }
        Ok(())
    }

    /// Gets an index by table and index name
    pub fn get_index(
        &self,
        table_name: &str,
        index_name: &str,
    ) -> Option<Arc<Mutex<BPlusTree<String, Vec<RecordId>>>>> {
        let key = (table_name.to_string(), index_name.to_string());
        self.indexes.get(&key).map(|e| e.index.clone())
    }

    /// Gets index entry (columns + index)
    pub fn get_index_entry(&self, table_name: &str, index_name: &str) -> Option<&IndexEntry> {
        let key = (table_name.to_string(), index_name.to_string());
        self.indexes.get(&key)
    }

    /// Lists indexes for a table
    pub fn list_indexes_for_table(&self, table_name: &str) -> Vec<(String, Vec<String>)> {
        self.indexes
            .iter()
            .filter(|((t, _), _)| t == table_name)
            .map(|((_, idx_name), entry)| (idx_name.clone(), entry.columns.clone()))
            .collect()
    }

    /// Inserts a record into all indexes of a table
    /// column_values: map from column name to serialized value
    pub fn insert_into_indexes(
        &self,
        table_name: &str,
        record_id: RecordId,
        column_values: &HashMap<String, String>,
    ) -> Result<()> {
        for ((t, _), entry) in self.indexes.iter().filter(|((t, _), _)| t == table_name) {
            let key = Self::build_index_key(&entry.columns, column_values)?;
            let mut index = entry
                .index
                .lock()
                .map_err(|_| Error::internal("Lock poisoned"))?;
            if let Some(mut ids) = index.search(&key)? {
                ids.push(record_id);
                index.insert(key, ids)?;
            } else {
                index.insert(key, vec![record_id])?;
            }
        }
        Ok(())
    }

    /// Removes a record from all indexes of a table
    pub fn delete_from_indexes(
        &self,
        table_name: &str,
        record_id: RecordId,
        column_values: &HashMap<String, String>,
    ) -> Result<()> {
        for ((t, _), entry) in self.indexes.iter().filter(|((t, _), _)| t == table_name) {
            let key = Self::build_index_key(&entry.columns, column_values)?;
            let mut index = entry
                .index
                .lock()
                .map_err(|_| Error::internal("Lock poisoned"))?;
            if let Some(mut ids) = index.search(&key)? {
                ids.retain(|&id| id != record_id);
                if ids.is_empty() {
                    index.delete(&key)?;
                } else {
                    index.insert(key, ids)?;
                }
            }
        }
        Ok(())
    }

    /// Updates indexes when a record changes (remove old, add new)
    pub fn update_indexes(
        &self,
        table_name: &str,
        record_id: RecordId,
        old_values: &HashMap<String, String>,
        new_values: &HashMap<String, String>,
    ) -> Result<()> {
        self.delete_from_indexes(table_name, record_id, old_values)?;
        self.insert_into_indexes(table_name, record_id, new_values)?;
        Ok(())
    }

    fn build_index_key(columns: &[String], values: &HashMap<String, String>) -> Result<String> {
        let parts: Vec<String> = columns
            .iter()
            .map(|col| values.get(col).cloned().unwrap_or_else(|| "".to_string()))
            .collect();
        Ok(parts.join("\0"))
    }
}
