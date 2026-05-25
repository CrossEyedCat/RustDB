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
#[derive(Debug, Clone)]
pub struct IndexEntry {
    /// Indexed column names
    pub columns: Vec<String>,
    /// B+ tree: key = serialized column value, value = list of record IDs
    pub index: Arc<Mutex<BPlusTree<String, Vec<RecordId>>>>,
}

/// Registry of indexes per table
#[derive(Debug, Default, Clone)]
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

    /// True if no indexes are registered.
    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }

    /// Removes every index defined on `table_name` (e.g. after `DROP TABLE`).
    pub fn remove_all_indexes_for_table(&mut self, table_name: &str) {
        self.indexes.retain(|(t, _), _| t.as_str() != table_name);
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

    /// Inserts a record into a single named index.
    pub fn insert_into_named_index(
        &self,
        table_name: &str,
        index_name: &str,
        record_id: RecordId,
        column_values: &HashMap<String, String>,
    ) -> Result<()> {
        let key = (table_name.to_string(), index_name.to_string());
        let entry = self.indexes.get(&key).ok_or_else(|| {
            Error::validation(format!(
                "Index {} not found on table {}",
                index_name, table_name
            ))
        })?;
        let index_key = Self::build_index_key(&entry.columns, column_values)?;
        let mut index = entry
            .index
            .lock()
            .map_err(|_| Error::internal("Lock poisoned"))?;
        if let Some(mut ids) = index.search(&index_key)? {
            ids.push(record_id);
            index.insert(index_key, ids)?;
        } else {
            index.insert(index_key, vec![record_id])?;
        }
        Ok(())
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

    /// True when every index key on `table_name` is identical in `old_values` and `new_values`.
    pub fn index_keys_unchanged(
        &self,
        table_name: &str,
        old_values: &HashMap<String, String>,
        new_values: &HashMap<String, String>,
    ) -> bool {
        for ((t, _), entry) in self.indexes.iter().filter(|((t, _), _)| t == table_name) {
            let old_key = match Self::build_index_key(&entry.columns, old_values) {
                Ok(k) => k,
                Err(_) => return false,
            };
            let new_key = match Self::build_index_key(&entry.columns, new_values) {
                Ok(k) => k,
                Err(_) => return false,
            };
            if old_key != new_key {
                return false;
            }
        }
        true
    }

    /// Updates indexes when a record changes (remove old, add new)
    pub fn update_indexes(
        &self,
        table_name: &str,
        record_id: RecordId,
        old_values: &HashMap<String, String>,
        new_values: &HashMap<String, String>,
    ) -> Result<()> {
        if self.index_keys_unchanged(table_name, old_values, new_values) {
            return Ok(());
        }
        self.delete_from_indexes(table_name, record_id, old_values)?;
        self.insert_into_indexes(table_name, record_id, new_values)?;
        Ok(())
    }

    /// Builds a composite index key from column values (same encoding as `build_index_key`).
    pub fn build_index_key_from_map(
        columns: &[String],
        values: &HashMap<String, String>,
    ) -> Result<String> {
        Self::build_index_key(columns, values)
    }

    /// Resolves `column = literal` predicates via the best matching index on `table_name`.
    ///
    /// `equalities` maps column names to index-key strings (same encoding as
    /// [`crate::network::sql_engine`] index maintenance). Returns `None` when no registered
    /// index has a leading prefix covered by `equalities`. An empty vector means the index
    /// matched but no rows were found.
    ///
    /// **Limits:** only equality on a leading prefix of the index column list is supported.
    /// Partial-prefix lookups use B+tree range search and may include false positives when
    /// index-key strings share prefixes (e.g. integer `1` vs `10`); callers must re-check the
    /// full `WHERE` clause on heap tuples.
    pub fn lookup_record_ids_by_equalities(
        &self,
        table_name: &str,
        equalities: &HashMap<String, String>,
    ) -> Result<Option<(Vec<RecordId>, bool)>> {
        if equalities.is_empty() {
            return Ok(None);
        }
        let Some((entry, prefix_len)) = self.best_index_for_equalities(table_name, equalities)
        else {
            return Ok(None);
        };
        let index = entry
            .index
            .lock()
            .map_err(|_| Error::internal("Lock poisoned"))?;
        let prefix_cols = &entry.columns[..prefix_len];
        let key = Self::build_index_key(prefix_cols, equalities)?;
        let exact_key = prefix_len == entry.columns.len();
        let rids = if exact_key {
            index.search(&key)?.unwrap_or_default()
        } else {
            let end = format!("{key}\0\u{10FFFF}");
            index
                .range_search(&key, &end)?
                .into_iter()
                .flat_map(|(_, ids)| ids)
                .collect()
        };
        Ok(Some((rids, exact_key)))
    }

    /// Picks the index with the longest leading column prefix present in `equalities`.
    fn best_index_for_equalities(
        &self,
        table_name: &str,
        equalities: &HashMap<String, String>,
    ) -> Option<(&IndexEntry, usize)> {
        let mut best: Option<(&IndexEntry, usize)> = None;
        for ((t, _), entry) in self.indexes.iter() {
            if t != table_name {
                continue;
            }
            let mut prefix = 0usize;
            for col in &entry.columns {
                if equalities.contains_key(col) {
                    prefix += 1;
                } else {
                    break;
                }
            }
            if prefix == 0 {
                continue;
            }
            if best.map(|(_, p)| prefix > p).unwrap_or(true) {
                best = Some((entry, prefix));
            }
        }
        best
    }

    fn build_index_key(columns: &[String], values: &HashMap<String, String>) -> Result<String> {
        let parts: Vec<String> = columns
            .iter()
            .map(|col| values.get(col).cloned().unwrap_or_else(|| "".to_string()))
            .collect();
        Ok(parts.join("\0"))
    }
}
