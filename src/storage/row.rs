//! Row and table structures for rustdb

use crate::common::{
    types::{ColumnValue, DataType, PageId},
    Error, Result,
};
use crate::storage::tuple::{Schema, Tuple};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Table row with versioning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    /// Row ID
    pub id: u64,
    /// Current row version
    pub current_tuple: Tuple,
    /// All row versions
    pub versions: HashMap<u64, Tuple>,
    /// Pointer to the next row in the table
    pub next_row: Option<u64>,
    /// Pointer to the previous row in the table
    pub prev_row: Option<u64>,
    /// Row statistics
    pub stats: RowStats,
}

impl Row {
    /// Creates a new row
    pub fn new(id: u64, tuple: Tuple) -> Self {
        let mut versions = HashMap::new();
        versions.insert(tuple.version, tuple.clone());

        Self {
            id,
            current_tuple: tuple,
            versions,
            next_row: None,
            prev_row: None,
            stats: RowStats::new(),
        }
    }

    /// Updates the row
    pub fn update(&mut self, new_values: HashMap<String, ColumnValue>) -> Result<()> {
        // Create a new version
        let mut new_tuple = self.current_tuple.create_new_version();

        // Update values
        for (column, value) in new_values {
            new_tuple.set_value(&column, value);
        }

        // Add version to history
        self.versions.insert(new_tuple.version, new_tuple.clone());

        // Update current version
        self.current_tuple = new_tuple;

        // Update statistics
        self.stats.update_count += 1;
        self.stats.last_updated = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Ok(())
    }

    /// Deletes the row
    pub fn delete(&mut self) -> Result<()> {
        self.current_tuple.mark_deleted();
        self.stats.delete_count += 1;
        self.stats.last_updated = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        Ok(())
    }

    /// Restores a deleted row
    pub fn restore(&mut self) -> Result<()> {
        if self.current_tuple.is_deleted() {
            self.current_tuple.is_deleted = false;
            self.stats.restore_count += 1;
            self.stats.last_updated = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
        }
        Ok(())
    }

    /// Gets a column value
    pub fn get_value(&self, column: &str) -> Option<&ColumnValue> {
        self.current_tuple.get_value(column)
    }

    /// Sets a column value
    pub fn set_value(&mut self, column: &str, value: ColumnValue) -> Result<()> {
        let mut new_values = HashMap::new();
        new_values.insert(column.to_string(), value);
        self.update(new_values)
    }

    /// Checks if the row is deleted
    pub fn is_deleted(&self) -> bool {
        self.current_tuple.is_deleted()
    }

    /// Returns a row version
    pub fn get_version(&self, version: u64) -> Option<&Tuple> {
        self.versions.get(&version)
    }

    /// Returns all row versions
    pub fn get_all_versions(&self) -> &HashMap<u64, Tuple> {
        &self.versions
    }

    /// Returns the number of versions
    pub fn version_count(&self) -> usize {
        self.versions.len()
    }

    /// Sets link to the next row
    pub fn set_next_row(&mut self, next_id: u64) {
        self.next_row = Some(next_id);
    }

    /// Sets link to the previous row
    pub fn set_prev_row(&mut self, prev_id: u64) {
        self.prev_row = Some(prev_id);
    }

    /// Serializes the row to bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(Error::BincodeSerialization)
    }

    /// Creates a row from bytes (deserialization)
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).map_err(Error::BincodeSerialization)
    }

    /// Returns the row size in bytes
    pub fn size(&self) -> usize {
        self.to_bytes().unwrap_or_default().len()
    }
}

/// Row statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowStats {
    /// Number of updates
    pub update_count: u64,
    /// Number of deletions
    pub delete_count: u64,
    /// Number of restorations
    pub restore_count: u64,
    /// Last update time
    pub last_updated: u64,
    /// Creation time
    pub created_at: u64,
}

impl Default for RowStats {
    fn default() -> Self {
        Self::new()
    }
}

impl RowStats {
    /// Creates new statistics
    pub fn new() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            update_count: 0,
            delete_count: 0,
            restore_count: 0,
            last_updated: now,
            created_at: now,
        }
    }
}

/// Table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    /// Table name
    pub name: String,
    /// Table schema
    pub schema: Schema,
    /// Number of rows
    pub row_count: u64,
    /// Table size in bytes
    pub size_bytes: u64,
    /// Creation time
    pub created_at: u64,
    /// Last modification time
    pub last_modified: u64,
    /// Table statistics
    pub stats: TableStats,
    /// Table options
    pub options: TableOptions,
}

impl TableMetadata {
    /// Creates new table metadata
    pub fn new(name: String, schema: Schema) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            name,
            schema,
            row_count: 0,
            size_bytes: 0,
            created_at: now,
            last_modified: now,
            stats: TableStats::new(),
            options: TableOptions::default(),
        }
    }

    /// Updates the row count
    pub fn update_row_count(&mut self, count: u64) {
        self.row_count = count;
        self.last_modified = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Updates the table size
    pub fn update_size(&mut self, size: u64) {
        self.size_bytes = size;
        self.last_modified = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
}

/// Table statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStats {
    /// Number of INSERT operations
    pub insert_count: u64,
    /// Number of UPDATE operations
    pub update_count: u64,
    /// Number of DELETE operations
    pub delete_count: u64,
    /// Number of SELECT operations
    pub select_count: u64,
    /// Last statistics reset time
    pub last_reset: u64,
}

impl Default for TableStats {
    fn default() -> Self {
        Self::new()
    }
}

impl TableStats {
    /// Creates new table statistics
    pub fn new() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            insert_count: 0,
            update_count: 0,
            delete_count: 0,
            select_count: 0,
            last_reset: now,
        }
    }

    /// Resets statistics
    pub fn reset(&mut self) {
        self.insert_count = 0;
        self.update_count = 0;
        self.delete_count = 0;
        self.select_count = 0;
        self.last_reset = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Records an INSERT operation
    pub fn record_insert(&mut self) {
        self.insert_count += 1;
    }

    /// Records an UPDATE operation
    pub fn record_update(&mut self) {
        self.update_count += 1;
    }

    /// Records a DELETE operation
    pub fn record_delete(&mut self) {
        self.delete_count += 1;
    }

    /// Records a SELECT operation
    pub fn record_select(&mut self) {
        self.select_count += 1;
    }
}

/// Table options
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TableOptions {
    /// Maximum number of rows
    pub max_rows: Option<u64>,
    /// Minimum number of rows
    pub min_rows: Option<u64>,
    /// Auto-increment
    pub auto_increment: Option<u64>,
    /// Comment
    pub comment: Option<String>,
    /// Temporary table flag
    pub is_temporary: bool,
    /// System table flag
    pub is_system: bool,
}

/// Table with data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    /// Table metadata
    pub metadata: TableMetadata,
    /// Table rows
    pub rows: HashMap<u64, Row>,
    /// Associated indexes
    pub indexes: HashMap<String, PageId>,
    /// Associated pages
    pub pages: Vec<PageId>,
}

impl Table {
    /// Creates a new table
    pub fn new(name: String, schema: Schema) -> Self {
        Self {
            metadata: TableMetadata::new(name, schema),
            rows: HashMap::new(),
            indexes: HashMap::new(),
            pages: Vec::new(),
        }
    }

    /// Inserts a row into the table
    pub fn insert_row(&mut self, row: Row) -> Result<()> {
        let row_id = row.id;

        // Validate against schema
        self.metadata.schema.validate_tuple(&row.current_tuple)?;

        // Add row
        self.rows.insert(row_id, row);

        // Update metadata
        self.metadata.update_row_count(self.rows.len() as u64);
        self.metadata.stats.record_insert();

        Ok(())
    }

    /// Updates a row in the table
    pub fn update_row(
        &mut self,
        row_id: u64,
        new_values: HashMap<String, ColumnValue>,
    ) -> Result<()> {
        if let Some(row) = self.rows.get_mut(&row_id) {
            row.update(new_values)?;
            self.metadata.stats.record_update();
            Ok(())
        } else {
            Err(Error::validation("Row not found"))
        }
    }

    /// Deletes a row from the table
    pub fn delete_row(&mut self, row_id: u64) -> Result<()> {
        if let Some(row) = self.rows.get_mut(&row_id) {
            row.delete()?;
            self.metadata.stats.record_delete();
            Ok(())
        } else {
            Err(Error::validation("Row not found"))
        }
    }

    /// Gets a row by ID
    pub fn get_row(&self, row_id: u64) -> Option<&Row> {
        self.rows.get(&row_id)
    }

    /// Gets a mutable reference to a row
    pub fn get_row_mut(&mut self, row_id: u64) -> Option<&mut Row> {
        self.rows.get_mut(&row_id)
    }

    /// Checks if the table contains a row
    pub fn contains_row(&self, row_id: u64) -> bool {
        self.rows.contains_key(&row_id)
    }

    /// Returns the number of rows
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Adds an index
    pub fn add_index(&mut self, name: String, page_id: PageId) {
        self.indexes.insert(name, page_id);
    }

    /// Removes an index
    pub fn remove_index(&mut self, name: &str) -> Option<PageId> {
        self.indexes.remove(name)
    }

    /// Adds a page
    pub fn add_page(&mut self, page_id: PageId) {
        if !self.pages.contains(&page_id) {
            self.pages.push(page_id);
        }
    }

    /// Removes a page
    pub fn remove_page(&mut self, page_id: PageId) -> bool {
        if let Some(pos) = self.pages.iter().position(|&id| id == page_id) {
            self.pages.remove(pos);
            true
        } else {
            false
        }
    }

    /// Clears the table
    pub fn clear(&mut self) {
        self.rows.clear();
        self.metadata.update_row_count(0);
        self.metadata.stats.reset();
    }

    /// Serializes the table to bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(Error::BincodeSerialization)
    }

    /// Creates a table from bytes (deserialization)
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).map_err(Error::BincodeSerialization)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::{ColumnValue, DataType};

    #[test]
    fn test_row_creation() {
        let tuple = Tuple::new(1);
        let row = Row::new(1, tuple);

        assert_eq!(row.id, 1);
        assert_eq!(row.version_count(), 1);
        assert!(!row.is_deleted());
    }

    #[test]
    fn test_row_update() {
        let tuple = Tuple::new(1);
        let mut row = Row::new(1, tuple);

        let mut new_values = HashMap::new();
        new_values.insert("age".to_string(), ColumnValue::new(DataType::Integer(25)));

        row.update(new_values).unwrap();
        assert_eq!(row.version_count(), 2);
        assert_eq!(
            row.get_value("age").unwrap().data_type,
            DataType::Integer(25)
        );
    }

    #[test]
    fn test_row_delete() {
        let tuple = Tuple::new(1);
        let mut row = Row::new(1, tuple);

        row.delete().unwrap();
        assert!(row.is_deleted());
    }

    #[test]
    fn test_table_creation() {
        let schema = Schema::new("users".to_string());
        let table = Table::new("users".to_string(), schema);

        assert_eq!(table.metadata.name, "users");
        assert_eq!(table.row_count(), 0);
    }

    #[test]
    fn test_table_insert() {
        let schema = Schema::new("users".to_string());
        let mut table = Table::new("users".to_string(), schema);

        let tuple = Tuple::new(1);
        let row = Row::new(1, tuple);

        table.insert_row(row).unwrap();
        assert_eq!(table.row_count(), 1);
        assert!(table.contains_row(1));
    }

    #[test]
    fn test_table_update() {
        let schema = Schema::new("users".to_string());
        let mut table = Table::new("users".to_string(), schema);

        let tuple = Tuple::new(1);
        let row = Row::new(1, tuple);
        table.insert_row(row).unwrap();

        let mut new_values = HashMap::new();
        new_values.insert(
            "name".to_string(),
            ColumnValue::new(DataType::Varchar("John".to_string())),
        );

        table.update_row(1, new_values).unwrap();
        let updated_row = table.get_row(1).unwrap();
        assert_eq!(
            updated_row.get_value("name").unwrap().data_type,
            DataType::Varchar("John".to_string())
        );
    }

    #[test]
    fn test_table_delete() {
        let schema = Schema::new("users".to_string());
        let mut table = Table::new("users".to_string(), schema);

        let tuple = Tuple::new(1);
        let row = Row::new(1, tuple);
        table.insert_row(row).unwrap();

        table.delete_row(1).unwrap();
        let deleted_row = table.get_row(1).unwrap();
        assert!(deleted_row.is_deleted());
    }
}
