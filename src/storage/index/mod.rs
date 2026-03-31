//! Index module for rustdb
//!
//! This module provides implementations of various index types,
//! including B+ trees and hash indexes.

pub mod btree;
pub mod hash_index;
pub mod simple_hash_index;

pub use btree::BPlusTree;
pub use hash_index::{CollisionResolution, HashIndex};
pub use simple_hash_index::SimpleHashIndex;

use crate::common::{
    types::{PageId, RecordId},
    Result,
};
use serde::{Deserialize, Serialize};

/// Trait for all index types
pub trait Index {
    type Key: Ord + Clone;
    type Value: Clone;

    /// Inserts key-value pair into index
    fn insert(&mut self, key: Self::Key, value: Self::Value) -> Result<()>;

    /// Searches for value by key
    fn search(&self, key: &Self::Key) -> Result<Option<Self::Value>>;

    /// Deletes key from index
    fn delete(&mut self, key: &Self::Key) -> Result<bool>;

    /// Returns all keys in range [start, end]
    fn range_search(
        &self,
        start: &Self::Key,
        end: &Self::Key,
    ) -> Result<Vec<(Self::Key, Self::Value)>>;

    /// Returns number of elements in index
    fn size(&self) -> usize;

    /// Checks if index is empty
    fn is_empty(&self) -> bool {
        self.size() == 0
    }
}

/// Index statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStatistics {
    /// Total number of elements
    pub total_elements: u64,
    /// Number of insert operations
    pub insert_operations: u64,
    /// Number of search operations
    pub search_operations: u64,
    /// Number of delete operations
    pub delete_operations: u64,
    /// Number of range search operations
    pub range_search_operations: u64,
    /// Index depth (for trees)
    pub depth: u32,
    /// Fill factor
    pub fill_factor: f64,
}

impl Default for IndexStatistics {
    fn default() -> Self {
        Self {
            total_elements: 0,
            insert_operations: 0,
            search_operations: 0,
            delete_operations: 0,
            range_search_operations: 0,
            depth: 0,
            fill_factor: 0.0,
        }
    }
}

/// Index type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexType {
    /// B+ tree
    BPlusTree,
    /// Hash index
    Hash,
}

/// Index configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    /// Index type
    pub index_type: IndexType,
    /// Maximum number of keys per node (for B+ tree)
    pub max_keys_per_node: usize,
    /// Initial hash table size
    pub initial_hash_size: usize,
    /// Load factor threshold for hash table expansion
    pub load_factor_threshold: f64,
    /// Enable caching
    pub enable_caching: bool,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            index_type: IndexType::BPlusTree,
            max_keys_per_node: 255, // Standard size for B+ tree
            initial_hash_size: 1024,
            load_factor_threshold: 0.75,
            enable_caching: true,
        }
    }
}
