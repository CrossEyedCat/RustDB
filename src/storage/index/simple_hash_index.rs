//! Simple hash index for rustdb
//!
//! Simplified hash index implementation without serialization
//! for fast key-based search.

use crate::common::{Error, Result};
use crate::storage::index::{Index, IndexStatistics};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

/// Simple hash index based on HashMap
#[derive(Debug, Clone)]
pub struct SimpleHashIndex<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    /// Internal hash table
    data: HashMap<K, V>,
    /// Operation statistics
    statistics: RefCell<IndexStatistics>,
}

impl<K, V> SimpleHashIndex<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    /// Creates a new simple hash index
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            statistics: RefCell::new(IndexStatistics::default()),
        }
    }

    /// Creates a new hash index with given initial capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: HashMap::with_capacity(capacity),
            statistics: RefCell::new(IndexStatistics::default()),
        }
    }

    /// Returns a snapshot of index statistics
    pub fn get_statistics(&self) -> IndexStatistics {
        self.statistics.borrow().clone()
    }

    /// Updates index statistics
    fn update_statistics(&self) {
        let mut s = self.statistics.borrow_mut();
        s.total_elements = self.data.len() as u64;
        s.fill_factor = if self.data.capacity() == 0 {
            0.0
        } else {
            self.data.len() as f64 / self.data.capacity() as f64
        };
        s.depth = 1; // Hash table has depth 1
    }
}

impl<K, V> Index for SimpleHashIndex<K, V>
where
    K: Hash + Eq + Clone + Ord + Debug,
    V: Clone + Debug,
{
    type Key = K;
    type Value = V;

    fn insert(&mut self, key: Self::Key, value: Self::Value) -> Result<()> {
        self.statistics.borrow_mut().insert_operations += 1;
        self.data.insert(key, value);
        self.update_statistics();
        Ok(())
    }

    fn search(&self, key: &Self::Key) -> Result<Option<Self::Value>> {
        self.statistics.borrow_mut().search_operations += 1;
        Ok(self.data.get(key).cloned())
    }

    fn delete(&mut self, key: &Self::Key) -> Result<bool> {
        self.statistics.borrow_mut().delete_operations += 1;
        let result = self.data.remove(key).is_some();
        self.update_statistics();
        Ok(result)
    }

    fn range_search(
        &self,
        _start: &Self::Key,
        _end: &Self::Key,
    ) -> Result<Vec<(Self::Key, Self::Value)>> {
        self.statistics.borrow_mut().range_search_operations += 1;

        // Hash indexes don't support efficient range queries
        // Return empty result
        Ok(Vec::new())
    }

    fn size(&self) -> usize {
        self.data.len()
    }
}

impl<K, V> Default for SimpleHashIndex<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_hash_index_creation() {
        let index: SimpleHashIndex<i32, String> = SimpleHashIndex::new();
        assert!(index.is_empty());
        assert_eq!(index.size(), 0);
    }

    #[test]
    fn test_simple_hash_index_insert_and_search() {
        let mut index = SimpleHashIndex::new();

        // Insert entries
        index
            .insert("key1".to_string(), "value1".to_string())
            .unwrap();
        index
            .insert("key2".to_string(), "value2".to_string())
            .unwrap();
        index
            .insert("key3".to_string(), "value3".to_string())
            .unwrap();

        assert_eq!(index.size(), 3);

        // Verify lookup
        assert_eq!(
            index.search(&"key1".to_string()).unwrap(),
            Some("value1".to_string())
        );
        assert_eq!(
            index.search(&"key2".to_string()).unwrap(),
            Some("value2".to_string())
        );
        assert_eq!(
            index.search(&"key3".to_string()).unwrap(),
            Some("value3".to_string())
        );
        assert_eq!(index.search(&"key4".to_string()).unwrap(), None);
    }

    #[test]
    fn test_simple_hash_index_deletion() {
        let mut index = SimpleHashIndex::new();

        // Insert entries
        for i in 1..=10 {
            index.insert(i, format!("value_{}", i)).unwrap();
        }

        assert_eq!(index.size(), 10);

        // Remove some entries
        assert!(index.delete(&5).unwrap());
        assert!(index.delete(&7).unwrap());
        assert!(!index.delete(&15).unwrap()); // Does not exist

        assert_eq!(index.size(), 8);

        // Ensure deleted entries are gone
        assert_eq!(index.search(&5).unwrap(), None);
        assert_eq!(index.search(&7).unwrap(), None);

        // Ensure remaining entries are intact
        assert_eq!(index.search(&1).unwrap(), Some("value_1".to_string()));
        assert_eq!(index.search(&10).unwrap(), Some("value_10".to_string()));
    }

    #[test]
    fn test_simple_hash_index_update() {
        let mut index = SimpleHashIndex::new();

        // Insert entry
        index
            .insert("key".to_string(), "original_value".to_string())
            .unwrap();
        assert_eq!(index.size(), 1);

        // Update same key
        index
            .insert("key".to_string(), "updated_value".to_string())
            .unwrap();
        assert_eq!(index.size(), 1); // Size should stay the same

        // Ensure value was updated
        assert_eq!(
            index.search(&"key".to_string()).unwrap(),
            Some("updated_value".to_string())
        );
    }

    #[test]
    fn test_simple_hash_index_range_search() {
        let mut index = SimpleHashIndex::new();

        // Insert entries
        for i in 1..=10 {
            index.insert(i, format!("value_{}", i)).unwrap();
        }

        // Range search should return an empty result
        let results = index.range_search(&3, &7).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_simple_hash_index_statistics() {
        let mut index = SimpleHashIndex::new();

        // Insert entries
        for i in 1..=5 {
            index.insert(i, format!("value_{}", i)).unwrap();
        }

        let stats = index.get_statistics();
        assert_eq!(stats.total_elements, 5);
        assert_eq!(stats.insert_operations, 5);
        assert!(stats.fill_factor > 0.0);
        assert_eq!(stats.depth, 1);

        // Remove entry
        index.delete(&3).unwrap();

        let stats = index.get_statistics();
        assert_eq!(stats.total_elements, 4);
        assert_eq!(stats.delete_operations, 1);
    }
}
