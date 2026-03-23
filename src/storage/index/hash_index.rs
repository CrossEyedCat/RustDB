//! Hash index for rustdb
//! 
//! Hash index implementation for fast key lookups.
//! Supports dynamic resizing and multiple collision resolution strategies.

use crate::common::{Result, Error};
use crate::storage::index::{Index, IndexStatistics};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::fmt::Debug;

/// Collision resolution strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CollisionResolution {
    /// Chaining
    Chaining,
    /// Open addressing
    OpenAddressing,
}

/// Hash table entry for chaining
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChainEntry<K, V> 
where
    K: Hash + Eq + Clone + Serialize + for<'de> Deserialize<'de>,
    V: Clone + Serialize + for<'de> Deserialize<'de>,
{
    key: K,
    value: V,
    next: Option<Box<ChainEntry<K, V>>>,
}

/// Hash table entry for open addressing
#[derive(Debug, Clone, Serialize, Deserialize)]
enum HashEntry<K, V>
where
    K: Hash + Eq + Clone + Serialize + for<'de> Deserialize<'de>,
    V: Clone + Serialize + for<'de> Deserialize<'de>,
{
    /// Empty slot
    Empty,
    /// Deleted slot (tombstone)
    Deleted,
    /// Occupied slot
    Occupied { key: K, value: V },
}

/// Hash index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashIndex<K, V>
where
    K: Hash + Eq + Clone + Serialize + for<'de> Deserialize<'de>,
    V: Clone + Serialize + for<'de> Deserialize<'de>,
{
    /// Chaining table
    chains: Option<Vec<Option<ChainEntry<K, V>>>>,
    /// Open addressing table
    open_table: Option<Vec<HashEntry<K, V>>>,
    /// Collision resolution strategy
    collision_resolution: CollisionResolution,
    /// Table size
    capacity: usize,
    /// Element count
    size: usize,
    /// Deleted entry count (open addressing)
    deleted_count: usize,
    /// Load factor threshold
    load_factor_threshold: f64,
    /// Operation statistics
    statistics: IndexStatistics,
}

impl<K, V> HashIndex<K, V>
where
    K: Hash + Eq + Clone + Serialize + for<'de> Deserialize<'de>,
    V: Clone + Serialize + for<'de> Deserialize<'de>,
{
    /// Creates a new hash index with the given parameters
    pub fn new(
        initial_capacity: usize,
        collision_resolution: CollisionResolution,
        load_factor_threshold: f64,
    ) -> Self {
        let capacity = initial_capacity.max(16); // Minimum size
        
        let (chains, open_table) = match collision_resolution {
            CollisionResolution::Chaining => {
                (Some(vec![None; capacity]), None)
            },
            CollisionResolution::OpenAddressing => {
                (None, Some(vec![HashEntry::Empty; capacity]))
            },
        };
        
        Self {
            chains,
            open_table,
            collision_resolution,
            capacity,
            size: 0,
            deleted_count: 0,
            load_factor_threshold,
            statistics: IndexStatistics::default(),
        }
    }
    
    /// Creates a new hash index with default parameters
    pub fn new_default() -> Self {
        Self::new(1024, CollisionResolution::Chaining, 0.75)
    }
    
    /// Returns index statistics
    pub fn get_statistics(&self) -> &IndexStatistics {
        &self.statistics
    }
    
    /// Computes key hash
    fn hash_key(&self, key: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.capacity
    }
    
    /// Computes hash for open addressing (double hashing)
    fn hash_key_double(&self, key: &K, attempt: usize) -> usize {
        let mut hasher1 = DefaultHasher::new();
        key.hash(&mut hasher1);
        let hash1 = hasher1.finish() as usize;
        
        let mut hasher2 = DefaultHasher::new();
        key.hash(&mut hasher2);
        let hash2 = (hasher2.finish() as usize) | 1; // Force odd secondary hash
        
        (hash1 + attempt * hash2) % self.capacity
    }
    
    /// Returns whether the table should grow
    fn should_resize(&self) -> bool {
        let load_factor = match self.collision_resolution {
            CollisionResolution::Chaining => {
                self.size as f64 / self.capacity as f64
            },
            CollisionResolution::OpenAddressing => {
                (self.size + self.deleted_count) as f64 / self.capacity as f64
            },
        };
        
        load_factor > self.load_factor_threshold
    }
    
    /// Grows the hash table
    fn resize(&mut self) -> Result<()> {
        let old_capacity = self.capacity;
        let new_capacity = old_capacity * 2;
        
        // Save old buckets
        let old_chains = self.chains.take();
        let old_open_table = self.open_table.take();
        
        // Allocate new buckets
        self.capacity = new_capacity;
        self.size = 0;
        self.deleted_count = 0;
        
        match self.collision_resolution {
            CollisionResolution::Chaining => {
                self.chains = Some(vec![None; new_capacity]);
            },
            CollisionResolution::OpenAddressing => {
                self.open_table = Some(vec![HashEntry::Empty; new_capacity]);
            },
        }
        
        // Rehash all entries
        match (old_chains, old_open_table) {
            (Some(chains), None) => {
                for chain_head in chains {
                    let mut current = chain_head;
                    while let Some(entry) = current {
                        self.insert_without_resize(entry.key, entry.value)?;
                        current = entry.next.map(|boxed| *boxed);
                    }
                }
            },
            (None, Some(table)) => {
                for entry in table {
                    if let HashEntry::Occupied { key, value } = entry {
                        self.insert_without_resize(key, value)?;
                    }
                }
            },
            _ => unreachable!(),
        }
        
        Ok(())
    }
    
    /// Insert without resize check
    fn insert_without_resize(&mut self, key: K, value: V) -> Result<()> {
        match self.collision_resolution {
            CollisionResolution::Chaining => {
                self.insert_chaining(key, value)
            },
            CollisionResolution::OpenAddressing => {
                self.insert_open_addressing(key, value)
            },
        }
    }
    
    /// Insert using chaining
    fn insert_chaining(&mut self, key: K, value: V) -> Result<()> {
        let chains = self.chains.as_mut().unwrap();
        let index = self.hash_key(&key);
        
        // Check if key already exists
        let mut current = &mut chains[index];
        loop {
            match current {
                None => {
                    // Insert new entry
                    *current = Some(ChainEntry {
                        key,
                        value,
                        next: None,
                    });
                    self.size += 1;
                    self.statistics.total_elements += 1;
                    break;
                },
                Some(ref mut entry) => {
                    if entry.key == key {
                        // Update existing entry
                        entry.value = value;
                        break;
                    }
                    
                    if entry.next.is_none() {
                        // Append to chain
                        entry.next = Some(Box::new(ChainEntry {
                            key,
                            value,
                            next: None,
                        }));
                        self.size += 1;
                        self.statistics.total_elements += 1;
                        break;
                    }
                    
                    // Walk chain
                    current = &mut entry.next.as_mut().unwrap().next;
                }
            }
        }
        
        Ok(())
    }
    
    /// Insert using open addressing
    fn insert_open_addressing(&mut self, key: K, value: V) -> Result<()> {
        let table = self.open_table.as_mut().unwrap();
        
        for attempt in 0..self.capacity {
            let index = self.hash_key_double(&key, attempt);
            
            match &table[index] {
                HashEntry::Empty | HashEntry::Deleted => {
                    if matches!(table[index], HashEntry::Deleted) {
                        self.deleted_count -= 1;
                    }
                    table[index] = HashEntry::Occupied { key, value };
                    self.size += 1;
                    self.statistics.total_elements += 1;
                    return Ok(());
                },
                HashEntry::Occupied { key: existing_key, .. } => {
                    if *existing_key == key {
                        // Update existing entry
                        table[index] = HashEntry::Occupied { key, value };
                        return Ok(());
                    }
                    // Continue probing
                }
            }
        }
        
        Err(Error::database("Hash table is full"))
    }
    
    /// Search using chaining
    fn search_chaining(&self, key: &K) -> Option<V> {
        let chains = self.chains.as_ref().unwrap();
        let index = self.hash_key(key);
        
        let mut current = &chains[index];
        while let Some(ref entry) = current {
            if entry.key == *key {
                return Some(entry.value.clone());
            }
            current = &entry.next.as_ref().map(|boxed| boxed.as_ref());
        }
        
        None
    }
    
    /// Search using open addressing
    fn search_open_addressing(&self, key: &K) -> Option<V> {
        let table = self.open_table.as_ref().unwrap();
        
        for attempt in 0..self.capacity {
            let index = self.hash_key_double(key, attempt);
            
            match &table[index] {
                HashEntry::Empty => return None,
                HashEntry::Deleted => continue,
                HashEntry::Occupied { key: existing_key, value } => {
                    if *existing_key == *key {
                        return Some(value.clone());
                    }
                }
            }
        }
        
        None
    }
    
    /// Delete using chaining
    fn delete_chaining(&mut self, key: &K) -> Result<bool> {
        let chains = self.chains.as_mut().unwrap();
        let index = self.hash_key(key);
        
        let chain_head = &mut chains[index];
        
        // Check head of chain
        if let Some(ref entry) = chain_head {
            if entry.key == *key {
                *chain_head = entry.next.as_ref().map(|boxed| *boxed.clone());
                self.size -= 1;
                self.statistics.total_elements -= 1;
                return Ok(true);
            }
        }
        
        // Search remainder of chain
        let mut current = chain_head;
        while let Some(ref mut entry) = current {
            if let Some(ref mut next_entry) = entry.next {
                if next_entry.key == *key {
                    entry.next = next_entry.next.take();
                    self.size -= 1;
                    self.statistics.total_elements -= 1;
                    return Ok(true);
                }
            }
            current = &mut entry.next.as_mut().map(|boxed| boxed.as_mut());
        }
        
        Ok(false)
    }
    
    /// Delete using open addressing
    fn delete_open_addressing(&mut self, key: &K) -> Result<bool> {
        let table = self.open_table.as_mut().unwrap();
        
        for attempt in 0..self.capacity {
            let index = self.hash_key_double(key, attempt);
            
            match &table[index] {
                HashEntry::Empty => return Ok(false),
                HashEntry::Deleted => continue,
                HashEntry::Occupied { key: existing_key, .. } => {
                    if *existing_key == *key {
                        table[index] = HashEntry::Deleted;
                        self.size -= 1;
                        self.deleted_count += 1;
                        self.statistics.total_elements -= 1;
                        return Ok(true);
                    }
                }
            }
        }
        
        Ok(false)
    }
    
    /// Refresh index statistics
    fn update_statistics(&mut self) {
        self.statistics.fill_factor = self.size as f64 / self.capacity as f64;
        self.statistics.depth = 1; // Hash table depth is 1
    }
}

impl<K, V> Index for HashIndex<K, V>
where
    K: Hash + Eq + Clone + Serialize + for<'de> Deserialize<'de> + Debug,
    V: Clone + Serialize + for<'de> Deserialize<'de> + Debug,
{
    type Key = K;
    type Value = V;
    
    fn insert(&mut self, key: Self::Key, value: Self::Value) -> Result<()> {
        self.statistics.insert_operations += 1;
        
        if self.should_resize() {
            self.resize()?;
        }
        
        self.insert_without_resize(key, value)?;
        self.update_statistics();
        Ok(())
    }
    
    fn search(&self, key: &Self::Key) -> Result<Option<Self::Value>> {
        // self.statistics.search_operations += 1; // TODO: make statistics mutable
        
        let result = match self.collision_resolution {
            CollisionResolution::Chaining => self.search_chaining(key),
            CollisionResolution::OpenAddressing => self.search_open_addressing(key),
        };
        
        Ok(result)
    }
    
    fn delete(&mut self, key: &Self::Key) -> Result<bool> {
        self.statistics.delete_operations += 1;
        
        let result = match self.collision_resolution {
            CollisionResolution::Chaining => self.delete_chaining(key)?,
            CollisionResolution::OpenAddressing => self.delete_open_addressing(key)?,
        };
        
        self.update_statistics();
        Ok(result)
    }
    
    fn range_search(&self, _start: &Self::Key, _end: &Self::Key) -> Result<Vec<(Self::Key, Self::Value)>> {
        // self.statistics.range_search_operations += 1; // TODO: make statistics mutable
        
        // Hash indexes do not support efficient range scans
        // Return empty result
        Ok(Vec::new())
    }
    
    fn size(&self) -> usize {
        self.size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_hash_index_chaining() {
        let mut index = HashIndex::new(16, CollisionResolution::Chaining, 0.75);
        
        // Insert entries
        index.insert("key1".to_string(), "value1".to_string()).unwrap();
        index.insert("key2".to_string(), "value2".to_string()).unwrap();
        index.insert("key3".to_string(), "value3".to_string()).unwrap();
        
        assert_eq!(index.size(), 3);
        
        // Verify lookup
        assert_eq!(index.search(&"key1".to_string()).unwrap(), Some("value1".to_string()));
        assert_eq!(index.search(&"key2".to_string()).unwrap(), Some("value2".to_string()));
        assert_eq!(index.search(&"key3".to_string()).unwrap(), Some("value3".to_string()));
        assert_eq!(index.search(&"key4".to_string()).unwrap(), None);
    }
    
    #[test]
    fn test_hash_index_open_addressing() {
        let mut index = HashIndex::new(16, CollisionResolution::OpenAddressing, 0.75);
        
        // Insert entries
        index.insert("key1".to_string(), "value1".to_string()).unwrap();
        index.insert("key2".to_string(), "value2".to_string()).unwrap();
        index.insert("key3".to_string(), "value3".to_string()).unwrap();
        
        assert_eq!(index.size(), 3);
        
        // Verify lookup
        assert_eq!(index.search(&"key1".to_string()).unwrap(), Some("value1".to_string()));
        assert_eq!(index.search(&"key2".to_string()).unwrap(), Some("value2".to_string()));
        assert_eq!(index.search(&"key3".to_string()).unwrap(), Some("value3".to_string()));
        assert_eq!(index.search(&"key4".to_string()).unwrap(), None);
    }
    
    #[test]
    fn test_hash_index_deletion() {
        let mut index = HashIndex::new_default();
        
        // Insert entries
        for i in 1..=10 {
            index.insert(i, format!("value_{}", i)).unwrap();
        }
        
        assert_eq!(index.size(), 10);
        
        // Delete some entries
        assert!(index.delete(&5).unwrap());
        assert!(index.delete(&7).unwrap());
        assert!(!index.delete(&15).unwrap()); // Does not exist
        
        assert_eq!(index.size(), 8);
        
        // Deleted keys are not found
        assert_eq!(index.search(&5).unwrap(), None);
        assert_eq!(index.search(&7).unwrap(), None);
        
        // Remaining keys still resolve
        assert_eq!(index.search(&1).unwrap(), Some("value_1".to_string()));
        assert_eq!(index.search(&10).unwrap(), Some("value_10".to_string()));
    }
    
    #[test]
    fn test_hash_index_resize() {
        let mut index = HashIndex::new(4, CollisionResolution::Chaining, 0.5); // Low threshold to trigger resize in test
        
        // Insert enough entries to trigger resize
        for i in 1..=20 {
            index.insert(i, format!("value_{}", i)).unwrap();
        }
        
        assert_eq!(index.size(), 20);
        assert!(index.capacity > 4); // Table should have grown
        
        // All entries remain accessible after resize
        for i in 1..=20 {
            assert_eq!(index.search(&i).unwrap(), Some(format!("value_{}", i)));
        }
    }
    
    #[test]
    fn test_hash_index_update() {
        let mut index = HashIndex::new_default();
        
        // Insert entry
        index.insert("key".to_string(), "original_value".to_string()).unwrap();
        assert_eq!(index.size(), 1);
        
        // Update same key
        index.insert("key".to_string(), "updated_value".to_string()).unwrap();
        assert_eq!(index.size(), 1); // Size must stay the same
        
        // Value must be updated
        assert_eq!(index.search(&"key".to_string()).unwrap(), Some("updated_value".to_string()));
    }
}
