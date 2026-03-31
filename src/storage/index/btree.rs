//! B+ tree for rustdb
//!
//! B+ tree implementation for efficient data indexing.
//! B+ tree supports fast search, insert, delete and range queries.

use crate::common::{Error, Result};
use crate::storage::index::{Index, IndexStatistics};
use std::cell::RefCell;
use std::cmp::Ordering;
use std::fmt::Debug;

/// Default maximum degree for B+ tree
pub const DEFAULT_DEGREE: usize = 128;

/// Minimum degree for B+ tree
pub const MIN_DEGREE: usize = 3;

/// B+ tree node
#[derive(Debug, Clone)]
pub struct BTreeNode<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    /// Keys in node
    pub keys: Vec<K>,
    /// Values in node (only for leaf nodes)
    pub values: Vec<V>,
    /// Child nodes (only for internal nodes)
    pub children: Vec<Box<BTreeNode<K, V>>>,
    /// Whether node is a leaf
    pub is_leaf: bool,
    /// Pointer to next leaf node (only for leaves)
    pub next_leaf: Option<Box<BTreeNode<K, V>>>,
}

impl<K, V> BTreeNode<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    /// Creates a new leaf node
    pub fn new_leaf() -> Self {
        Self {
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            is_leaf: true,
            next_leaf: None,
        }
    }

    /// Creates a new internal node
    pub fn new_internal() -> Self {
        Self {
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            is_leaf: false,
            next_leaf: None,
        }
    }

    /// Checks if node is full
    pub fn is_full(&self, degree: usize) -> bool {
        self.keys.len() >= degree - 1
    }

    /// Checks if node is underfull
    pub fn is_underfull(&self, degree: usize) -> bool {
        self.keys.len() < (degree - 1) / 2
    }

    /// Finds position for key insertion (leaf: sorted insert / update index)
    pub fn find_key_position(&self, key: &K) -> usize {
        self.keys.binary_search(key).unwrap_or_else(|pos| pos)
    }

    /// Child index for internal-node descent: `Err(i)` → child `i`;
    /// if key equals separator `keys[i]`, use child `i + 1` (right subtree).
    pub fn descend_child_index(&self, key: &K) -> usize {
        match self.keys.binary_search(key) {
            Ok(i) => i + 1,
            Err(i) => i,
        }
    }

    /// Searches for key in node
    pub fn search_key(&self, key: &K) -> Option<usize> {
        self.keys.binary_search(key).ok()
    }
}

/// B+ tree
#[derive(Debug, Clone)]
pub struct BPlusTree<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    /// Root node
    root: Option<Box<BTreeNode<K, V>>>,
    /// Tree degree (maximum number of children)
    degree: usize,
    /// Operation statistics (interior mutability for [`Index::search`])
    statistics: RefCell<IndexStatistics>,
}

impl<K, V> BPlusTree<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    /// Creates a new B+ tree with given degree
    pub fn new(degree: usize) -> Self {
        let degree = if degree < MIN_DEGREE {
            MIN_DEGREE
        } else {
            degree
        };

        Self {
            root: None,
            degree,
            statistics: RefCell::new(IndexStatistics::default()),
        }
    }

    /// Creates a new B+ tree with default degree
    pub fn new_default() -> Self {
        Self::new(DEFAULT_DEGREE)
    }

    /// Returns a snapshot of tree statistics
    pub fn get_statistics(&self) -> IndexStatistics {
        self.statistics.borrow().clone()
    }

    /// Calculates tree depth
    pub fn calculate_depth(&self) -> u32 {
        self.calculate_node_depth(self.root.as_ref(), 0)
    }

    fn calculate_node_depth(&self, node: Option<&Box<BTreeNode<K, V>>>, current_depth: u32) -> u32 {
        match node {
            None => current_depth,
            Some(node) => {
                if node.is_leaf {
                    current_depth + 1
                } else {
                    let mut max_depth = current_depth + 1;
                    for child in &node.children {
                        let child_depth = self.calculate_node_depth(Some(child), current_depth + 1);
                        max_depth = max_depth.max(child_depth);
                    }
                    max_depth
                }
            }
        }
    }

    /// Updates tree statistics
    fn update_statistics(&self) {
        let mut s = self.statistics.borrow_mut();
        s.depth = self.calculate_depth();
        s.fill_factor = self.calculate_fill_factor();
    }

    /// Calculates tree fill factor
    fn calculate_fill_factor(&self) -> f64 {
        if self.root.is_none() {
            return 0.0;
        }

        let (total_slots, used_slots) = self.calculate_fill_stats(self.root.as_ref());
        if total_slots == 0 {
            0.0
        } else {
            used_slots as f64 / total_slots as f64
        }
    }

    fn calculate_fill_stats(&self, node: Option<&Box<BTreeNode<K, V>>>) -> (usize, usize) {
        match node {
            None => (0, 0),
            Some(node) => {
                let mut total_slots = self.degree - 1; // Maximum number of keys
                let mut used_slots = node.keys.len();

                if !node.is_leaf {
                    for child in &node.children {
                        let (child_total, child_used) = self.calculate_fill_stats(Some(child));
                        total_slots += child_total;
                        used_slots += child_used;
                    }
                }

                (total_slots, used_slots)
            }
        }
    }

    /// Splits leaf node
    fn split_leaf(&mut self, node: &mut BTreeNode<K, V>) -> Result<(K, Box<BTreeNode<K, V>>)> {
        let mid = node.keys.len() / 2;

        let mut new_node = BTreeNode::new_leaf();
        new_node.keys = node.keys.split_off(mid);
        new_node.values = node.values.split_off(mid);

        // Link leaf nodes (right sibling; parent also receives `new_box`)
        new_node.next_leaf = node.next_leaf.take();
        let separator_key = new_node.keys[0].clone();
        let new_box = Box::new(new_node);
        node.next_leaf = Some(new_box.clone());
        Ok((separator_key, new_box))
    }

    /// Splits internal node
    fn split_internal(&mut self, node: &mut BTreeNode<K, V>) -> Result<(K, Box<BTreeNode<K, V>>)> {
        let mid = node.keys.len() / 2;

        let mut new_node = BTreeNode::new_internal();

        // Split keys (middle key goes up)
        let separator_key = node.keys[mid].clone();
        new_node.keys = node.keys.split_off(mid + 1);
        node.keys.truncate(mid);

        // Split children
        new_node.children = node.children.split_off(mid + 1);

        Ok((separator_key, Box::new(new_node)))
    }

    /// Inserts key-value into leaf node
    fn insert_into_leaf(
        &mut self,
        node: &mut BTreeNode<K, V>,
        key: K,
        value: V,
    ) -> Result<Option<(K, Box<BTreeNode<K, V>>)>> {
        let pos = node.find_key_position(&key);

        // Check if key already exists
        if pos < node.keys.len() && node.keys[pos] == key {
            // Update existing value
            node.values[pos] = value;
            return Ok(None);
        }

        // Insert new key-value
        node.keys.insert(pos, key);
        node.values.insert(pos, value);
        self.statistics.borrow_mut().total_elements += 1;

        // Check if split is needed
        if node.is_full(self.degree) {
            let (separator_key, new_node) = self.split_leaf(node)?;
            Ok(Some((separator_key, new_node)))
        } else {
            Ok(None)
        }
    }

    /// Inserts key into internal node
    fn insert_into_internal(
        &mut self,
        node: &mut BTreeNode<K, V>,
        key: K,
        value: V,
    ) -> Result<Option<(K, Box<BTreeNode<K, V>>)>> {
        let pos = node.descend_child_index(&key);

        // Recursively insert into appropriate child
        let split_result = if node.children[pos].is_leaf {
            self.insert_into_leaf(&mut node.children[pos], key, value)?
        } else {
            self.insert_into_internal(&mut node.children[pos], key, value)?
        };

        // If child was split, need to update current node
        if let Some((separator_key, new_child)) = split_result {
            node.keys.insert(pos, separator_key);
            node.children.insert(pos + 1, new_child);

            // Check if current node needs splitting
            if node.is_full(self.degree) {
                let (separator_key, new_node) = self.split_internal(node)?;
                Ok(Some((separator_key, new_node)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Searches for value by key in node
    fn search_in_node(&self, node: &BTreeNode<K, V>, key: &K) -> Option<V> {
        if node.is_leaf {
            // Search in leaf node
            if let Some(pos) = node.search_key(key) {
                Some(node.values[pos].clone())
            } else {
                None
            }
        } else {
            let pos = node.descend_child_index(key);
            if pos < node.children.len() {
                self.search_in_node(&node.children[pos], key)
            } else {
                None
            }
        }
    }

    /// Collects all key-values in range from leaf nodes
    fn collect_range_from_leaf(
        &self,
        node: &BTreeNode<K, V>,
        start: &K,
        end: &K,
        result: &mut Vec<(K, V)>,
    ) {
        if !node.is_leaf {
            return;
        }

        for (i, key) in node.keys.iter().enumerate() {
            if key >= start && key <= end {
                result.push((key.clone(), node.values[i].clone()));
            } else if key > end {
                break;
            }
        }

        // Move to next leaf node
        if let Some(ref next) = node.next_leaf {
            if !next.keys.is_empty() && next.keys[0] <= *end {
                self.collect_range_from_leaf(next, start, end, result);
            }
        }
    }

    /// Collects all keys in range recursively
    fn collect_range_recursive(
        &self,
        node: &BTreeNode<K, V>,
        start: &K,
        end: &K,
        result: &mut Vec<(K, V)>,
    ) {
        if node.is_leaf {
            // Collect keys from leaf node
            for (i, key) in node.keys.iter().enumerate() {
                if key >= start && key <= end {
                    result.push((key.clone(), node.values[i].clone()));
                }
            }
        } else {
            // Recursively traverse all children
            for child in &node.children {
                self.collect_range_recursive(child, start, end, result);
            }
        }
    }
}

impl<K, V> Index for BPlusTree<K, V>
where
    K: Ord + Clone + Debug,
    V: Clone + Debug,
{
    type Key = K;
    type Value = V;

    fn insert(&mut self, key: Self::Key, value: Self::Value) -> Result<()> {
        self.statistics.borrow_mut().insert_operations += 1;

        if self.root.is_none() {
            let mut root = BTreeNode::new_leaf();
            root.keys.push(key);
            root.values.push(value);
            self.root = Some(Box::new(root));
            self.statistics.borrow_mut().total_elements = 1;
            self.update_statistics();
            return Ok(());
        }

        let mut root_box = self.root.take().unwrap();
        let split_result = if root_box.is_leaf {
            self.insert_into_leaf(&mut root_box, key, value)?
        } else {
            self.insert_into_internal(&mut root_box, key, value)?
        };
        self.root = Some(root_box);

        if let Some((separator_key, new_child)) = split_result {
            let mut new_root = BTreeNode::new_internal();
            new_root.keys.push(separator_key);
            new_root.children.push(self.root.take().unwrap());
            new_root.children.push(new_child);
            self.root = Some(Box::new(new_root));
        }

        self.update_statistics();
        Ok(())
    }

    fn search(&self, key: &Self::Key) -> Result<Option<Self::Value>> {
        self.statistics.borrow_mut().search_operations += 1;

        match &self.root {
            None => Ok(None),
            Some(root) => Ok(self.search_in_node(root, key)),
        }
    }

    fn delete(&mut self, key: &Self::Key) -> Result<bool> {
        self.statistics.borrow_mut().delete_operations += 1;

        if self.root.is_none() {
            return Ok(false);
        }

        let root = self.root.as_mut().unwrap();
        if root.is_leaf {
            if let Some(pos) = root.search_key(key) {
                root.keys.remove(pos);
                root.values.remove(pos);
                {
                    let mut s = self.statistics.borrow_mut();
                    s.total_elements = s.total_elements.saturating_sub(1);
                }
                if root.keys.is_empty() {
                    self.root = None;
                }
                self.update_statistics();
                return Ok(true);
            }
        } else {
            let pos = root.descend_child_index(key);
            if pos < root.children.len() {
                let child = &mut root.children[pos];
                if child.is_leaf {
                    if let Some(idx) = child.search_key(key) {
                        child.keys.remove(idx);
                        child.values.remove(idx);
                        {
                            let mut s = self.statistics.borrow_mut();
                            s.total_elements = s.total_elements.saturating_sub(1);
                        }
                        self.update_statistics();
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }

    fn range_search(
        &self,
        start: &Self::Key,
        end: &Self::Key,
    ) -> Result<Vec<(Self::Key, Self::Value)>> {
        self.statistics.borrow_mut().range_search_operations += 1;

        if start > end {
            return Ok(Vec::new());
        }

        let mut result = Vec::new();

        if let Some(ref root) = self.root {
            // Simplified range search - just collect all matching keys
            self.collect_range_recursive(root, start, end, &mut result);
        }

        Ok(result)
    }

    fn size(&self) -> usize {
        self.statistics.borrow().total_elements as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_creation() {
        let btree: BPlusTree<i32, String> = BPlusTree::new_default();
        assert!(btree.is_empty());
        assert_eq!(btree.size(), 0);
    }

    #[test]
    fn test_btree_insert_and_search() {
        let mut btree = BPlusTree::new_default();

        // Insert several elements
        btree.insert(1, "one".to_string()).unwrap();
        btree.insert(3, "three".to_string()).unwrap();
        btree.insert(2, "two".to_string()).unwrap();

        assert_eq!(btree.size(), 3);

        // Check search
        assert_eq!(btree.search(&1).unwrap(), Some("one".to_string()));
        assert_eq!(btree.search(&2).unwrap(), Some("two".to_string()));
        assert_eq!(btree.search(&3).unwrap(), Some("three".to_string()));
        assert_eq!(btree.search(&4).unwrap(), None);
    }

    #[test]
    fn test_btree_range_search() {
        let mut btree = BPlusTree::new_default();

        // Insert elements
        for i in 1..=10 {
            btree.insert(i, format!("value_{}", i)).unwrap();
        }

        // Test range search
        let results = btree.range_search(&3, &7).unwrap();
        assert_eq!(results.len(), 5);

        for (i, (key, value)) in results.iter().enumerate() {
            assert_eq!(*key, (i + 3) as i32);
            assert_eq!(*value, format!("value_{}", i + 3));
        }
    }

    #[test]
    fn test_btree_large_dataset() {
        let mut btree = BPlusTree::new(4); // Small degree for testing splits

        // Insert many elements
        for i in 1..=1000 {
            btree.insert(i, format!("value_{}", i)).unwrap();
        }

        assert_eq!(btree.size(), 1000);

        // Check random elements
        assert_eq!(btree.search(&1).unwrap(), Some("value_1".to_string()));
        assert_eq!(btree.search(&500).unwrap(), Some("value_500".to_string()));
        assert_eq!(btree.search(&1000).unwrap(), Some("value_1000".to_string()));
        assert_eq!(btree.search(&1001).unwrap(), None);

        // Check tree depth (for simplified version always 1)
        let depth = btree.calculate_depth();
        assert!(depth >= 1); // Should be at least single-level tree
        assert!(depth < 20); // But not too deep
    }
}
