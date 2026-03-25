//! Integration tests for rustdb indexes
//!
//! These tests validate index integration with other storage components
//! and their behavior in realistic scenarios.

use crate::common::Result;
use crate::storage::index::{BPlusTree, Index, SimpleHashIndex};

#[test]
fn test_btree_with_different_types() {
    // Exercise B+ tree with multiple data types

    // String keys
    let mut string_btree: BPlusTree<String, i32> = BPlusTree::new_default();
    let words = ["apple", "banana", "cherry", "date", "elderberry"];

    for (i, word) in words.iter().enumerate() {
        string_btree.insert(word.to_string(), i as i32).unwrap();
    }

    assert_eq!(string_btree.size(), 5);
    assert_eq!(string_btree.search(&"banana".to_string()).unwrap(), Some(1));
    assert_eq!(string_btree.search(&"fig".to_string()).unwrap(), None);

    // Range query over strings
    let range = string_btree
        .range_search(&"banana".to_string(), &"date".to_string())
        .unwrap();
    assert_eq!(range.len(), 3); // banana, cherry, date

    // Numeric keys with floating-point payloads
    let mut float_btree: BPlusTree<i32, f64> = BPlusTree::new_default();
    for i in 1..=10 {
        float_btree.insert(i, (i as f64) * 1.5).unwrap();
    }

    assert_eq!(float_btree.size(), 10);
    assert_eq!(float_btree.search(&5).unwrap(), Some(7.5));
}

#[test]
fn test_hash_index_with_complex_types() {
    // Exercise hash index with complex value types

    #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
    struct UserId(u64);

    #[derive(Debug, Clone, PartialEq)]
    struct UserInfo {
        name: String,
        age: u32,
        email: String,
    }

    let mut user_index: SimpleHashIndex<UserId, UserInfo> = SimpleHashIndex::new();

    let users = [
        (
            UserId(1),
            UserInfo {
                name: "Alice".to_string(),
                age: 25,
                email: "alice@example.com".to_string(),
            },
        ),
        (
            UserId(2),
            UserInfo {
                name: "Bob".to_string(),
                age: 30,
                email: "bob@example.com".to_string(),
            },
        ),
        (
            UserId(3),
            UserInfo {
                name: "Charlie".to_string(),
                age: 35,
                email: "charlie@example.com".to_string(),
            },
        ),
    ];

    for (id, info) in users.clone() {
        user_index.insert(id, info).unwrap();
    }

    assert_eq!(user_index.size(), 3);

    // Lookup user
    let alice = user_index.search(&UserId(1)).unwrap().unwrap();
    assert_eq!(alice.name, "Alice");
    assert_eq!(alice.age, 25);

    // Update record
    let updated_alice = UserInfo {
        name: "Alice Smith".to_string(),
        age: 26,
        email: "alice.smith@example.com".to_string(),
    };
    user_index.insert(UserId(1), updated_alice.clone()).unwrap();

    let retrieved = user_index.search(&UserId(1)).unwrap().unwrap();
    assert_eq!(retrieved.name, "Alice Smith");
    assert_eq!(retrieved.age, 26);
}

#[test]
#[cfg_attr(miri, ignore = "wall-clock thresholds are meaningless under Miri")]
fn test_index_performance_comparison() {
    // Compare performance between index types
    use std::time::Instant;

    const TEST_SIZE: i32 = 1000;

    // Prepare data set
    let test_data: Vec<(i32, String)> = (1..=TEST_SIZE)
        .map(|i| (i, format!("value_{}", i)))
        .collect();

    // Benchmark B+ tree
    let mut btree: BPlusTree<i32, String> = BPlusTree::new_default();
    let btree_start = Instant::now();

    for (key, value) in &test_data {
        btree.insert(*key, value.clone()).unwrap();
    }

    let btree_insert_time = btree_start.elapsed();

    // Search via B+ tree
    let btree_search_start = Instant::now();
    for i in 1..=TEST_SIZE {
        let _ = btree.search(&i).unwrap();
    }
    let btree_search_time = btree_search_start.elapsed();

    // Benchmark hash index
    let mut hash_index: SimpleHashIndex<i32, String> =
        SimpleHashIndex::with_capacity(TEST_SIZE as usize);
    let hash_start = Instant::now();

    for (key, value) in &test_data {
        hash_index.insert(*key, value.clone()).unwrap();
    }

    let hash_insert_time = hash_start.elapsed();

    // Search via hash index
    let hash_search_start = Instant::now();
    for i in 1..=TEST_SIZE {
        let _ = hash_index.search(&i).unwrap();
    }
    let hash_search_time = hash_search_start.elapsed();

    // Ensure both indexes remain consistent
    assert_eq!(btree.size(), TEST_SIZE as usize);
    assert_eq!(hash_index.size(), TEST_SIZE as usize);

    // Print metrics for manual inspection
    println!("Performance for {} elements:", TEST_SIZE);
    println!(
        "B+ tree - insert: {:?}, search: {:?}",
        btree_insert_time, btree_search_time
    );
    println!(
        "Hash index - insert: {:?}, search: {:?}",
        hash_insert_time, hash_search_time
    );

    // Both indexes should be fast enough
    assert!(btree_insert_time.as_millis() < 100);
    assert!(hash_insert_time.as_millis() < 100);
}

#[test]
fn test_btree_range_queries() {
    // Detailed range-query testing of the B+ tree
    let mut btree: BPlusTree<i32, String> = BPlusTree::new_default();

    // Insert data out of order
    let data = [10, 5, 15, 3, 7, 12, 18, 1, 6, 8, 11, 14, 16, 20];
    for &key in &data {
        btree.insert(key, format!("value_{}", key)).unwrap();
    }

    // Test various ranges

    // Full range
    let full_range = btree.range_search(&1, &20).unwrap();
    assert_eq!(full_range.len(), data.len());

    // Ensure results are sorted
    for i in 1..full_range.len() {
        assert!(full_range[i - 1].0 <= full_range[i].0);
    }

    // Partial ranges
    let mid_range = btree.range_search(&5, &15).unwrap();
    assert!(mid_range.len() >= 7); // Should include at least 7 elements

    // Empty range
    let empty_range = btree.range_search(&25, &30).unwrap();
    assert!(empty_range.is_empty());

    // Single-element range
    let single_range = btree.range_search(&10, &10).unwrap();
    assert_eq!(single_range.len(), 1);
    assert_eq!(single_range[0].0, 10);

    // Reverse range (start > end)
    let reverse_range = btree.range_search(&15, &5).unwrap();
    assert!(reverse_range.is_empty());
}

#[test]
fn test_hash_index_collision_handling() {
    // Test collision handling in the hash index
    let mut hash_index: SimpleHashIndex<String, i32> = SimpleHashIndex::with_capacity(4); // Small capacity to force collisions

    // Insert many elements to provoke collisions
    let test_keys: Vec<String> = (1..=20).map(|i| format!("key_{:03}", i)).collect();

    for (i, key) in test_keys.iter().enumerate() {
        hash_index.insert(key.clone(), i as i32).unwrap();
    }

    assert_eq!(hash_index.size(), 20);

    // Ensure every element can be retrieved
    for (i, key) in test_keys.iter().enumerate() {
        let value = hash_index.search(key).unwrap().unwrap();
        assert_eq!(value, i as i32);
    }

    // Remove half the elements
    for i in (0..10).step_by(2) {
        let removed = hash_index.delete(&test_keys[i]).unwrap();
        assert!(removed);
    }

    assert_eq!(hash_index.size(), 15);

    // Deleted elements should not be found
    for i in (0..10).step_by(2) {
        assert_eq!(hash_index.search(&test_keys[i]).unwrap(), None);
    }

    // Remaining elements must still be accessible
    for i in (1..20).step_by(2) {
        let value = hash_index.search(&test_keys[i]).unwrap().unwrap();
        assert_eq!(value, i as i32);
    }
}

#[test]
fn test_index_statistics_accuracy() {
    // Validate index statistics accuracy
    let mut btree: BPlusTree<i32, String> = BPlusTree::new_default();
    let mut hash_index: SimpleHashIndex<i32, String> = SimpleHashIndex::new();

    // Initial statistics
    assert_eq!(btree.get_statistics().total_elements, 0);
    assert_eq!(btree.get_statistics().insert_operations, 0);
    assert_eq!(hash_index.get_statistics().total_elements, 0);
    assert_eq!(hash_index.get_statistics().insert_operations, 0);

    // Insert elements
    for i in 1..=10 {
        btree.insert(i, format!("value_{}", i)).unwrap();
        hash_index.insert(i, format!("value_{}", i)).unwrap();
    }

    // Check statistics after insertion
    let btree_stats = btree.get_statistics();
    assert_eq!(btree_stats.total_elements, 10);
    assert_eq!(btree_stats.insert_operations, 10);
    assert!(btree_stats.fill_factor > 0.0);

    let hash_stats = hash_index.get_statistics();
    assert_eq!(hash_stats.total_elements, 10);
    assert_eq!(hash_stats.insert_operations, 10);
    assert!(hash_stats.fill_factor > 0.0);

    // Remove elements from the hash index
    for i in 1..=5 {
        hash_index.delete(&i).unwrap();
    }

    let hash_stats_after_delete = hash_index.get_statistics();
    assert_eq!(hash_stats_after_delete.total_elements, 5);
    assert_eq!(hash_stats_after_delete.delete_operations, 5);
}

#[test]
fn test_index_edge_cases() {
    // Exercise edge cases

    // Empty indexes
    let btree: BPlusTree<i32, String> = BPlusTree::new_default();
    let hash_index: SimpleHashIndex<i32, String> = SimpleHashIndex::new();

    assert!(btree.is_empty());
    assert!(hash_index.is_empty());
    assert_eq!(btree.search(&1).unwrap(), None);
    assert_eq!(hash_index.search(&1).unwrap(), None);

    // Range search in an empty tree
    let empty_range = btree.range_search(&1, &10).unwrap();
    assert!(empty_range.is_empty());

    // Range search in a hash index (always empty)
    let hash_range = hash_index.range_search(&1, &10).unwrap();
    assert!(hash_range.is_empty());

    // Deleting from an empty index
    let mut empty_hash: SimpleHashIndex<i32, String> = SimpleHashIndex::new();
    assert!(!empty_hash.delete(&1).unwrap());

    // Insert a single element
    let mut single_btree: BPlusTree<i32, String> = BPlusTree::new_default();
    single_btree.insert(42, "answer".to_string()).unwrap();

    assert_eq!(single_btree.size(), 1);
    assert_eq!(
        single_btree.search(&42).unwrap(),
        Some("answer".to_string())
    );

    let single_range = single_btree.range_search(&42, &42).unwrap();
    assert_eq!(single_range.len(), 1);
    assert_eq!(single_range[0], (42, "answer".to_string()));
}

#[test]
fn test_index_memory_efficiency() {
    // Evaluate memory-efficiency helpers
    use std::mem;

    let btree: BPlusTree<i32, String> = BPlusTree::new_default();
    let hash_index: SimpleHashIndex<i32, String> = SimpleHashIndex::new();

    // Check that empty indexes don't take up much memory
    let btree_size = mem::size_of_val(&btree);
    let hash_size = mem::size_of_val(&hash_index);

    println!("Size of empty B+ tree: {} bytes", btree_size);
    println!("Size of empty hash index: {} bytes", hash_size);

    // Both indexes should be sufficiently compact
    assert!(btree_size < 1024); // Less than 1KB
    assert!(hash_size < 1024); // Less than 1KB
}

#[cfg(test)]
mod concurrent_tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::thread;

    #[test]
    fn test_index_thread_safety_simulation() {
        // Simulate multi-threaded usage using Mutex
        // (In a real system, more complex synchronization is needed)

        let btree = Arc::new(Mutex::new(BPlusTree::<i32, String>::new_default()));
        let hash_index = Arc::new(Mutex::new(SimpleHashIndex::<i32, String>::new()));

        let mut handles = vec![];

        // Start multiple threads for writing
        for thread_id in 0..4 {
            let btree_clone = Arc::clone(&btree);
            let hash_clone = Arc::clone(&hash_index);

            let handle = thread::spawn(move || {
                let start = thread_id * 100;
                let end = start + 100;

                for i in start..end {
                    {
                        let mut btree_lock = btree_clone.lock().unwrap();
                        btree_lock.insert(i, format!("btree_value_{}", i)).unwrap();
                    }

                    {
                        let mut hash_lock = hash_clone.lock().unwrap();
                        hash_lock.insert(i, format!("hash_value_{}", i)).unwrap();
                    }
                }
            });

            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Check results
        {
            let btree_lock = btree.lock().unwrap();
            let hash_lock = hash_index.lock().unwrap();

            assert_eq!(btree_lock.size(), 400);
            assert_eq!(hash_lock.size(), 400);

            // Check a few random elements
            for i in [0, 100, 200, 300, 399] {
                assert!(btree_lock.search(&i).unwrap().is_some());
                assert!(hash_lock.search(&i).unwrap().is_some());
            }
        }
    }
}
