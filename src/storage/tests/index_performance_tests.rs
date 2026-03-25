//! Performance tests for rustdb indexes
//!
//! These tests measure index operation throughput and help uncover bottlenecks.

use crate::storage::index::{BPlusTree, Index, SimpleHashIndex};
use std::time::{Duration, Instant};

/// Holds benchmark results
#[derive(Debug)]
struct BenchmarkResult {
    operation: String,
    index_type: String,
    elements: usize,
    duration: Duration,
    ops_per_second: f64,
}

impl BenchmarkResult {
    fn new(operation: &str, index_type: &str, elements: usize, duration: Duration) -> Self {
        let ops_per_second = if duration.as_secs_f64() > 0.0 {
            elements as f64 / duration.as_secs_f64()
        } else {
            0.0
        };

        Self {
            operation: operation.to_string(),
            index_type: index_type.to_string(),
            elements,
            duration,
            ops_per_second,
        }
    }

    fn print(&self) {
        println!(
            "  {} - {}: {} elements in {:?} ({:.0} ops/sec)",
            self.index_type, self.operation, self.elements, self.duration, self.ops_per_second
        );
    }
}

#[test]
#[cfg_attr(miri, ignore = "throughput thresholds are meaningless under Miri")]
fn test_insertion_performance() {
    println!("🚀 Insertion performance test:");

    let test_sizes = [100, 1_000, 10_000];
    let mut results = Vec::new();

    for &size in &test_sizes {
        // Benchmark B+ tree
        let mut btree: BPlusTree<i32, String> = BPlusTree::new_default();
        let start = Instant::now();

        for i in 1..=size {
            btree.insert(i as i32, format!("value_{}", i)).unwrap();
        }

        let btree_duration = start.elapsed();
        results.push(BenchmarkResult::new(
            "insert",
            "B+ tree",
            size,
            btree_duration,
        ));

        // Benchmark hash index
        let mut hash_index: SimpleHashIndex<i32, String> = SimpleHashIndex::with_capacity(size);
        let start = Instant::now();

        for i in 1..=size {
            hash_index.insert(i as i32, format!("value_{}", i)).unwrap();
        }

        let hash_duration = start.elapsed();
        results.push(BenchmarkResult::new(
            "insert",
            "Hash index",
            size,
            hash_duration,
        ));
    }

    for result in &results {
        result.print();
    }

    // Ensure operations complete within reasonable time
    for result in &results {
        assert!(
            result.ops_per_second > 1000.0,
            "Insert benchmark too slow: {:.0} ops/sec",
            result.ops_per_second
        );
    }
}

#[test]
#[cfg_attr(miri, ignore = "throughput thresholds are meaningless under Miri")]
fn test_search_performance() {
    println!("🔍 Search performance test:");

    const SIZE: usize = 10_000;
    let mut results = Vec::new();

    // Populate B+ tree
    let mut btree: BPlusTree<i32, String> = BPlusTree::new_default();
    for i in 1..=SIZE {
        btree.insert(i as i32, format!("value_{}", i)).unwrap();
    }

    // Populate hash index
    let mut hash_index: SimpleHashIndex<i32, String> = SimpleHashIndex::with_capacity(SIZE);
    for i in 1..=SIZE {
        hash_index.insert(i as i32, format!("value_{}", i)).unwrap();
    }

    // Sequential searches
    let start = Instant::now();
    for i in 1..=SIZE {
        let _ = btree.search(&(i as i32)).unwrap();
    }
    let btree_sequential = start.elapsed();
    results.push(BenchmarkResult::new(
        "sequential search",
        "B+ tree",
        SIZE,
        btree_sequential,
    ));

    let start = Instant::now();
    for i in 1..=SIZE {
        let _ = hash_index.search(&(i as i32)).unwrap();
    }
    let hash_sequential = start.elapsed();
    results.push(BenchmarkResult::new(
        "sequential search",
        "Hash index",
        SIZE,
        hash_sequential,
    ));

    // Random searches
    let random_keys: Vec<i32> = (1..=SIZE).map(|i| ((i * 7919) % SIZE + 1) as i32).collect();

    let start = Instant::now();
    for &key in &random_keys {
        let _ = btree.search(&key).unwrap();
    }
    let btree_random = start.elapsed();
    results.push(BenchmarkResult::new(
        "random search",
        "B+ tree",
        SIZE,
        btree_random,
    ));

    let start = Instant::now();
    for &key in &random_keys {
        let _ = hash_index.search(&key).unwrap();
    }
    let hash_random = start.elapsed();
    results.push(BenchmarkResult::new(
        "random search",
        "Hash index",
        SIZE,
        hash_random,
    ));

    for result in &results {
        result.print();
    }

    // Guardrail assertions
    for result in &results {
        assert!(
            result.ops_per_second > 10_000.0,
            "Search benchmark too slow: {:.0} ops/sec",
            result.ops_per_second
        );
    }
}

#[test]
#[cfg_attr(miri, ignore = "wall-clock thresholds are meaningless under Miri")]
fn test_range_query_performance() {
    println!("📊 Range-query performance test:");

    const SIZE: usize = 10_000;
    let mut btree: BPlusTree<i32, String> = BPlusTree::new_default();

    // Populate tree
    for i in 1..=SIZE {
        btree.insert(i as i32, format!("value_{}", i)).unwrap();
    }

    let range_sizes = [10, 100, 1_000, 5_000];

    for &range_size in &range_sizes {
        let start_key = (SIZE / 2 - range_size / 2) as i32;
        let end_key = (SIZE / 2 + range_size / 2) as i32;

        let start = Instant::now();
        let results = btree.range_search(&start_key, &end_key).unwrap();
        let duration = start.elapsed();

        println!(
            "  Range size {}: {} results in {:?}",
            range_size,
            results.len(),
            duration
        );

        // Validate result set
        assert!(results.len() <= range_size + 1); // +1 due to inclusive bounds
        assert!(duration.as_millis() < 100); // Should remain fast
    }
}

#[test]
fn test_deletion_performance() {
    println!("🗑️ Deletion performance test:");

    const SIZE: usize = 10_000;
    let mut results = Vec::new();

    // Populate hash index
    let mut hash_index: SimpleHashIndex<i32, String> = SimpleHashIndex::with_capacity(SIZE);
    for i in 1..=SIZE {
        hash_index.insert(i as i32, format!("value_{}", i)).unwrap();
    }

    // Delete every second element
    let keys_to_delete: Vec<i32> = (1..=SIZE).step_by(2).map(|i| i as i32).collect();
    let delete_count = keys_to_delete.len();

    let start = Instant::now();
    for &key in &keys_to_delete {
        hash_index.delete(&key).unwrap();
    }
    let deletion_duration = start.elapsed();

    results.push(BenchmarkResult::new(
        "delete",
        "Hash index",
        delete_count,
        deletion_duration,
    ));

    // Confirm elements were removed
    for &key in &keys_to_delete {
        assert_eq!(hash_index.search(&key).unwrap(), None);
    }

    // Remaining elements should still be present
    for i in (2..=SIZE).step_by(2) {
        assert!(hash_index.search(&(i as i32)).unwrap().is_some());
    }

    for result in &results {
        result.print();
    }

    assert_eq!(hash_index.size(), SIZE - delete_count);
}

#[test]
#[cfg_attr(miri, ignore = "throughput thresholds are meaningless under Miri")]
fn test_mixed_operations_performance() {
    println!("🔄 Mixed operations performance test:");

    const OPERATIONS: usize = 10_000;
    let mut hash_index: SimpleHashIndex<i32, String> = SimpleHashIndex::new();

    let start = Instant::now();

    for i in 0..OPERATIONS {
        match i % 4 {
            0 => {
                // Insert
                hash_index.insert(i as i32, format!("value_{}", i)).unwrap();
            }
            1 => {
                // Search existing element
                if i > 0 {
                    let _ = hash_index.search(&((i - 1) as i32)).unwrap();
                }
            }
            2 => {
                // Search missing element
                let _ = hash_index.search(&(-1)).unwrap();
            }
            3 => {
                // Delete
                if i > 3 {
                    hash_index.delete(&((i - 3) as i32)).unwrap();
                }
            }
            _ => unreachable!(),
        }
    }

    let total_duration = start.elapsed();
    let ops_per_second = OPERATIONS as f64 / total_duration.as_secs_f64();

    println!(
        "  Mixed operations: {} ops in {:?} ({:.0} ops/sec)",
        OPERATIONS, total_duration, ops_per_second
    );

    assert!(
        ops_per_second > 50_000.0,
        "Mixed operations too slow: {:.0} ops/sec",
        ops_per_second
    );
}

#[test]
fn test_memory_usage_scaling() {
    println!("💾 Memory usage scaling test:");

    use std::mem;

    let sizes = [100, 1_000, 10_000];

    for &size in &sizes {
        // Evaluate B+ tree
        let mut btree: BPlusTree<i32, String> = BPlusTree::new_default();
        for i in 1..=size {
            btree.insert(i as i32, format!("value_{:06}", i)).unwrap();
        }

        let btree_size = mem::size_of_val(&btree);
        println!(
            "  B+ tree with {} elements: ~{} bytes for base structure",
            size, btree_size
        );

        // Evaluate hash index
        let mut hash_index: SimpleHashIndex<i32, String> = SimpleHashIndex::new();
        for i in 1..=size {
            hash_index
                .insert(i as i32, format!("value_{:06}", i))
                .unwrap();
        }

        let hash_size = mem::size_of_val(&hash_index);
        println!(
            "  Hash index with {} elements: ~{} bytes for base structure",
            size, hash_size
        );

        // Base structures should not grow dramatically
        assert!(btree_size < 1024);
        assert!(hash_size < 1024);
    }
}

#[test]
#[cfg_attr(miri, ignore = "throughput thresholds are meaningless under Miri")]
fn test_cache_efficiency() {
    println!("⚡ Cache efficiency test:");

    const SIZE: usize = 100_000;
    let mut btree: BPlusTree<i32, i32> = BPlusTree::new_default();

    // Populate index
    for i in 1..=SIZE {
        btree.insert(i as i32, i as i32).unwrap();
    }

    // Sequential access pattern
    let start = Instant::now();
    for i in 1..=SIZE {
        let _ = btree.search(&(i as i32)).unwrap();
    }
    let sequential_duration = start.elapsed();

    // Random access pattern
    let random_keys: Vec<i32> = (1..=SIZE)
        .map(|i| ((i * 31337) % SIZE + 1) as i32)
        .collect();
    let start = Instant::now();
    for &key in &random_keys {
        let _ = btree.search(&key).unwrap();
    }
    let random_duration = start.elapsed();

    println!("  Sequential access: {:?}", sequential_duration);
    println!("  Random access: {:?}", random_duration);

    // Sequential access should be faster due to cache locality
    let sequential_ops_per_sec = SIZE as f64 / sequential_duration.as_secs_f64();
    let random_ops_per_sec = SIZE as f64 / random_duration.as_secs_f64();

    println!("  Sequential: {:.0} ops/sec", sequential_ops_per_sec);
    println!("  Random: {:.0} ops/sec", random_ops_per_sec);

    // Both access patterns should remain performant
    assert!(sequential_ops_per_sec > 100_000.0);
    assert!(random_ops_per_sec > 50_000.0);
}
