//! Example of using rustdb indexes
//!
//! This example demonstrates the use of a B+ tree and hash indexes
//! for quick data retrieval.

use rustdb::storage::index::{BPlusTree, Index, SimpleHashIndex};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🌳 Example of using rustdb indexes");

    // B+ tree demo
    println!("\n📊 B+ tree:");
    btree_example()?;

    // Hash Index Demonstration
    println!("\n🔗 Hash index:");
    hash_index_example()?;

    // Performance Comparison
    println!("\n⚡ Performance comparison:");
    performance_comparison()?;

    Ok(())
}

fn btree_example() -> Result<(), Box<dyn std::error::Error>> {
    let mut btree: BPlusTree<i32, String> = BPlusTree::new_default();

    // Inserting data
    println!("Inserting data into a B+ tree...");
    for i in [5, 2, 8, 1, 9, 3, 7, 4, 6] {
        btree.insert(i, format!("Meaning {}", i))?;
    }

    println!("Tree size: {} elements", btree.size());

    // Search for individual elements
    println!("Search for elements:");
    for key in [1, 5, 9, 10] {
        match btree.search(&key)? {
            Some(value) => println!("Key {}: {}", key, value),
            None => println!("Key {} not found", key),
        }
    }

    // Range search
    println!("Range search (3-7):");
    let range_results = btree.range_search(&3, &7)?;
    for (key, value) in range_results {
        println!("  {}: {}", key, value);
    }

    // Statistics
    let stats = btree.get_statistics();
    println!("B+ tree statistics:");
    println!("Insert operations: {}", stats.insert_operations);
    println!("Depth: {}", stats.depth);
    println!("Fill factor: {:.2}", stats.fill_factor);

    Ok(())
}

fn hash_index_example() -> Result<(), Box<dyn std::error::Error>> {
    let mut hash_index: SimpleHashIndex<String, i32> = SimpleHashIndex::new();

    // Inserting data
    println!("Inserting data into the hash index...");
    let users = [
        ("alice", 25),
        ("bob", 30),
        ("charlie", 35),
        ("diana", 28),
        ("eve", 32),
    ];

    for (name, age) in users {
        hash_index.insert(name.to_string(), age)?;
    }

    println!("Index size: {} elements", hash_index.size());

    // Search for elements
    println!("Search for users:");
    for name in ["alice", "bob", "frank"] {
        match hash_index.search(&name.to_string())? {
            Some(age) => println!("{}: {} years", name, age),
            None => println!("{} not found", name),
        }
    }

    // Removing an element
    println!("Deleting user 'charlie'...");
    if hash_index.delete(&"charlie".to_string())? {
        println!("User deleted");
    }

    println!("Size after removal: {} elements", hash_index.size());

    // Update value
    println!("Update Alice's age...");
    hash_index.insert("alice".to_string(), 26)?;

    if let Some(age) = hash_index.search(&"alice".to_string())? {
        println!("Alice's new age: {} years", age);
    }

    // Statistics
    let stats = hash_index.get_statistics();
    println!("Hash index statistics:");
    println!("Insert operations: {}", stats.insert_operations);
    println!("Delete operations: {}", stats.delete_operations);
    println!("Fill factor: {:.2}", stats.fill_factor);

    Ok(())
}

fn performance_comparison() -> Result<(), Box<dyn std::error::Error>> {
    use std::time::Instant;

    const N: i32 = 10000;

    // Testing the B+ tree
    let mut btree: BPlusTree<i32, String> = BPlusTree::new_default();
    let start = Instant::now();

    for i in 1..=N {
        btree.insert(i, format!("value_{}", i))?;
    }

    let btree_insert_time = start.elapsed();

    let start = Instant::now();
    for i in 1..=N {
        let _ = btree.search(&i)?;
    }
    let btree_search_time = start.elapsed();

    // Testing the hash index
    let mut hash_index: SimpleHashIndex<i32, String> = SimpleHashIndex::with_capacity(N as usize);
    let start = Instant::now();

    for i in 1..=N {
        hash_index.insert(i, format!("value_{}", i))?;
    }

    let hash_insert_time = start.elapsed();

    let start = Instant::now();
    for i in 1..=N {
        let _ = hash_index.search(&i)?;
    }
    let hash_search_time = start.elapsed();

    println!("Results for {} elements:", N);
    println!("B+ tree:");
    println!("Insertion time: {:?}", btree_insert_time);
    println!("Search time: {:?}", btree_search_time);
    println!("Depth: {}", btree.get_statistics().depth);

    println!("Hash index:");
    println!("Insertion time: {:?}", hash_insert_time);
    println!("Search time: {:?}", hash_search_time);
    println!(
        "Fill factor: {:.2}",
        hash_index.get_statistics().fill_factor
    );

    println!("\nConclusions:");
    if hash_search_time < btree_search_time {
        println!("✅ Hash index is faster for searching individual elements");
    } else {
        println!("✅ B+ tree is competitive for search");
    }
    println!("📊 B+ tree supports range queries");
    println!("🔗 Hash index is optimal for precise searches");

    Ok(())
}
