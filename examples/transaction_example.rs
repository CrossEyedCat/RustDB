//! An example of using the rustdb transaction manager
//!
//! Demonstrates the main capabilities of the transaction system:
//! - Creation and management of transactions
//! - Working with blocking
//! - Handling concurrent access
//! - Statistics and monitoring

use rustdb::core::{
    IsolationLevel, LockMode, LockType, TransactionManager, TransactionManagerConfig,
};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn main() {
    println!("🚀 Demonstration of the rustdb transaction manager\n");

    // Demonstration of basic operations
    basic_transaction_operations();

    // Demonstration of working with locks
    lock_operations_demo();

    // Demonstration of concurrent access
    concurrent_transactions_demo();

    // Demonstration of different levels of insulation
    isolation_levels_demo();

    // Statistics demonstration
    statistics_demo();

    println!("✅ Demonstration completed successfully!");
}

fn basic_transaction_operations() {
    println!("📋 1. Basic operations with transactions");
    println!("=====================================");

    let tm = TransactionManager::new().unwrap();

    // Creating a transaction
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();
    println!("✓ Transaction created: {}", txn_id);

    // Retrieving transaction information
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    println!("✓ Isolation level: {:?}", info.isolation_level);
    println!("✓ Read-only: {}", info.read_only);
    println!("✓ Status: {:?}", info.state);

    // Committing a transaction
    tm.commit_transaction(txn_id).unwrap();
    println!("✓ Transaction committed\n");
}

fn lock_operations_demo() {
    println!("🔒 2. Operations with locks");
    println!("=============================");

    let tm = TransactionManager::new().unwrap();
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();

    // Obtaining different types of locks
    let resources = vec![
        (
            "users_table",
            LockType::Table("users".to_string()),
            LockMode::Shared,
        ),
        (
            "user_email_index",
            LockType::Index("idx_user_email".to_string()),
            LockMode::Shared,
        ),
        ("user_record_1", LockType::Record(1, 1), LockMode::Exclusive),
        (
            "temp_resource",
            LockType::Resource("temp".to_string()),
            LockMode::Exclusive,
        ),
    ];

    for (name, lock_type, lock_mode) in &resources {
        tm.acquire_lock(
            txn_id,
            name.to_string(),
            lock_type.clone(),
            lock_mode.clone(),
        )
        .unwrap();
        println!("✓ Lock received {:?} on {}", lock_mode, name);
    }

    // Checking blocked resources
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    println!(
        "✓ Total blocked resources: {}",
        info.locked_resources.len()
    );

    // Release one lock
    tm.release_lock(txn_id, "temp_resource".to_string())
        .unwrap();
    println!("✓ The temp_resource lock has been released");

    // When committed, all other locks are released automatically
    tm.commit_transaction(txn_id).unwrap();
    println!("✓ All locks are released when commit\n");
}

fn concurrent_transactions_demo() {
    println!("🔄 3. Competitive transactions");
    println!("=============================");

    let tm = Arc::new(TransactionManager::new().unwrap());
    let mut handles = vec![];

    // We launch several parallel transactions
    for i in 0..4 {
        let tm_clone = Arc::clone(&tm);
        let handle = thread::spawn(move || {
            let txn_id = tm_clone
                .begin_transaction(IsolationLevel::ReadCommitted, false)
                .unwrap();
            println!("🔵 Thread {}: Transaction {} started", i, txn_id);

            // Each transaction works with its own resources
            let shared_resource = "shared_data".to_string();
            let unique_resource = format!("data_partition_{}", i);

            // Obtaining a shared lock on a shared resource
            tm_clone
                .acquire_lock(
                    txn_id,
                    shared_resource,
                    LockType::Resource("shared".to_string()),
                    LockMode::Shared,
                )
                .unwrap();

            // We obtain an exclusive lock on a unique resource
            tm_clone
                .acquire_lock(
                    txn_id,
                    unique_resource.clone(),
                    LockType::Resource(format!("partition_{}", i)),
                    LockMode::Exclusive,
                )
                .unwrap();

            println!("🟢 Thread {}: Locks acquired", i);

            // We imitate work
            thread::sleep(Duration::from_millis(100));

            // We fix the transaction
            tm_clone.commit_transaction(txn_id).unwrap();
            println!("✅ Flow {}: Transaction committed", i);
        });
        handles.push(handle);
    }

    // Waiting for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    println!("✓ All competitive transactions are completed\n");
}

fn isolation_levels_demo() {
    println!("🎯 4. Insulation levels");
    println!("====================");

    let tm = TransactionManager::new().unwrap();

    let levels = vec![
        (
            IsolationLevel::ReadUncommitted,
            "Reading Uncommitted Data",
        ),
        (
            IsolationLevel::ReadCommitted,
            "Reading Captured Data",
        ),
        (IsolationLevel::RepeatableRead, "Repeatable reading"),
        (IsolationLevel::Serializable, "Serializability"),
    ];

    for (level, description) in levels {
        let txn_id = tm.begin_transaction(level.clone(), false).unwrap();
        println!("   ✓ {:?}: {}", level, description);

        // We get a lock (behavior may differ depending on the level)
        tm.acquire_lock(
            txn_id,
            format!("resource_{:?}", level),
            LockType::Resource("demo".to_string()),
            LockMode::Shared,
        )
        .unwrap();

        tm.commit_transaction(txn_id).unwrap();
    }

    println!("✓ All insulation levels are demonstrated\n");
}

fn statistics_demo() {
    println!("📊 5. Statistics and monitoring");
    println!("=============================");

    // Creating a manager with limited configuration for demonstration
    let config = TransactionManagerConfig {
        max_concurrent_transactions: 10,
        lock_timeout_ms: 5000,
        deadlock_detection_interval_ms: 500,
        max_idle_time_seconds: 1800,
        enable_deadlock_detection: true,
    };

    let tm = TransactionManager::with_config(config).unwrap();

    println!("📋 Configuration:");
    let cfg = tm.get_config();
    println!(
        "• Max. simultaneous transactions: {}",
        cfg.max_concurrent_transactions
    );
    println!("• Lock timeout: {} ms", cfg.lock_timeout_ms);
    println!(
        "• Deadlock detection: {}",
        cfg.enable_deadlock_detection
    );

    // We perform several operations to collect statistics
    let mut transaction_ids = Vec::new();

    // Create several transactions
    for i in 0..5 {
        let read_only = i % 2 == 0;
        let txn_id = tm
            .begin_transaction(IsolationLevel::ReadCommitted, read_only)
            .unwrap();
        transaction_ids.push(txn_id);

        // Getting blocked
        tm.acquire_lock(
            txn_id,
            format!("resource_{}", i),
            LockType::Resource(format!("r{}", i)),
            LockMode::Shared,
        )
        .unwrap();
    }

    // We fix some transactions and cancel the rest
    for (i, &txn_id) in transaction_ids.iter().enumerate() {
        if i % 2 == 0 {
            tm.commit_transaction(txn_id).unwrap();
        } else {
            tm.abort_transaction(txn_id).unwrap();
        }
    }

    // Showing statistics
    let stats = tm.get_statistics().unwrap();
    println!("\n 📈 Statistics:");
    println!("• Total transactions: {}", stats.total_transactions);
    println!("• Active transactions: {}", stats.active_transactions);
    println!("• Fixed: {}", stats.committed_transactions);
    println!("• Canceled: {}", stats.aborted_transactions);
    println!("• Blocking operations: {}", stats.lock_operations);
    println!(
        "• Unlock operations: {}",
        stats.unlock_operations
    );
    println!("• Deadlocks detected: {}", stats.deadlocks_detected);

    println!("\n ✓ Statistics collected and displayed\n");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_example_runs_without_panic() {
        // Checking that the example runs without panic
        basic_transaction_operations();
        lock_operations_demo();
        isolation_levels_demo();
        statistics_demo();
    }
}
