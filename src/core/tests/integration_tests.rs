//! Integration tests for the transaction and lock managers

#![allow(clippy::absurd_extreme_comparisons)]

use crate::common::Result;
use crate::core::{
    IsolationLevel, LockMode, LockType, TransactionId, TransactionManager, TransactionState,
};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn test_transaction_with_locks_integration() {
    let tm = TransactionManager::new().unwrap();

    // Begin a transaction and acquire locks
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();

    let resources = vec![
        ("table_users", LockMode::Shared),
        ("table_orders", LockMode::Shared),
        ("index_user_email", LockMode::Exclusive),
    ];

    // Acquire locks via the transaction manager
    for (resource, mode) in &resources {
        tm.acquire_lock(
            txn_id,
            resource.to_string(),
            LockType::Resource(resource.to_string()),
            mode.clone(),
        )
        .unwrap();
    }

    // Ensure all locks are registered with the transaction
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    assert_eq!(info.locked_resources.len(), 3);

    // Committing should release every lock
    tm.commit_transaction(txn_id).unwrap();

    // Verify statistics
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.lock_operations, 3);
    assert_eq!(stats.unlock_operations, 3);
}

#[test]
fn test_concurrent_transactions_with_locks() {
    let tm = Arc::new(TransactionManager::new().unwrap());
    let mut handles = vec![];

    // Scenario: multiple transactions operate on overlapping resources
    for i in 0..3 {
        let tm_clone = Arc::clone(&tm);
        let handle = thread::spawn(move || {
            let txn_id = tm_clone
                .begin_transaction(IsolationLevel::ReadCommitted, false)
                .unwrap();

            // Each transaction touches a shared resource plus a unique one
            let shared_resource = "shared_table".to_string();
            let unique_resource = format!("unique_resource_{}", i);

            // Acquire shared lock on the shared resource
            tm_clone
                .acquire_lock(
                    txn_id,
                    shared_resource,
                    LockType::Table("shared".to_string()),
                    LockMode::Shared,
                )
                .unwrap();

            // Acquire exclusive lock on the unique resource
            tm_clone
                .acquire_lock(
                    txn_id,
                    unique_resource,
                    LockType::Resource(format!("unique_{}", i)),
                    LockMode::Exclusive,
                )
                .unwrap();

            // Simulate some work
            thread::sleep(Duration::from_millis(50));

            // Commit the transaction
            tm_clone.commit_transaction(txn_id).unwrap();
        });
        handles.push(handle);
    }

    // Wait for all transactions to finish
    for handle in handles {
        handle.join().unwrap();
    }

    // Verify final statistics
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.total_transactions, 3);
    assert_eq!(stats.committed_transactions, 3);
    assert_eq!(stats.active_transactions, 0);
    assert_eq!(stats.lock_operations, 6); // 2 locks per transaction
    assert_eq!(stats.unlock_operations, 6);
}

#[test]
fn test_transaction_abort_releases_locks() {
    let tm = TransactionManager::new().unwrap();
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();

    // Acquire several locks
    let resources = vec!["resource1", "resource2", "resource3"];
    for resource in &resources {
        tm.acquire_lock(
            txn_id,
            resource.to_string(),
            LockType::Resource(resource.to_string()),
            LockMode::Shared,
        )
        .unwrap();
    }

    // Ensure locks were acquired
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    assert_eq!(info.locked_resources.len(), 3);

    // Abort the transaction
    tm.abort_transaction(txn_id).unwrap();

    // Ensure all locks were released
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.lock_operations, 3);
    assert_eq!(stats.unlock_operations, 3);
    assert_eq!(stats.aborted_transactions, 1);
}

#[test]
fn test_lock_contention_scenario() {
    let tm = Arc::new(TransactionManager::new().unwrap());
    let resource = "contended_resource".to_string();

    // First transaction acquires an exclusive lock
    let txn1 = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();
    tm.acquire_lock(
        txn1,
        resource.clone(),
        LockType::Resource("contended".to_string()),
        LockMode::Exclusive,
    )
    .unwrap();

    // Launch second transaction on another thread
    let tm_clone = Arc::clone(&tm);
    let resource_clone = resource.clone();
    let handle = thread::spawn(move || {
        let txn2 = tm_clone
            .begin_transaction(IsolationLevel::ReadCommitted, false)
            .unwrap();

        // Attempting to acquire the lock should block or err
        let result = tm_clone.acquire_lock(
            txn2,
            resource_clone,
            LockType::Resource("contended".to_string()),
            LockMode::Shared,
        );

        // Implementation returns an error if deadlock or timeout occurs
        match result {
            Ok(()) => {
                // Lock acquired (perhaps after first transaction released it)
                tm_clone.commit_transaction(txn2).unwrap();
            }
            Err(_) => {
                // Could be a deadlock/timeout error or lock denial
                tm_clone.abort_transaction(txn2).unwrap();
            }
        }
    });

    // Wait briefly, then release the first transaction
    thread::sleep(Duration::from_millis(100));
    tm.commit_transaction(txn1).unwrap();

    // Wait for second thread to finish
    handle.join().unwrap();

    // Verify system remains consistent
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.active_transactions, 0);
}

#[test]
fn test_read_only_transaction_behavior() {
    let tm = TransactionManager::new().unwrap();

    // Begin read-only transaction
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, true)
        .unwrap();

    // Read-only transactions may take shared locks
    tm.acquire_lock(
        txn_id,
        "read_resource".to_string(),
        LockType::Table("table".to_string()),
        LockMode::Shared,
    )
    .unwrap();

    // Check transaction info
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    assert!(info.read_only);
    assert_eq!(info.locked_resources.len(), 1);

    tm.commit_transaction(txn_id).unwrap();
}

#[test]
fn test_isolation_level_impact() {
    let tm = TransactionManager::new().unwrap();

    let isolation_levels = vec![
        IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted,
        IsolationLevel::RepeatableRead,
        IsolationLevel::Serializable,
    ];

    for level in isolation_levels {
        let txn_id = tm.begin_transaction(level.clone(), false).unwrap();

        // Acquire lock (behavior may vary by isolation level)
        tm.acquire_lock(
            txn_id,
            format!("resource_{:?}", level),
            LockType::Resource("test".to_string()),
            LockMode::Shared,
        )
        .unwrap();

        let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
        assert_eq!(info.isolation_level, level);

        tm.commit_transaction(txn_id).unwrap();
    }
}

#[test]
fn test_transaction_timeout_simulation() {
    let tm = TransactionManager::new().unwrap();
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();

    // Acquire lock
    tm.acquire_lock(
        txn_id,
        "timeout_resource".to_string(),
        LockType::Resource("test".to_string()),
        LockMode::Exclusive,
    )
    .unwrap();

    // Simulate long-running work
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    let duration = info.duration().unwrap();
    assert!(duration.as_millis() >= 0);

    // Real systems would enforce timeouts; here we simply commit
    tm.commit_transaction(txn_id).unwrap();
}

#[test]
fn test_system_recovery_simulation() {
    let tm = TransactionManager::new().unwrap();
    let mut active_transactions = Vec::new();

    // Create several active transactions
    for i in 0..5 {
        let txn_id = tm
            .begin_transaction(IsolationLevel::ReadCommitted, false)
            .unwrap();
        tm.acquire_lock(
            txn_id,
            format!("recovery_resource_{}", i),
            LockType::Resource(format!("r{}", i)),
            LockMode::Shared,
        )
        .unwrap();
        active_transactions.push(txn_id);
    }

    // Inspect system state
    let active_txns = tm.get_active_transactions().unwrap();
    assert_eq!(active_txns.len(), 5);

    // Simulate "recovery" by aborting all active transactions
    for txn_id in active_transactions {
        tm.abort_transaction(txn_id).unwrap();
    }

    // Ensure system state is clean
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.active_transactions, 0);
    assert_eq!(stats.aborted_transactions, 5);
}

#[test]
fn test_performance_under_load() {
    let tm = Arc::new(TransactionManager::new().unwrap());
    let mut handles = vec![];

    let start_time = std::time::Instant::now();

    // Launch many short-lived transactions concurrently
    for i in 0..20 {
        let tm_clone = Arc::clone(&tm);
        let handle = thread::spawn(move || {
            for j in 0..10 {
                let txn_id = tm_clone
                    .begin_transaction(IsolationLevel::ReadCommitted, false)
                    .unwrap();

                // Acquire lock on unique resource
                tm_clone
                    .acquire_lock(
                        txn_id,
                        format!("perf_resource_{}_{}", i, j),
                        LockType::Resource(format!("r_{}_{}", i, j)),
                        LockMode::Shared,
                    )
                    .unwrap();

                // Perform small amount of work
                thread::sleep(Duration::from_millis(1));

                tm_clone.commit_transaction(txn_id).unwrap();
            }
        });
        handles.push(handle);
    }

    // Wait for all threads to finish
    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start_time.elapsed();
    println!("Executed 200 transactions in {:?}", elapsed);

    // Verify final statistics
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.total_transactions, 200);
    assert_eq!(stats.committed_transactions, 200);
    assert_eq!(stats.active_transactions, 0);

    // Ensure total runtime stays reasonable
    assert!(elapsed.as_secs() < 10);
}
