//! Tests for the RustDB transaction manager

use crate::common::Result;
use crate::core::{
    IsolationLevel, LockMode, LockType, TransactionId, TransactionManager,
    TransactionManagerConfig, TransactionState,
};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn test_transaction_manager_creation() {
    let tm = TransactionManager::new().unwrap();
    let config = tm.get_config();

    assert_eq!(config.max_concurrent_transactions, 1000);
    assert_eq!(config.lock_timeout_ms, 30000);
    assert!(config.enable_deadlock_detection);

    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.total_transactions, 0);
    assert_eq!(stats.active_transactions, 0);
}

#[test]
fn test_transaction_lifecycle() {
    let tm = TransactionManager::new().unwrap();

    // Start a transaction
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();

    // Verify the transaction was created
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    assert_eq!(info.id, txn_id);
    assert_eq!(info.state, TransactionState::Active);
    assert_eq!(info.isolation_level, IsolationLevel::ReadCommitted);
    assert!(!info.read_only);

    // Check statistics
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.total_transactions, 1);
    assert_eq!(stats.active_transactions, 1);

    // Commit the transaction
    tm.commit_transaction(txn_id).unwrap();

    // Ensure the transaction is removed from the active set
    assert!(tm.get_transaction_info(txn_id).unwrap().is_none());

    // Check statistics
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.total_transactions, 1);
    assert_eq!(stats.active_transactions, 0);
    assert_eq!(stats.committed_transactions, 1);
}

#[test]
fn test_transaction_abort() {
    let tm = TransactionManager::new().unwrap();

    let txn_id = tm
        .begin_transaction(IsolationLevel::Serializable, true)
        .unwrap();

    // Abort the transaction
    tm.abort_transaction(txn_id).unwrap();

    // Check statistics
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.total_transactions, 1);
    assert_eq!(stats.active_transactions, 0);
    assert_eq!(stats.aborted_transactions, 1);
}

#[test]
fn test_multiple_transactions() {
    let tm = TransactionManager::new().unwrap();
    let mut txn_ids = Vec::new();

    // Create multiple transactions
    for i in 0..5 {
        let read_only = i % 2 == 0;
        let isolation = if i % 2 == 0 {
            IsolationLevel::ReadCommitted
        } else {
            IsolationLevel::Serializable
        };
        let txn_id = tm.begin_transaction(isolation, read_only).unwrap();
        txn_ids.push(txn_id);
    }

    // Ensure they are all active
    let active_txns = tm.get_active_transactions().unwrap();
    assert_eq!(active_txns.len(), 5);

    // Commit even-indexed transactions and abort odd ones
    for (i, &txn_id) in txn_ids.iter().enumerate() {
        if i % 2 == 0 {
            tm.commit_transaction(txn_id).unwrap();
        } else {
            tm.abort_transaction(txn_id).unwrap();
        }
    }

    // Check statistics
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.total_transactions, 5);
    assert_eq!(stats.active_transactions, 0);
    assert_eq!(stats.committed_transactions, 3); // 0, 2, 4
    assert_eq!(stats.aborted_transactions, 2); // 1, 3
}

#[test]
fn test_transaction_limit() {
    let config = TransactionManagerConfig {
        max_concurrent_transactions: 2,
        ..Default::default()
    };
    let tm = TransactionManager::with_config(config).unwrap();

    // Create the maximum number of transactions
    let txn1 = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();
    let txn2 = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();

    // Attempting to create a third should fail
    let result = tm.begin_transaction(IsolationLevel::ReadCommitted, false);
    assert!(result.is_err());

    // Release one transaction
    tm.commit_transaction(txn1).unwrap();

    // Now we can create a new one
    let txn3 = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();

    // Clean up
    tm.abort_transaction(txn2).unwrap();
    tm.commit_transaction(txn3).unwrap();
}

#[test]
fn test_lock_operations() {
    let tm = TransactionManager::new().unwrap();
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();

    // Acquire a shared lock
    let resource = "table_users".to_string();
    tm.acquire_lock(
        txn_id,
        resource.clone(),
        LockType::Table("users".to_string()),
        LockMode::Shared,
    )
    .unwrap();

    // Ensure the resource is locked
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    assert!(info.locked_resources.contains(&resource));

    // Release the lock
    tm.release_lock(txn_id, resource.clone()).unwrap();

    // Ensure the resource is released
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    assert!(!info.locked_resources.contains(&resource));

    tm.commit_transaction(txn_id).unwrap();
}

#[test]
fn test_multiple_locks() {
    let tm = TransactionManager::new().unwrap();
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();

    let resources = vec![
        "table_users".to_string(),
        "table_orders".to_string(),
        "index_user_email".to_string(),
    ];

    // Acquire locks on all resources
    for resource in &resources {
        tm.acquire_lock(
            txn_id,
            resource.clone(),
            LockType::Resource(resource.clone()),
            LockMode::Shared,
        )
        .unwrap();
    }

    // Ensure all resources are locked
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    assert_eq!(info.locked_resources.len(), 3);
    for resource in &resources {
        assert!(info.locked_resources.contains(resource));
    }

    // Committing should automatically release all locks
    tm.commit_transaction(txn_id).unwrap();

    // Check lock statistics
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.lock_operations, 3);
    assert_eq!(stats.unlock_operations, 3);
}

#[test]
fn test_transaction_states() {
    let tm = TransactionManager::new().unwrap();
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();

    // Initially active
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    assert_eq!(info.state, TransactionState::Active);

    // Attempting to commit an inactive transaction should fail
    // (though we have no way to change the state externally)

    tm.commit_transaction(txn_id).unwrap();
}

#[test]
fn test_invalid_operations() {
    let tm = TransactionManager::new().unwrap();
    let invalid_txn_id = TransactionId::new(999999);

    // Operations with a nonexistent transaction
    assert!(tm.commit_transaction(invalid_txn_id).is_err());
    assert!(tm.abort_transaction(invalid_txn_id).is_err());
    assert!(tm.get_transaction_info(invalid_txn_id).unwrap().is_none());

    // Attempt to acquire a lock for a nonexistent transaction
    let result = tm.acquire_lock(
        invalid_txn_id,
        "resource".to_string(),
        LockType::Resource("resource".to_string()),
        LockMode::Shared,
    );
    assert!(result.is_err());
}

#[test]
fn test_transaction_duration() {
    let tm = TransactionManager::new().unwrap();
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();

    // Small delay
    thread::sleep(Duration::from_millis(20));

    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    let duration = info.duration().unwrap();
    assert!(duration.as_millis() >= 1); // Minimal requirement: just confirm time advances

    tm.commit_transaction(txn_id).unwrap();
}

#[test]
fn test_concurrent_transactions() {
    let tm = Arc::new(TransactionManager::new().unwrap());
    let mut handles = vec![];

    // Spawn several threads with transactions
    for i in 0..4 {
        let tm_clone = Arc::clone(&tm);
        let handle = thread::spawn(move || {
            let txn_id = tm_clone
                .begin_transaction(IsolationLevel::ReadCommitted, false)
                .unwrap();

            // Acquire a lock on a unique resource
            let resource = format!("resource_{}", i);
            tm_clone
                .acquire_lock(
                    txn_id,
                    resource,
                    LockType::Resource(format!("resource_{}", i)),
                    LockMode::Exclusive,
                )
                .unwrap();

            // Small delay
            thread::sleep(Duration::from_millis(50));

            tm_clone.commit_transaction(txn_id).unwrap();
        });
        handles.push(handle);
    }

    // Wait for all threads to finish
    for handle in handles {
        handle.join().unwrap();
    }

    // Check final statistics
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.total_transactions, 4);
    assert_eq!(stats.active_transactions, 0);
    assert_eq!(stats.committed_transactions, 4);
    assert_eq!(stats.lock_operations, 4);
    assert_eq!(stats.unlock_operations, 4);
}

#[test]
fn test_isolation_levels() {
    let tm = TransactionManager::new().unwrap();

    let levels = [
        IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted,
        IsolationLevel::RepeatableRead,
        IsolationLevel::Serializable,
    ];

    for level in &levels {
        let txn_id = tm.begin_transaction(level.clone(), false).unwrap();
        let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
        assert_eq!(info.isolation_level, *level);
        tm.commit_transaction(txn_id).unwrap();
    }
}

#[test]
fn test_read_only_transactions() {
    let tm = TransactionManager::new().unwrap();

    // Read-only transaction
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, true)
        .unwrap();
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    assert!(info.read_only);

    tm.commit_transaction(txn_id).unwrap();

    // Regular transaction
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    assert!(!info.read_only);

    tm.commit_transaction(txn_id).unwrap();
}

#[test]
fn test_transaction_manager_statistics() {
    let tm = TransactionManager::new().unwrap();

    // Initial statistics
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.total_transactions, 0);
    assert_eq!(stats.active_transactions, 0);
    assert_eq!(stats.committed_transactions, 0);
    assert_eq!(stats.aborted_transactions, 0);
    assert_eq!(stats.lock_operations, 0);
    assert_eq!(stats.unlock_operations, 0);

    // Execute operations
    let txn1 = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();
    let txn2 = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();

    tm.acquire_lock(
        txn1,
        "resource1".to_string(),
        LockType::Resource("resource1".to_string()),
        LockMode::Shared,
    )
    .unwrap();
    tm.acquire_lock(
        txn2,
        "resource2".to_string(),
        LockType::Resource("resource2".to_string()),
        LockMode::Exclusive,
    )
    .unwrap();

    tm.commit_transaction(txn1).unwrap();
    tm.abort_transaction(txn2).unwrap();

    // Check final statistics
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.total_transactions, 2);
    assert_eq!(stats.active_transactions, 0);
    assert_eq!(stats.committed_transactions, 1);
    assert_eq!(stats.aborted_transactions, 1);
    assert_eq!(stats.lock_operations, 2);
    assert_eq!(stats.unlock_operations, 2);
}
