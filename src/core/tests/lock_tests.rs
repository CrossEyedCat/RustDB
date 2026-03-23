//! Tests for rustdb lock manager

use crate::common::Result;
use crate::core::{LockManager, LockMode, LockType, TransactionId};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn test_lock_manager_creation() {
    let lm = LockManager::new().unwrap();
    let stats = lm.get_statistics().unwrap();

    assert_eq!(stats.total_lock_requests, 0);
    assert_eq!(stats.locks_acquired, 0);
    assert_eq!(stats.active_locks, 0);
}

#[test]
fn test_shared_locks_compatibility() {
    let lm = LockManager::new().unwrap();
    let resource = "test_resource".to_string();

    let txn1 = TransactionId::new(1);
    let txn2 = TransactionId::new(2);

    // The first transaction acquires a shared lock
    let acquired1 = lm
        .acquire_lock(
            txn1,
            resource.clone(),
            LockType::Resource("test".to_string()),
            LockMode::Shared,
        )
        .unwrap();
    assert!(acquired1);

    // The second transaction can also acquire a shared lock.
    let acquired2 = lm
        .acquire_lock(
            txn2,
            resource.clone(),
            LockType::Resource("test".to_string()),
            LockMode::Shared,
        )
        .unwrap();
    assert!(acquired2);

    // Checking the statistics
    let stats = lm.get_statistics().unwrap();
    assert_eq!(stats.locks_acquired, 2);
    assert_eq!(stats.active_locks, 2);

    // Freeing the locks
    lm.release_lock(txn1, resource.clone()).unwrap();
    lm.release_lock(txn2, resource.clone()).unwrap();

    let stats = lm.get_statistics().unwrap();
    assert_eq!(stats.locks_released, 2);
    assert_eq!(stats.active_locks, 0);
}

#[test]
fn test_exclusive_lock_incompatibility() {
    let lm = LockManager::new().unwrap();
    let resource = "exclusive_resource".to_string();

    let txn1 = TransactionId::new(1);
    let txn2 = TransactionId::new(2);

    // The first transaction receives an exclusive lock
    let acquired1 = lm
        .acquire_lock(
            txn1,
            resource.clone(),
            LockType::Resource("test".to_string()),
            LockMode::Exclusive,
        )
        .unwrap();
    assert!(acquired1);

    // Second transaction cannot acquire shared lock
    let acquired2 = lm
        .acquire_lock(
            txn2,
            resource.clone(),
            LockType::Resource("test".to_string()),
            LockMode::Shared,
        )
        .unwrap();
    assert!(!acquired2);

    // Checking the statistics
    let stats = lm.get_statistics().unwrap();
    assert_eq!(stats.locks_acquired, 1);
    assert_eq!(stats.blocked_requests, 1);
    assert_eq!(stats.waiting_requests, 1);

    // Release the lock on the first transaction
    lm.release_lock(txn1, resource.clone()).unwrap();

    // Now the second transaction should acquire the lock automatically
    // (this happens in process_wait_queue)

    // Release the second lock
    lm.release_lock(txn2, resource).unwrap();
}

#[test]
fn test_different_lock_types() {
    let lm = LockManager::new().unwrap();
    let txn_id = TransactionId::new(1);

    let lock_types = vec![
        (LockType::Page(123), "page_123"),
        (LockType::Table("users".to_string()), "table_users"),
        (LockType::Record(456, 789), "record_456_789"),
        (LockType::Index("idx_email".to_string()), "index_idx_email"),
        (LockType::Resource("custom".to_string()), "resource_custom"),
    ];

    // Getting different types of locks
    for (lock_type, resource) in &lock_types {
        let acquired = lm
            .acquire_lock(
                txn_id,
                resource.to_string(),
                lock_type.clone(),
                LockMode::Shared,
            )
            .unwrap();
        assert!(acquired);
    }

    let stats = lm.get_statistics().unwrap();
    assert_eq!(stats.locks_acquired, lock_types.len() as u64);

    // Release all locks
    for (_, resource) in &lock_types {
        lm.release_lock(txn_id, resource.to_string()).unwrap();
    }

    let stats = lm.get_statistics().unwrap();
    assert_eq!(stats.locks_released, lock_types.len() as u64);
}

#[test]
fn test_lock_upgrade() {
    let lm = LockManager::new().unwrap();
    let resource = "upgrade_resource".to_string();
    let txn_id = TransactionId::new(1);

    // Getting a shared lock
    let acquired1 = lm
        .acquire_lock(
            txn_id,
            resource.clone(),
            LockType::Resource("test".to_string()),
            LockMode::Shared,
        )
        .unwrap();
    assert!(acquired1);

    // Trying to acquire an exclusive lock with the same transaction
    // This should work as an upgrade if there are no other locks
    let acquired2 = lm
        .acquire_lock(
            txn_id,
            resource.clone(),
            LockType::Resource("test".to_string()),
            LockMode::Exclusive,
        )
        .unwrap();
    assert!(acquired2);

    lm.release_lock(txn_id, resource).unwrap();
}

#[test]
fn test_same_transaction_multiple_requests() {
    let lm = LockManager::new().unwrap();
    let resource = "same_txn_resource".to_string();
    let txn_id = TransactionId::new(1);

    // First request
    let acquired1 = lm
        .acquire_lock(
            txn_id,
            resource.clone(),
            LockType::Resource("test".to_string()),
            LockMode::Shared,
        )
        .unwrap();
    assert!(acquired1);

    // Re-requesting the same transaction for the same mode
    let acquired2 = lm
        .acquire_lock(
            txn_id,
            resource.clone(),
            LockType::Resource("test".to_string()),
            LockMode::Shared,
        )
        .unwrap();
    assert!(acquired2);

    lm.release_lock(txn_id, resource).unwrap();
}

#[test]
fn test_deadlock_detection_simple() {
    let lm = LockManager::new().unwrap();

    let resource1 = "resource1".to_string();
    let resource2 = "resource2".to_string();
    let txn1 = TransactionId::new(1);
    let txn2 = TransactionId::new(2);

    // Transaction 1 acquires a lock on resource 1
    let acquired = lm
        .acquire_lock(
            txn1,
            resource1.clone(),
            LockType::Resource("r1".to_string()),
            LockMode::Exclusive,
        )
        .unwrap();
    assert!(acquired);

    // Transaction 2 acquires a lock on resource 2
    let acquired = lm
        .acquire_lock(
            txn2,
            resource2.clone(),
            LockType::Resource("r2".to_string()),
            LockMode::Exclusive,
        )
        .unwrap();
    assert!(acquired);

    // Transaction 1 tries to acquire a lock on resource 2
    let acquired = lm
        .acquire_lock(
            txn1,
            resource2.clone(),
            LockType::Resource("r2".to_string()),
            LockMode::Exclusive,
        )
        .unwrap();
    assert!(!acquired);

    // Transaction 2 tries to acquire a lock on resource 1
    // This should trigger deadlock detection
    let result = lm.acquire_lock(
        txn2,
        resource1.clone(),
        LockType::Resource("r1".to_string()),
        LockMode::Exclusive,
    );

    // Expecting a deadlock error
    assert!(result.is_err());

    // Cleaning
    lm.release_lock(txn1, resource1).unwrap();
    lm.release_lock(txn2, resource2).unwrap();
}

#[test]
fn test_wait_queue_processing() {
    let lm = LockManager::new().unwrap();
    let resource = "wait_queue_resource".to_string();

    let txn1 = TransactionId::new(1);
    let txn2 = TransactionId::new(2);
    let txn3 = TransactionId::new(3);

    // Transaction 1 acquires an exclusive lock
    let acquired = lm
        .acquire_lock(
            txn1,
            resource.clone(),
            LockType::Resource("test".to_string()),
            LockMode::Exclusive,
        )
        .unwrap();
    assert!(acquired);

    // Transactions 2 and 3 are added to the waiting queue
    let acquired2 = lm
        .acquire_lock(
            txn2,
            resource.clone(),
            LockType::Resource("test".to_string()),
            LockMode::Shared,
        )
        .unwrap();
    assert!(!acquired2);

    let acquired3 = lm
        .acquire_lock(
            txn3,
            resource.clone(),
            LockType::Resource("test".to_string()),
            LockMode::Shared,
        )
        .unwrap();
    assert!(!acquired3);

    // Checking the statistics
    let stats = lm.get_statistics().unwrap();
    assert_eq!(stats.waiting_requests, 2);

    // Release the lock on transaction 1
    lm.release_lock(txn1, resource.clone()).unwrap();

    // Transactions 2 and 3 should automatically acquire locks
    // (since they are compatible with each other)

    // Freeing the remaining locks
    lm.release_lock(txn2, resource.clone()).unwrap();
    lm.release_lock(txn3, resource).unwrap();
}

#[test]
fn test_lock_manager_statistics() {
    let lm = LockManager::new().unwrap();

    // Initial statistics
    let stats = lm.get_statistics().unwrap();
    assert_eq!(stats.total_lock_requests, 0);
    assert_eq!(stats.locks_acquired, 0);
    assert_eq!(stats.locks_released, 0);
    assert_eq!(stats.active_locks, 0);
    assert_eq!(stats.blocked_requests, 0);
    assert_eq!(stats.deadlocks_detected, 0);

    let txn1 = TransactionId::new(1);
    let txn2 = TransactionId::new(2);
    let resource = "stats_resource".to_string();

    // We carry out operations
    lm.acquire_lock(
        txn1,
        resource.clone(),
        LockType::Resource("test".to_string()),
        LockMode::Exclusive,
    )
    .unwrap();
    lm.acquire_lock(
        txn2,
        resource.clone(),
        LockType::Resource("test".to_string()),
        LockMode::Shared,
    )
    .unwrap();

    let stats = lm.get_statistics().unwrap();
    assert_eq!(stats.total_lock_requests, 2);
    assert_eq!(stats.locks_acquired, 1);
    assert_eq!(stats.blocked_requests, 1);
    assert_eq!(stats.active_locks, 1);

    // Freeing the locks
    lm.release_lock(txn1, resource.clone()).unwrap();
    lm.release_lock(txn2, resource).unwrap();

    let stats = lm.get_statistics().unwrap();
    assert_eq!(stats.locks_released, 2);
    assert_eq!(stats.active_locks, 0);
}

#[test]
fn test_concurrent_lock_operations() {
    let lm = Arc::new(LockManager::new().unwrap());
    let mut handles = vec![];

    // We launch several threads, each working with its own resource
    for i in 0..4 {
        let lm_clone = Arc::clone(&lm);
        let handle = thread::spawn(move || {
            let txn_id = TransactionId::new(i as u64 + 1);
            let resource = format!("concurrent_resource_{}", i);

            // We get blocked
            let acquired = lm_clone
                .acquire_lock(
                    txn_id,
                    resource.clone(),
                    LockType::Resource(format!("r{}", i)),
                    LockMode::Exclusive,
                )
                .unwrap();
            assert!(acquired);

            // Slight delay
            thread::sleep(Duration::from_millis(10));

            // Release the lock
            lm_clone.release_lock(txn_id, resource).unwrap();
        });
        handles.push(handle);
    }

    // Waiting for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    // Checking the final statistics
    let stats = lm.get_statistics().unwrap();
    assert_eq!(stats.locks_acquired, 4);
    assert_eq!(stats.locks_released, 4);
    assert_eq!(stats.active_locks, 0);
}

#[test]
fn test_lock_type_display() {
    let lock_types = vec![
        LockType::Page(123),
        LockType::Table("users".to_string()),
        LockType::Record(456, 789),
        LockType::Index("idx_email".to_string()),
        LockType::Resource("custom".to_string()),
    ];

    for lock_type in lock_types {
        let display = format!("{}", lock_type);
        assert!(!display.is_empty());
    }
}

#[test]
fn test_lock_mode_compatibility() {
    // Testing the compatibility logic of blocking modes
    assert!(LockMode::Shared.is_compatible(&LockMode::Shared));
    assert!(!LockMode::Shared.is_compatible(&LockMode::Exclusive));
    assert!(!LockMode::Exclusive.is_compatible(&LockMode::Shared));
    assert!(!LockMode::Exclusive.is_compatible(&LockMode::Exclusive));
}

#[test]
fn test_active_locks_inspection() {
    let lm = LockManager::new().unwrap();
    let txn_id = TransactionId::new(1);
    let resource = "inspect_resource".to_string();

    // We get blocked
    lm.acquire_lock(
        txn_id,
        resource.clone(),
        LockType::Resource("test".to_string()),
        LockMode::Shared,
    )
    .unwrap();

    // Checking active locks
    let active_locks = lm.get_active_locks().unwrap();
    assert!(active_locks.contains_key(&resource));

    let locks = &active_locks[&resource];
    assert_eq!(locks.len(), 1);
    assert_eq!(locks[0].transaction_id, txn_id);

    lm.release_lock(txn_id, resource).unwrap();

    let active_locks = lm.get_active_locks().unwrap();
    assert!(!active_locks.contains_key("inspect_resource"));
}

#[test]
fn test_waiting_requests_inspection() {
    let lm = LockManager::new().unwrap();
    let resource = "waiting_resource".to_string();

    let txn1 = TransactionId::new(1);
    let txn2 = TransactionId::new(2);

    // The first transaction receives an exclusive lock
    lm.acquire_lock(
        txn1,
        resource.clone(),
        LockType::Resource("test".to_string()),
        LockMode::Exclusive,
    )
    .unwrap();

    // The second transaction is added to the waiting queue
    lm.acquire_lock(
        txn2,
        resource.clone(),
        LockType::Resource("test".to_string()),
        LockMode::Shared,
    )
    .unwrap();

    // Checking the waiting queue
    let waiting_requests = lm.get_waiting_requests().unwrap();
    assert!(waiting_requests.contains_key(&resource));

    let requests = &waiting_requests[&resource];
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].transaction_id, txn2);

    // Cleaning
    lm.release_lock(txn1, resource.clone()).unwrap();
    lm.release_lock(txn2, resource).unwrap();
}
