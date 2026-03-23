//! An example of using a comprehensive competition management system

use rustdb::core::concurrency::IsolationLevel as ConcIsolationLevel;
use rustdb::core::{
    ConcurrencyConfig, ConcurrencyManager, LockGranularity, ResourceType, RowKey, Timestamp,
    TransactionId,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== A comprehensive example of competitiveness management ===\n");

    // 1. Creating a manager with settings
    println!("1. Creation of a competitiveness manager");
    let config = ConcurrencyConfig {
        default_isolation_level: ConcIsolationLevel::ReadCommitted,
        default_lock_granularity: LockGranularity::Row,
        enable_mvcc: true,
        vacuum_interval: Duration::from_secs(30),
        ..Default::default()
    };
    let manager = ConcurrencyManager::new(config);
    println!("✓ The manager was created with MVCC and Row-level locks\n");

    // 2. MVCC Demo - Multiple Versions
    println!("2. Create multiple versions of a post");
    let key = RowKey::new(1, 100);

    // Transaction 1: creates the first version
    let tx1 = TransactionId::new(1);
    let snapshot1 = manager.begin_transaction(tx1, ConcIsolationLevel::ReadCommitted)?;
    println!("Transaction {} started", tx1);

    let data_v1 = b"Alice, age: 25, salary: 50000".to_vec();
    manager.write(tx1, key.clone(), data_v1).await?;
    println!("✓ Version 1 was created by transaction {}", tx1);

    manager.commit_transaction(tx1)?;
    println!("✓ Transaction {} is committed\n", tx1);

    // Transaction 2: creates a second version
    let tx2 = TransactionId::new(2);
    manager.begin_transaction(tx2, ConcIsolationLevel::ReadCommitted)?;
    println!("Transaction {} started", tx2);

    let data_v2 = b"Alice, age: 26, salary: 55000".to_vec();
    manager.write(tx2, key.clone(), data_v2).await?;
    println!("✓ Version 2 created by transaction {}", tx2);

    manager.commit_transaction(tx2)?;
    println!("✓ Transaction {} is committed\n", tx2);

    // 3. Demonstration of transaction isolation
    println!("3. Demonstration of isolation");

    // Transaction 3 reads from snapshot before update
    let tx3 = TransactionId::new(3);
    manager.begin_transaction(tx3, ConcIsolationLevel::ReadCommitted)?;
    let old_data = manager.read(tx3, &key, snapshot1).await?;
    if let Some(data) = old_data {
        println!(
            "TX3 reads old version: {:?}",
            String::from_utf8_lossy(&data)
        );
    }

    // Transaction 4 reads with new snapshot
    let tx4 = TransactionId::new(4);
    let snapshot2 = Timestamp::now();
    manager.begin_transaction(tx4, ConcIsolationLevel::ReadCommitted)?;
    let new_data = manager.read(tx4, &key, snapshot2).await?;
    if let Some(data) = new_data {
        println!(
            "TX4 reads new version: {:?}",
            String::from_utf8_lossy(&data)
        );
    }
    println!();

    // 4. Demonstration of blocking
    println!("4. Demonstration of blocking");
    let tx5 = TransactionId::new(5);
    let resource = ResourceType::Record(1, 200);

    // We get an exclusive lock
    manager
        .acquire_write_lock(tx5, resource.clone(), Some(Duration::from_millis(100)))
        .await?;
    println!("✓ Transaction {} received an exclusive lock", tx5);

    // Transaction 6 tries to acquire the same lock
    let tx6 = TransactionId::new(6);
    match manager
        .acquire_write_lock(tx6, resource.clone(), Some(Duration::from_millis(10)))
        .await
    {
        Ok(_) => println!("Transaction {} acquired a lock", tx6),
        Err(_) => println!(
            "✓ Transaction {} failed to acquire a lock (timed out)",
            tx6
        ),
    }

    // Release the lock
    manager.commit_transaction(tx5)?;
    println!("✓ Transaction {} released the lock\n", tx5);

    // 5. Demonstration of transaction rollback
    println!("5. Demonstration of transaction rollback");
    let tx7 = TransactionId::new(7);
    let key2 = RowKey::new(1, 101);
    let data_tx7 = b"Bob, age: 30".to_vec();

    manager.begin_transaction(tx7, ConcIsolationLevel::ReadCommitted)?;
    manager.write(tx7, key2.clone(), data_tx7).await?;
    println!("Transaction {} created a version", tx7);

    manager.abort_transaction(tx7)?;
    println!("✓ Transaction {} rolled back (version deleted)\n", tx7);

    // 6. Statistics
    println!("6. System statistics");
    let lock_stats = manager.get_lock_statistics();
    println!("Locks:");
    println!("Total locks: {}", lock_stats.total_locks);
    println!("Timeouts: {}", lock_stats.lock_timeouts);
    println!(
        "Deadlocks detected: {}",
        lock_stats.deadlocks_detected
    );

    let mvcc_stats = manager.get_mvcc_statistics();
    println!("\n   MVCC:");
    println!("Total versions: {}", mvcc_stats.total_versions);
    println!("Active: {}", mvcc_stats.active_versions);
    println!("Fixed: {}", mvcc_stats.committed_versions);
    println!("Recovered: {}", mvcc_stats.aborted_versions);

    // 7. Cleaning up old versions (VACUUM)
    println!("\n7. Cleaning up old versions (VACUUM)");
    manager.update_min_active_transaction(TransactionId::new(100));
    let cleaned = manager.vacuum()?;
    println!("✓ Versions cleared: {}", cleaned);

    let mvcc_stats = manager.get_mvcc_statistics();
    println!("Versions after cleanup: {}", mvcc_stats.total_versions);
    println!("VACUUM operations: {}", mvcc_stats.vacuum_operations);

    // 8. Demonstration of different levels of insulation
    println!("\n8. Insulation levels");
    println!("Supported levels:");
    println!("- ReadUncommitted (minimal isolation)");
    println!("- ReadCommitted (default)");
    println!("- RepeatableRead (repeatable reading)");
    println!("- Serializable (complete isolation)");

    // 9. Demonstration of lock granularity
    println!("\n9. Lock granularity");
    println!("Supported levels:");
    println!("- Database (entire database)");
    println!("- Table");
    println!("- Page");
    println!("- Row [current mode]");

    println!("\n=== Example completed successfully ===");
    println!("\n📝 Key features:");
    println!("✓ MVCC for transaction isolation without read locks");
    println!("✓ Deadlock detection with automatic victim selection");
    println!("✓ Timeout mechanisms with automatic rollback");
    println!("✓ Granular locks (Row/Page/Table/Database)");
    println!("✓ VACUUM to clean old versions");
    println!("✓ Supports all standard isolation levels");

    Ok(())
}
