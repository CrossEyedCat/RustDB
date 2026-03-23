//! Example of using MVCC (Multi-Version Concurrency Control)

use rustdb::core::{MVCCManager, RowKey, Timestamp, TransactionId};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Example of MVCC system operation ===\n");

    // 1. Creating an MVCC manager
    println!("1. Creating an MVCC manager");
    let mvcc = MVCCManager::new();
    println!("✓ MVCC manager created\n");

    // 2. Creating post versions
    println!("2. Creating post versions");
    let key = RowKey::new(1, 100);
    let tx1 = TransactionId::new(1);

    let data_v1 = b"Alice, age: 25".to_vec();
    let version1 = mvcc.create_version(key.clone(), tx1, data_v1.clone())?;
    println!("✓ Version {} created by transaction {}", version1, tx1);

    // 3. Fixing the transaction
    println!("\n3. Committing a transaction");
    mvcc.commit_transaction(tx1)?;
    println!("✓ Transaction {} is committed", tx1);

    // 4. Read version
    println!("\n4. Read version");
    let snapshot = Timestamp::now();
    let read_data = mvcc.read_version(&key, tx1, snapshot)?;
    if let Some(data) = read_data {
        println!("✓ Read: {:?}", String::from_utf8_lossy(&data));
    }

    // 5. Updating a record (creating a new version)
    println!("\n5. Updating a record (creating a new version)");
    let tx2 = TransactionId::new(2);
    let data_v2 = b"Alice, age: 26".to_vec();
    let version2 = mvcc.create_version(key.clone(), tx2, data_v2.clone())?;
    println!("✓ Version {} created by transaction {}", version2, tx2);

    // 6. Demonstration of isolation
    println!("\n6. Transaction isolation demonstration");
    println!("Transaction 3 reads data (sees old version):");
    let tx3 = TransactionId::new(3);
    let old_snapshot = snapshot;
    let old_data = mvcc.read_version(&key, tx3, old_snapshot)?;
    if let Some(data) = old_data {
        println!("✓ Old version: {:?}", String::from_utf8_lossy(&data));
    }

    println!("\n We commit transaction 2:");
    mvcc.commit_transaction(tx2)?;
    println!("✓ Transaction {} is committed", tx2);

    println!("\nTransaction 4 reads the data (sees the new version):");
    let tx4 = TransactionId::new(4);
    let new_snapshot = Timestamp::now();
    let new_data = mvcc.read_version(&key, tx4, new_snapshot)?;
    if let Some(data) = new_data {
        println!("✓ New version: {:?}", String::from_utf8_lossy(&data));
    }

    // 7. Statistics
    println!("\n7. MVCC Statistics");
    let stats = mvcc.get_statistics();
    println!("Total versions: {}", stats.total_versions);
    println!("Active: {}", stats.active_versions);
    println!("Fixed: {}", stats.committed_versions);
    println!(
        "Number of versions to record: {}",
        mvcc.get_version_count(&key)
    );

    // 8. Delete an entry
    println!("\n8. Deleting an entry");
    let tx5 = TransactionId::new(5);
    mvcc.delete_version(&key, tx5)?;
    println!("✓ The record is marked for deletion by transaction {}", tx5);

    let stats = mvcc.get_statistics();
    println!("Marked for deletion: {}", stats.marked_for_deletion);

    // 9. Rolling back a transaction
    println!("\n9. Rolling back a transaction");
    let tx6 = TransactionId::new(6);
    let data_v3 = b"Bob, age: 30".to_vec();
    mvcc.create_version(RowKey::new(1, 101), tx6, data_v3)?;
    println!("Version created by transaction {}", tx6);

    mvcc.abort_transaction(tx6)?;
    println!("✓ Transaction {} rolled back", tx6);

    let stats = mvcc.get_statistics();
    println!("Versions rolled back: {}", stats.aborted_versions);

    // 10. Cleaning (VACUUM)
    println!("\n10. Cleaning up old versions (VACUUM)");
    mvcc.update_min_active_transaction(TransactionId::new(100));
    let cleaned = mvcc.vacuum()?;
    println!("✓ Versions cleared: {}", cleaned);

    let stats = mvcc.get_statistics();
    println!("Total versions after cleanup: {}", stats.total_versions);
    println!("VACUUM operations: {}", stats.vacuum_operations);
    println!("Total cleared: {}", stats.versions_cleaned);

    // 11. Summary statistics
    println!("\n11. Summary statistics");
    println!("{:#?}", stats);

    println!("\n=== Example completed successfully ===");

    Ok(())
}
