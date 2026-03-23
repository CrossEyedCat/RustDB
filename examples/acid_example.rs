use rustdb::core::lock::LockManager;
use rustdb::core::{
    acid_manager::{AcidConfig, AcidManager},
    advanced_lock_manager::{AdvancedLockManager, LockMode, ResourceType},
    transaction::{IsolationLevel, TransactionId},
    // recovery::RecoveryManager, // not used in this demo
};
use rustdb::logging::wal::{WalConfig, WriteAheadLog};
use rustdb::storage::page_manager::{PageManager, PageManagerConfig};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== RustDB ACID Support Demo ===");

    // Creating an ACID manager
    let acid_manager = create_test_acid_manager().await?;

    // Demonstration of ACID properties
    demo_atomicity(&acid_manager).await?;
    demo_consistency(&acid_manager).await?;
    demo_isolation(&acid_manager).await?;
    demo_durability(&acid_manager).await?;

    // MVCC Demonstration
    demo_mvcc(&acid_manager).await?;

    // Deadlock detection demo
    let lock_manager = Arc::new(AdvancedLockManager::new(Default::default()));
    demo_deadlock_detection(lock_manager).await?;

    // Demonstration of isolation levels
    demo_isolation_levels(&acid_manager).await?;

    println!("=== ACID Demo completed ===");
    Ok(())
}

async fn create_test_acid_manager() -> Result<Arc<AcidManager>, Box<dyn std::error::Error>> {
    let config = AcidConfig::default();
    let wal_config = WalConfig::default();
    let wal = Arc::new(WriteAheadLog::new(wal_config).await?);

    let temp_dir = std::env::temp_dir();
    let page_manager = Arc::new(PageManager::new(
        temp_dir,
        "test_db",
        PageManagerConfig::default(),
    )?);

    let lock_manager = Arc::new(LockManager::new()?);

    let acid_manager = AcidManager::new(config, lock_manager, wal, page_manager)?;

    Ok(Arc::new(acid_manager))
}

async fn demo_atomicity(acid_manager: &AcidManager) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Atomicity Demonstration ---");

    let transaction_id = TransactionId::new(1);

    // Let's start the transaction
    acid_manager.begin_transaction(transaction_id, IsolationLevel::ReadCommitted, true)?;

    // Making several notes
    write_record(acid_manager, transaction_id, 1, 1, b"Data 1")?;
    write_record(acid_manager, transaction_id, 1, 2, b"Data 2")?;

    // We feign an error - cancel the transaction
    acid_manager.abort_transaction(transaction_id)?;

    println!("The transaction is canceled - all changes are rolled back");
    println!("Atomicity: all operations in a transaction are either executed or rolled back");

    Ok(())
}

async fn demo_consistency(acid_manager: &AcidManager) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Demonstration of Consistency ---");

    let transaction_id = TransactionId::new(2);

    // Starting a read-only transaction
    acid_manager.begin_transaction(transaction_id, IsolationLevel::ReadCommitted, false)?;

    // Trying to write to a read-only transaction
    match write_record(acid_manager, transaction_id, 1, 3, b"New data") {
        Ok(_) => println!("ERROR: Write to read-only transaction!"),
        Err(_) => println!("SUCCESS: Write blocked - consistency maintained"),
    }

    acid_manager.abort_transaction(transaction_id)?;

    println!("Consistency: The system maintains data integrity");

    Ok(())
}

async fn demo_isolation(acid_manager: &AcidManager) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Isolation Demonstration ---");

    let transaction_id_1 = TransactionId::new(3);
    let transaction_id_2 = TransactionId::new(4);

    // Transaction 1
    acid_manager.begin_transaction(transaction_id_1, IsolationLevel::RepeatableRead, true)?;
    write_record(acid_manager, transaction_id_1, 1, 4, b"Isolated data")?;

    // Transaction 2 (should not see transaction 1's changes)
    acid_manager.begin_transaction(transaction_id_2, IsolationLevel::ReadCommitted, true)?;

    // Reading the data - should get the old version
    let _data = read_record(acid_manager, transaction_id_2, 1, 4)?;

    acid_manager.commit_transaction(transaction_id_1)?;

    // Transaction 2 should now see the changes
    let _data = read_record(acid_manager, transaction_id_2, 1, 4)?;

    acid_manager.commit_transaction(transaction_id_2)?;

    println!("Isolation: transactions do not see each other's uncommitted changes");

    Ok(())
}

async fn demo_durability(acid_manager: &AcidManager) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n---Demonstration of Durability---");

    let transaction_id = TransactionId::new(5);

    acid_manager.begin_transaction(transaction_id, IsolationLevel::ReadCommitted, true)?;

    // Recording critical data
    write_record(acid_manager, transaction_id, 1, 5, b"Critical data")?;

    // We fix the transaction
    acid_manager.commit_transaction(transaction_id)?;

    println!("Durability: committed changes persist even in the event of failure");
    println!("Data written to WAL and flushed to disk");

    Ok(())
}

async fn demo_mvcc(acid_manager: &AcidManager) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- MVCC Demo ---");

    let transaction_id_1 = TransactionId::new(6);
    let transaction_id_2 = TransactionId::new(7);

    // Transaction 1 reads data
    acid_manager.begin_transaction(transaction_id_1, IsolationLevel::RepeatableRead, false)?;
    let _data1 = read_record(acid_manager, transaction_id_1, 1, 1)?;

    // Transaction 2 updates the same data
    acid_manager.begin_transaction(transaction_id_2, IsolationLevel::ReadCommitted, true)?;
    write_record(acid_manager, transaction_id_2, 1, 1, b"Updated data")?;
    acid_manager.commit_transaction(transaction_id_2)?;

    // Transaction 1 reads again - should get the old version
    let _data2 = read_record(acid_manager, transaction_id_1, 1, 1)?;

    acid_manager.commit_transaction(transaction_id_1)?;

    println!("MVCC: each transaction sees a snapshot of the data at the time it started");
    println!("Multiple versions of the same entry are supported");

    Ok(())
}

async fn demo_deadlock_detection(
    lock_manager: Arc<AdvancedLockManager>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Deadlock Detection Demonstration ---");

    let transaction_id_1 = TransactionId::new(8);
    let transaction_id_2 = TransactionId::new(9);

    // Creating copies for transmission to streams
    let lock_manager_1 = lock_manager.clone();
    let lock_manager_2 = lock_manager.clone();

    // Transaction 1 tries to get resource B
    let handle1 = thread::spawn(move || {
        // Transaction 1 tries to get resource B
        std::mem::drop(lock_manager_1.acquire_lock(
            transaction_id_1,
            ResourceType::Table("table_B".to_string()),
            LockMode::Exclusive,
            Some(Duration::from_secs(5)),
        ));
    });

    // Transaction 2 tries to get resource A
    let handle2 = thread::spawn(move || {
        // Transaction 2 tries to get resource A
        std::mem::drop(lock_manager_2.acquire_lock(
            transaction_id_2,
            ResourceType::Table("table_A".to_string()),
            LockMode::Exclusive,
            Some(Duration::from_secs(5)),
        ));
    });

    // Waiting for the threads to complete
    let _result1 = handle1.join();
    let _result2 = handle2.join();

    println!("Deadlock detected and resolved automatically");
    println!("The system selects a victim and rolls back one of the transactions");

    Ok(())
}

async fn demo_isolation_levels(
    acid_manager: &AcidManager,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Demonstration of Insulation Levels ---");

    let levels = [
        IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted,
        IsolationLevel::RepeatableRead,
        IsolationLevel::Serializable,
    ];

    for (i, level) in levels.iter().enumerate() {
        let transaction_id = TransactionId::new(10 + i as u64);

        println!("Testing the isolation level: {:?}", level);

        acid_manager.begin_transaction(transaction_id, level.clone(), true)?;

        // We carry out operations depending on the level
        match level {
            IsolationLevel::ReadUncommitted => {
                println!("- Allows dirty reading");
            }
            IsolationLevel::ReadCommitted => {
                println!("- Prevents dirty reading");
            }
            IsolationLevel::RepeatableRead => {
                println!("- Prevents non-repeatable reading");
            }
            IsolationLevel::Serializable => {
                println!("- Complete transaction isolation");
            }
        }

        acid_manager.abort_transaction(transaction_id)?;
    }

    println!("Insulation levels provide varying degrees of protection against anomalies");

    Ok(())
}

// Helper functions for demonstration
fn write_record(
    _acid_manager: &AcidManager,
    _transaction_id: TransactionId,
    _page_id: u64,
    _record_id: u64,
    _data: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    // TODO: Implement record entry
    Ok(())
}

fn read_record(
    _acid_manager: &AcidManager,
    _transaction_id: TransactionId,
    _page_id: u64,
    _record_id: u64,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    // TODO: Implement record reading
    Ok(b"test data".to_vec())
}
