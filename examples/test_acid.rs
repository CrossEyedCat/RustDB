// Example of demonstration of corrected ACID tests
use rustdb::core::advanced_lock_manager::{AdvancedLockConfig, AdvancedLockManager};
use rustdb::core::concurrency::{ConcurrencyConfig, ConcurrencyManager};
use rustdb::core::{AdvancedLockMode, ResourceType, RowKey, Timestamp, TransactionId};
use std::sync::Arc;
use std::time::Duration;
// removed redundant single-component import

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Demonstration of corrected ACID tests");

    // Creating a lock manager
    let config = AdvancedLockConfig::default();
    let lock_manager = Arc::new(AdvancedLockManager::new(config));

    // Creating a competition manager
    let concurrency_config = ConcurrencyConfig::default();
    let concurrency_manager = ConcurrencyManager::new(concurrency_config);

    // Testing basic functionality
    let tx1 = TransactionId(1);
    let tx2 = TransactionId(2);
    let resource = ResourceType::Record(1, 100);

    println!("✅ Test 1: Obtaining an exclusive lock");
    let result1 = lock_manager
        .acquire_lock(
            tx1,
            resource.clone(),
            AdvancedLockMode::Exclusive,
            Some(Duration::from_millis(100)),
        )
        .await;

    match result1 {
        Ok(_) => println!("✅ Lock received successfully"),
        Err(e) => println!("❌ Error getting lock: {}", e),
    }

    println!("✅ Test 2: Attempting to obtain a conflicting lock");
    let result2 = lock_manager
        .acquire_lock(
            tx2,
            resource.clone(),
            AdvancedLockMode::Exclusive,
            Some(Duration::from_millis(50)),
        )
        .await;

    match result2 {
        Ok(_) => println!("❌ Blocked received (unexpectedly)"),
        Err(_) => println!("✅ Blocking correctly rejected (conflict)"),
    }

    println!("✅ Test 3: Freeing the lock");
    let result3 = lock_manager.release_lock(tx1, resource.clone());
    match result3 {
        Ok(_) => println!("✅ Lock released successfully"),
        Err(e) => println!("❌ Lock release error: {}", e),
    }

    println!("✅ Test 4: Obtaining a lock after being released");
    let result4 = lock_manager
        .acquire_lock(
            tx2,
            resource.clone(),
            AdvancedLockMode::Exclusive,
            Some(Duration::from_millis(100)),
        )
        .await;

    match result4 {
        Ok(_) => println!("✅ Lock received after release"),
        Err(e) => println!("❌ Error getting lock: {}", e),
    }

    // Testing ConcurrencyManager
    println!("✅ Test 5: ConcurrencyManager - recording");
    let row_key = RowKey {
        table_id: 1,
        row_id: 100,
    };
    let write_result = concurrency_manager
        .write(tx1, row_key.clone(), b"test data".to_vec())
        .await;

    match write_result {
        Ok(_) => println!("✅Recording completed successfully"),
        Err(e) => println!("❌ Write error: {}", e),
    }

    println!("✅ Test 6: ConcurrencyManager - reading");
    let read_result = concurrency_manager
        .read(tx2, &row_key, Timestamp::now())
        .await;

    match read_result {
        Ok(data) => println!("✅ Reading completed successfully: {:?}", data),
        Err(e) => println!("❌ Read error: {}", e),
    }

    println!("\n🎉 All tests are completed!");
    println!("📊 Statistics:");
    let stats = lock_manager.get_statistics();
    println!("- Total blocking: {}", stats.total_locks);
    println!("- Pending transactions: {}", stats.waiting_transactions);
    println!("- Deadlocks detected: {}", stats.deadlocks_detected);

    Ok(())
}
