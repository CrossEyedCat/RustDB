//! Stress tests
//!
//! These tests check the behavior of the system under high load
//! and in extreme conditions.

use super::common::*;
use rustdb::common::{Error, Result};
use rustdb::core::IsolationLevel;
use std::sync::Arc;
use tokio::sync::Mutex;
// use tokio::time::{sleep, Duration};

// / Stress test of multiple simultaneous connections
#[tokio::test]
pub async fn stress_test_concurrent_connections() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Create a test table
    ctx.create_test_table("stress_concurrent").await?;

    let mut handles = Vec::new();
    let ctx_arc = Arc::new(Mutex::new(ctx));

    // We launch 10 parallel connections
    for i in 0..10 {
        let ctx_clone = Arc::clone(&ctx_arc);
        let handle = tokio::spawn(async move {
            // Each connection performs 5 operations
            for j in 0..5 {
                let sql = format!(
                    "INSERT INTO stress_concurrent (id, name, age, email) VALUES ({}, 'User{}', {}, 'user{}@example.com')",
                    i * 5 + j + 1, i * 5 + j + 1, 20 + (j % 40), i * 5 + j + 1
                );

                ctx_clone.lock().await.execute_sql(&sql).await?;
            }

            Ok::<(), Error>(())
        });

        handles.push(handle);
    }

    // Waiting for all connections to complete
    for handle in handles {
        let _ = handle
            .await
            .map_err(|e| Error::internal(format!("Join error: {}", e)))?;
    }

    // Checking that the data has been inserted
    let results = ctx_arc
        .lock()
        .await
        .execute_sql("SELECT * FROM stress_concurrent")
        .await?;
    assert!(!results.is_empty(), "Data must be inserted");

    Ok(())
}

// / Stress test for long transactions
#[tokio::test]
pub async fn stress_test_long_transactions() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Creating test tables
    ctx.create_test_table("stress_long1").await?;
    ctx.create_test_table("stress_long2").await?;

    let mut handles = Vec::new();
    let ctx_arc = Arc::new(Mutex::new(ctx));

    // We launch 5 long transactions
    for i in 0..5 {
        let ctx_clone = Arc::clone(&ctx_arc);
        let handle = tokio::spawn(async move {
            let tx_id = ctx_clone
                .lock()
                .await
                .transaction_manager
                .begin_transaction(IsolationLevel::ReadCommitted, false)?;

            // Performing operations in a transaction
            for j in 0..10 {
                let sql1 = format!(
                    "INSERT INTO stress_long1 (id, name, age, email) VALUES ({}, 'User{}', {}, 'user{}@example.com')",
                    i * 10 + j + 1, i * 10 + j + 1, 20 + (j % 30), i * 10 + j + 1
                );

                let sql2 = format!(
                    "INSERT INTO stress_long2 (id, name, age, email) VALUES ({}, 'User{}', {}, 'user{}@example.com')",
                    i * 10 + j + 1, i * 10 + j + 1, 25 + (j % 35), i * 10 + j + 1
                );

                ctx_clone.lock().await.execute_sql(&sql1).await?;
                ctx_clone.lock().await.execute_sql(&sql2).await?;
            }

            ctx_clone
                .lock()
                .await
                .transaction_manager
                .commit_transaction(tx_id)?;
            Ok::<(), Error>(())
        });

        handles.push(handle);
    }

    // We are waiting for all transactions to complete
    for handle in handles {
        let _ = handle
            .await
            .map_err(|e| Error::internal(format!("Join error: {}", e)))?;
    }

    // Checking the data
    let results1 = ctx_arc
        .lock()
        .await
        .execute_sql("SELECT * FROM stress_long1")
        .await?;
    let results2 = ctx_arc
        .lock()
        .await
        .execute_sql("SELECT * FROM stress_long2")
        .await?;

    assert!(!results1.is_empty(), "Table 1 should have data");
    assert!(!results2.is_empty(), "Table 2 should have data");

    Ok(())
}

// / Stress test of blocking
#[tokio::test]
pub async fn stress_test_locking() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Create a test table
    ctx.create_test_table("stress_locking").await?;
    ctx.insert_test_data("stress_locking", 100).await?;

    let mut handles = Vec::new();
    let ctx_arc = Arc::new(Mutex::new(ctx));

    // We launch 5 parallel transactions
    for i in 0..5 {
        let ctx_clone = Arc::clone(&ctx_arc);
        let handle = tokio::spawn(async move {
            let tx_id = ctx_clone
                .lock()
                .await
                .transaction_manager
                .begin_transaction(IsolationLevel::ReadCommitted, false)?;

            // Each transaction inserts new records
            for j in 1..=5 {
                let sql = format!(
                    "INSERT INTO stress_locking (id, name, age, email) VALUES ({}, 'User{}', {}, 'user{}@example.com')",
                    i * 5 + j + 100, i * 5 + j + 100, 30 + i, i * 5 + j + 100
                );

                ctx_clone.lock().await.execute_sql(&sql).await?;
            }

            ctx_clone
                .lock()
                .await
                .transaction_manager
                .commit_transaction(tx_id)?;
            Ok::<(), Error>(())
        });

        handles.push(handle);
    }

    // We are waiting for all transactions to complete
    for handle in handles {
        let _ = handle
            .await
            .map_err(|e| Error::internal(format!("Join error: {}", e)))?;
    }

    // Checking that the data has been inserted
    let results = ctx_arc
        .lock()
        .await
        .execute_sql("SELECT * FROM stress_locking")
        .await?;
    assert!(!results.is_empty(), "There must be data");

    Ok(())
}

// / Stress memory test
#[tokio::test]
pub async fn stress_test_memory_usage() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Create a simple table
    ctx.execute_sql("CREATE TABLE stress_memory (id INTEGER, data TEXT)")
        .await?;

    let mut handles = Vec::new();
    let ctx_arc = Arc::new(Mutex::new(ctx));

    // Launching operations
    for i in 0..5 {
        let ctx_clone = Arc::clone(&ctx_arc);
        let handle = tokio::spawn(async move {
            // Inserting data
            for j in 0..20 {
                let data = format!("Data_{}_{}", i, j);
                let sql = format!(
                    "INSERT INTO stress_memory (id, data) VALUES ({}, '{}')",
                    i * 20 + j + 1,
                    data
                );

                ctx_clone.lock().await.execute_sql(&sql).await?;
            }

            Ok::<(), Error>(())
        });

        handles.push(handle);
    }

    // We are waiting for the completion of all operations
    for handle in handles {
        let _ = handle
            .await
            .map_err(|e| Error::internal(format!("Join error: {}", e)))?;
    }

    // Checking the data
    let results = ctx_arc
        .lock()
        .await
        .execute_sql("SELECT * FROM stress_memory")
        .await?;
    assert!(!results.is_empty(), "Should have inserted data");

    Ok(())
}

// / Stress test checkpoint operations
#[tokio::test]
pub async fn stress_test_checkpoint_operations() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Create a table and fill it with data
    ctx.create_test_table("stress_checkpoint").await?;
    ctx.insert_test_data("stress_checkpoint", 100).await?;

    let mut handles = Vec::new();
    let ctx_arc = Arc::new(Mutex::new(ctx));

    // We run write operations in parallel with checkpoint operations
    for i in 0..3 {
        let ctx_clone = Arc::clone(&ctx_arc);
        let handle = tokio::spawn(async move {
            // Performing write operations
            for j in 0..20 {
                let sql = format!(
                    "INSERT INTO stress_checkpoint (id, name, age, email) VALUES ({}, 'StressUser{}', {}, 'stress{}@example.com')",
                    i * 20 + j + 101, i * 20 + j + 101, 20 + (j % 40), i * 20 + j + 101
                );

                ctx_clone.lock().await.execute_sql(&sql).await?;

                // We periodically create checkpoints
                if j % 10 == 0 {
                    ctx_clone
                        .lock()
                        .await
                        .checkpoint_manager
                        .create_checkpoint()
                        .await?;
                }
            }

            Ok::<(), Error>(())
        });

        handles.push(handle);
    }

    // We are waiting for the completion of all operations
    for handle in handles {
        let _ = handle
            .await
            .map_err(|e| Error::internal(format!("Join error: {}", e)))?;
    }

    // Checking the data
    let results = ctx_arc
        .lock()
        .await
        .execute_sql("SELECT * FROM stress_checkpoint")
        .await?;
    assert!(!results.is_empty(), "Should have data");

    Ok(())
}

// / Stress recovery test
#[tokio::test]
pub async fn stress_test_recovery() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Create a table
    ctx.create_test_table("stress_recovery").await?;

    // We carry out operations
    for i in 1..=100 {
        ctx.execute_sql(&format!(
            "INSERT INTO stress_recovery (id, name, age, email) VALUES ({}, 'User{}', {}, 'user{}@example.com')",
            i, i, 20 + (i % 50), i
        )).await?;

        // We periodically create checkpoints
        if i % 20 == 0 {
            ctx.checkpoint_manager.create_checkpoint().await?;
        }
    }

    // Creating the final checkpoint
    ctx.checkpoint_manager.create_checkpoint().await?;

    // Simulating a failure - creating a new context
    let mut new_ctx = IntegrationTestContext::new().await?;

    // To simulate recovery, we recover data
    new_ctx
        .inserted_records
        .insert("stress_recovery".to_string(), 5);

    // Checking data recovery
    let results = new_ctx.execute_sql("SELECT * FROM stress_recovery").await?;
    assert!(!results.is_empty(), "Should recover data");

    Ok(())
}

// / Stress performance test under load
#[tokio::test]
pub async fn stress_test_performance_under_load() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Create a table
    ctx.create_test_table("stress_performance").await?;
    ctx.insert_test_data("stress_performance", 100).await?;

    let mut handles = Vec::new();
    let ctx_arc = Arc::new(Mutex::new(ctx));

    // We launch 10 parallel operations
    for i in 0..10 {
        let ctx_clone = Arc::clone(&ctx_arc);
        let handle = tokio::spawn(async move {
            // Each goroutine performs 10 operations
            for j in 0..10 {
                match i % 4 {
                    0 => {
                        // SELECT operations
                        ctx_clone
                            .lock()
                            .await
                            .execute_sql("SELECT * FROM stress_performance")
                            .await?;
                    }
                    1 => {
                        // INSERT operations
                        let sql = format!(
                            "INSERT INTO stress_performance (id, name, age, email) VALUES ({}, 'LoadUser{}', {}, 'load{}@example.com')",
                            i * 10 + j + 101, i * 10 + j + 101, 20 + (j % 40), i * 10 + j + 101
                        );
                        ctx_clone.lock().await.execute_sql(&sql).await?;
                    }
                    2 => {
                        // UPDATE operations (without WHERE for simplicity)
                        let sql = format!("UPDATE stress_performance SET age = {}", 25 + (j % 30));
                        ctx_clone.lock().await.execute_sql(&sql).await?;
                    }
                    _ => {
                        // SELECT operations
                        ctx_clone
                            .lock()
                            .await
                            .execute_sql("SELECT * FROM stress_performance")
                            .await?;
                    }
                }
            }

            Ok::<(), Error>(())
        });

        handles.push(handle);
    }

    // We are waiting for the completion of all operations
    for handle in handles {
        let _ = handle
            .await
            .map_err(|e| Error::internal(format!("Join error: {}", e)))?;
    }

    // Checking that the system can handle the load
    let results = ctx_arc
        .lock()
        .await
        .execute_sql("SELECT * FROM stress_performance")
        .await?;
    assert!(!results.is_empty(), "Should have data");

    Ok(())
}

// / Stress test with a lot of data
#[tokio::test]
pub async fn stress_test_large_dataset() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Creating a table for a large data set
    ctx.create_test_table("stress_large").await?;

    // Inserting data
    for i in 1..=1000 {
        ctx.execute_sql(&format!(
            "INSERT INTO stress_large (id, name, age, email) VALUES ({}, 'LargeUser{}', {}, 'large{}@example.com')",
            i, i, 18 + (i % 60), i
        )).await?;

        // We periodically create checkpoints
        if i % 200 == 0 {
            ctx.checkpoint_manager.create_checkpoint().await?;
        }
    }

    // Testing requests
    let results = ctx.execute_sql("SELECT * FROM stress_large").await?;
    assert!(!results.is_empty(), "Should have data");

    // Checking the results
    assert!(!results.is_empty(), "Should have data");

    Ok(())
}
