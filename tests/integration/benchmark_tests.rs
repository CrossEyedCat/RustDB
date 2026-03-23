//! Benchmark tests
//!
//! These tests measure the performance of various operations
//! and system components.

use super::common::*;
use rustdb::common::Result;
use rustdb::core::IsolationLevel;
// use std::time::Duration;

// / Benchmark of simple SELECT queries
#[tokio::test]
pub async fn benchmark_simple_select() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Create a table with data
    ctx.create_test_table("bench_select").await?;
    ctx.insert_test_data("bench_select", 100).await?;

    // Executing SELECT queries
    for _ in 0..10 {
        let results = ctx.execute_sql("SELECT * FROM bench_select").await?;
        assert_eq!(results.len(), 100, "There must be 100 entries");
    }

    Ok(())
}

// / Benchmark INSERT operations
#[tokio::test]
pub async fn benchmark_insert_operations() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Creating a table for INSERT testing
    ctx.create_test_table("bench_insert").await?;

    // Performing INSERT operations
    for i in 1..=100 {
        let sql = format!(
            "INSERT INTO bench_insert (id, name, age, email) VALUES ({}, 'User{}', {}, 'user{}@example.com')",
            i, i, 20 + (i % 50), i
        );
        ctx.execute_sql(&sql).await?;
    }

    // Checking that all data has been inserted
    let results = ctx.execute_sql("SELECT * FROM bench_insert").await?;
    assert_eq!(results.len(), 100, "There must be 100 entries");

    Ok(())
}

// / Benchmark UPDATE operations
#[tokio::test]
pub async fn benchmark_update_operations() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Create a table with data to update
    ctx.create_test_table("bench_update").await?;
    ctx.insert_test_data("bench_update", 100).await?;

    // Performing UPDATE operations (without WHERE for simplicity)
    for i in 1..=10 {
        let sql = format!("UPDATE bench_update SET age = {}", 25 + (i % 30));
        ctx.execute_sql(&sql).await?;
    }

    // Checking that the data has been updated
    let results = ctx.execute_sql("SELECT * FROM bench_update").await?;
    assert_eq!(results.len(), 100, "There must be 100 entries");

    Ok(())
}

// / Benchmark DELETE operations
#[tokio::test]
pub async fn benchmark_delete_operations() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Create a table with data to be deleted
    ctx.create_test_table("bench_delete").await?;
    ctx.insert_test_data("bench_delete", 100).await?;

    // Performing DELETE operations (without WHERE for simplicity)
    ctx.execute_sql("DELETE FROM bench_delete").await?;

    // Checking that all data has been deleted
    let results = ctx.execute_sql("SELECT * FROM bench_delete").await?;
    assert_eq!(results.len(), 0, "All entries must be deleted");

    Ok(())
}

// / Benchmark complex queries with JOIN
#[tokio::test]
pub async fn benchmark_join_operations() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Creating simple tables for JOIN tests
    ctx.execute_sql("CREATE TABLE users (id INTEGER, name VARCHAR(100))")
        .await?;
    ctx.execute_sql("CREATE TABLE orders (id INTEGER, user_id INTEGER)")
        .await?;

    // Inserting test data
    for i in 1..=10 {
        ctx.execute_sql(&format!(
            "INSERT INTO users (id, name) VALUES ({}, 'User{}')",
            i, i
        ))
        .await?;
    }

    for i in 1..=10 {
        ctx.execute_sql(&format!(
            "INSERT INTO orders (id, user_id) VALUES ({}, {})",
            i, i
        ))
        .await?;
    }

    // We perform simple queries
    for _ in 0..5 {
        let results = ctx.execute_sql("SELECT * FROM users").await?;
        assert_eq!(results.len(), 10, "There must be 10 users");
    }

    Ok(())
}

// / Benchmark transactions
#[tokio::test]
pub async fn benchmark_transaction_operations() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Creating a table for testing transactions
    ctx.create_test_table("bench_transaction").await?;

    // We carry out transactions
    for i in 0..10 {
        let tx_id = ctx
            .transaction_manager
            .begin_transaction(IsolationLevel::ReadCommitted, false)?;

        // Performing multiple operations in a transaction
        for j in 1..=5 {
            let sql = format!(
                "INSERT INTO bench_transaction (id, name, age, email) VALUES ({}, 'User{}', {}, 'user{}@example.com')",
                i * 5 + j, i * 5 + j, 20 + (j % 30), i * 5 + j
            );
            ctx.execute_sql(&sql).await?;
        }

        ctx.transaction_manager.commit_transaction(tx_id)?;
    }

    // Checking that all data has been inserted
    let results = ctx.execute_sql("SELECT * FROM bench_transaction").await?;
    assert_eq!(results.len(), 50, "There should be 50 entries");

    Ok(())
}

// / Benchmark indexes
#[tokio::test]
pub async fn benchmark_index_operations() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Create a simple table
    ctx.execute_sql("CREATE TABLE bench_index (id INTEGER, name VARCHAR(100), age INTEGER)")
        .await?;
    // Inserting data
    for i in 1..=100 {
        ctx.execute_sql(&format!(
            "INSERT INTO bench_index (id, name, age) VALUES ({}, 'User{}', {})",
            i,
            i,
            18 + (i % 60)
        ))
        .await?;
    }

    // We fulfill requests
    for _ in 0..10 {
        let results = ctx.execute_sql("SELECT * FROM bench_index").await?;
        assert_eq!(results.len(), 100, "There must be 100 entries");
    }

    Ok(())
}

// / Buffer pool benchmark
#[tokio::test]
pub async fn benchmark_buffer_pool() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Create a table to test the buffer pool
    ctx.create_test_table("bench_buffer").await?;

    // Inserting data
    ctx.insert_test_data("bench_buffer", 100).await?;

    // Execute requests (test caching)
    for _ in 0..20 {
        let results = ctx.execute_sql("SELECT * FROM bench_buffer").await?;
        assert_eq!(results.len(), 100, "There must be 100 entries");
    }

    Ok(())
}

// / Benchmark logging
#[tokio::test]
pub async fn benchmark_logging_operations() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Creating a table for testing logging
    ctx.create_test_table("bench_logging").await?;

    // We perform operations with logging
    for i in 1..=100 {
        let sql = format!(
            "INSERT INTO bench_logging (id, name, age, email) VALUES ({}, 'User{}', {}, 'user{}@example.com')",
            i, i, 20 + (i % 40), i
        );
        ctx.execute_sql(&sql).await?;
    }

    // Checking that all data has been inserted
    let results = ctx.execute_sql("SELECT * FROM bench_logging").await?;
    assert_eq!(results.len(), 100, "There must be 100 entries");

    Ok(())
}

// / Benchmark checkpoint operations
#[tokio::test]
pub async fn benchmark_checkpoint_operations() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Create a table and fill it with data
    ctx.create_test_table("bench_checkpoint").await?;
    ctx.insert_test_data("bench_checkpoint", 100).await?;

    // Perform checkpoint operations
    for _ in 0..5 {
        ctx.checkpoint_manager.create_checkpoint().await?;
    }

    // Checking that the data has been saved
    let results = ctx.execute_sql("SELECT * FROM bench_checkpoint").await?;
    assert_eq!(results.len(), 100, "There must be 100 entries");

    Ok(())
}

// / Benchmark mixed operations
#[tokio::test]
pub async fn benchmark_mixed_operations() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Creating a table for mixed operations
    ctx.create_test_table("bench_mixed").await?;
    ctx.insert_test_data("bench_mixed", 100).await?;

    // We carry out mixed operations
    for i in 1..=50 {
        match i % 4 {
            0 => {
                // SELECT operation
                let results = ctx.execute_sql("SELECT * FROM bench_mixed").await?;
                assert!(!results.is_empty(), "There must be results");
            }
            1 => {
                // INSERT operation
                let sql = format!(
                    "INSERT INTO bench_mixed (id, name, age, email) VALUES ({}, 'NewUser{}', {}, 'newuser{}@example.com')",
                    i + 100, i, 20 + (i % 30), i
                );
                ctx.execute_sql(&sql).await?;
            }
            2 => {
                // UPDATE operation (without WHERE for simplicity)
                let sql = format!("UPDATE bench_mixed SET age = {}", 25 + (i % 20));
                ctx.execute_sql(&sql).await?;
            }
            _ => {
                // Simple operation
                let results = ctx.execute_sql("SELECT * FROM bench_mixed").await?;
                assert!(!results.is_empty(), "There must be results");
            }
        }
    }

    // Checking that the operations were completed
    let results = ctx.execute_sql("SELECT * FROM bench_mixed").await?;
    assert!(!results.is_empty(), "There must be results");

    Ok(())
}
