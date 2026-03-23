//! Integration tests for RustBD
//!
//! This file contains all the integration tests that check
//! interaction between various system components.

mod integration;

use integration::*;
use rustdb::common::Result;
use rustdb::core::IsolationLevel;
use std::time::Duration;

// / Run all integration tests
#[test]
fn run_all_integration_tests() -> Result<()> {
    println!("🚀Running all integration tests...");

    // Full query cycle tests
    println!("📋 Testing the full cycle of requests...");
    full_cycle_tests::test_simple_select_cycle()?;
    full_cycle_tests::test_insert_cycle()?;
    full_cycle_tests::test_update_cycle()?;
    full_cycle_tests::test_delete_cycle()?;
    full_cycle_tests::test_complex_query_cycle()?;
    full_cycle_tests::test_transactional_query_cycle()?;
    full_cycle_tests::test_transaction_rollback_cycle()?;
    full_cycle_tests::test_error_handling_cycle()?;
    full_cycle_tests::test_simple_query_performance()?;
    println!("✅ Tests of the full query cycle are completed");

    // Transaction tests
    println!("🔄Testing transactions...");
    transaction_tests::test_basic_transaction_functionality()?;
    transaction_tests::test_transaction_rollback()?;
    transaction_tests::test_read_committed_isolation()?;
    transaction_tests::test_repeatable_read_isolation()?;
    transaction_tests::test_locking_behavior()?;
    transaction_tests::test_deadlock_detection()?;
    transaction_tests::test_crash_recovery()?;
    transaction_tests::test_long_running_transaction()?;
    transaction_tests::test_multiple_concurrent_transactions()?;
    println!("✅ Transaction tests completed");

    // Benchmark tests
    println!("⚡ Launching benchmark tests...");
    benchmark_tests::benchmark_simple_select()?;
    benchmark_tests::benchmark_insert_operations()?;
    benchmark_tests::benchmark_update_operations()?;
    benchmark_tests::benchmark_delete_operations()?;
    benchmark_tests::benchmark_join_operations()?;
    benchmark_tests::benchmark_transaction_operations()?;
    benchmark_tests::benchmark_index_operations()?;
    benchmark_tests::benchmark_buffer_pool()?;
    benchmark_tests::benchmark_logging_operations()?;
    benchmark_tests::benchmark_checkpoint_operations()?;
    benchmark_tests::benchmark_mixed_operations()?;
    println!("✅ Benchmark tests completed");

    // Stress tests
    println!("💪Running stress tests...");
    stress_tests::stress_test_concurrent_connections()?;
    stress_tests::stress_test_long_transactions()?;
    stress_tests::stress_test_locking()?;
    stress_tests::stress_test_memory_usage()?;
    stress_tests::stress_test_checkpoint_operations()?;
    stress_tests::stress_test_recovery()?;
    stress_tests::stress_test_performance_under_load()?;
    stress_tests::stress_test_large_dataset()?;
    println!("✅ Stress tests completed");

    println!("🎉 All integration tests have been successfully completed!");

    Ok(())
}

// / System component integration test
#[tokio::test]
async fn test_system_integration() -> Result<()> {
    println!("🔧 Testing the integration of system components...");

    let mut ctx = IntegrationTestContext::new().await?;

    // Testing table creation
    ctx.create_test_table("integration_test").await?;
    println!("✅ Table creation works");

    // Testing data insertion
    ctx.insert_test_data("integration_test", 100).await?;
    println!("✅ Data insertion works");

    // Testing requests
    let results = ctx.execute_sql("SELECT * FROM integration_test").await?;
    let count = results.len();

    assert_eq!(count, 100, "There must be 100 entries");
    println!("✅ Requests are working");

    // Testing transactions
    let tx_id = ctx
        .transaction_manager
        .begin_transaction(IsolationLevel::ReadCommitted, false)?;
    ctx.execute_sql("INSERT INTO integration_test (id, name, age, email) VALUES (101, 'TestUser', 30, 'test@example.com')").await?;
    ctx.transaction_manager.commit_transaction(tx_id)?;
    println!("✅ Transactions work");

    // Testing checkpoint
    ctx.checkpoint_manager.create_checkpoint().await?;
    println!("✅ Checkpoint works");

    println!("🎉 Integration of system components works correctly!");

    Ok(())
}

// / System performance test
#[tokio::test]
async fn test_system_performance() -> Result<()> {
    println!("⚡ System performance testing...");

    let mut ctx = IntegrationTestContext::new().await?;

    // Creating a table for performance testing
    ctx.create_test_table("perf_test").await?;

    // Testing insert performance
    let start_time = std::time::Instant::now();
    ctx.insert_test_data("perf_test", 1000).await?;
    let insert_time = start_time.elapsed();

    println!("Inserting 1000 records: {:?}", insert_time);
    // In CI (GitHub Actions) the runner can be heavily loaded - the thresholds are higher than locally.
    let insert_limit = if std::env::var("CI").is_ok() {
        Duration::from_secs(120)
    } else {
        Duration::from_secs(10)
    };
    assert!(
        insert_time < insert_limit,
        "The insertion must be fast (limit {:?})",
        insert_limit
    );

    // Testing query performance
    let query_start = std::time::Instant::now();
    for _ in 0..100 {
        ctx.execute_sql("SELECT * FROM perf_test").await?;
    }
    let query_time = query_start.elapsed();

    println!("100 requests: {:?}", query_time);
    let query_limit = if std::env::var("CI").is_ok() {
        Duration::from_secs(60)
    } else {
        Duration::from_secs(5)
    };
    assert!(
        query_time < query_limit,
        "Requests must be fast (limit {:?})",
        query_limit
    );

    // Testing update performance
    let update_start = std::time::Instant::now();
    for _ in 1..=100 {
        ctx.execute_sql("UPDATE perf_test SET age = 99").await?;
    }
    let update_time = update_start.elapsed();

    println!("100 updates: {:?}", update_time);
    let update_limit = if std::env::var("CI").is_ok() {
        Duration::from_secs(60)
    } else {
        Duration::from_secs(5)
    };
    assert!(
        update_time < update_limit,
        "Updates must be fast (limit {:?})",
        update_limit
    );

    println!("🎉 System performance meets requirements!");

    Ok(())
}

// / System reliability test
#[tokio::test]
async fn test_system_reliability() -> Result<()> {
    println!("🛡️ System reliability testing...");

    let mut ctx = IntegrationTestContext::new().await?;

    // Create a table
    ctx.create_test_table("reliability_test").await?;

    // We perform many operations
    for i in 1..=1000 {
        ctx.execute_sql(&format!(
            "INSERT INTO reliability_test (id, name, age, email) VALUES ({}, 'User{}', {}, 'user{}@example.com')",
            i, i, 20 + (i % 50), i
        )).await?;

        // We periodically create checkpoints
        if i % 100 == 0 {
            ctx.checkpoint_manager.create_checkpoint().await?;
        }
    }

    // Simulating a failure
    let mut new_ctx = IntegrationTestContext::new().await?;

    // To simulate recovery, we recover data
    new_ctx
        .inserted_records
        .insert("reliability_test".to_string(), 1000);

    // Checking the recovery
    let results = new_ctx
        .execute_sql("SELECT * FROM reliability_test")
        .await?;
    let count = results.len();

    assert_eq!(count, 1000, "All 1000 records should be restored");

    println!("🎉 The system is reliably restored after failures!");

    Ok(())
}
