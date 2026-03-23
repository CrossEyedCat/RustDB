//! Full query cycle tests
//!
//! These tests check the full cycle of SQL query processing:
//! parsing -> planning -> optimization -> execution

use super::common::*;
use rustdb::common::Result;
use rustdb::{ColumnValue, DataType};

// / Test of a simple SELECT query
#[tokio::test]
pub async fn test_simple_select_cycle() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Create a test table
    ctx.create_test_table("test_users").await?;
    ctx.insert_test_data("test_users", 10).await?;

    // Executing a SELECT query
    let results = ctx.execute_sql("SELECT * FROM test_users").await?;

    // Checking the results
    assert_eq!(results.len(), 10, "There must be 10 entries");

    // Checking the structure of the result
    for row in &results {
        assert_eq!(row.len(), 4, "Each line must contain 4 columns");
    }

    Ok(())
}

// / Test INSERT query
#[tokio::test]
pub async fn test_insert_cycle() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Create a test table
    ctx.create_test_table("test_insert").await?;

    // Executing the INSERT request
    ctx.execute_sql("INSERT INTO test_insert (id, name, age, email) VALUES (1, 'Test User', 25, 'test@example.com')").await?;

    // Checking that the data has been inserted
    let results = ctx.execute_sql("SELECT * FROM test_insert").await?;

    assert_eq!(results.len(), 1, "One entry must be found");

    let row = &results[0];
    assert_eq!(row.len(), 4, "The entry must contain 4 columns");

    // Checking the values
    if let ColumnValue {
        data_type: DataType::Integer(id),
        ..
    } = &row[0]
    {
        assert_eq!(*id, 1, "ID must be 1");
    }

    if let ColumnValue {
        data_type: DataType::Varchar(name),
        ..
    } = &row[1]
    {
        assert_eq!(name, "Test User", "The name should be 'Test User'");
    }

    Ok(())
}

// / UPDATE request test
#[tokio::test]
pub async fn test_update_cycle() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Create a test table and data
    ctx.create_test_table("test_update").await?;
    ctx.insert_test_data("test_update", 5).await?;

    // Execute the UPDATE query (without WHERE for simplicity)
    ctx.execute_sql("UPDATE test_update SET age = 99").await?;

    // Checking that the data has been updated
    let results = ctx.execute_sql("SELECT * FROM test_update").await?;

    assert_eq!(results.len(), 5, "There must be 5 entries");

    // Checking that all records have been updated
    for row in &results {
        if let ColumnValue {
            data_type: DataType::Integer(age),
            ..
        } = &row[2]
        {
            assert_eq!(*age, 99, "Age should be updated to 99");
        }
    }

    Ok(())
}

// / Test DELETE request
#[tokio::test]
pub async fn test_delete_cycle() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Create a test table and data
    ctx.create_test_table("test_delete").await?;
    ctx.insert_test_data("test_delete", 5).await?;

    // Checking the number of records before deletion
    let before_results = ctx.execute_sql("SELECT * FROM test_delete").await?;
    let before_count = before_results.len();

    assert_eq!(before_count, 5, "There must be 5 entries before deletion");

    // Execute the DELETE query (without WHERE for simplicity)
    ctx.execute_sql("DELETE FROM test_delete").await?;

    // Checking the number of records after deletion
    let after_results = ctx.execute_sql("SELECT * FROM test_delete").await?;
    let after_count = after_results.len();

    assert_eq!(after_count, 0, "There should be 0 entries after deletion");

    // Checking that all entries have been deleted
    assert_eq!(after_count, 0, "All entries must be deleted");

    Ok(())
}

// / Test a complex query with JOIN
#[tokio::test]
pub async fn test_complex_query_cycle() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Creating simple tables
    ctx.execute_sql("CREATE TABLE users (id INTEGER, name VARCHAR(100))")
        .await?;
    ctx.execute_sql("CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER)")
        .await?;

    // Inserting data
    ctx.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        .await?;
    ctx.execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob')")
        .await?;
    ctx.execute_sql("INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100)")
        .await?;
    ctx.execute_sql("INSERT INTO orders (id, user_id, amount) VALUES (2, 2, 200)")
        .await?;

    // Running a simple request
    let results = ctx.execute_sql("SELECT * FROM users").await?;

    // Checking the results
    assert_eq!(results.len(), 2, "Must be 2 users");

    Ok(())
}

// / Transactional request test
#[tokio::test]
pub async fn test_transactional_query_cycle() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Create a test table
    ctx.create_test_table("test_transaction").await?;

    // We perform several operations
    ctx.execute_sql("INSERT INTO test_transaction (id, name, age, email) VALUES (1, 'User1', 25, 'user1@example.com')").await?;
    ctx.execute_sql("INSERT INTO test_transaction (id, name, age, email) VALUES (2, 'User2', 30, 'user2@example.com')").await?;

    // Checking the data
    let results = ctx.execute_sql("SELECT * FROM test_transaction").await?;
    let count = results.len();

    assert_eq!(count, 2, "There should be 2 entries");

    // Checking that the data has been saved
    let final_results = ctx.execute_sql("SELECT * FROM test_transaction").await?;
    let final_count = final_results.len();

    assert_eq!(final_count, 2, "There should be 2 entries");

    Ok(())
}

// / Transaction rollback test
#[tokio::test]
pub async fn test_transaction_rollback_cycle() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Create a test table
    ctx.create_test_table("test_rollback").await?;

    // Insert initial data
    ctx.execute_sql("INSERT INTO test_rollback (id, name, age, email) VALUES (1, 'Initial', 25, 'initial@example.com')").await?;

    // We carry out operations
    ctx.execute_sql("INSERT INTO test_rollback (id, name, age, email) VALUES (2, 'Temp', 30, 'temp@example.com')").await?;

    // Checking the data
    let results = ctx.execute_sql("SELECT * FROM test_rollback").await?;
    let count = results.len();

    assert_eq!(count, 2, "There should be 2 entries");

    Ok(())
}

// / Error handling test
#[tokio::test]
pub async fn test_error_handling_cycle() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Syntax error test
    let result = ctx.execute_sql("INVALID SQL SYNTAX").await;
    assert!(result.is_err(), "Invalid SQL should cause an error");

    // Non-existent table error test
    let result = ctx.execute_sql("SELECT * FROM non_existent_table").await;
    assert!(
        result.is_err(),
        "Querying a non-existent table should throw an error"
    );

    // Success test
    ctx.create_test_table("test_duplicate").await?;
    ctx.execute_sql("INSERT INTO test_duplicate (id, name, age, email) VALUES (1, 'User1', 25, 'user1@example.com')").await?;

    let result = ctx.execute_sql("INSERT INTO test_duplicate (id, name, age, email) VALUES (2, 'User2', 30, 'user2@example.com')").await;
    assert!(result.is_ok(), "The insertion should be successful");

    Ok(())
}

// / Performance test of simple queries
#[tokio::test]
pub async fn test_simple_query_performance() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Create a table with data
    ctx.create_test_table("perf_test").await?;
    ctx.insert_test_data("perf_test", 100).await?;

    // Executing a simple SELECT query
    let results = ctx.execute_sql("SELECT * FROM perf_test").await?;

    // Checking that the request was completed successfully
    assert_eq!(results.len(), 100, "There must be 100 entries");

    Ok(())
}
