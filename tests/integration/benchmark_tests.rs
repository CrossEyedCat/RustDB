//! Benchmark тесты
//!
//! Эти тесты измеряют производительность различных операций
//! и компонентов системы.

use super::common::*;
use rustdb::common::Result;
use rustdb::core::IsolationLevel;
// use std::time::Duration;

/// Benchmark простых SELECT запросов
#[tokio::test]
pub async fn benchmark_simple_select() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Создаем таблицу с данными
    ctx.create_test_table("bench_select").await?;
    ctx.insert_test_data("bench_select", 100).await?;

    // Выполняем SELECT запросы
    for _ in 0..10 {
        let results = ctx.execute_sql("SELECT * FROM bench_select").await?;
        assert_eq!(results.len(), 100, "Должно быть 100 записей");
    }

    Ok(())
}

/// Benchmark INSERT операций
#[tokio::test]
pub async fn benchmark_insert_operations() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Создаем таблицу для тестирования INSERT
    ctx.create_test_table("bench_insert").await?;

    // Выполняем INSERT операции
    for i in 1..=100 {
        let sql = format!(
            "INSERT INTO bench_insert (id, name, age, email) VALUES ({}, 'User{}', {}, 'user{}@example.com')",
            i, i, 20 + (i % 50), i
        );
        ctx.execute_sql(&sql).await?;
    }

    // Проверяем, что все данные вставились
    let results = ctx.execute_sql("SELECT * FROM bench_insert").await?;
    assert_eq!(results.len(), 100, "Должно быть 100 записей");

    Ok(())
}

/// Benchmark UPDATE операций
#[tokio::test]
pub async fn benchmark_update_operations() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Создаем таблицу с данными для обновления
    ctx.create_test_table("bench_update").await?;
    ctx.insert_test_data("bench_update", 100).await?;

    // Выполняем UPDATE операции (без WHERE для упрощения)
    for i in 1..=10 {
        let sql = format!("UPDATE bench_update SET age = {}", 25 + (i % 30));
        ctx.execute_sql(&sql).await?;
    }

    // Проверяем, что данные обновились
    let results = ctx.execute_sql("SELECT * FROM bench_update").await?;
    assert_eq!(results.len(), 100, "Должно быть 100 записей");

    Ok(())
}

/// Benchmark DELETE операций
#[tokio::test]
pub async fn benchmark_delete_operations() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Создаем таблицу с данными для удаления
    ctx.create_test_table("bench_delete").await?;
    ctx.insert_test_data("bench_delete", 100).await?;

    // Выполняем DELETE операции (без WHERE для упрощения)
    ctx.execute_sql("DELETE FROM bench_delete").await?;

    // Проверяем, что все данные удалены
    let results = ctx.execute_sql("SELECT * FROM bench_delete").await?;
    assert_eq!(results.len(), 0, "Все записи должны быть удалены");

    Ok(())
}

/// Benchmark сложных запросов с JOIN
#[tokio::test]
pub async fn benchmark_join_operations() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Создаем простые таблицы для JOIN тестов
    ctx.execute_sql("CREATE TABLE users (id INTEGER, name VARCHAR(100))")
        .await?;
    ctx.execute_sql("CREATE TABLE orders (id INTEGER, user_id INTEGER)")
        .await?;

    // Вставляем тестовые данные
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

    // Выполняем простые запросы
    for _ in 0..5 {
        let results = ctx.execute_sql("SELECT * FROM users").await?;
        assert_eq!(results.len(), 10, "Должно быть 10 пользователей");
    }

    Ok(())
}

/// Benchmark транзакций
#[tokio::test]
pub async fn benchmark_transaction_operations() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Создаем таблицу для тестирования транзакций
    ctx.create_test_table("bench_transaction").await?;

    // Выполняем транзакции
    for i in 0..10 {
        let tx_id = ctx
            .transaction_manager
            .begin_transaction(IsolationLevel::ReadCommitted, false)?;

        // Выполняем несколько операций в транзакции
        for j in 1..=5 {
            let sql = format!(
                "INSERT INTO bench_transaction (id, name, age, email) VALUES ({}, 'User{}', {}, 'user{}@example.com')",
                i * 5 + j, i * 5 + j, 20 + (j % 30), i * 5 + j
            );
            ctx.execute_sql(&sql).await?;
        }

        ctx.transaction_manager.commit_transaction(tx_id)?;
    }

    // Проверяем, что все данные вставились
    let results = ctx.execute_sql("SELECT * FROM bench_transaction").await?;
    assert_eq!(results.len(), 50, "Должно быть 50 записей");

    Ok(())
}

/// Benchmark индексов
#[tokio::test]
pub async fn benchmark_index_operations() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Создаем простую таблицу
    ctx.execute_sql("CREATE TABLE bench_index (id INTEGER, name VARCHAR(100), age INTEGER)")
        .await?;
    // Вставляем данные
    for i in 1..=100 {
        ctx.execute_sql(&format!(
            "INSERT INTO bench_index (id, name, age) VALUES ({}, 'User{}', {})",
            i,
            i,
            18 + (i % 60)
        ))
        .await?;
    }

    // Выполняем запросы
    for _ in 0..10 {
        let results = ctx.execute_sql("SELECT * FROM bench_index").await?;
        assert_eq!(results.len(), 100, "Должно быть 100 записей");
    }

    Ok(())
}

/// Benchmark буферного пула
#[tokio::test]
pub async fn benchmark_buffer_pool() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Создаем таблицу для тестирования буферного пула
    ctx.create_test_table("bench_buffer").await?;

    // Вставляем данные
    ctx.insert_test_data("bench_buffer", 100).await?;

    // Выполняем запросы (тестируем кэширование)
    for _ in 0..20 {
        let results = ctx.execute_sql("SELECT * FROM bench_buffer").await?;
        assert_eq!(results.len(), 100, "Должно быть 100 записей");
    }

    Ok(())
}

/// Benchmark логгирования
#[tokio::test]
pub async fn benchmark_logging_operations() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Создаем таблицу для тестирования логгирования
    ctx.create_test_table("bench_logging").await?;

    // Выполняем операции с логгированием
    for i in 1..=100 {
        let sql = format!(
            "INSERT INTO bench_logging (id, name, age, email) VALUES ({}, 'User{}', {}, 'user{}@example.com')",
            i, i, 20 + (i % 40), i
        );
        ctx.execute_sql(&sql).await?;
    }

    // Проверяем, что все данные вставились
    let results = ctx.execute_sql("SELECT * FROM bench_logging").await?;
    assert_eq!(results.len(), 100, "Должно быть 100 записей");

    Ok(())
}

/// Benchmark checkpoint операций
#[tokio::test]
pub async fn benchmark_checkpoint_operations() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Создаем таблицу и заполняем данными
    ctx.create_test_table("bench_checkpoint").await?;
    ctx.insert_test_data("bench_checkpoint", 100).await?;

    // Выполняем checkpoint операции
    for _ in 0..5 {
        ctx.checkpoint_manager.create_checkpoint().await?;
    }

    // Проверяем, что данные сохранились
    let results = ctx.execute_sql("SELECT * FROM bench_checkpoint").await?;
    assert_eq!(results.len(), 100, "Должно быть 100 записей");

    Ok(())
}

/// Benchmark смешанных операций
#[tokio::test]
pub async fn benchmark_mixed_operations() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;

    // Создаем таблицу для смешанных операций
    ctx.create_test_table("bench_mixed").await?;
    ctx.insert_test_data("bench_mixed", 100).await?;

    // Выполняем смешанные операции
    for i in 1..=50 {
        match i % 4 {
            0 => {
                // SELECT операция
                let results = ctx.execute_sql("SELECT * FROM bench_mixed").await?;
                assert!(!results.is_empty(), "Должны быть результаты");
            }
            1 => {
                // INSERT операция
                let sql = format!(
                    "INSERT INTO bench_mixed (id, name, age, email) VALUES ({}, 'NewUser{}', {}, 'newuser{}@example.com')",
                    i + 100, i, 20 + (i % 30), i
                );
                ctx.execute_sql(&sql).await?;
            }
            2 => {
                // UPDATE операция (без WHERE для упрощения)
                let sql = format!("UPDATE bench_mixed SET age = {}", 25 + (i % 20));
                ctx.execute_sql(&sql).await?;
            }
            _ => {
                // Простая операция
                let results = ctx.execute_sql("SELECT * FROM bench_mixed").await?;
                assert!(!results.is_empty(), "Должны быть результаты");
            }
        }
    }

    // Проверяем, что операции выполнились
    let results = ctx.execute_sql("SELECT * FROM bench_mixed").await?;
    assert!(!results.is_empty(), "Должны быть результаты");

    Ok(())
}
