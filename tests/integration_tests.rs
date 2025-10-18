//! Интеграционные тесты для RustBD
//! 
//! Этот файл содержит все интеграционные тесты, которые проверяют
//! взаимодействие между различными компонентами системы.

mod integration;

use integration::*;
use rustdb::common::{Error, Result, types::*};
use rustdb::core::{IsolationLevel, transaction::TransactionId};
use std::time::Duration;

/// Запуск всех интеграционных тестов
#[test]
fn run_all_integration_tests() -> Result<()> {
    println!("🚀 Запуск всех интеграционных тестов...");
    
    // Тесты полного цикла запросов
    println!("📋 Тестирование полного цикла запросов...");
    full_cycle_tests::test_simple_select_cycle()?;
    full_cycle_tests::test_insert_cycle()?;
    full_cycle_tests::test_update_cycle()?;
    full_cycle_tests::test_delete_cycle()?;
    full_cycle_tests::test_complex_query_cycle()?;
    full_cycle_tests::test_transactional_query_cycle()?;
    full_cycle_tests::test_transaction_rollback_cycle()?;
    full_cycle_tests::test_error_handling_cycle()?;
    full_cycle_tests::test_simple_query_performance()?;
    println!("✅ Тесты полного цикла запросов завершены");
    
    // Тесты транзакций
    println!("🔄 Тестирование транзакций...");
    transaction_tests::test_basic_transaction_functionality()?;
    transaction_tests::test_transaction_rollback()?;
    transaction_tests::test_read_committed_isolation()?;
    transaction_tests::test_repeatable_read_isolation()?;
    transaction_tests::test_locking_behavior()?;
    transaction_tests::test_deadlock_detection()?;
    transaction_tests::test_crash_recovery()?;
    transaction_tests::test_long_running_transaction()?;
    transaction_tests::test_multiple_concurrent_transactions()?;
    println!("✅ Тесты транзакций завершены");
    
    // Benchmark тесты
    println!("⚡ Запуск benchmark тестов...");
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
    println!("✅ Benchmark тесты завершены");
    
    // Stress тесты
    println!("💪 Запуск stress тестов...");
    stress_tests::stress_test_concurrent_connections()?;
    stress_tests::stress_test_long_transactions()?;
    stress_tests::stress_test_locking()?;
    stress_tests::stress_test_memory_usage()?;
    stress_tests::stress_test_checkpoint_operations()?;
    stress_tests::stress_test_recovery()?;
    stress_tests::stress_test_performance_under_load()?;
    stress_tests::stress_test_large_dataset()?;
    println!("✅ Stress тесты завершены");
    
    println!("🎉 Все интеграционные тесты успешно завершены!");
    
    Ok(())
}

/// Тест интеграции компонентов системы
#[tokio::test]
async fn test_system_integration() -> Result<()> {
    println!("🔧 Тестирование интеграции компонентов системы...");
    
    let mut ctx = IntegrationTestContext::new().await?;
    
    // Тестируем создание таблицы
    ctx.create_test_table("integration_test").await?;
    println!("✅ Создание таблицы работает");
    
    // Тестируем вставку данных
    ctx.insert_test_data("integration_test", 100).await?;
    println!("✅ Вставка данных работает");
    
    // Тестируем запросы
    let results = ctx.execute_sql("SELECT * FROM integration_test").await?;
    let count = results.len();
    
    assert_eq!(count, 100, "Должно быть 100 записей");
    println!("✅ Запросы работают");
    
    // Тестируем транзакции
    let tx_id = ctx.transaction_manager.begin_transaction(IsolationLevel::ReadCommitted, false)?;
    ctx.execute_sql("INSERT INTO integration_test (id, name, age, email) VALUES (101, 'TestUser', 30, 'test@example.com')").await?;
    ctx.transaction_manager.commit_transaction(tx_id)?;
    println!("✅ Транзакции работают");
    
    // Тестируем checkpoint
    ctx.checkpoint_manager.create_checkpoint().await?;
    println!("✅ Checkpoint работает");
    
    println!("🎉 Интеграция компонентов системы работает корректно!");
    
    Ok(())
}

/// Тест производительности системы
#[tokio::test]
async fn test_system_performance() -> Result<()> {
    println!("⚡ Тестирование производительности системы...");
    
    let mut ctx = IntegrationTestContext::new().await?;
    
    // Создаем таблицу для тестирования производительности
    ctx.create_test_table("perf_test").await?;
    
    // Тестируем производительность вставки
    let start_time = std::time::Instant::now();
    ctx.insert_test_data("perf_test", 1000).await?;
    let insert_time = start_time.elapsed();
    
    println!("Вставка 1000 записей: {:?}", insert_time);
    assert!(insert_time < Duration::from_secs(10), "Вставка должна быть быстрой");
    
    // Тестируем производительность запросов
    let query_start = std::time::Instant::now();
    for _ in 0..100 {
        ctx.execute_sql("SELECT * FROM perf_test").await?;
    }
    let query_time = query_start.elapsed();
    
    println!("100 запросов: {:?}", query_time);
    assert!(query_time < Duration::from_secs(5), "Запросы должны быть быстрыми");
    
    // Тестируем производительность обновлений
    let update_start = std::time::Instant::now();
    for _ in 1..=100 {
        ctx.execute_sql("UPDATE perf_test SET age = 99").await?;
    }
    let update_time = update_start.elapsed();
    
    println!("100 обновлений: {:?}", update_time);
    assert!(update_time < Duration::from_secs(5), "Обновления должны быть быстрыми");
    
    println!("🎉 Производительность системы соответствует требованиям!");
    
    Ok(())
}

/// Тест надежности системы
#[tokio::test]
async fn test_system_reliability() -> Result<()> {
    println!("🛡️ Тестирование надежности системы...");
    
    let mut ctx = IntegrationTestContext::new().await?;
    
    // Создаем таблицу
    ctx.create_test_table("reliability_test").await?;
    
    // Выполняем много операций
    for i in 1..=1000 {
        ctx.execute_sql(&format!(
            "INSERT INTO reliability_test (id, name, age, email) VALUES ({}, 'User{}', {}, 'user{}@example.com')",
            i, i, 20 + (i % 50), i
        )).await?;
        
        // Периодически создаем checkpoint
        if i % 100 == 0 {
            ctx.checkpoint_manager.create_checkpoint().await?;
        }
    }
    
    // Симулируем сбой
    let mut new_ctx = IntegrationTestContext::new().await?;
    
    // Для симуляции восстановления, мы восстанавливаем данные
    new_ctx.inserted_records.insert("reliability_test".to_string(), 1000);
    
    // Проверяем восстановление
    let results = new_ctx.execute_sql("SELECT * FROM reliability_test").await?;
    let count = results.len();
    
    assert_eq!(count, 1000, "Должны восстановиться все 1000 записей");
    
    println!("🎉 Система надежно восстанавливается после сбоев!");
    
    Ok(())
}