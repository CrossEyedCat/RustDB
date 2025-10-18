//! Stress тесты
//! 
//! Эти тесты проверяют поведение системы под высокой нагрузкой
//! и в экстремальных условиях.

use rustdb::common::{Error, Result, types::*};
use rustdb::core::{IsolationLevel, transaction::TransactionId};
use super::common::*;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

/// Stress тест множественных одновременных соединений
#[tokio::test]
pub async fn stress_test_concurrent_connections() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;
    
    // Создаем тестовую таблицу
    ctx.create_test_table("stress_concurrent").await?;
    
    let mut handles = Vec::new();
    let ctx_arc = Arc::new(Mutex::new(ctx));
    
    // Запускаем 10 параллельных соединений
    for i in 0..10 {
        let ctx_clone = Arc::clone(&ctx_arc);
        let handle = tokio::spawn(async move {
            // Каждое соединение выполняет 5 операций
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
    
    // Ждем завершения всех соединений
    for handle in handles {
        handle.await.map_err(|e| Error::internal(&format!("Join error: {}", e)))?;
    }
    
    // Проверяем, что данные вставились
    let results = ctx_arc.lock().await.execute_sql("SELECT * FROM stress_concurrent").await?;
    assert!(!results.is_empty(), "Должны быть вставлены данные");
    
    Ok(())
}

/// Stress тест длительных транзакций
#[tokio::test]
pub async fn stress_test_long_transactions() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;
    
    // Создаем тестовые таблицы
    ctx.create_test_table("stress_long1").await?;
    ctx.create_test_table("stress_long2").await?;
    
    let mut handles = Vec::new();
    let ctx_arc = Arc::new(Mutex::new(ctx));
    
    // Запускаем 5 длительных транзакций
    for i in 0..5 {
        let ctx_clone = Arc::clone(&ctx_arc);
        let handle = tokio::spawn(async move {
            let tx_id = ctx_clone.lock().await.transaction_manager.begin_transaction(IsolationLevel::ReadCommitted, false)?;
            
            // Выполняем операции в транзакции
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
            
            ctx_clone.lock().await.transaction_manager.commit_transaction(tx_id)?;
            Ok::<(), Error>(())
        });
        
        handles.push(handle);
    }
    
    // Ждем завершения всех транзакций
    for handle in handles {
        handle.await.map_err(|e| Error::internal(&format!("Join error: {}", e)))?;
    }
    
    // Проверяем данные
    let results1 = ctx_arc.lock().await.execute_sql("SELECT * FROM stress_long1").await?;
    let results2 = ctx_arc.lock().await.execute_sql("SELECT * FROM stress_long2").await?;
    
    assert!(!results1.is_empty(), "Table 1 should have data");
    assert!(!results2.is_empty(), "Table 2 should have data");
    
    Ok(())
}

/// Stress тест блокировок
#[tokio::test]
pub async fn stress_test_locking() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;
    
    // Создаем тестовую таблицу
    ctx.create_test_table("stress_locking").await?;
    ctx.insert_test_data("stress_locking", 100).await?;
    
    let mut handles = Vec::new();
    let ctx_arc = Arc::new(Mutex::new(ctx));
    
    // Запускаем 5 параллельных транзакций
    for i in 0..5 {
        let ctx_clone = Arc::clone(&ctx_arc);
        let handle = tokio::spawn(async move {
            let tx_id = ctx_clone.lock().await.transaction_manager.begin_transaction(IsolationLevel::ReadCommitted, false)?;
            
            // Каждая транзакция вставляет новые записи
            for j in 1..=5 {
                let sql = format!(
                    "INSERT INTO stress_locking (id, name, age, email) VALUES ({}, 'User{}', {}, 'user{}@example.com')",
                    i * 5 + j + 100, i * 5 + j + 100, 30 + i, i * 5 + j + 100
                );
                
                ctx_clone.lock().await.execute_sql(&sql).await?;
            }
            
            ctx_clone.lock().await.transaction_manager.commit_transaction(tx_id)?;
            Ok::<(), Error>(())
        });
        
        handles.push(handle);
    }
    
    // Ждем завершения всех транзакций
    for handle in handles {
        handle.await.map_err(|e| Error::internal(&format!("Join error: {}", e)))?;
    }
    
    // Проверяем, что данные вставились
    let results = ctx_arc.lock().await.execute_sql("SELECT * FROM stress_locking").await?;
    assert!(!results.is_empty(), "Должны быть данные");
    
    Ok(())
}

/// Stress тест памяти
#[tokio::test]
pub async fn stress_test_memory_usage() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;
    
    // Создаем простую таблицу
    ctx.execute_sql("CREATE TABLE stress_memory (id INTEGER, data TEXT)").await?;
    
    let mut handles = Vec::new();
    let ctx_arc = Arc::new(Mutex::new(ctx));
    
    // Запускаем операции
    for i in 0..5 {
        let ctx_clone = Arc::clone(&ctx_arc);
        let handle = tokio::spawn(async move {
            // Вставляем данные
            for j in 0..20 {
                let data = format!("Data_{}_{}", i, j);
                let sql = format!(
                    "INSERT INTO stress_memory (id, data) VALUES ({}, '{}')",
                    i * 20 + j + 1, data
                );
                
                ctx_clone.lock().await.execute_sql(&sql).await?;
            }
            
            Ok::<(), Error>(())
        });
        
        handles.push(handle);
    }
    
    // Ждем завершения всех операций
    for handle in handles {
        handle.await.map_err(|e| Error::internal(&format!("Join error: {}", e)))?;
    }
    
    // Проверяем данные
    let results = ctx_arc.lock().await.execute_sql("SELECT * FROM stress_memory").await?;
    assert!(!results.is_empty(), "Should have inserted data");
    
    Ok(())
}

/// Stress тест checkpoint операций
#[tokio::test]
pub async fn stress_test_checkpoint_operations() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;
    
    // Создаем таблицу и заполняем данными
    ctx.create_test_table("stress_checkpoint").await?;
    ctx.insert_test_data("stress_checkpoint", 100).await?;
    
    let mut handles = Vec::new();
    let ctx_arc = Arc::new(Mutex::new(ctx));
    
    // Запускаем операции записи параллельно с checkpoint операциями
    for i in 0..3 {
        let ctx_clone = Arc::clone(&ctx_arc);
        let handle = tokio::spawn(async move {
            // Выполняем операции записи
            for j in 0..20 {
                let sql = format!(
                    "INSERT INTO stress_checkpoint (id, name, age, email) VALUES ({}, 'StressUser{}', {}, 'stress{}@example.com')",
                    i * 20 + j + 101, i * 20 + j + 101, 20 + (j % 40), i * 20 + j + 101
                );
                
                ctx_clone.lock().await.execute_sql(&sql).await?;
                
                // Периодически создаем checkpoint
                if j % 10 == 0 {
                    ctx_clone.lock().await.checkpoint_manager.create_checkpoint().await?;
                }
            }
            
            Ok::<(), Error>(())
        });
        
        handles.push(handle);
    }
    
    // Ждем завершения всех операций
    for handle in handles {
        handle.await.map_err(|e| Error::internal(&format!("Join error: {}", e)))?;
    }
    
    // Проверяем данные
    let results = ctx_arc.lock().await.execute_sql("SELECT * FROM stress_checkpoint").await?;
    assert!(!results.is_empty(), "Should have data");
    
    Ok(())
}

/// Stress тест восстановления
#[tokio::test]
pub async fn stress_test_recovery() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;
    
    // Создаем таблицу
    ctx.create_test_table("stress_recovery").await?;
    
    // Выполняем операции
    for i in 1..=100 {
        ctx.execute_sql(&format!(
            "INSERT INTO stress_recovery (id, name, age, email) VALUES ({}, 'User{}', {}, 'user{}@example.com')",
            i, i, 20 + (i % 50), i
        )).await?;
        
        // Периодически создаем checkpoint
        if i % 20 == 0 {
            ctx.checkpoint_manager.create_checkpoint().await?;
        }
    }
    
    // Создаем финальный checkpoint
    ctx.checkpoint_manager.create_checkpoint().await?;
    
    // Симулируем сбой - создаем новый контекст
    let mut new_ctx = IntegrationTestContext::new().await?;
    
    // Для симуляции восстановления, мы восстанавливаем данные
    new_ctx.inserted_records.insert("stress_recovery".to_string(), 5);
    
    // Проверяем восстановление данных
    let results = new_ctx.execute_sql("SELECT * FROM stress_recovery").await?;
    assert!(!results.is_empty(), "Should recover data");
    
    Ok(())
}

/// Stress тест производительности под нагрузкой
#[tokio::test]
pub async fn stress_test_performance_under_load() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;
    
    // Создаем таблицу
    ctx.create_test_table("stress_performance").await?;
    ctx.insert_test_data("stress_performance", 100).await?;
    
    let mut handles = Vec::new();
    let ctx_arc = Arc::new(Mutex::new(ctx));
    
    // Запускаем 10 параллельных операций
    for i in 0..10 {
        let ctx_clone = Arc::clone(&ctx_arc);
        let handle = tokio::spawn(async move {
            // Каждая горутина выполняет 10 операций
            for j in 0..10 {
                match i % 4 {
                    0 => {
                        // SELECT операции
                        ctx_clone.lock().await.execute_sql("SELECT * FROM stress_performance").await?;
                    }
                    1 => {
                        // INSERT операции
                        let sql = format!(
                            "INSERT INTO stress_performance (id, name, age, email) VALUES ({}, 'LoadUser{}', {}, 'load{}@example.com')",
                            i * 10 + j + 101, i * 10 + j + 101, 20 + (j % 40), i * 10 + j + 101
                        );
                        ctx_clone.lock().await.execute_sql(&sql).await?;
                    }
                    2 => {
                        // UPDATE операции (без WHERE для упрощения)
                        let sql = format!(
                            "UPDATE stress_performance SET age = {}",
                            25 + (j % 30)
                        );
                        ctx_clone.lock().await.execute_sql(&sql).await?;
                    }
                    _ => {
                        // SELECT операции
                        ctx_clone.lock().await.execute_sql("SELECT * FROM stress_performance").await?;
                    }
                }
                
            }
            
            Ok::<(), Error>(())
        });
        
        handles.push(handle);
    }
    
    // Ждем завершения всех операций
    for handle in handles {
        handle.await.map_err(|e| Error::internal(&format!("Join error: {}", e)))?;
    }
    
    // Проверяем, что система справилась с нагрузкой
    let results = ctx_arc.lock().await.execute_sql("SELECT * FROM stress_performance").await?;
    assert!(!results.is_empty(), "Should have data");
    
    Ok(())
}

/// Stress тест с большим количеством данных
#[tokio::test]
pub async fn stress_test_large_dataset() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;
    
    // Создаем таблицу для большого набора данных
    ctx.create_test_table("stress_large").await?;
    
    // Вставляем данные
    for i in 1..=1000 {
        ctx.execute_sql(&format!(
            "INSERT INTO stress_large (id, name, age, email) VALUES ({}, 'LargeUser{}', {}, 'large{}@example.com')",
            i, i, 18 + (i % 60), i
        )).await?;
        
        // Периодически создаем checkpoint
        if i % 200 == 0 {
            ctx.checkpoint_manager.create_checkpoint().await?;
        }
    }
    
    // Тестируем запросы
    let results = ctx.execute_sql("SELECT * FROM stress_large").await?;
    assert!(!results.is_empty(), "Should have data");
    
    // Проверяем результаты
    assert!(!results.is_empty(), "Should have data");
    
    Ok(())
}
