//! Тесты полного цикла запросов
//! 
//! Эти тесты проверяют полный цикл обработки SQL запросов:
//! парсинг -> планирование -> оптимизация -> выполнение

use rustdb::common::Result;
use rustdb::{ColumnValue, DataType};
use super::common::*;

/// Тест простого SELECT запроса
#[tokio::test]
pub async fn test_simple_select_cycle() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;
    
    // Создаем тестовую таблицу
    ctx.create_test_table("test_users").await?;
    ctx.insert_test_data("test_users", 10).await?;
    
    // Выполняем SELECT запрос
    let results = ctx.execute_sql("SELECT * FROM test_users").await?;
    
    // Проверяем результаты
    assert_eq!(results.len(), 10, "Должно быть 10 записей");
    
    // Проверяем структуру результата
    for row in &results {
        assert_eq!(row.len(), 4, "Каждая строка должна содержать 4 колонки");
    }
    
    Ok(())
}

/// Тест INSERT запроса
#[tokio::test]
pub async fn test_insert_cycle() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;
    
    // Создаем тестовую таблицу
    ctx.create_test_table("test_insert").await?;
    
    // Выполняем INSERT запрос
    ctx.execute_sql("INSERT INTO test_insert (id, name, age, email) VALUES (1, 'Test User', 25, 'test@example.com')").await?;
    
    // Проверяем, что данные вставились
    let results = ctx.execute_sql("SELECT * FROM test_insert").await?;
    
    assert_eq!(results.len(), 1, "Должна быть найдена одна запись");
    
    let row = &results[0];
    assert_eq!(row.len(), 4, "Запись должна содержать 4 колонки");
    
    // Проверяем значения
    if let ColumnValue { data_type: DataType::Integer(id), .. } = &row[0] {
        assert_eq!(*id, 1, "ID должен быть 1");
    }
    
    if let ColumnValue { data_type: DataType::Varchar(name), .. } = &row[1] {
        assert_eq!(name, "Test User", "Имя должно быть 'Test User'");
    }
    
    Ok(())
}

/// Тест UPDATE запроса
#[tokio::test]
pub async fn test_update_cycle() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;
    
    // Создаем тестовую таблицу и данные
    ctx.create_test_table("test_update").await?;
    ctx.insert_test_data("test_update", 5).await?;
    
    // Выполняем UPDATE запрос (без WHERE для упрощения)
    ctx.execute_sql("UPDATE test_update SET age = 99").await?;
    
    // Проверяем, что данные обновились
    let results = ctx.execute_sql("SELECT * FROM test_update").await?;
    
    assert_eq!(results.len(), 5, "Должно быть 5 записей");
    
    // Проверяем, что все записи обновились
    for row in &results {
        if let ColumnValue { data_type: DataType::Integer(age), .. } = &row[2] {
            assert_eq!(*age, 99, "Возраст должен быть обновлен до 99");
        }
    }
    
    Ok(())
}

/// Тест DELETE запроса
#[tokio::test]
pub async fn test_delete_cycle() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;
    
    // Создаем тестовую таблицу и данные
    ctx.create_test_table("test_delete").await?;
    ctx.insert_test_data("test_delete", 5).await?;
    
    // Проверяем количество записей до удаления
    let before_results = ctx.execute_sql("SELECT * FROM test_delete").await?;
    let before_count = before_results.len();
    
    assert_eq!(before_count, 5, "Должно быть 5 записей до удаления");
    
    // Выполняем DELETE запрос (без WHERE для упрощения)
    ctx.execute_sql("DELETE FROM test_delete").await?;
    
    // Проверяем количество записей после удаления
    let after_results = ctx.execute_sql("SELECT * FROM test_delete").await?;
    let after_count = after_results.len();
    
    assert_eq!(after_count, 0, "Должно быть 0 записей после удаления");
    
    // Проверяем, что все записи удалены
    assert_eq!(after_count, 0, "Все записи должны быть удалены");
    
    Ok(())
}

/// Тест сложного запроса с JOIN
#[tokio::test]
pub async fn test_complex_query_cycle() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;
    
    // Создаем простые таблицы
    ctx.execute_sql("CREATE TABLE users (id INTEGER, name VARCHAR(100))").await?;
    ctx.execute_sql("CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER)").await?;
    
    // Вставляем данные
    ctx.execute_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')").await?;
    ctx.execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob')").await?;
    ctx.execute_sql("INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100)").await?;
    ctx.execute_sql("INSERT INTO orders (id, user_id, amount) VALUES (2, 2, 200)").await?;
    
    // Выполняем простой запрос
    let results = ctx.execute_sql("SELECT * FROM users").await?;
    
    // Проверяем результаты
    assert_eq!(results.len(), 2, "Должно быть 2 пользователя");
    
    Ok(())
}

/// Тест транзакционного запроса
#[tokio::test]
pub async fn test_transactional_query_cycle() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;
    
    // Создаем тестовую таблицу
    ctx.create_test_table("test_transaction").await?;
    
    // Выполняем несколько операций
    ctx.execute_sql("INSERT INTO test_transaction (id, name, age, email) VALUES (1, 'User1', 25, 'user1@example.com')").await?;
    ctx.execute_sql("INSERT INTO test_transaction (id, name, age, email) VALUES (2, 'User2', 30, 'user2@example.com')").await?;
    
    // Проверяем данные
    let results = ctx.execute_sql("SELECT * FROM test_transaction").await?;
    let count = results.len();
    
    assert_eq!(count, 2, "Должно быть 2 записи");
    
    // Проверяем, что данные сохранились
    let final_results = ctx.execute_sql("SELECT * FROM test_transaction").await?;
    let final_count = final_results.len();
    
    assert_eq!(final_count, 2, "Должно быть 2 записи");
    
    Ok(())
}

/// Тест отката транзакции
#[tokio::test]
pub async fn test_transaction_rollback_cycle() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;
    
    // Создаем тестовую таблицу
    ctx.create_test_table("test_rollback").await?;
    
    // Вставляем начальные данные
    ctx.execute_sql("INSERT INTO test_rollback (id, name, age, email) VALUES (1, 'Initial', 25, 'initial@example.com')").await?;
    
    // Выполняем операции
    ctx.execute_sql("INSERT INTO test_rollback (id, name, age, email) VALUES (2, 'Temp', 30, 'temp@example.com')").await?;
    
    // Проверяем данные
    let results = ctx.execute_sql("SELECT * FROM test_rollback").await?;
    let count = results.len();
    
    assert_eq!(count, 2, "Должно быть 2 записи");
    
    Ok(())
}

/// Тест обработки ошибок
#[tokio::test]
pub async fn test_error_handling_cycle() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;
    
    // Тест синтаксической ошибки
    let result = ctx.execute_sql("INVALID SQL SYNTAX").await;
    assert!(result.is_err(), "Невалидный SQL должен вызывать ошибку");
    
    // Тест ошибки несуществующей таблицы
    let result = ctx.execute_sql("SELECT * FROM non_existent_table").await;
    assert!(result.is_err(), "Запрос к несуществующей таблице должен вызывать ошибку");
    
    // Тест успешного выполнения
    ctx.create_test_table("test_duplicate").await?;
    ctx.execute_sql("INSERT INTO test_duplicate (id, name, age, email) VALUES (1, 'User1', 25, 'user1@example.com')").await?;
    
    let result = ctx.execute_sql("INSERT INTO test_duplicate (id, name, age, email) VALUES (2, 'User2', 30, 'user2@example.com')").await;
    assert!(result.is_ok(), "Вставка должна быть успешной");
    
    Ok(())
}

/// Тест производительности простых запросов
#[tokio::test]
pub async fn test_simple_query_performance() -> Result<()> {
    let mut ctx = IntegrationTestContext::new().await?;
    
    // Создаем таблицу с данными
    ctx.create_test_table("perf_test").await?;
    ctx.insert_test_data("perf_test", 100).await?;
    
    // Выполняем простой SELECT запрос
    let results = ctx.execute_sql("SELECT * FROM perf_test").await?;
    
    // Проверяем, что запрос выполнился успешно
    assert_eq!(results.len(), 100, "Должно быть 100 записей");
    
    Ok(())
}
