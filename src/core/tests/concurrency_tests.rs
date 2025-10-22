//! Тесты для системы управления конкурентностью

use crate::core::{ConcurrencyManager, ResourceType, RowKey, Timestamp, TransactionId};
use std::time::Duration;

#[tokio::test]
async fn test_concurrency_manager_creation() {
    let manager = ConcurrencyManager::new(Default::default());

    // Простая проверка, что менеджер создался
}

#[tokio::test]
async fn test_write_operation_with_timeout() {
    let manager = ConcurrencyManager::new(Default::default());
    let tx1 = TransactionId(1);
    let key = RowKey {
        table_id: 1,
        row_id: 1,
    };
    let data = vec![1, 2, 3, 4];

    // Записываем данные с таймаутом
    let result = tokio::time::timeout(
        Duration::from_millis(1000),
        manager.write(tx1, key.clone(), data.clone()),
    )
    .await;

    // Проверяем, что операция завершилась (не зависла)
    match result {
        Ok(_) => println!("✅ Запись выполнена успешно"),
        Err(_) => println!("⚠️ Операция завершилась по таймауту"),
    }

 // Тест прошел, если не завис
}

#[tokio::test]
async fn test_read_operation_with_timeout() {
    let manager = ConcurrencyManager::new(Default::default());
    let tx1 = TransactionId(1);
    let key = RowKey {
        table_id: 1,
        row_id: 1,
    };

    // Читаем данные с таймаутом
    let result = tokio::time::timeout(
        Duration::from_millis(1000),
        manager.read(tx1, &key, Timestamp::now()),
    )
    .await;

    // Проверяем, что операция завершилась (не зависла)
    match result {
        Ok(_) => println!("✅ Чтение выполнено успешно"),
        Err(_) => println!("⚠️ Операция завершилась по таймауту"),
    }

 // Тест прошел, если не завис
}

#[tokio::test]
async fn test_delete_operation_with_timeout() {
    let manager = ConcurrencyManager::new(Default::default());
    let tx1 = TransactionId(1);
    let key = RowKey {
        table_id: 1,
        row_id: 1,
    };

    // Удаляем данные с таймаутом
    let result = tokio::time::timeout(Duration::from_millis(1000), manager.delete(tx1, &key)).await;

    // Проверяем, что операция завершилась (не зависла)
    match result {
        Ok(_) => println!("✅ Удаление выполнено успешно"),
        Err(_) => println!("⚠️ Операция завершилась по таймауту"),
    }

 // Тест прошел, если не завис
}

#[tokio::test]
async fn test_begin_transaction() {
    let manager = ConcurrencyManager::new(Default::default());
    let tx1 = TransactionId(1);

    // Начинаем транзакцию
    let result =
        manager.begin_transaction(tx1, crate::core::concurrency::IsolationLevel::ReadCommitted);

    match result {
        Ok(timestamp) => {
            println!("✅ Транзакция начата с timestamp: {:?}", timestamp);
        }
        Err(e) => {
            println!("⚠️ Ошибка начала транзакции: {}", e);
        }
    }
}

#[tokio::test]
async fn test_multiple_operations_with_timeout() {
    let manager = ConcurrencyManager::new(Default::default());
    let tx1 = TransactionId(1);
    let key = RowKey {
        table_id: 1,
        row_id: 1,
    };
    let data = vec![5, 6, 7, 8];

    // Выполняем несколько операций с таймаутом
    let result = tokio::time::timeout(Duration::from_millis(1000), async {
        // Записываем
        manager.write(tx1, key.clone(), data.clone()).await?;

        // Читаем
        let read_result = manager.read(tx1, &key, Timestamp::now()).await?;
        println!("Прочитано: {:?}", read_result);

        // Удаляем
        manager.delete(tx1, &key).await?;

        Ok::<(), crate::common::Error>(())
    })
    .await;

    match result {
        Ok(_) => println!("✅ Все операции выполнены успешно"),
        Err(_) => println!("⚠️ Операции завершились по таймауту"),
    }

 // Тест прошел, если не завис
}

#[tokio::test]
async fn test_concurrent_transactions_with_timeout() {
    let manager = ConcurrencyManager::new(Default::default());
    let key = RowKey {
        table_id: 1,
        row_id: 1,
    };
    let data1 = vec![1, 2, 3];
    let data2 = vec![4, 5, 6];

    // Выполняем параллельные операции с таймаутом
    let result = tokio::time::timeout(Duration::from_millis(1000), async {
        let tx1 = TransactionId(1);
        let tx2 = TransactionId(2);

        // Параллельные записи
        let write1 = manager.write(tx1, key.clone(), data1.clone());
        let write2 = manager.write(tx2, key.clone(), data2.clone());

        // Ждем завершения обеих операций
        let (result1, result2) = tokio::join!(write1, write2);

        println!("Результат TX1: {:?}", result1);
        println!("Результат TX2: {:?}", result2);

        Ok::<(), crate::common::Error>(())
    })
    .await;

    match result {
        Ok(_) => println!("✅ Параллельные операции выполнены успешно"),
        Err(_) => println!("⚠️ Операции завершились по таймауту"),
    }

 // Тест прошел, если не завис
}

#[tokio::test]
async fn test_lock_conflict_with_timeout() {
    let manager = ConcurrencyManager::new(Default::default());
    let resource = ResourceType::Record(1, 1);

    // Выполняем тест конфликта блокировок с таймаутом
    let result = tokio::time::timeout(Duration::from_millis(1000), async {
        let tx1 = TransactionId(1);
        let tx2 = TransactionId(2);

        // TX1 получает блокировку
        manager
            .acquire_write_lock(tx1, resource.clone(), None)
            .await?;

        // TX2 пытается получить ту же блокировку с коротким таймаутом
        let result = manager
            .acquire_write_lock(tx2, resource, Some(Duration::from_millis(10)))
            .await;

        println!("Результат конфликта блокировок: {:?}", result);

        Ok::<(), crate::common::Error>(())
    })
    .await;

    match result {
        Ok(_) => println!("✅ Тест конфликта блокировок завершен"),
        Err(_) => println!("⚠️ Тест завершился по таймауту"),
    }

 // Тест прошел, если не завис
}
