// Пример демонстрации исправленных ACID тестов
use rustdb::core::advanced_lock_manager::{AdvancedLockConfig, AdvancedLockManager};
use rustdb::core::concurrency::{ConcurrencyConfig, ConcurrencyManager};
use rustdb::core::{AdvancedLockMode, ResourceType, RowKey, Timestamp, TransactionId};
use std::sync::Arc;
use std::time::Duration;
// removed redundant single-component import

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Демонстрация исправленных ACID тестов");

    // Создаем менеджер блокировок
    let config = AdvancedLockConfig::default();
    let lock_manager = Arc::new(AdvancedLockManager::new(config));

    // Создаем менеджер конкурентности
    let concurrency_config = ConcurrencyConfig::default();
    let concurrency_manager = ConcurrencyManager::new(concurrency_config);

    // Тестируем базовую функциональность
    let tx1 = TransactionId(1);
    let tx2 = TransactionId(2);
    let resource = ResourceType::Record(1, 100);

    println!("✅ Тест 1: Получение эксклюзивной блокировки");
    let result1 = lock_manager
        .acquire_lock(
            tx1,
            resource.clone(),
            AdvancedLockMode::Exclusive,
            Some(Duration::from_millis(100)),
        )
        .await;

    match result1 {
        Ok(_) => println!("   ✅ Блокировка получена успешно"),
        Err(e) => println!("   ❌ Ошибка получения блокировки: {}", e),
    }

    println!("✅ Тест 2: Попытка получить конфликтующую блокировку");
    let result2 = lock_manager
        .acquire_lock(
            tx2,
            resource.clone(),
            AdvancedLockMode::Exclusive,
            Some(Duration::from_millis(50)),
        )
        .await;

    match result2 {
        Ok(_) => println!("   ❌ Блокировка получена (неожиданно)"),
        Err(_) => println!("   ✅ Блокировка корректно отклонена (конфликт)"),
    }

    println!("✅ Тест 3: Освобождение блокировки");
    let result3 = lock_manager.release_lock(tx1, resource.clone());
    match result3 {
        Ok(_) => println!("   ✅ Блокировка освобождена успешно"),
        Err(e) => println!("   ❌ Ошибка освобождения блокировки: {}", e),
    }

    println!("✅ Тест 4: Получение блокировки после освобождения");
    let result4 = lock_manager
        .acquire_lock(
            tx2,
            resource.clone(),
            AdvancedLockMode::Exclusive,
            Some(Duration::from_millis(100)),
        )
        .await;

    match result4 {
        Ok(_) => println!("   ✅ Блокировка получена после освобождения"),
        Err(e) => println!("   ❌ Ошибка получения блокировки: {}", e),
    }

    // Тестируем ConcurrencyManager
    println!("✅ Тест 5: ConcurrencyManager - запись");
    let row_key = RowKey {
        table_id: 1,
        row_id: 100,
    };
    let write_result = concurrency_manager
        .write(tx1, row_key.clone(), b"test data".to_vec())
        .await;

    match write_result {
        Ok(_) => println!("   ✅ Запись выполнена успешно"),
        Err(e) => println!("   ❌ Ошибка записи: {}", e),
    }

    println!("✅ Тест 6: ConcurrencyManager - чтение");
    let read_result = concurrency_manager
        .read(tx2, &row_key, Timestamp::now())
        .await;

    match read_result {
        Ok(data) => println!("   ✅ Чтение выполнено успешно: {:?}", data),
        Err(e) => println!("   ❌ Ошибка чтения: {}", e),
    }

    println!("\n🎉 Все тесты завершены!");
    println!("📊 Статистика:");
    let stats = lock_manager.get_statistics();
    println!("   - Всего блокировок: {}", stats.total_locks);
    println!("   - Ожидающих транзакций: {}", stats.waiting_transactions);
    println!("   - Deadlock'ов обнаружено: {}", stats.deadlocks_detected);

    Ok(())
}
