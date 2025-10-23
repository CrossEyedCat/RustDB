//! Пример использования комплексной системы управления конкурентностью

use rustdb::core::concurrency::IsolationLevel as ConcIsolationLevel;
use rustdb::core::{
    ConcurrencyConfig, ConcurrencyManager, LockGranularity, ResourceType, RowKey, Timestamp,
    TransactionId,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Комплексный пример управления конкурентностью ===\n");

    // 1. Создание менеджера с настройками
    println!("1. Создание менеджера конкурентности");
    let config = ConcurrencyConfig {
        default_isolation_level: ConcIsolationLevel::ReadCommitted,
        default_lock_granularity: LockGranularity::Row,
        enable_mvcc: true,
        vacuum_interval: Duration::from_secs(30),
        ..Default::default()
    };
    let manager = ConcurrencyManager::new(config);
    println!("   ✓ Менеджер создан с MVCC и Row-level блокировками\n");

    // 2. Демонстрация MVCC - множественные версии
    println!("2. Создание множественных версий записи");
    let key = RowKey::new(1, 100); // Таблица 1, запись 100

    // Транзакция 1: создаёт первую версию
    let tx1 = TransactionId::new(1);
    let snapshot1 = manager.begin_transaction(tx1, ConcIsolationLevel::ReadCommitted)?;
    println!("   Транзакция {} начата", tx1);

    let data_v1 = b"Alice, age: 25, salary: 50000".to_vec();
    manager.write(tx1, key.clone(), data_v1).await?;
    println!("   ✓ Версия 1 создана транзакцией {}", tx1);

    manager.commit_transaction(tx1)?;
    println!("   ✓ Транзакция {} зафиксирована\n", tx1);

    // Транзакция 2: создаёт вторую версию
    let tx2 = TransactionId::new(2);
    manager.begin_transaction(tx2, ConcIsolationLevel::ReadCommitted)?;
    println!("   Транзакция {} начата", tx2);

    let data_v2 = b"Alice, age: 26, salary: 55000".to_vec();
    manager.write(tx2, key.clone(), data_v2).await?;
    println!("   ✓ Версия 2 создана транзакцией {}", tx2);

    manager.commit_transaction(tx2)?;
    println!("   ✓ Транзакция {} зафиксирована\n", tx2);

    // 3. Демонстрация изоляции транзакций
    println!("3. Демонстрация изоляции");

    // Транзакция 3 читает с snapshot до обновления
    let tx3 = TransactionId::new(3);
    manager.begin_transaction(tx3, ConcIsolationLevel::ReadCommitted)?;
    let old_data = manager.read(tx3, &key, snapshot1).await?;
    if let Some(data) = old_data {
        println!(
            "   TX3 читает старую версию: {:?}",
            String::from_utf8_lossy(&data)
        );
    }

    // Транзакция 4 читает с новым snapshot
    let tx4 = TransactionId::new(4);
    let snapshot2 = Timestamp::now();
    manager.begin_transaction(tx4, ConcIsolationLevel::ReadCommitted)?;
    let new_data = manager.read(tx4, &key, snapshot2).await?;
    if let Some(data) = new_data {
        println!(
            "   TX4 читает новую версию: {:?}",
            String::from_utf8_lossy(&data)
        );
    }
    println!();

    // 4. Демонстрация блокировок
    println!("4. Демонстрация блокировок");
    let tx5 = TransactionId::new(5);
    let resource = ResourceType::Record(1, 200);

    // Получаем exclusive блокировку
    manager
        .acquire_write_lock(tx5, resource.clone(), Some(Duration::from_millis(100)))
        .await?;
    println!("   ✓ Транзакция {} получила exclusive блокировку", tx5);

    // Транзакция 6 пытается получить ту же блокировку
    let tx6 = TransactionId::new(6);
    match manager
        .acquire_write_lock(tx6, resource.clone(), Some(Duration::from_millis(10)))
        .await
    {
        Ok(_) => println!("   Транзакция {} получила блокировку", tx6),
        Err(_) => println!(
            "   ✓ Транзакция {} не смогла получить блокировку (таймаут)",
            tx6
        ),
    }

    // Освобождаем блокировку
    manager.commit_transaction(tx5)?;
    println!("   ✓ Транзакция {} освободила блокировку\n", tx5);

    // 5. Демонстрация отката транзакции
    println!("5. Демонстрация отката транзакции");
    let tx7 = TransactionId::new(7);
    let key2 = RowKey::new(1, 101);
    let data_tx7 = b"Bob, age: 30".to_vec();

    manager.begin_transaction(tx7, ConcIsolationLevel::ReadCommitted)?;
    manager.write(tx7, key2.clone(), data_tx7).await?;
    println!("   Транзакция {} создала версию", tx7);

    manager.abort_transaction(tx7)?;
    println!("   ✓ Транзакция {} откачена (версия удалена)\n", tx7);

    // 6. Статистика
    println!("6. Статистика системы");
    let lock_stats = manager.get_lock_statistics();
    println!("   Блокировки:");
    println!("     Всего блокировок: {}", lock_stats.total_locks);
    println!("     Таймаутов: {}", lock_stats.lock_timeouts);
    println!(
        "     Deadlock'ов обнаружено: {}",
        lock_stats.deadlocks_detected
    );

    let mvcc_stats = manager.get_mvcc_statistics();
    println!("\n   MVCC:");
    println!("     Всего версий: {}", mvcc_stats.total_versions);
    println!("     Активных: {}", mvcc_stats.active_versions);
    println!("     Зафиксированных: {}", mvcc_stats.committed_versions);
    println!("     Откаченных: {}", mvcc_stats.aborted_versions);

    // 7. Очистка старых версий (VACUUM)
    println!("\n7. Очистка старых версий (VACUUM)");
    manager.update_min_active_transaction(TransactionId::new(100));
    let cleaned = manager.vacuum()?;
    println!("   ✓ Очищено версий: {}", cleaned);

    let mvcc_stats = manager.get_mvcc_statistics();
    println!("   Версий после очистки: {}", mvcc_stats.total_versions);
    println!("   Операций VACUUM: {}", mvcc_stats.vacuum_operations);

    // 8. Демонстрация разных уровней изоляции
    println!("\n8. Уровни изоляции");
    println!("   Поддерживаемые уровни:");
    println!("     - ReadUncommitted (минимальная изоляция)");
    println!("     - ReadCommitted (по умолчанию)");
    println!("     - RepeatableRead (повторяемое чтение)");
    println!("     - Serializable (полная изоляция)");

    // 9. Демонстрация гранулярности блокировок
    println!("\n9. Гранулярность блокировок");
    println!("   Поддерживаемые уровни:");
    println!("     - Database (вся БД)");
    println!("     - Table (таблица)");
    println!("     - Page (страница)");
    println!("     - Row (строка) [текущий режим]");

    println!("\n=== Пример завершён успешно ===");
    println!("\n📝 Ключевые возможности:");
    println!("  ✓ MVCC для изоляции транзакций без блокировок чтения");
    println!("  ✓ Deadlock detection с автоматическим выбором жертвы");
    println!("  ✓ Timeout механизмы с автоматическим откатом");
    println!("  ✓ Гранулярные блокировки (Row/Page/Table/Database)");
    println!("  ✓ VACUUM для очистки старых версий");
    println!("  ✓ Поддержка всех стандартных уровней изоляции");

    Ok(())
}
