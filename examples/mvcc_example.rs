//! Пример использования MVCC (Multi-Version Concurrency Control)

use rustdb::core::{
    MVCCManager, RowKey, Timestamp,
    TransactionId,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Пример работы MVCC системы ===\n");
    
    // 1. Создание MVCC менеджера
    println!("1. Создание MVCC менеджера");
    let mvcc = MVCCManager::new();
    println!("   ✓ MVCC менеджер создан\n");
    
    // 2. Создание версий записей
    println!("2. Создание версий записей");
    let key = RowKey::new(1, 100); // Таблица 1, запись 100
    let tx1 = TransactionId::new(1);
    
    let data_v1 = b"Alice, age: 25".to_vec();
    let version1 = mvcc.create_version(key.clone(), tx1, data_v1.clone())?;
    println!("   ✓ Версия {} создана транзакцией {}", version1, tx1);
    
    // 3. Фиксация транзакции
    println!("\n3. Фиксация транзакции");
    mvcc.commit_transaction(tx1)?;
    println!("   ✓ Транзакция {} зафиксирована", tx1);
    
    // 4. Чтение версии
    println!("\n4. Чтение версии");
    let snapshot = Timestamp::now();
    let read_data = mvcc.read_version(&key, tx1, snapshot)?;
    if let Some(data) = read_data {
        println!("   ✓ Прочитано: {:?}", String::from_utf8_lossy(&data));
    }
    
    // 5. Обновление записи (создание новой версии)
    println!("\n5. Обновление записи (создание новой версии)");
    let tx2 = TransactionId::new(2);
    let data_v2 = b"Alice, age: 26".to_vec();
    let version2 = mvcc.create_version(key.clone(), tx2, data_v2.clone())?;
    println!("   ✓ Версия {} создана транзакцией {}", version2, tx2);
    
    // 6. Демонстрация изоляции
    println!("\n6. Демонстрация изоляции транзакций");
    println!("   Транзакция 3 читает данные (видит старую версию):");
    let tx3 = TransactionId::new(3);
    let old_snapshot = snapshot;
    let old_data = mvcc.read_version(&key, tx3, old_snapshot)?;
    if let Some(data) = old_data {
        println!("   ✓ Старая версия: {:?}", String::from_utf8_lossy(&data));
    }
    
    println!("\n   Фиксируем транзакцию 2:");
    mvcc.commit_transaction(tx2)?;
    println!("   ✓ Транзакция {} зафиксирована", tx2);
    
    println!("\n   Транзакция 4 читает данные (видит новую версию):");
    let tx4 = TransactionId::new(4);
    let new_snapshot = Timestamp::now();
    let new_data = mvcc.read_version(&key, tx4, new_snapshot)?;
    if let Some(data) = new_data {
        println!("   ✓ Новая версия: {:?}", String::from_utf8_lossy(&data));
    }
    
    // 7. Статистика
    println!("\n7. Статистика MVCC");
    let stats = mvcc.get_statistics();
    println!("   Всего версий: {}", stats.total_versions);
    println!("   Активных: {}", stats.active_versions);
    println!("   Зафиксированных: {}", stats.committed_versions);
    println!("   Количество версий для записи: {}", mvcc.get_version_count(&key));
    
    // 8. Удаление записи
    println!("\n8. Удаление записи");
    let tx5 = TransactionId::new(5);
    mvcc.delete_version(&key, tx5)?;
    println!("   ✓ Запись помечена для удаления транзакцией {}", tx5);
    
    let stats = mvcc.get_statistics();
    println!("   Помечено для удаления: {}", stats.marked_for_deletion);
    
    // 9. Откат транзакции
    println!("\n9. Откат транзакции");
    let tx6 = TransactionId::new(6);
    let data_v3 = b"Bob, age: 30".to_vec();
    mvcc.create_version(RowKey::new(1, 101), tx6, data_v3)?;
    println!("   Версия создана транзакцией {}", tx6);
    
    mvcc.abort_transaction(tx6)?;
    println!("   ✓ Транзакция {} откачена", tx6);
    
    let stats = mvcc.get_statistics();
    println!("   Откаченных версий: {}", stats.aborted_versions);
    
    // 10. Очистка (VACUUM)
    println!("\n10. Очистка старых версий (VACUUM)");
    mvcc.update_min_active_transaction(TransactionId::new(100));
    let cleaned = mvcc.vacuum()?;
    println!("   ✓ Очищено версий: {}", cleaned);
    
    let stats = mvcc.get_statistics();
    println!("   Всего версий после очистки: {}", stats.total_versions);
    println!("   Операций VACUUM: {}", stats.vacuum_operations);
    println!("   Всего очищено: {}", stats.versions_cleaned);
    
    // 11. Итоговая статистика
    println!("\n11. Итоговая статистика");
    println!("{:#?}", stats);
    
    println!("\n=== Пример завершён успешно ===");
    
    Ok(())
}


