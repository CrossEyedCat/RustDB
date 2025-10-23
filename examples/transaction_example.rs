//! Пример использования менеджера транзакций rustdb
//!
//! Демонстрирует основные возможности системы транзакций:
//! - Создание и управление транзакциями
//! - Работа с блокировками
//! - Обработка конкурентного доступа
//! - Статистика и мониторинг

use rustdb::core::{
    IsolationLevel, LockMode, LockType, TransactionManager, TransactionManagerConfig,
};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn main() {
    println!("🚀 Демонстрация менеджера транзакций rustdb\n");

    // Демонстрация базовых операций
    basic_transaction_operations();

    // Демонстрация работы с блокировками
    lock_operations_demo();

    // Демонстрация конкурентного доступа
    concurrent_transactions_demo();

    // Демонстрация различных уровней изоляции
    isolation_levels_demo();

    // Демонстрация статистики
    statistics_demo();

    println!("✅ Демонстрация завершена успешно!");
}

fn basic_transaction_operations() {
    println!("📋 1. Базовые операции с транзакциями");
    println!("=====================================");

    let tm = TransactionManager::new().unwrap();

    // Создание транзакции
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();
    println!("   ✓ Создана транзакция: {}", txn_id);

    // Получение информации о транзакции
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    println!("   ✓ Уровень изоляции: {:?}", info.isolation_level);
    println!("   ✓ Только для чтения: {}", info.read_only);
    println!("   ✓ Состояние: {:?}", info.state);

    // Фиксация транзакции
    tm.commit_transaction(txn_id).unwrap();
    println!("   ✓ Транзакция зафиксирована\n");
}

fn lock_operations_demo() {
    println!("🔒 2. Операции с блокировками");
    println!("=============================");

    let tm = TransactionManager::new().unwrap();
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();

    // Получение различных типов блокировок
    let resources = vec![
        (
            "users_table",
            LockType::Table("users".to_string()),
            LockMode::Shared,
        ),
        (
            "user_email_index",
            LockType::Index("idx_user_email".to_string()),
            LockMode::Shared,
        ),
        ("user_record_1", LockType::Record(1, 1), LockMode::Exclusive),
        (
            "temp_resource",
            LockType::Resource("temp".to_string()),
            LockMode::Exclusive,
        ),
    ];

    for (name, lock_type, lock_mode) in &resources {
        tm.acquire_lock(
            txn_id,
            name.to_string(),
            lock_type.clone(),
            lock_mode.clone(),
        )
        .unwrap();
        println!("   ✓ Получена блокировка {:?} на {}", lock_mode, name);
    }

    // Проверка заблокированных ресурсов
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    println!(
        "   ✓ Всего заблокировано ресурсов: {}",
        info.locked_resources.len()
    );

    // Освобождение одной блокировки
    tm.release_lock(txn_id, "temp_resource".to_string())
        .unwrap();
    println!("   ✓ Освобождена блокировка temp_resource");

    // При фиксации все остальные блокировки освобождаются автоматически
    tm.commit_transaction(txn_id).unwrap();
    println!("   ✓ Все блокировки освобождены при фиксации\n");
}

fn concurrent_transactions_demo() {
    println!("🔄 3. Конкурентные транзакции");
    println!("=============================");

    let tm = Arc::new(TransactionManager::new().unwrap());
    let mut handles = vec![];

    // Запускаем несколько параллельных транзакций
    for i in 0..4 {
        let tm_clone = Arc::clone(&tm);
        let handle = thread::spawn(move || {
            let txn_id = tm_clone
                .begin_transaction(IsolationLevel::ReadCommitted, false)
                .unwrap();
            println!("   🔵 Поток {}: Начата транзакция {}", i, txn_id);

            // Каждая транзакция работает со своими ресурсами
            let shared_resource = "shared_data".to_string();
            let unique_resource = format!("data_partition_{}", i);

            // Получаем разделяемую блокировку на общий ресурс
            tm_clone
                .acquire_lock(
                    txn_id,
                    shared_resource,
                    LockType::Resource("shared".to_string()),
                    LockMode::Shared,
                )
                .unwrap();

            // Получаем исключительную блокировку на уникальный ресурс
            tm_clone
                .acquire_lock(
                    txn_id,
                    unique_resource.clone(),
                    LockType::Resource(format!("partition_{}", i)),
                    LockMode::Exclusive,
                )
                .unwrap();

            println!("   🟢 Поток {}: Получены блокировки", i);

            // Имитируем работу
            thread::sleep(Duration::from_millis(100));

            // Фиксируем транзакцию
            tm_clone.commit_transaction(txn_id).unwrap();
            println!("   ✅ Поток {}: Транзакция зафиксирована", i);
        });
        handles.push(handle);
    }

    // Ждем завершения всех потоков
    for handle in handles {
        handle.join().unwrap();
    }

    println!("   ✓ Все конкурентные транзакции завершены\n");
}

fn isolation_levels_demo() {
    println!("🎯 4. Уровни изоляции");
    println!("====================");

    let tm = TransactionManager::new().unwrap();

    let levels = vec![
        (
            IsolationLevel::ReadUncommitted,
            "Чтение незафиксированных данных",
        ),
        (
            IsolationLevel::ReadCommitted,
            "Чтение зафиксированных данных",
        ),
        (IsolationLevel::RepeatableRead, "Повторяемое чтение"),
        (IsolationLevel::Serializable, "Сериализуемость"),
    ];

    for (level, description) in levels {
        let txn_id = tm.begin_transaction(level.clone(), false).unwrap();
        println!("   ✓ {:?}: {}", level, description);

        // Получаем блокировку (поведение может отличаться в зависимости от уровня)
        tm.acquire_lock(
            txn_id,
            format!("resource_{:?}", level),
            LockType::Resource("demo".to_string()),
            LockMode::Shared,
        )
        .unwrap();

        tm.commit_transaction(txn_id).unwrap();
    }

    println!("   ✓ Продемонстрированы все уровни изоляции\n");
}

fn statistics_demo() {
    println!("📊 5. Статистика и мониторинг");
    println!("=============================");

    // Создаем менеджер с ограниченной конфигурацией для демонстрации
    let config = TransactionManagerConfig {
        max_concurrent_transactions: 10,
        lock_timeout_ms: 5000,
        deadlock_detection_interval_ms: 500,
        max_idle_time_seconds: 1800,
        enable_deadlock_detection: true,
    };

    let tm = TransactionManager::with_config(config).unwrap();

    println!("   📋 Конфигурация:");
    let cfg = tm.get_config();
    println!(
        "      • Макс. одновременных транзакций: {}",
        cfg.max_concurrent_transactions
    );
    println!("      • Таймаут блокировки: {} мс", cfg.lock_timeout_ms);
    println!(
        "      • Обнаружение дедлоков: {}",
        cfg.enable_deadlock_detection
    );

    // Выполняем несколько операций для сбора статистики
    let mut transaction_ids = Vec::new();

    // Создаем несколько транзакций
    for i in 0..5 {
        let read_only = i % 2 == 0;
        let txn_id = tm
            .begin_transaction(IsolationLevel::ReadCommitted, read_only)
            .unwrap();
        transaction_ids.push(txn_id);

        // Получаем блокировки
        tm.acquire_lock(
            txn_id,
            format!("resource_{}", i),
            LockType::Resource(format!("r{}", i)),
            LockMode::Shared,
        )
        .unwrap();
    }

    // Фиксируем часть транзакций, отменяем остальные
    for (i, &txn_id) in transaction_ids.iter().enumerate() {
        if i % 2 == 0 {
            tm.commit_transaction(txn_id).unwrap();
        } else {
            tm.abort_transaction(txn_id).unwrap();
        }
    }

    // Показываем статистику
    let stats = tm.get_statistics().unwrap();
    println!("\n   📈 Статистика:");
    println!("      • Всего транзакций: {}", stats.total_transactions);
    println!("      • Активных транзакций: {}", stats.active_transactions);
    println!("      • Зафиксированных: {}", stats.committed_transactions);
    println!("      • Отмененных: {}", stats.aborted_transactions);
    println!("      • Операций блокирования: {}", stats.lock_operations);
    println!(
        "      • Операций разблокирования: {}",
        stats.unlock_operations
    );
    println!("      • Обнаружено дедлоков: {}", stats.deadlocks_detected);

    println!("\n   ✓ Статистика собрана и отображена\n");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_example_runs_without_panic() {
        // Проверяем, что пример выполняется без паники
        basic_transaction_operations();
        lock_operations_demo();
        isolation_levels_demo();
        statistics_demo();
    }
}
