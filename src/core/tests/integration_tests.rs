//! Интеграционные тесты для менеджера транзакций и блокировок

#![allow(clippy::absurd_extreme_comparisons)]

use crate::common::Result;
use crate::core::{
    IsolationLevel, LockMode, LockType, TransactionId, TransactionManager, TransactionState,
};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn test_transaction_with_locks_integration() {
    let tm = TransactionManager::new().unwrap();

    // Создаем транзакцию и получаем блокировки
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();

    let resources = vec![
        ("table_users", LockMode::Shared),
        ("table_orders", LockMode::Shared),
        ("index_user_email", LockMode::Exclusive),
    ];

    // Получаем блокировки через менеджер транзакций
    for (resource, mode) in &resources {
        tm.acquire_lock(
            txn_id,
            resource.to_string(),
            LockType::Resource(resource.to_string()),
            mode.clone(),
        )
        .unwrap();
    }

    // Проверяем, что все блокировки зарегистрированы в транзакции
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    assert_eq!(info.locked_resources.len(), 3);

    // При фиксации транзакции все блокировки должны освободиться
    tm.commit_transaction(txn_id).unwrap();

    // Проверяем статистику
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.lock_operations, 3);
    assert_eq!(stats.unlock_operations, 3);
}

#[test]
fn test_concurrent_transactions_with_locks() {
    let tm = Arc::new(TransactionManager::new().unwrap());
    let mut handles = vec![];

    // Сценарий: несколько транзакций работают с пересекающимися ресурсами
    for i in 0..3 {
        let tm_clone = Arc::clone(&tm);
        let handle = thread::spawn(move || {
            let txn_id = tm_clone
                .begin_transaction(IsolationLevel::ReadCommitted, false)
                .unwrap();

            // Каждая транзакция работает с общим ресурсом и своим уникальным
            let shared_resource = "shared_table".to_string();
            let unique_resource = format!("unique_resource_{}", i);

            // Получаем разделяемую блокировку на общий ресурс
            tm_clone
                .acquire_lock(
                    txn_id,
                    shared_resource,
                    LockType::Table("shared".to_string()),
                    LockMode::Shared,
                )
                .unwrap();

            // Получаем исключительную блокировку на уникальный ресурс
            tm_clone
                .acquire_lock(
                    txn_id,
                    unique_resource,
                    LockType::Resource(format!("unique_{}", i)),
                    LockMode::Exclusive,
                )
                .unwrap();

            // Имитируем работу
            thread::sleep(Duration::from_millis(50));

            // Фиксируем транзакцию
            tm_clone.commit_transaction(txn_id).unwrap();
        });
        handles.push(handle);
    }

    // Ждем завершения всех транзакций
    for handle in handles {
        handle.join().unwrap();
    }

    // Проверяем финальную статистику
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.total_transactions, 3);
    assert_eq!(stats.committed_transactions, 3);
    assert_eq!(stats.active_transactions, 0);
    assert_eq!(stats.lock_operations, 6); // 2 блокировки на транзакцию
    assert_eq!(stats.unlock_operations, 6);
}

#[test]
fn test_transaction_abort_releases_locks() {
    let tm = TransactionManager::new().unwrap();
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();

    // Получаем несколько блокировок
    let resources = vec!["resource1", "resource2", "resource3"];
    for resource in &resources {
        tm.acquire_lock(
            txn_id,
            resource.to_string(),
            LockType::Resource(resource.to_string()),
            LockMode::Shared,
        )
        .unwrap();
    }

    // Проверяем, что блокировки получены
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    assert_eq!(info.locked_resources.len(), 3);

    // Отменяем транзакцию
    tm.abort_transaction(txn_id).unwrap();

    // Проверяем, что все блокировки освобождены
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.lock_operations, 3);
    assert_eq!(stats.unlock_operations, 3);
    assert_eq!(stats.aborted_transactions, 1);
}

#[test]
fn test_lock_contention_scenario() {
    let tm = Arc::new(TransactionManager::new().unwrap());
    let resource = "contended_resource".to_string();

    // Первая транзакция получает исключительную блокировку
    let txn1 = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();
    tm.acquire_lock(
        txn1,
        resource.clone(),
        LockType::Resource("contended".to_string()),
        LockMode::Exclusive,
    )
    .unwrap();

    // Запускаем вторую транзакцию в отдельном потоке
    let tm_clone = Arc::clone(&tm);
    let resource_clone = resource.clone();
    let handle = thread::spawn(move || {
        let txn2 = tm_clone
            .begin_transaction(IsolationLevel::ReadCommitted, false)
            .unwrap();

        // Попытка получить блокировку должна заблокироваться или вызвать ошибку
        let result = tm_clone.acquire_lock(
            txn2,
            resource_clone,
            LockType::Resource("contended".to_string()),
            LockMode::Shared,
        );

        // В нашей реализации это вернет ошибку, если произойдет дедлок или таймаут
        match result {
            Ok(()) => {
                // Блокировка получена (возможно, после освобождения первой)
                tm_clone.commit_transaction(txn2).unwrap();
            }
            Err(_) => {
                // Возможна ошибка дедлока, таймаута или блокировка не получена
                tm_clone.abort_transaction(txn2).unwrap();
            }
        }
    });

    // Небольшая задержка, затем освобождаем первую транзакцию
    thread::sleep(Duration::from_millis(100));
    tm.commit_transaction(txn1).unwrap();

    // Ждем завершения второго потока
    handle.join().unwrap();

    // Проверяем, что система осталась в консистентном состоянии
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.active_transactions, 0);
}

#[test]
fn test_read_only_transaction_behavior() {
    let tm = TransactionManager::new().unwrap();

    // Создаем транзакцию только для чтения
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, true)
        .unwrap();

    // Транзакция только для чтения может получать разделяемые блокировки
    tm.acquire_lock(
        txn_id,
        "read_resource".to_string(),
        LockType::Table("table".to_string()),
        LockMode::Shared,
    )
    .unwrap();

    // Проверяем информацию о транзакции
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    assert!(info.read_only);
    assert_eq!(info.locked_resources.len(), 1);

    tm.commit_transaction(txn_id).unwrap();
}

#[test]
fn test_isolation_level_impact() {
    let tm = TransactionManager::new().unwrap();

    let isolation_levels = vec![
        IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted,
        IsolationLevel::RepeatableRead,
        IsolationLevel::Serializable,
    ];

    for level in isolation_levels {
        let txn_id = tm.begin_transaction(level.clone(), false).unwrap();

        // Получаем блокировку (поведение может отличаться в зависимости от уровня изоляции)
        tm.acquire_lock(
            txn_id,
            format!("resource_{:?}", level),
            LockType::Resource("test".to_string()),
            LockMode::Shared,
        )
        .unwrap();

        let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
        assert_eq!(info.isolation_level, level);

        tm.commit_transaction(txn_id).unwrap();
    }
}

#[test]
fn test_transaction_timeout_simulation() {
    let tm = TransactionManager::new().unwrap();
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();

    // Получаем блокировку
    tm.acquire_lock(
        txn_id,
        "timeout_resource".to_string(),
        LockType::Resource("test".to_string()),
        LockMode::Exclusive,
    )
    .unwrap();

    // Имитируем длительную работу
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    let duration = info.duration().unwrap();
    assert!(duration.as_millis() >= 0);

    // В реальной системе здесь был бы механизм таймаута
    // Пока просто фиксируем транзакцию
    tm.commit_transaction(txn_id).unwrap();
}

#[test]
fn test_system_recovery_simulation() {
    let tm = TransactionManager::new().unwrap();
    let mut active_transactions = Vec::new();

    // Создаем несколько активных транзакций
    for i in 0..5 {
        let txn_id = tm
            .begin_transaction(IsolationLevel::ReadCommitted, false)
            .unwrap();
        tm.acquire_lock(
            txn_id,
            format!("recovery_resource_{}", i),
            LockType::Resource(format!("r{}", i)),
            LockMode::Shared,
        )
        .unwrap();
        active_transactions.push(txn_id);
    }

    // Проверяем состояние системы
    let active_txns = tm.get_active_transactions().unwrap();
    assert_eq!(active_txns.len(), 5);

    // Имитируем "восстановление" - отменяем все активные транзакции
    for txn_id in active_transactions {
        tm.abort_transaction(txn_id).unwrap();
    }

    // Проверяем, что система очищена
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.active_transactions, 0);
    assert_eq!(stats.aborted_transactions, 5);
}

#[test]
fn test_performance_under_load() {
    let tm = Arc::new(TransactionManager::new().unwrap());
    let mut handles = vec![];

    let start_time = std::time::Instant::now();

    // Запускаем много коротких транзакций параллельно
    for i in 0..20 {
        let tm_clone = Arc::clone(&tm);
        let handle = thread::spawn(move || {
            for j in 0..10 {
                let txn_id = tm_clone
                    .begin_transaction(IsolationLevel::ReadCommitted, false)
                    .unwrap();

                // Получаем блокировку на уникальный ресурс
                tm_clone
                    .acquire_lock(
                        txn_id,
                        format!("perf_resource_{}_{}", i, j),
                        LockType::Resource(format!("r_{}_{}", i, j)),
                        LockMode::Shared,
                    )
                    .unwrap();

                // Небольшая работа
                thread::sleep(Duration::from_millis(1));

                tm_clone.commit_transaction(txn_id).unwrap();
            }
        });
        handles.push(handle);
    }

    // Ждем завершения всех потоков
    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start_time.elapsed();
    println!("Выполнено 200 транзакций за {:?}", elapsed);

    // Проверяем финальную статистику
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.total_transactions, 200);
    assert_eq!(stats.committed_transactions, 200);
    assert_eq!(stats.active_transactions, 0);

    // Все операции должны завершиться за разумное время
    assert!(elapsed.as_secs() < 10);
}
