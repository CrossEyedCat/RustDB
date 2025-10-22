//! Тесты для менеджера транзакций rustdb

use crate::common::Result;
use crate::core::{
    IsolationLevel, LockMode, LockType, TransactionId, TransactionManager,
    TransactionManagerConfig, TransactionState,
};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn test_transaction_manager_creation() {
    let tm = TransactionManager::new().unwrap();
    let config = tm.get_config();

    assert_eq!(config.max_concurrent_transactions, 1000);
    assert_eq!(config.lock_timeout_ms, 30000);
    assert!(config.enable_deadlock_detection);

    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.total_transactions, 0);
    assert_eq!(stats.active_transactions, 0);
}

#[test]
fn test_transaction_lifecycle() {
    let tm = TransactionManager::new().unwrap();

    // Начинаем транзакцию
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();

    // Проверяем, что транзакция создалась
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    assert_eq!(info.id, txn_id);
    assert_eq!(info.state, TransactionState::Active);
    assert_eq!(info.isolation_level, IsolationLevel::ReadCommitted);
    assert!(!info.read_only);

    // Проверяем статистику
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.total_transactions, 1);
    assert_eq!(stats.active_transactions, 1);

    // Фиксируем транзакцию
    tm.commit_transaction(txn_id).unwrap();

    // Проверяем, что транзакция удалена из активных
    assert!(tm.get_transaction_info(txn_id).unwrap().is_none());

    // Проверяем статистику
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.total_transactions, 1);
    assert_eq!(stats.active_transactions, 0);
    assert_eq!(stats.committed_transactions, 1);
}

#[test]
fn test_transaction_abort() {
    let tm = TransactionManager::new().unwrap();

    let txn_id = tm
        .begin_transaction(IsolationLevel::Serializable, true)
        .unwrap();

    // Отменяем транзакцию
    tm.abort_transaction(txn_id).unwrap();

    // Проверяем статистику
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.total_transactions, 1);
    assert_eq!(stats.active_transactions, 0);
    assert_eq!(stats.aborted_transactions, 1);
}

#[test]
fn test_multiple_transactions() {
    let tm = TransactionManager::new().unwrap();
    let mut txn_ids = Vec::new();

    // Создаем несколько транзакций
    for i in 0..5 {
        let read_only = i % 2 == 0;
        let isolation = if i % 2 == 0 {
            IsolationLevel::ReadCommitted
        } else {
            IsolationLevel::Serializable
        };
        let txn_id = tm.begin_transaction(isolation, read_only).unwrap();
        txn_ids.push(txn_id);
    }

    // Проверяем, что все активны
    let active_txns = tm.get_active_transactions().unwrap();
    assert_eq!(active_txns.len(), 5);

    // Фиксируем четные, отменяем нечетные
    for (i, &txn_id) in txn_ids.iter().enumerate() {
        if i % 2 == 0 {
            tm.commit_transaction(txn_id).unwrap();
        } else {
            tm.abort_transaction(txn_id).unwrap();
        }
    }

    // Проверяем статистику
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.total_transactions, 5);
    assert_eq!(stats.active_transactions, 0);
    assert_eq!(stats.committed_transactions, 3); // 0, 2, 4
    assert_eq!(stats.aborted_transactions, 2); // 1, 3
}

#[test]
fn test_transaction_limit() {
    let config = TransactionManagerConfig {
        max_concurrent_transactions: 2,
        ..Default::default()
    };
    let tm = TransactionManager::with_config(config).unwrap();

    // Создаем максимальное количество транзакций
    let txn1 = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();
    let txn2 = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();

    // Попытка создать третью должна провалиться
    let result = tm.begin_transaction(IsolationLevel::ReadCommitted, false);
    assert!(result.is_err());

    // Освобождаем одну транзакцию
    tm.commit_transaction(txn1).unwrap();

    // Теперь можем создать новую
    let txn3 = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();

    // Очищаем
    tm.abort_transaction(txn2).unwrap();
    tm.commit_transaction(txn3).unwrap();
}

#[test]
fn test_lock_operations() {
    let tm = TransactionManager::new().unwrap();
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();

    // Получаем разделяемую блокировку
    let resource = "table_users".to_string();
    tm.acquire_lock(
        txn_id,
        resource.clone(),
        LockType::Table("users".to_string()),
        LockMode::Shared,
    )
    .unwrap();

    // Проверяем, что ресурс заблокирован
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    assert!(info.locked_resources.contains(&resource));

    // Освобождаем блокировку
    tm.release_lock(txn_id, resource.clone()).unwrap();

    // Проверяем, что ресурс освобожден
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    assert!(!info.locked_resources.contains(&resource));

    tm.commit_transaction(txn_id).unwrap();
}

#[test]
fn test_multiple_locks() {
    let tm = TransactionManager::new().unwrap();
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();

    let resources = vec![
        "table_users".to_string(),
        "table_orders".to_string(),
        "index_user_email".to_string(),
    ];

    // Получаем блокировки на все ресурсы
    for resource in &resources {
        tm.acquire_lock(
            txn_id,
            resource.clone(),
            LockType::Resource(resource.clone()),
            LockMode::Shared,
        )
        .unwrap();
    }

    // Проверяем, что все ресурсы заблокированы
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    assert_eq!(info.locked_resources.len(), 3);
    for resource in &resources {
        assert!(info.locked_resources.contains(resource));
    }

    // При фиксации все блокировки должны освободиться автоматически
    tm.commit_transaction(txn_id).unwrap();

    // Проверяем статистику блокировок
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.lock_operations, 3);
    assert_eq!(stats.unlock_operations, 3);
}

#[test]
fn test_transaction_states() {
    let tm = TransactionManager::new().unwrap();
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();

    // Изначально активна
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    assert_eq!(info.state, TransactionState::Active);

    // Попытка зафиксировать неактивную транзакцию должна провалиться
    // (но у нас нет способа перевести в другое состояние извне)

    tm.commit_transaction(txn_id).unwrap();
}

#[test]
fn test_invalid_operations() {
    let tm = TransactionManager::new().unwrap();
    let invalid_txn_id = TransactionId::new(999999);

    // Операции с несуществующей транзакцией
    assert!(tm.commit_transaction(invalid_txn_id).is_err());
    assert!(tm.abort_transaction(invalid_txn_id).is_err());
    assert!(tm.get_transaction_info(invalid_txn_id).unwrap().is_none());

    // Попытка получить блокировку для несуществующей транзакции
    let result = tm.acquire_lock(
        invalid_txn_id,
        "resource".to_string(),
        LockType::Resource("resource".to_string()),
        LockMode::Shared,
    );
    assert!(result.is_err());
}

#[test]
fn test_transaction_duration() {
    let tm = TransactionManager::new().unwrap();
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();

    // Небольшая задержка
    thread::sleep(Duration::from_millis(20));

    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    let duration = info.duration().unwrap();
    assert!(duration.as_millis() >= 1); // Минимальное требование - просто проверяем, что время идет

    tm.commit_transaction(txn_id).unwrap();
}

#[test]
fn test_concurrent_transactions() {
    let tm = Arc::new(TransactionManager::new().unwrap());
    let mut handles = vec![];

    // Запускаем несколько потоков с транзакциями
    for i in 0..4 {
        let tm_clone = Arc::clone(&tm);
        let handle = thread::spawn(move || {
            let txn_id = tm_clone
                .begin_transaction(IsolationLevel::ReadCommitted, false)
                .unwrap();

            // Получаем блокировку на уникальный ресурс
            let resource = format!("resource_{}", i);
            tm_clone
                .acquire_lock(
                    txn_id,
                    resource,
                    LockType::Resource(format!("resource_{}", i)),
                    LockMode::Exclusive,
                )
                .unwrap();

            // Небольшая задержка
            thread::sleep(Duration::from_millis(50));

            tm_clone.commit_transaction(txn_id).unwrap();
        });
        handles.push(handle);
    }

    // Ждем завершения всех потоков
    for handle in handles {
        handle.join().unwrap();
    }

    // Проверяем финальную статистику
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.total_transactions, 4);
    assert_eq!(stats.active_transactions, 0);
    assert_eq!(stats.committed_transactions, 4);
    assert_eq!(stats.lock_operations, 4);
    assert_eq!(stats.unlock_operations, 4);
}

#[test]
fn test_isolation_levels() {
    let tm = TransactionManager::new().unwrap();

    let levels = [
        IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted,
        IsolationLevel::RepeatableRead,
        IsolationLevel::Serializable,
    ];

    for level in &levels {
        let txn_id = tm.begin_transaction(level.clone(), false).unwrap();
        let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
        assert_eq!(info.isolation_level, *level);
        tm.commit_transaction(txn_id).unwrap();
    }
}

#[test]
fn test_read_only_transactions() {
    let tm = TransactionManager::new().unwrap();

    // Транзакция только для чтения
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, true)
        .unwrap();
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    assert!(info.read_only);

    tm.commit_transaction(txn_id).unwrap();

    // Обычная транзакция
    let txn_id = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();
    let info = tm.get_transaction_info(txn_id).unwrap().unwrap();
    assert!(!info.read_only);

    tm.commit_transaction(txn_id).unwrap();
}

#[test]
fn test_transaction_manager_statistics() {
    let tm = TransactionManager::new().unwrap();

    // Начальная статистика
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.total_transactions, 0);
    assert_eq!(stats.active_transactions, 0);
    assert_eq!(stats.committed_transactions, 0);
    assert_eq!(stats.aborted_transactions, 0);
    assert_eq!(stats.lock_operations, 0);
    assert_eq!(stats.unlock_operations, 0);

    // Выполняем операции
    let txn1 = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();
    let txn2 = tm
        .begin_transaction(IsolationLevel::ReadCommitted, false)
        .unwrap();

    tm.acquire_lock(
        txn1,
        "resource1".to_string(),
        LockType::Resource("resource1".to_string()),
        LockMode::Shared,
    )
    .unwrap();
    tm.acquire_lock(
        txn2,
        "resource2".to_string(),
        LockType::Resource("resource2".to_string()),
        LockMode::Exclusive,
    )
    .unwrap();

    tm.commit_transaction(txn1).unwrap();
    tm.abort_transaction(txn2).unwrap();

    // Проверяем финальную статистику
    let stats = tm.get_statistics().unwrap();
    assert_eq!(stats.total_transactions, 2);
    assert_eq!(stats.active_transactions, 0);
    assert_eq!(stats.committed_transactions, 1);
    assert_eq!(stats.aborted_transactions, 1);
    assert_eq!(stats.lock_operations, 2);
    assert_eq!(stats.unlock_operations, 2);
}
