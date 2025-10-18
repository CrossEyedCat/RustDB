//! Тесты для менеджера блокировок rustdb

use crate::core::{LockManager, LockType, LockMode, TransactionId};
use crate::common::Result;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn test_lock_manager_creation() {
    let lm = LockManager::new().unwrap();
    let stats = lm.get_statistics().unwrap();
    
    assert_eq!(stats.total_lock_requests, 0);
    assert_eq!(stats.locks_acquired, 0);
    assert_eq!(stats.active_locks, 0);
}

#[test]
fn test_shared_locks_compatibility() {
    let lm = LockManager::new().unwrap();
    let resource = "test_resource".to_string();
    
    let txn1 = TransactionId::new(1);
    let txn2 = TransactionId::new(2);
    
    // Первая транзакция получает разделяемую блокировку
    let acquired1 = lm.acquire_lock(
        txn1,
        resource.clone(),
        LockType::Resource("test".to_string()),
        LockMode::Shared
    ).unwrap();
    assert!(acquired1);
    
    // Вторая транзакция тоже может получить разделяемую блокировку
    let acquired2 = lm.acquire_lock(
        txn2,
        resource.clone(),
        LockType::Resource("test".to_string()),
        LockMode::Shared
    ).unwrap();
    assert!(acquired2);
    
    // Проверяем статистику
    let stats = lm.get_statistics().unwrap();
    assert_eq!(stats.locks_acquired, 2);
    assert_eq!(stats.active_locks, 2);
    
    // Освобождаем блокировки
    lm.release_lock(txn1, resource.clone()).unwrap();
    lm.release_lock(txn2, resource.clone()).unwrap();
    
    let stats = lm.get_statistics().unwrap();
    assert_eq!(stats.locks_released, 2);
    assert_eq!(stats.active_locks, 0);
}

#[test]
fn test_exclusive_lock_incompatibility() {
    let lm = LockManager::new().unwrap();
    let resource = "exclusive_resource".to_string();
    
    let txn1 = TransactionId::new(1);
    let txn2 = TransactionId::new(2);
    
    // Первая транзакция получает исключительную блокировку
    let acquired1 = lm.acquire_lock(
        txn1,
        resource.clone(),
        LockType::Resource("test".to_string()),
        LockMode::Exclusive
    ).unwrap();
    assert!(acquired1);
    
    // Вторая транзакция не может получить разделяемую блокировку
    let acquired2 = lm.acquire_lock(
        txn2,
        resource.clone(),
        LockType::Resource("test".to_string()),
        LockMode::Shared
    ).unwrap();
    assert!(!acquired2); // Должна быть добавлена в очередь ожидания
    
    // Проверяем статистику
    let stats = lm.get_statistics().unwrap();
    assert_eq!(stats.locks_acquired, 1);
    assert_eq!(stats.blocked_requests, 1);
    assert_eq!(stats.waiting_requests, 1);
    
    // Освобождаем блокировку первой транзакции
    lm.release_lock(txn1, resource.clone()).unwrap();
    
    // Теперь вторая транзакция должна получить блокировку автоматически
    // (это происходит в process_wait_queue)
    
    // Освобождаем вторую блокировку
    lm.release_lock(txn2, resource).unwrap();
}

#[test]
fn test_different_lock_types() {
    let lm = LockManager::new().unwrap();
    let txn_id = TransactionId::new(1);
    
    let lock_types = vec![
        (LockType::Page(123), "page_123"),
        (LockType::Table("users".to_string()), "table_users"),
        (LockType::Record(456, 789), "record_456_789"),
        (LockType::Index("idx_email".to_string()), "index_idx_email"),
        (LockType::Resource("custom".to_string()), "resource_custom"),
    ];
    
    // Получаем блокировки разных типов
    for (lock_type, resource) in &lock_types {
        let acquired = lm.acquire_lock(
            txn_id,
            resource.to_string(),
            lock_type.clone(),
            LockMode::Shared
        ).unwrap();
        assert!(acquired);
    }
    
    let stats = lm.get_statistics().unwrap();
    assert_eq!(stats.locks_acquired, lock_types.len() as u64);
    
    // Освобождаем все блокировки
    for (_, resource) in &lock_types {
        lm.release_lock(txn_id, resource.to_string()).unwrap();
    }
    
    let stats = lm.get_statistics().unwrap();
    assert_eq!(stats.locks_released, lock_types.len() as u64);
}

#[test]
fn test_lock_upgrade() {
    let lm = LockManager::new().unwrap();
    let resource = "upgrade_resource".to_string();
    let txn_id = TransactionId::new(1);
    
    // Получаем разделяемую блокировку
    let acquired1 = lm.acquire_lock(
        txn_id,
        resource.clone(),
        LockType::Resource("test".to_string()),
        LockMode::Shared
    ).unwrap();
    assert!(acquired1);
    
    // Пытаемся получить исключительную блокировку той же транзакцией
    // Это должно работать как upgrade, если нет других блокировок
    let acquired2 = lm.acquire_lock(
        txn_id,
        resource.clone(),
        LockType::Resource("test".to_string()),
        LockMode::Exclusive
    ).unwrap();
    assert!(acquired2);
    
    lm.release_lock(txn_id, resource).unwrap();
}

#[test]
fn test_same_transaction_multiple_requests() {
    let lm = LockManager::new().unwrap();
    let resource = "same_txn_resource".to_string();
    let txn_id = TransactionId::new(1);
    
    // Первый запрос
    let acquired1 = lm.acquire_lock(
        txn_id,
        resource.clone(),
        LockType::Resource("test".to_string()),
        LockMode::Shared
    ).unwrap();
    assert!(acquired1);
    
    // Повторный запрос той же транзакции на тот же режим
    let acquired2 = lm.acquire_lock(
        txn_id,
        resource.clone(),
        LockType::Resource("test".to_string()),
        LockMode::Shared
    ).unwrap();
    assert!(acquired2); // Должен вернуть true, так как уже владеет блокировкой
    
    lm.release_lock(txn_id, resource).unwrap();
}

#[test]
fn test_deadlock_detection_simple() {
    let lm = LockManager::new().unwrap();
    
    let resource1 = "resource1".to_string();
    let resource2 = "resource2".to_string();
    let txn1 = TransactionId::new(1);
    let txn2 = TransactionId::new(2);
    
    // Транзакция 1 получает блокировку на ресурс 1
    let acquired = lm.acquire_lock(
        txn1,
        resource1.clone(),
        LockType::Resource("r1".to_string()),
        LockMode::Exclusive
    ).unwrap();
    assert!(acquired);
    
    // Транзакция 2 получает блокировку на ресурс 2
    let acquired = lm.acquire_lock(
        txn2,
        resource2.clone(),
        LockType::Resource("r2".to_string()),
        LockMode::Exclusive
    ).unwrap();
    assert!(acquired);
    
    // Транзакция 1 пытается получить блокировку на ресурс 2
    let acquired = lm.acquire_lock(
        txn1,
        resource2.clone(),
        LockType::Resource("r2".to_string()),
        LockMode::Exclusive
    ).unwrap();
    assert!(!acquired); // Добавляется в очередь ожидания
    
    // Транзакция 2 пытается получить блокировку на ресурс 1
    // Это должно вызвать обнаружение дедлока
    let result = lm.acquire_lock(
        txn2,
        resource1.clone(),
        LockType::Resource("r1".to_string()),
        LockMode::Exclusive
    );
    
    // Ожидаем ошибку дедлока
    assert!(result.is_err());
    
    // Очищаем
    lm.release_lock(txn1, resource1).unwrap();
    lm.release_lock(txn2, resource2).unwrap();
}

#[test]
fn test_wait_queue_processing() {
    let lm = LockManager::new().unwrap();
    let resource = "wait_queue_resource".to_string();
    
    let txn1 = TransactionId::new(1);
    let txn2 = TransactionId::new(2);
    let txn3 = TransactionId::new(3);
    
    // Транзакция 1 получает исключительную блокировку
    let acquired = lm.acquire_lock(
        txn1,
        resource.clone(),
        LockType::Resource("test".to_string()),
        LockMode::Exclusive
    ).unwrap();
    assert!(acquired);
    
    // Транзакции 2 и 3 добавляются в очередь ожидания
    let acquired2 = lm.acquire_lock(
        txn2,
        resource.clone(),
        LockType::Resource("test".to_string()),
        LockMode::Shared
    ).unwrap();
    assert!(!acquired2);
    
    let acquired3 = lm.acquire_lock(
        txn3,
        resource.clone(),
        LockType::Resource("test".to_string()),
        LockMode::Shared
    ).unwrap();
    assert!(!acquired3);
    
    // Проверяем статистику
    let stats = lm.get_statistics().unwrap();
    assert_eq!(stats.waiting_requests, 2);
    
    // Освобождаем блокировку транзакции 1
    lm.release_lock(txn1, resource.clone()).unwrap();
    
    // Транзакции 2 и 3 должны автоматически получить блокировки
    // (поскольку они совместимы между собой)
    
    // Освобождаем оставшиеся блокировки
    lm.release_lock(txn2, resource.clone()).unwrap();
    lm.release_lock(txn3, resource).unwrap();
}

#[test]
fn test_lock_manager_statistics() {
    let lm = LockManager::new().unwrap();
    
    // Начальная статистика
    let stats = lm.get_statistics().unwrap();
    assert_eq!(stats.total_lock_requests, 0);
    assert_eq!(stats.locks_acquired, 0);
    assert_eq!(stats.locks_released, 0);
    assert_eq!(stats.active_locks, 0);
    assert_eq!(stats.blocked_requests, 0);
    assert_eq!(stats.deadlocks_detected, 0);
    
    let txn1 = TransactionId::new(1);
    let txn2 = TransactionId::new(2);
    let resource = "stats_resource".to_string();
    
    // Выполняем операции
    lm.acquire_lock(txn1, resource.clone(), LockType::Resource("test".to_string()), LockMode::Exclusive).unwrap();
    lm.acquire_lock(txn2, resource.clone(), LockType::Resource("test".to_string()), LockMode::Shared).unwrap(); // Блокируется
    
    let stats = lm.get_statistics().unwrap();
    assert_eq!(stats.total_lock_requests, 2);
    assert_eq!(stats.locks_acquired, 1);
    assert_eq!(stats.blocked_requests, 1);
    assert_eq!(stats.active_locks, 1);
    
    // Освобождаем блокировки
    lm.release_lock(txn1, resource.clone()).unwrap();
    lm.release_lock(txn2, resource).unwrap();
    
    let stats = lm.get_statistics().unwrap();
    assert_eq!(stats.locks_released, 2);
    assert_eq!(stats.active_locks, 0);
}

#[test]
fn test_concurrent_lock_operations() {
    let lm = Arc::new(LockManager::new().unwrap());
    let mut handles = vec![];
    
    // Запускаем несколько потоков, каждый работает со своим ресурсом
    for i in 0..4 {
        let lm_clone = Arc::clone(&lm);
        let handle = thread::spawn(move || {
            let txn_id = TransactionId::new(i as u64 + 1);
            let resource = format!("concurrent_resource_{}", i);
            
            // Получаем блокировку
            let acquired = lm_clone.acquire_lock(
                txn_id,
                resource.clone(),
                LockType::Resource(format!("r{}", i)),
                LockMode::Exclusive
            ).unwrap();
            assert!(acquired);
            
            // Небольшая задержка
            thread::sleep(Duration::from_millis(10));
            
            // Освобождаем блокировку
            lm_clone.release_lock(txn_id, resource).unwrap();
        });
        handles.push(handle);
    }
    
    // Ждем завершения всех потоков
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Проверяем финальную статистику
    let stats = lm.get_statistics().unwrap();
    assert_eq!(stats.locks_acquired, 4);
    assert_eq!(stats.locks_released, 4);
    assert_eq!(stats.active_locks, 0);
}

#[test]
fn test_lock_type_display() {
    let lock_types = vec![
        LockType::Page(123),
        LockType::Table("users".to_string()),
        LockType::Record(456, 789),
        LockType::Index("idx_email".to_string()),
        LockType::Resource("custom".to_string()),
    ];
    
    for lock_type in lock_types {
        let display = format!("{}", lock_type);
        assert!(!display.is_empty());
    }
}

#[test]
fn test_lock_mode_compatibility() {
    // Тестируем логику совместимости режимов блокировки
    assert!(LockMode::Shared.is_compatible(&LockMode::Shared));
    assert!(!LockMode::Shared.is_compatible(&LockMode::Exclusive));
    assert!(!LockMode::Exclusive.is_compatible(&LockMode::Shared));
    assert!(!LockMode::Exclusive.is_compatible(&LockMode::Exclusive));
}

#[test]
fn test_active_locks_inspection() {
    let lm = LockManager::new().unwrap();
    let txn_id = TransactionId::new(1);
    let resource = "inspect_resource".to_string();
    
    // Получаем блокировку
    lm.acquire_lock(
        txn_id,
        resource.clone(),
        LockType::Resource("test".to_string()),
        LockMode::Shared
    ).unwrap();
    
    // Проверяем активные блокировки
    let active_locks = lm.get_active_locks().unwrap();
    assert!(active_locks.contains_key(&resource));
    
    let locks = &active_locks[&resource];
    assert_eq!(locks.len(), 1);
    assert_eq!(locks[0].transaction_id, txn_id);
    
    lm.release_lock(txn_id, resource).unwrap();
    
    let active_locks = lm.get_active_locks().unwrap();
    assert!(!active_locks.contains_key("inspect_resource"));
}

#[test]
fn test_waiting_requests_inspection() {
    let lm = LockManager::new().unwrap();
    let resource = "waiting_resource".to_string();
    
    let txn1 = TransactionId::new(1);
    let txn2 = TransactionId::new(2);
    
    // Первая транзакция получает исключительную блокировку
    lm.acquire_lock(
        txn1,
        resource.clone(),
        LockType::Resource("test".to_string()),
        LockMode::Exclusive
    ).unwrap();
    
    // Вторая транзакция добавляется в очередь ожидания
    lm.acquire_lock(
        txn2,
        resource.clone(),
        LockType::Resource("test".to_string()),
        LockMode::Shared
    ).unwrap();
    
    // Проверяем очередь ожидания
    let waiting_requests = lm.get_waiting_requests().unwrap();
    assert!(waiting_requests.contains_key(&resource));
    
    let requests = &waiting_requests[&resource];
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].transaction_id, txn2);
    
    // Очищаем
    lm.release_lock(txn1, resource.clone()).unwrap();
    lm.release_lock(txn2, resource).unwrap();
}
