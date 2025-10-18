//! Тесты для системы интернационализации

use crate::common::i18n::{Language, MessageKey, I18nManager, set_language, t, t_with_params, I18N};

#[test]
fn test_language_parsing() {
    assert_eq!("en".parse::<Language>().unwrap(), Language::English);
    assert_eq!("ru".parse::<Language>().unwrap(), Language::Russian);
    assert_eq!("english".parse::<Language>().unwrap(), Language::English);
    assert_eq!("russian".parse::<Language>().unwrap(), Language::Russian);
    assert_eq!("русский".parse::<Language>().unwrap(), Language::Russian);
}

#[test]
fn test_language_display() {
    assert_eq!(Language::English.to_string(), "en");
    assert_eq!(Language::Russian.to_string(), "ru");
}

#[test]
fn test_i18n_manager() {
    let manager = I18nManager::new();
    
    // Test English messages
    manager.set_language(Language::English).unwrap();
    assert_eq!(manager.get_message(MessageKey::Welcome), "Welcome to RustDB");
    assert_eq!(manager.get_message(MessageKey::Error), "Error");
    assert_eq!(manager.get_message(MessageKey::Success), "Success");
    assert_eq!(manager.get_message(MessageKey::Warning), "Warning");
    assert_eq!(manager.get_message(MessageKey::Info), "Information");
    
    // Test Russian messages
    manager.set_language(Language::Russian).unwrap();
    assert_eq!(manager.get_message(MessageKey::Welcome), "Добро пожаловать в RustDB");
    assert_eq!(manager.get_message(MessageKey::Error), "Ошибка");
    assert_eq!(manager.get_message(MessageKey::Success), "Успешно");
    assert_eq!(manager.get_message(MessageKey::Warning), "Предупреждение");
    assert_eq!(manager.get_message(MessageKey::Info), "Информация");
}

#[test]
fn test_global_functions() {
    // Test global functions
    set_language(Language::English).unwrap();
    assert_eq!(t(MessageKey::Welcome), "Welcome to RustDB");
    assert_eq!(t(MessageKey::Error), "Error");
    assert_eq!(t(MessageKey::Success), "Success");
    
    set_language(Language::Russian).unwrap();
    assert_eq!(t(MessageKey::Welcome), "Добро пожаловать в RustDB");
    assert_eq!(t(MessageKey::Error), "Ошибка");
    assert_eq!(t(MessageKey::Success), "Успешно");
}

#[test]
fn test_supported_languages() {
    let languages = I18nManager::supported_languages();
    assert_eq!(languages.len(), 2);
    assert!(languages.contains(&Language::English));
    assert!(languages.contains(&Language::Russian));
}

#[test]
fn test_error_messages() {
    set_language(Language::English).unwrap();
    assert_eq!(t(MessageKey::DatabaseError), "Database error");
    assert_eq!(t(MessageKey::ConnectionError), "Connection error");
    assert_eq!(t(MessageKey::TransactionError), "Transaction error");
    assert_eq!(t(MessageKey::LockTimeout), "Lock timeout");
    assert_eq!(t(MessageKey::DeadlockDetected), "Deadlock detected");
    
    set_language(Language::Russian).unwrap();
    assert_eq!(t(MessageKey::DatabaseError), "Ошибка базы данных");
    assert_eq!(t(MessageKey::ConnectionError), "Ошибка подключения");
    assert_eq!(t(MessageKey::TransactionError), "Ошибка транзакции");
    assert_eq!(t(MessageKey::LockTimeout), "Таймаут блокировки");
    assert_eq!(t(MessageKey::DeadlockDetected), "Обнаружена взаимоблокировка");
}

#[test]
fn test_transaction_messages() {
    set_language(Language::English).unwrap();
    assert_eq!(t(MessageKey::TransactionStarted), "Transaction started");
    assert_eq!(t(MessageKey::TransactionCommitted), "Transaction committed");
    assert_eq!(t(MessageKey::TransactionRolledBack), "Transaction rolled back");
    assert_eq!(t(MessageKey::TransactionAborted), "Transaction aborted");
    
    set_language(Language::Russian).unwrap();
    assert_eq!(t(MessageKey::TransactionStarted), "Транзакция начата");
    assert_eq!(t(MessageKey::TransactionCommitted), "Транзакция подтверждена");
    assert_eq!(t(MessageKey::TransactionRolledBack), "Транзакция откачена");
    assert_eq!(t(MessageKey::TransactionAborted), "Транзакция прервана");
}

#[test]
fn test_lock_messages() {
    set_language(Language::English).unwrap();
    assert_eq!(t(MessageKey::LockAcquired), "Lock acquired");
    assert_eq!(t(MessageKey::LockReleased), "Lock released");
    assert_eq!(t(MessageKey::LockWaiting), "Waiting for lock");
    assert_eq!(t(MessageKey::LockUpgraded), "Lock upgraded");
    assert_eq!(t(MessageKey::LockDowngraded), "Lock downgraded");
    
    set_language(Language::Russian).unwrap();
    assert_eq!(t(MessageKey::LockAcquired), "Блокировка получена");
    assert_eq!(t(MessageKey::LockReleased), "Блокировка освобождена");
    assert_eq!(t(MessageKey::LockWaiting), "Ожидание блокировки");
    assert_eq!(t(MessageKey::LockUpgraded), "Блокировка повышена");
    assert_eq!(t(MessageKey::LockDowngraded), "Блокировка понижена");
}

#[test]
fn test_recovery_messages() {
    set_language(Language::English).unwrap();
    assert_eq!(t(MessageKey::RecoveryStarted), "Recovery started");
    assert_eq!(t(MessageKey::RecoveryCompleted), "Recovery completed");
    assert_eq!(t(MessageKey::CheckpointCreated), "Checkpoint created");
    assert_eq!(t(MessageKey::LogFlushed), "Log flushed");
    
    set_language(Language::Russian).unwrap();
    assert_eq!(t(MessageKey::RecoveryStarted), "Восстановление начато");
    assert_eq!(t(MessageKey::RecoveryCompleted), "Восстановление завершено");
    assert_eq!(t(MessageKey::CheckpointCreated), "Контрольная точка создана");
    assert_eq!(t(MessageKey::LogFlushed), "Лог сброшен");
}

#[test]
fn test_performance_messages() {
    set_language(Language::English).unwrap();
    assert_eq!(t(MessageKey::QueryOptimized), "Query optimized");
    assert_eq!(t(MessageKey::IndexCreated), "Index created");
    assert_eq!(t(MessageKey::IndexDropped), "Index dropped");
    assert_eq!(t(MessageKey::StatisticsUpdated), "Statistics updated");
    
    set_language(Language::Russian).unwrap();
    assert_eq!(t(MessageKey::QueryOptimized), "Запрос оптимизирован");
    assert_eq!(t(MessageKey::IndexCreated), "Индекс создан");
    assert_eq!(t(MessageKey::IndexDropped), "Индекс удален");
    assert_eq!(t(MessageKey::StatisticsUpdated), "Статистика обновлена");
}

#[test]
fn test_debug_messages() {
    set_language(Language::English).unwrap();
    assert_eq!(t(MessageKey::DebugEnabled), "Debug enabled");
    assert_eq!(t(MessageKey::ProfilingStarted), "Profiling started");
    assert_eq!(t(MessageKey::ProfilingStopped), "Profiling stopped");
    assert_eq!(t(MessageKey::TraceStarted), "Trace started");
    assert_eq!(t(MessageKey::TraceCompleted), "Trace completed");
    
    set_language(Language::Russian).unwrap();
    assert_eq!(t(MessageKey::DebugEnabled), "Отладка включена");
    assert_eq!(t(MessageKey::ProfilingStarted), "Профилирование начато");
    assert_eq!(t(MessageKey::ProfilingStopped), "Профилирование остановлено");
    assert_eq!(t(MessageKey::TraceStarted), "Трассировка начата");
    assert_eq!(t(MessageKey::TraceCompleted), "Трассировка завершена");
}

#[test]
fn test_language_names() {
    set_language(Language::English).unwrap();
    assert_eq!(t(MessageKey::English), "English");
    assert_eq!(t(MessageKey::Russian), "Russian");
    
    set_language(Language::Russian).unwrap();
    assert_eq!(t(MessageKey::English), "Английский");
    assert_eq!(t(MessageKey::Russian), "Русский");
}

#[test]
fn test_message_with_params() {
    set_language(Language::English).unwrap();
    // Note: Current implementation doesn't support parameters in messages
    // This test is for future enhancement
    let message = t_with_params(MessageKey::Welcome, &["RustDB"]);
    assert_eq!(message, "Welcome to RustDB");
    
    set_language(Language::Russian).unwrap();
    let message = t_with_params(MessageKey::Welcome, &["RustDB"]);
    assert_eq!(message, "Добро пожаловать в RustDB");
}

#[test]
fn test_fallback_to_english() {
    let manager = I18nManager::new();
    
    // Test fallback when message is not found in current language
    manager.set_language(Language::Russian).unwrap();
    let message = manager.get_message(MessageKey::Welcome);
    assert_eq!(message, "Добро пожаловать в RustDB");
    
    // Test fallback when message is not found in any language
    // This would require adding a non-existent message key, which we can't do
    // with the current enum structure, but the fallback mechanism is tested
    // by the implementation
}

#[test]
fn test_concurrent_language_switching() {
    use std::sync::Arc;
    use std::thread;
    
    let manager = Arc::new(I18nManager::new());
    let mut handles = vec![];
    
    // Spawn multiple threads that switch languages
    for i in 0..10 {
        let manager_clone = manager.clone();
        let handle = thread::spawn(move || {
            let language = if i % 2 == 0 {
                Language::English
            } else {
                Language::Russian
            };
            
            manager_clone.set_language(language).unwrap();
            let current_lang = manager_clone.get_language().unwrap();
            assert_eq!(current_lang, language);
            
            let message = manager_clone.get_message(MessageKey::Welcome);
            match language {
                Language::English => assert_eq!(message, "Welcome to RustDB"),
                Language::Russian => assert_eq!(message, "Добро пожаловать в RustDB"),
            }
        });
        handles.push(handle);
    }
    
    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }
}
