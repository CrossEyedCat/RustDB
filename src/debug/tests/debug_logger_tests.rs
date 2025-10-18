//! Тесты для детального логгера отладки

use crate::debug::debug_logger::*;
use crate::debug::DebugConfig;
use std::time::Duration;

#[tokio::test]
async fn test_debug_logger_creation() {
    let config = DebugConfig {
        enable_debug_logging: true,
        detail_level: 5,
        ..Default::default()
    };

    let logger = DebugLogger::new(&config);
    
    // Проверяем, что логгер создался
    let stats = logger.get_stats();
    assert_eq!(stats.total_entries, 0);
}

#[tokio::test]
async fn test_debug_logging_levels() {
    let config = DebugConfig {
        enable_debug_logging: true,
        detail_level: 3, // Только до Info уровня
        ..Default::default()
    };

    let logger = DebugLogger::new(&config);

    // Логируем на разных уровнях
    logger.log(LogEntry::new(
        LogLevel::Info,
        LogCategory::System,
        "TestComponent",
        "Info message",
    ));

    logger.log(LogEntry::new(
        LogLevel::Debug,
        LogCategory::System,
        "TestComponent",
        "Debug message",
    ));

    logger.log(LogEntry::new(
        LogLevel::Trace,
        LogCategory::System,
        "TestComponent",
        "Trace message",
    ));

    // Ждем немного для обработки
    tokio::time::sleep(Duration::from_millis(100)).await;

    let stats = logger.get_stats();
    // Должны быть только Info сообщения (Debug и Trace отфильтрованы)
    assert_eq!(stats.total_entries, 1);
}

#[tokio::test]
async fn test_transaction_logging() {
    let config = DebugConfig {
        enable_debug_logging: true,
        detail_level: 5,
        ..Default::default()
    };

    let logger = DebugLogger::new(&config);

    // Логируем операции с транзакциями
    logger.log_transaction_operation(
        LogLevel::Info,
        "BEGIN",
        123,
        Some(Duration::from_millis(10)),
        Some(256),
    );

    logger.log_transaction_operation(
        LogLevel::Info,
        "COMMIT",
        123,
        Some(Duration::from_millis(5)),
        Some(512),
    );

    tokio::time::sleep(Duration::from_millis(100)).await;

    let stats = logger.get_stats();
    assert_eq!(stats.total_entries, 2);
    assert!(stats.entries_by_category.contains_key("TX"));
}

#[tokio::test]
async fn test_data_operation_logging() {
    let config = DebugConfig {
        enable_debug_logging: true,
        detail_level: 5,
        ..Default::default()
    };

    let logger = DebugLogger::new(&config);

    // Логируем операции с данными
    logger.log_data_operation(
        LogLevel::Debug,
        "INSERT",
        "users",
        Some(Duration::from_millis(15)),
        Some(1024),
    );

    logger.log_data_operation(
        LogLevel::Debug,
        "UPDATE",
        "users",
        Some(Duration::from_millis(8)),
        Some(2048),
    );

    tokio::time::sleep(Duration::from_millis(100)).await;

    let stats = logger.get_stats();
    assert_eq!(stats.total_entries, 2);
    assert!(stats.entries_by_category.contains_key("DATA"));
}

#[tokio::test]
async fn test_query_operation_logging() {
    let config = DebugConfig {
        enable_debug_logging: true,
        detail_level: 5,
        ..Default::default()
    };

    let logger = DebugLogger::new(&config);

    // Логируем операции с запросами
    logger.log_query_operation(
        LogLevel::Info,
        "EXECUTE",
        "query_001",
        Some(Duration::from_millis(50)),
        Some(4096),
    );

    logger.log_query_operation(
        LogLevel::Warning,
        "SLOW_QUERY",
        "query_002",
        Some(Duration::from_secs(5)),
        Some(8192),
    );

    tokio::time::sleep(Duration::from_millis(100)).await;

    let stats = logger.get_stats();
    assert_eq!(stats.total_entries, 2);
    assert!(stats.entries_by_category.contains_key("QUERY"));
}

#[tokio::test]
async fn test_system_operation_logging() {
    let config = DebugConfig {
        enable_debug_logging: true,
        detail_level: 5,
        ..Default::default()
    };

    let logger = DebugLogger::new(&config);

    // Логируем системные операции
    logger.log_system_operation(
        LogLevel::Warning,
        "BufferManager",
        "Buffer overflow detected",
        Some(Duration::from_millis(1)),
    );

    logger.log_system_operation(
        LogLevel::Error,
        "FileManager",
        "Failed to open file",
        Some(Duration::from_millis(100)),
    );

    tokio::time::sleep(Duration::from_millis(100)).await;

    let stats = logger.get_stats();
    assert_eq!(stats.total_entries, 2);
    assert!(stats.entries_by_category.contains_key("SYSTEM"));
}

#[test]
fn test_log_entry_creation() {
    let entry = LogEntry::new(
        LogLevel::Info,
        LogCategory::Transaction,
        "TestComponent",
        "Test message",
    );

    assert_eq!(entry.level, LogLevel::Info);
    assert_eq!(entry.category, LogCategory::Transaction);
    assert_eq!(entry.component, "TestComponent");
    assert_eq!(entry.message, "Test message");
    assert!(entry.timestamp > 0);
}

#[test]
fn test_log_entry_with_data() {
    let data = serde_json::json!({"key": "value", "count": 42});
    let entry = LogEntry::new(
        LogLevel::Debug,
        LogCategory::Data,
        "TestComponent",
        "Test message with data",
    ).with_data(data.clone())
     .with_transaction_id(123)
     .with_duration(Duration::from_millis(100))
     .with_data_size(1024);

    assert_eq!(entry.data, Some(data));
    assert_eq!(entry.transaction_id, Some(123));
    assert_eq!(entry.duration_us, Some(100_000));
    assert_eq!(entry.data_size, Some(1024));
}

#[test]
fn test_log_entry_formatting() {
    let entry = LogEntry::new(
        LogLevel::Error,
        LogCategory::System,
        "TestComponent",
        "Test error message",
    ).with_transaction_id(456)
     .with_duration(Duration::from_micros(500))
     .with_data_size(2048);

    let formatted = entry.format();
    assert!(formatted.contains("ERROR:SYSTEM"));
    assert!(formatted.contains("TestComponent"));
    assert!(formatted.contains("TX:456"));
    assert!(formatted.contains("(500μs)"));
    assert!(formatted.contains("[2048B]"));
    assert!(formatted.contains("Test error message"));
}

#[tokio::test]
async fn test_logger_status_report() {
    let config = DebugConfig {
        enable_debug_logging: true,
        detail_level: 5,
        ..Default::default()
    };

    let logger = DebugLogger::new(&config);

    // Добавляем несколько записей
    logger.log(LogEntry::new(
        LogLevel::Info,
        LogCategory::System,
        "TestComponent",
        "Test message 1",
    ));

    logger.log(LogEntry::new(
        LogLevel::Warning,
        LogCategory::Data,
        "TestComponent",
        "Test message 2",
    ));

    tokio::time::sleep(Duration::from_millis(100)).await;

    let report = logger.generate_status_report();
    assert!(report.contains("Общее количество записей"));
    assert!(report.contains("Записи по уровням"));
    assert!(report.contains("Записи по категориям"));
}
