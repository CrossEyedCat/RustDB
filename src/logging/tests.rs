//! Тесты для системы логирования

use crate::logging::{
    log_record::{LogRecord, LogRecordType, LogSequenceNumber, LogPriority, LogOperationData},
    log_writer::{LogWriter, LogWriterConfig, SyncLevel},
    wal::{WalManager, WalConfig},
    recovery::RecoveryManager,
    checkpoint::{CheckpointManager, CheckpointConfig},
    compaction::CompactionManager,
    metrics::LogMetrics,
};
use crate::common::types::TransactionId;
use std::time::Duration;
use tempfile::TempDir;
use std::collections::HashMap;

#[test]
fn test_log_record_creation() {
    let record = LogRecord {
        record_type: LogRecordType::Checkpoint,
        transaction_id: TransactionId(1),
        lsn: LogSequenceNumber(1),
        timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
        priority: LogPriority::Normal,
        record_size: 100,
        checksum: 0,
        operation_data: LogOperationData::Raw(b"test".to_vec()),
        prev_lsn: None,
        metadata: HashMap::new(),
    };

    assert_eq!(record.transaction_id, TransactionId(1));
    assert_eq!(record.lsn, LogSequenceNumber(1));
    assert_eq!(record.priority, LogPriority::Normal);
}

#[test]
fn test_log_sequence_number_ordering() {
    let lsn1 = LogSequenceNumber(1);
    let lsn2 = LogSequenceNumber(2);
    let lsn3 = LogSequenceNumber(1); // Дубликат
    
    assert!(lsn1 < lsn2);
    assert!(lsn2 > lsn1);
    assert_eq!(lsn1, lsn3);
    assert_ne!(lsn1, lsn2);
}

#[test]
fn test_log_priority_ordering() {
    assert!(LogPriority::Critical > LogPriority::High);
    assert!(LogPriority::High > LogPriority::Normal);
    assert!(LogPriority::Normal > LogPriority::Low);
    
    // Проверяем численные значения
    assert_eq!(LogPriority::Critical as u8, 3);
    assert_eq!(LogPriority::High as u8, 2);
    assert_eq!(LogPriority::Normal as u8, 1);
    assert_eq!(LogPriority::Low as u8, 0);
}

#[test]
fn test_log_record_types() {
    let types = vec![
        LogRecordType::Checkpoint,
        LogRecordType::BeginTransaction,
        LogRecordType::CommitTransaction,
        LogRecordType::RollbackTransaction,
    ];
    
    for record_type in types {
        let record = LogRecord {
            record_type: record_type.clone(),
            transaction_id: TransactionId(1),
            lsn: LogSequenceNumber(1),
            timestamp: 0,
            priority: LogPriority::Normal,
            record_size: 100,
            checksum: 0,
            operation_data: LogOperationData::Raw(b"test".to_vec()),
            prev_lsn: None,
            metadata: HashMap::new(),
        };
        
        assert_eq!(record.record_type, record_type);
    }
}

#[test]
fn test_log_operation_data() {
    let raw_data = LogOperationData::Raw(b"raw data".to_vec());
    let insert_data = LogOperationData::Insert {
        table_name: "users".to_string(),
        values: HashMap::new(),
    };
    let update_data = LogOperationData::Update {
        table_name: "products".to_string(),
        old_values: HashMap::new(),
        new_values: HashMap::new(),
        condition: "id = 1".to_string(),
    };
    let delete_data = LogOperationData::Delete {
        table_name: "orders".to_string(),
        condition: "status = 'cancelled'".to_string(),
    };
    
    // Проверяем, что все варианты создаются корректно
    match raw_data {
        LogOperationData::Raw(data) => assert_eq!(data, b"raw data"),
        _ => panic!("Unexpected variant"),
    }
    
    match insert_data {
        LogOperationData::Insert { table_name, .. } => assert_eq!(table_name, "users"),
        _ => panic!("Unexpected variant"),
    }
    
    match update_data {
        LogOperationData::Update { table_name, .. } => assert_eq!(table_name, "products"),
        _ => panic!("Unexpected variant"),
    }
    
    match delete_data {
        LogOperationData::Delete { table_name, .. } => assert_eq!(table_name, "orders"),
        _ => panic!("Unexpected variant"),
    }
}

#[tokio::test]
async fn test_log_writer_creation() {
    let temp_dir = TempDir::new().unwrap();
    let config = LogWriterConfig {
        log_directory: temp_dir.path().to_path_buf(),
        max_log_file_size: 1024 * 1024,
        max_log_files: 10,
        write_buffer_size: 4096,
        max_buffer_time: Duration::from_millis(100),
        writer_thread_pool_size: 2,
        sync_level: SyncLevel::Async,
        enable_compression: false,
        enable_integrity_check: true,
    };

    let log_writer = LogWriter::new(config);
    assert!(log_writer.is_ok());
}

#[tokio::test]
async fn test_log_writer_write_operation() {
    let temp_dir = TempDir::new().unwrap();
    let config = LogWriterConfig {
        log_directory: temp_dir.path().to_path_buf(),
        max_log_file_size: 1024 * 1024,
        max_log_files: 10,
        write_buffer_size: 4096,
        max_buffer_time: Duration::from_millis(100),
        writer_thread_pool_size: 2,
        sync_level: SyncLevel::Async,
        enable_compression: false,
        enable_integrity_check: true,
    };

    let mut log_writer = LogWriter::new(config).unwrap();

    let record = LogRecord {
        record_type: LogRecordType::Checkpoint,
        transaction_id: TransactionId(1),
        lsn: LogSequenceNumber(1),
        timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
        priority: LogPriority::Normal,
        record_size: 100,
        checksum: 0,
        operation_data: LogOperationData::Raw(b"test data".to_vec()),
        prev_lsn: None,
        metadata: HashMap::new(),
    };

    let result = log_writer.write_log(record).await;
    assert!(result.is_ok());
    
    // Принудительный сброс
    let flush_result = log_writer.flush().await;
    assert!(flush_result.is_ok());
}

#[tokio::test]
async fn test_log_writer_statistics() {
    let temp_dir = TempDir::new().unwrap();
    let config = LogWriterConfig {
        log_directory: temp_dir.path().to_path_buf(),
        max_log_file_size: 1024 * 1024,
        max_log_files: 10,
        write_buffer_size: 4096,
        max_buffer_time: Duration::from_millis(100),
        writer_thread_pool_size: 2,
        sync_level: SyncLevel::Async,
        enable_compression: false,
        enable_integrity_check: true,
    };

    let log_writer = LogWriter::new(config).unwrap();
    let stats = log_writer.get_statistics();
    
    // Изначально статистика должна быть пустой
    assert_eq!(stats.total_records_written, 0);
    assert_eq!(stats.total_bytes_written, 0);
}

#[test]
fn test_wal_config_creation() {
    let temp_dir = TempDir::new().unwrap();
    let log_writer_config = LogWriterConfig {
        log_directory: temp_dir.path().to_path_buf(),
        max_log_file_size: 1024 * 1024,
        max_log_files: 10,
        write_buffer_size: 4096,
        max_buffer_time: Duration::from_millis(100),
        writer_thread_pool_size: 2,
        sync_level: SyncLevel::Async,
        enable_compression: false,
        enable_integrity_check: true,
    };
    
    let wal_config = WalConfig {
        log_writer_config,
        strict_mode: true,
        lock_timeout_ms: 1000,
        transaction_pool_size: 100,
        auto_checkpoint: true,
        checkpoint_interval_ms: 30000,
        max_concurrent_transactions: 50,
    };
    
    assert!(wal_config.strict_mode);
    assert_eq!(wal_config.lock_timeout_ms, 1000);
    assert_eq!(wal_config.transaction_pool_size, 100);
}

#[test]
fn test_checkpoint_config_creation() {
    let config = CheckpointConfig {
        checkpoint_interval: Duration::from_secs(30),
        enable_auto_checkpoint: true,
        max_active_transactions: 100,
        max_dirty_pages: 1000,
        max_log_size: 10 * 1024 * 1024, // 10MB
        max_checkpoint_time: Duration::from_secs(60),
        flush_threads: 4,
        flush_batch_size: 100,
    };
    
    assert!(config.enable_auto_checkpoint);
    assert_eq!(config.max_active_transactions, 100);
    assert_eq!(config.flush_threads, 4);
}

#[test]
fn test_log_metrics_operations() {
    let mut metrics = LogMetrics::new();
    
    // Изначально метрики пустые
    assert_eq!(metrics.get_total_records(), 0);
    
    // Записываем несколько операций
    metrics.record_log_write(LogRecordType::Checkpoint, 100, Duration::from_millis(10));
    metrics.record_log_write(LogRecordType::BeginTransaction, 50, Duration::from_millis(5));
    metrics.record_log_write(LogRecordType::CommitTransaction, 75, Duration::from_millis(8));
    
    assert_eq!(metrics.get_total_records(), 3);
    assert!(metrics.get_total_bytes_written() > 0);
    assert!(metrics.get_average_write_time() > Duration::from_millis(0));
}

#[test]
fn test_log_record_serialization() {
    let record = LogRecord {
        record_type: LogRecordType::Checkpoint,
        transaction_id: TransactionId(42),
        lsn: LogSequenceNumber(100),
        timestamp: 1234567890,
        priority: LogPriority::High,
        record_size: 200,
        checksum: 12345,
        operation_data: LogOperationData::Raw(b"serialization test".to_vec()),
        prev_lsn: Some(LogSequenceNumber(99)),
        metadata: {
            let mut meta = HashMap::new();
            meta.insert("key1".to_string(), "value1".to_string());
            meta.insert("key2".to_string(), "value2".to_string());
            meta
        },
    };
    
    // Сериализуем запись
    let serialized = bincode::serialize(&record).unwrap();
    assert!(!serialized.is_empty());
    
    // Десериализуем запись
    let deserialized: LogRecord = bincode::deserialize(&serialized).unwrap();
    
    assert_eq!(deserialized.record_type, record.record_type);
    assert_eq!(deserialized.transaction_id, record.transaction_id);
    assert_eq!(deserialized.lsn, record.lsn);
    assert_eq!(deserialized.timestamp, record.timestamp);
    assert_eq!(deserialized.priority, record.priority);
    assert_eq!(deserialized.prev_lsn, record.prev_lsn);
    assert_eq!(deserialized.metadata, record.metadata);
}

#[test]
fn test_transaction_id_operations() {
    let tx1 = TransactionId(1);
    let tx2 = TransactionId(2);
    let tx3 = TransactionId(1);
    
    assert_eq!(tx1, tx3);
    assert_ne!(tx1, tx2);
    assert!(tx1 < tx2);
    
    // Проверяем, что можно использовать в HashMap
    let mut map = HashMap::new();
    map.insert(tx1, "transaction 1");
    map.insert(tx2, "transaction 2");
    
    assert_eq!(map.get(&tx1), Some(&"transaction 1"));
    assert_eq!(map.get(&tx2), Some(&"transaction 2"));
    assert_eq!(map.get(&TransactionId(3)), None);
}

#[test]
fn test_log_record_validation() {
    let valid_record = LogRecord {
        record_type: LogRecordType::Checkpoint,
        transaction_id: TransactionId(1),
        lsn: LogSequenceNumber(1),
        timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
        priority: LogPriority::Normal,
        record_size: 100,
        checksum: 0,
        operation_data: LogOperationData::Raw(b"valid data".to_vec()),
        prev_lsn: None,
        metadata: HashMap::new(),
    };
    
    // Проверяем, что запись корректна
    assert_eq!(valid_record.transaction_id.0, 1);
    assert_eq!(valid_record.lsn.0, 1);
    assert!(valid_record.timestamp > 0);
    
    match valid_record.operation_data {
        LogOperationData::Raw(ref data) => assert_eq!(data, b"valid data"),
        _ => panic!("Unexpected operation data type"),
    }
}

#[test]
fn test_log_record_with_metadata() {
    let mut metadata = HashMap::new();
    metadata.insert("user_id".to_string(), "12345".to_string());
    metadata.insert("session_id".to_string(), "abcdef".to_string());
    metadata.insert("client_ip".to_string(), "192.168.1.100".to_string());
    
    let record = LogRecord {
        record_type: LogRecordType::BeginTransaction,
        transaction_id: TransactionId(100),
        lsn: LogSequenceNumber(500),
        timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
        priority: LogPriority::High,
        record_size: 300,
        checksum: 0,
        operation_data: LogOperationData::Raw(b"transaction begin".to_vec()),
        prev_lsn: Some(LogSequenceNumber(499)),
        metadata: metadata.clone(),
    };
    
    assert_eq!(record.metadata, metadata);
    assert_eq!(record.metadata.get("user_id"), Some(&"12345".to_string()));
    assert_eq!(record.metadata.get("session_id"), Some(&"abcdef".to_string()));
    assert_eq!(record.metadata.get("nonexistent"), None);
}

#[test]
fn test_complex_operation_data() {
    let mut insert_values = HashMap::new();
    insert_values.insert("id".to_string(), "1".to_string());
    insert_values.insert("name".to_string(), "John Doe".to_string());
    insert_values.insert("email".to_string(), "john@example.com".to_string());
    
    let insert_data = LogOperationData::Insert {
        table_name: "users".to_string(),
        values: insert_values.clone(),
    };
    
    let mut old_values = HashMap::new();
    old_values.insert("status".to_string(), "pending".to_string());
    
    let mut new_values = HashMap::new();
    new_values.insert("status".to_string(), "completed".to_string());
    
    let update_data = LogOperationData::Update {
        table_name: "orders".to_string(),
        old_values: old_values.clone(),
        new_values: new_values.clone(),
        condition: "id = 42".to_string(),
    };
    
    let delete_data = LogOperationData::Delete {
        table_name: "temp_data".to_string(),
        condition: "created_at < '2023-01-01'".to_string(),
    };
    
    // Проверяем INSERT данные
    match insert_data {
        LogOperationData::Insert { table_name, values } => {
            assert_eq!(table_name, "users");
            assert_eq!(values, insert_values);
        }
        _ => panic!("Expected Insert variant"),
    }
    
    // Проверяем UPDATE данные
    match update_data {
        LogOperationData::Update { table_name, old_values: old, new_values: new, condition } => {
            assert_eq!(table_name, "orders");
            assert_eq!(old, old_values);
            assert_eq!(new, new_values);
            assert_eq!(condition, "id = 42");
        }
        _ => panic!("Expected Update variant"),
    }
    
    // Проверяем DELETE данные
    match delete_data {
        LogOperationData::Delete { table_name, condition } => {
            assert_eq!(table_name, "temp_data");
            assert_eq!(condition, "created_at < '2023-01-01'");
        }
        _ => panic!("Expected Delete variant"),
    }
}

#[test]
fn test_log_record_size_calculation() {
    let small_record = LogRecord {
        record_type: LogRecordType::Checkpoint,
        transaction_id: TransactionId(1),
        lsn: LogSequenceNumber(1),
        timestamp: 0,
        priority: LogPriority::Low,
        record_size: 50,
        checksum: 0,
        operation_data: LogOperationData::Raw(b"small".to_vec()),
        prev_lsn: None,
        metadata: HashMap::new(),
    };
    
    let large_record = LogRecord {
        record_type: LogRecordType::BeginTransaction,
        transaction_id: TransactionId(999),
        lsn: LogSequenceNumber(999999),
        timestamp: u64::MAX,
        priority: LogPriority::Critical,
        record_size: 10000,
        checksum: u32::MAX,
        operation_data: LogOperationData::Raw("large data".repeat(1000).into_bytes()),
        prev_lsn: Some(LogSequenceNumber(999998)),
        metadata: {
            let mut meta = HashMap::new();
            for i in 0..100 {
                meta.insert(format!("key_{}", i), format!("value_{}", i));
            }
            meta
        },
    };
    
    let small_serialized = bincode::serialize(&small_record).unwrap();
    let large_serialized = bincode::serialize(&large_record).unwrap();
    
    assert!(large_serialized.len() > small_serialized.len());
    assert!(small_serialized.len() > 0);
}

#[test]
fn test_boundary_conditions() {
    // Минимальные значения
    let min_record = LogRecord {
        record_type: LogRecordType::Checkpoint,
        transaction_id: TransactionId(0),
        lsn: LogSequenceNumber(0),
        timestamp: 0,
        priority: LogPriority::Low,
        record_size: 0,
        checksum: 0,
        operation_data: LogOperationData::Raw(vec![]),
        prev_lsn: None,
        metadata: HashMap::new(),
    };
    
    // Максимальные значения
    let max_record = LogRecord {
        record_type: LogRecordType::RollbackTransaction,
        transaction_id: TransactionId(u64::MAX),
        lsn: LogSequenceNumber(u64::MAX),
        timestamp: u64::MAX,
        priority: LogPriority::Critical,
        record_size: u32::MAX,
        checksum: u32::MAX,
        operation_data: LogOperationData::Raw(vec![0xFF; 1000]),
        prev_lsn: Some(LogSequenceNumber(u64::MAX - 1)),
        metadata: HashMap::new(),
    };
    
    // Проверяем, что записи создаются корректно
    assert_eq!(min_record.transaction_id.0, 0);
    assert_eq!(min_record.lsn.0, 0);
    
    assert_eq!(max_record.transaction_id.0, u64::MAX);
    assert_eq!(max_record.lsn.0, u64::MAX);
    
    // Проверяем сериализацию
    let min_serialized = bincode::serialize(&min_record).unwrap();
    let max_serialized = bincode::serialize(&max_record).unwrap();
    
    assert!(!min_serialized.is_empty());
    assert!(!max_serialized.is_empty());
    assert!(max_serialized.len() > min_serialized.len());
}
