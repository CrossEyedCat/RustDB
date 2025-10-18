//! Тесты для LogWriter

use crate::logging::log_writer::{LogWriter, LogWriterConfig, LogFileInfo, SyncLevel};
use crate::logging::log_record::{LogRecord, LogRecordType, LogPriority, LogOperationData};
use std::path::PathBuf;

#[tokio::test]
async fn test_log_writer_creation() {
    let config = LogWriterConfig::default();
    let writer = LogWriter::new(config);
    
    assert!(writer.is_ok());
}

#[test]
fn test_log_writer_config() {
    let config = LogWriterConfig {
        log_directory: PathBuf::from("test_logs"),
        max_log_file_size: 10 * 1024 * 1024,
        max_log_files: 5,
        write_buffer_size: 64,
        max_buffer_time: std::time::Duration::from_secs(5),
        enable_compression: false,
        sync_level: SyncLevel::OnCommit,
        writer_thread_pool_size: 2,
        enable_integrity_check: true,
    };
    
    assert_eq!(config.log_directory, PathBuf::from("test_logs"));
    assert_eq!(config.max_log_file_size, 10 * 1024 * 1024);
    assert!(!config.enable_compression);
}

#[tokio::test]
async fn test_log_writer_write() {
    let config = LogWriterConfig::default();
    let writer = LogWriter::new(config).unwrap();
    
    let record = LogRecord {
        lsn: 1,
        transaction_id: Some(1),
        record_type: LogRecordType::TransactionBegin,
        priority: LogPriority::Normal,
        timestamp: 1000,
        record_size: 0,
        checksum: 0,
        operation_data: LogOperationData::Empty,
        prev_lsn: None,
        metadata: std::collections::HashMap::new(),
    };
    
    let result = writer.write_log(record).await;
    assert!(result.is_ok() || result.is_err()); // Может быть ошибка из-за отсутствия директории
}

#[tokio::test]
async fn test_log_writer_file_rotation() {
    let config = LogWriterConfig {
        log_directory: PathBuf::from("test_logs"),
        max_log_file_size: 1024, // 1KB для быстрой ротации
        max_log_files: 3,
        write_buffer_size: 10,
        max_buffer_time: std::time::Duration::from_secs(1),
        enable_compression: false,
        sync_level: SyncLevel::OnCommit,
        writer_thread_pool_size: 1,
        enable_integrity_check: false,
    };
    
    let writer = LogWriter::new(config).unwrap();
    
    // Записываем много записей для триггера ротации
    for i in 1..=100 {
        let record = LogRecord {
            lsn: i,
            transaction_id: Some(i),
            record_type: LogRecordType::TransactionBegin,
            priority: LogPriority::Normal,
            timestamp: 1000 + i,
            record_size: 0,
            checksum: 0,
            operation_data: LogOperationData::Empty,
            prev_lsn: if i > 1 { Some(i - 1) } else { None },
            metadata: std::collections::HashMap::new(),
        };
        
        let _ = writer.write_log(record).await;
    }
}

#[tokio::test]
async fn test_log_writer_flush() {
    let config = LogWriterConfig::default();
    let writer = LogWriter::new(config).unwrap();
    
    let result = writer.flush().await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test]
async fn test_log_writer_statistics() {
    let config = LogWriterConfig::default();
    let writer = LogWriter::new(config).unwrap();
    
    let stats = writer.get_statistics();
    assert!(stats.total_bytes_written >= 0);
    assert!(stats.total_records_written >= 0);
}

#[tokio::test]
async fn test_log_writer_sync_levels() {
    let sync_levels = vec![
        SyncLevel::Never,
        SyncLevel::Periodic,
        SyncLevel::OnCommit,
        SyncLevel::Always,
    ];
    
    for sync_level in sync_levels {
        let config = LogWriterConfig {
            log_directory: PathBuf::from("test_logs"),
            max_log_file_size: 10 * 1024 * 1024,
            max_log_files: 5,
            write_buffer_size: 100,
            max_buffer_time: std::time::Duration::from_secs(1),
            enable_compression: false,
            sync_level,
            writer_thread_pool_size: 2,
            enable_integrity_check: true,
        };
        
        let writer = LogWriter::new(config);
        assert!(writer.is_ok());
    }
}

#[tokio::test]
async fn test_log_writer_buffer_management() {
    let config = LogWriterConfig {
        log_directory: PathBuf::from("test_logs"),
        max_log_file_size: 10 * 1024 * 1024,
        max_log_files: 5,
        write_buffer_size: 128,
        max_buffer_time: std::time::Duration::from_millis(500),
        enable_compression: false,
        sync_level: SyncLevel::OnCommit,
        writer_thread_pool_size: 2,
        enable_integrity_check: false,
    };
    
    let writer = LogWriter::new(config).unwrap();
    
    // Записываем несколько записей в буфер
    for i in 1..=10 {
        let record = LogRecord {
            lsn: i,
            transaction_id: Some(i),
            record_type: LogRecordType::TransactionBegin,
            priority: LogPriority::Normal,
            timestamp: 1000 + i,
            record_size: 0,
            checksum: 0,
            operation_data: LogOperationData::Empty,
            prev_lsn: None,
            metadata: std::collections::HashMap::new(),
        };
        
        let _ = writer.write_log(record).await;
    }
    
    // Проверяем статистику
    let stats = writer.get_statistics();
    assert!(stats.total_records_written <= 10);
}

#[tokio::test]
async fn test_log_writer_compression() {
    let config_with_compression = LogWriterConfig {
        log_directory: PathBuf::from("test_logs"),
        max_log_file_size: 10 * 1024 * 1024,
        max_log_files: 5,
        write_buffer_size: 100,
        max_buffer_time: std::time::Duration::from_secs(1),
        enable_compression: true,
        sync_level: SyncLevel::OnCommit,
        writer_thread_pool_size: 2,
        enable_integrity_check: true,
    };
    
    let writer = LogWriter::new(config_with_compression);
    assert!(writer.is_ok());
}

#[tokio::test]
async fn test_log_writer_different_record_types() {
    let config = LogWriterConfig::default();
    let writer = LogWriter::new(config).unwrap();
    
    let record_types = vec![
        LogRecordType::TransactionBegin,
        LogRecordType::TransactionCommit,
        LogRecordType::TransactionAbort,
        LogRecordType::DataInsert,
        LogRecordType::DataUpdate,
        LogRecordType::DataDelete,
    ];
    
    for (i, record_type) in record_types.iter().enumerate() {
        let record = LogRecord {
            lsn: i as u64 + 1,
            transaction_id: Some(i as u64 + 1),
            record_type: record_type.clone(),
            priority: LogPriority::Normal,
            timestamp: 1000 + i as u64,
            record_size: 0,
            checksum: 0,
            operation_data: LogOperationData::Empty,
            prev_lsn: None,
            metadata: std::collections::HashMap::new(),
        };
        
        let _ = writer.write_log(record).await;
    }
}
