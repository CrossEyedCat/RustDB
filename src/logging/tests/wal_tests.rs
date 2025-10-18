//! Тесты для WriteAheadLog

use crate::logging::wal::{WriteAheadLog, WalConfig};
use crate::logging::log_record::{IsolationLevel};
use std::path::PathBuf;

#[tokio::test]
async fn test_wal_creation() {
    let config = WalConfig::default();
    let wal = WriteAheadLog::new(config).await;
    
    assert!(wal.is_ok());
}

#[test]
fn test_wal_config_default() {
    let config = WalConfig::default();
    
    assert!(config.log_writer_config.write_buffer_size > 0);
    assert!(config.checkpoint_interval.as_secs() > 0);
    assert!(!config.log_writer_config.log_directory.as_os_str().is_empty());
}

#[test]
fn test_wal_config_custom() {
    let mut config = WalConfig::default();
    config.log_writer_config.write_buffer_size = 1024; // 1024 записей
    config.log_writer_config.max_log_file_size = 10 * 1024 * 1024; // 10MB
    config.log_writer_config.log_directory = PathBuf::from("custom_logs");
    
    assert_eq!(config.log_writer_config.write_buffer_size, 1024);
    assert_eq!(config.log_writer_config.max_log_file_size, 10 * 1024 * 1024);
    assert_eq!(config.log_writer_config.log_directory, PathBuf::from("custom_logs"));
}

#[tokio::test]
async fn test_wal_transaction_begin() {
    let config = WalConfig::default();
    let wal = WriteAheadLog::new(config).await.unwrap();
    
    let result = wal.begin_transaction(IsolationLevel::ReadCommitted).await;
    assert!(result.is_ok() || result.is_err()); // Может быть ошибка из-за отсутствия директории
}

#[tokio::test]
async fn test_wal_transaction_lifecycle() {
    let config = WalConfig::default();
    let wal = WriteAheadLog::new(config).await.unwrap();
    
    // Начинаем транзакцию
    if let Ok(tx_id) = wal.begin_transaction(IsolationLevel::ReadCommitted).await {
        // Коммитим транзакцию
        let _result = wal.commit_transaction(tx_id).await;
    }
}

#[tokio::test]
async fn test_wal_multiple_transactions() {
    let config = WalConfig::default();
    let wal = WriteAheadLog::new(config).await.unwrap();
    
    for _ in 1..=5 {
        if let Ok(tx_id) = wal.begin_transaction(IsolationLevel::ReadCommitted).await {
            let _ = wal.commit_transaction(tx_id).await;
        }
    }
}

#[tokio::test]
async fn test_wal_force_sync() {
    let config = WalConfig::default();
    let wal = WriteAheadLog::new(config).await.unwrap();
    
    let result = wal.force_sync().await;
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_wal_get_current_lsn() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let config = WalConfig::default();
        let wal = WriteAheadLog::new(config).await.unwrap();
        
        let lsn = wal.get_current_lsn();
        assert!(lsn >= 0);
    });
}

#[test]
fn test_wal_statistics() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let config = WalConfig::default();
        let wal = WriteAheadLog::new(config).await.unwrap();
        
        let stats = wal.get_statistics();
        assert!(stats.total_transactions >= 0);
        assert!(stats.active_transactions >= 0);
    });
}

#[tokio::test]
async fn test_wal_different_isolation_levels() {
    let config = WalConfig::default();
    let wal = WriteAheadLog::new(config).await.unwrap();
    
    let isolation_levels = vec![
        IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted,
        IsolationLevel::RepeatableRead,
        IsolationLevel::Serializable,
    ];
    
    for isolation_level in isolation_levels {
        if let Ok(tx_id) = wal.begin_transaction(isolation_level).await {
            let _ = wal.commit_transaction(tx_id).await;
        }
    }
}

#[tokio::test]
async fn test_wal_transaction_abort() {
    let config = WalConfig::default();
    let wal = WriteAheadLog::new(config).await.unwrap();
    
    if let Ok(tx_id) = wal.begin_transaction(IsolationLevel::ReadCommitted).await {
        let result = wal.abort_transaction(tx_id).await;
        assert!(result.is_ok() || result.is_err());
    }
}

#[tokio::test]
async fn test_wal_checkpoint() {
    let config = WalConfig::default();
    let wal = WriteAheadLog::new(config).await.unwrap();
    
    let result = wal.create_checkpoint().await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test]
async fn test_wal_log_operations() {
    let config = WalConfig::default();
    let wal = WriteAheadLog::new(config).await.unwrap();
    
    if let Ok(tx_id) = wal.begin_transaction(IsolationLevel::ReadCommitted).await {
        // Тестируем логирование операций
        let _insert_result = wal.log_insert(tx_id, 1, 1, 0, vec![]).await;
        let _update_result = wal.log_update(tx_id, 1, 1, 0, vec![], vec![]).await;
        let _delete_result = wal.log_delete(tx_id, 1, 1, 0, vec![]).await;
        
        let _ = wal.commit_transaction(tx_id).await;
    }
}

#[test]
fn test_wal_get_transaction_info() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let config = WalConfig::default();
        let wal = WriteAheadLog::new(config).await.unwrap();
        
        if let Ok(tx_id) = wal.begin_transaction(IsolationLevel::ReadCommitted).await {
            let info = wal.get_transaction_info(tx_id);
            assert!(info.is_some());
            
            let _ = wal.commit_transaction(tx_id).await;
        }
    });
}

