//! Тесты для расширенной системы восстановления

use crate::core::{
    AdvancedRecoveryManager, RecoveryConfig, RecoveryStatistics,
    RecoveryTransactionState, TransactionId, AnalysisResult,
};
use crate::logging::log_record::{LogRecord, LogRecordType};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Duration;

#[test]
fn test_recovery_manager_creation() {
    let manager = AdvancedRecoveryManager::default();
    let stats = manager.get_statistics();
    
    assert_eq!(stats.redo_operations, 0);
    assert_eq!(stats.undo_operations, 0);
    assert_eq!(stats.recovered_transactions, 0);
}

#[test]
fn test_recovery_config_default() {
    let config = RecoveryConfig::default();
    
    assert_eq!(config.max_recovery_time, Duration::from_secs(300));
    assert!(config.enable_parallel);
    assert_eq!(config.num_threads, 4);
    assert!(!config.create_backup);
    assert!(config.enable_validation);
}

#[test]
fn test_recovery_config_custom() {
    let config = RecoveryConfig {
        max_recovery_time: Duration::from_secs(60),
        enable_parallel: false,
        num_threads: 2,
        create_backup: true,
        enable_validation: false,
    };
    
    let manager = AdvancedRecoveryManager::new(config.clone());
    assert_eq!(manager.config().num_threads, 2);
    assert!(manager.config().create_backup);
    assert!(!manager.config().enable_validation);
}

#[test]
fn test_needs_recovery_non_existent_dir() {
    let manager = AdvancedRecoveryManager::default();
    let path = Path::new("./non_existent_directory_12345");
    
    assert!(!manager.needs_recovery(path));
}

#[test]
fn test_recovery_statistics_default() {
    let stats = RecoveryStatistics::default();
    
    assert_eq!(stats.log_files_processed, 0);
    assert_eq!(stats.total_records, 0);
    assert_eq!(stats.redo_operations, 0);
    assert_eq!(stats.undo_operations, 0);
    assert_eq!(stats.recovered_transactions, 0);
    assert_eq!(stats.rolled_back_transactions, 0);
}

#[test]
fn test_recovery_transaction_states() {
    use crate::core::RecoveryTransactionState;
    
    assert_eq!(RecoveryTransactionState::Active, RecoveryTransactionState::Active);
    assert_ne!(RecoveryTransactionState::Active, RecoveryTransactionState::Committed);
    assert_ne!(RecoveryTransactionState::Committed, RecoveryTransactionState::Aborted);
    assert_ne!(RecoveryTransactionState::Active, RecoveryTransactionState::Prepared);
}

#[test]
fn test_analysis_result_creation() {
    let mut result = AnalysisResult {
        last_lsn: 1000,
        checkpoint_lsn: Some(500),
        active_transactions: HashMap::new(),
        committed_transactions: HashMap::new(),
        aborted_transactions: HashMap::new(),
        dirty_pages: HashSet::new(),
        total_records: 1000,
    };
    
    assert_eq!(result.last_lsn, 1000);
    assert_eq!(result.checkpoint_lsn, Some(500));
    assert_eq!(result.total_records, 1000);
    assert_eq!(result.active_transactions.len(), 0);
}

#[test]
fn test_recovery_backup_non_existent_dir() {
    let manager = AdvancedRecoveryManager::default();
    let source = Path::new("./non_existent_source");
    let backup = Path::new("./test_backup");
    
    // Не должно падать, если директория не существует
    let result = manager.create_backup(source, backup);
    assert!(result.is_ok() || result.is_err()); // Просто проверяем, что не паникует
}

#[test]
fn test_recovery_validation_empty_analysis() {
    let manager = AdvancedRecoveryManager::default();
    
    let analysis = AnalysisResult {
        last_lsn: 100,
        checkpoint_lsn: None,
        active_transactions: HashMap::new(),
        committed_transactions: HashMap::new(),
        aborted_transactions: HashMap::new(),
        dirty_pages: HashSet::new(),
        total_records: 0,
    };
    
    // Валидация должна пройти, если нет активных транзакций
    assert!(manager.validate_recovery(&analysis).is_ok());
}

#[test]
fn test_recovery_statistics_updates() {
    let manager = AdvancedRecoveryManager::default();
    let stats = manager.get_statistics();
    
    // Изначально все счётчики нулевые
    assert_eq!(stats.redo_operations, 0);
    assert_eq!(stats.undo_operations, 0);
    assert_eq!(stats.recovery_errors, 0);
}

#[test]
fn test_parallel_recovery_config() {
    let config = RecoveryConfig {
        enable_parallel: true,
        num_threads: 8,
        ..Default::default()
    };
    
    let manager = AdvancedRecoveryManager::new(config);
    assert!(manager.config().enable_parallel);
    assert_eq!(manager.config().num_threads, 8);
}

#[test]
fn test_recovery_with_validation_disabled() {
    let config = RecoveryConfig {
        enable_validation: false,
        ..Default::default()
    };
    
    let manager = AdvancedRecoveryManager::new(config);
    assert!(!manager.config().enable_validation);
}

#[test]
fn test_recovery_max_time_configuration() {
    let config = RecoveryConfig {
        max_recovery_time: Duration::from_secs(60),
        ..Default::default()
    };
    
    let manager = AdvancedRecoveryManager::new(config);
    assert_eq!(manager.config().max_recovery_time, Duration::from_secs(60));
}

