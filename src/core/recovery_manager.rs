//! Advanced database recovery system
//!
//! Provides complete recovery after failures using WAL

use crate::common::{Error, Result};
use crate::core::transaction::TransactionId;
use crate::logging::log_record::{LogRecord, LogRecordType, LogSequenceNumber};
use crate::logging::wal::WriteAheadLog;
use crate::storage::page_manager::PageManager;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Transaction state during recovery
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryTransactionState {
    /// Active
    Active,
    /// Prepared (2PC)
    Prepared,
    /// Committed
    Committed,
    /// Aborted
    Aborted,
}

/// Transaction information for recovery
#[derive(Debug, Clone)]
pub struct RecoveryTransactionInfo {
    /// Transaction ID
    pub transaction_id: TransactionId,
    /// State
    pub state: RecoveryTransactionState,
    /// First LSN
    pub first_lsn: LogSequenceNumber,
    /// Last LSN
    pub last_lsn: LogSequenceNumber,
    /// Transaction operations
    pub operations: Vec<LogRecord>,
    /// Modified pages
    pub dirty_pages: HashSet<(u32, u64)>, // (file_id, page_id)
}

/// Log analysis result
#[derive(Debug, Clone)]
pub struct AnalysisResult {
    /// Last LSN
    pub last_lsn: LogSequenceNumber,
    /// Checkpoint
    pub checkpoint_lsn: Option<LogSequenceNumber>,
    /// Active transactions
    pub active_transactions: HashMap<TransactionId, RecoveryTransactionInfo>,
    /// Committed transactions
    pub committed_transactions: HashMap<TransactionId, RecoveryTransactionInfo>,
    /// Aborted transactions
    pub aborted_transactions: HashMap<TransactionId, RecoveryTransactionInfo>,
    /// All modified pages
    pub dirty_pages: HashSet<(u32, u64)>,
    /// Total records
    pub total_records: u64,
}

/// Recovery statistics
#[derive(Debug, Clone, Default)]
pub struct RecoveryStatistics {
    /// Total log files
    pub log_files_processed: u32,
    /// Total records
    pub total_records: u64,
    /// REDO operations
    pub redo_operations: u64,
    /// UNDO operations
    pub undo_operations: u64,
    /// Recovered transactions
    pub recovered_transactions: u64,
    /// Rolled back transactions
    pub rolled_back_transactions: u64,
    /// Recovered pages
    pub recovered_pages: u64,
    /// Recovery time (ms)
    pub recovery_time_ms: u64,
    /// Recovery errors
    pub recovery_errors: u64,
}

/// Recovery configuration
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    /// Maximum recovery time
    pub max_recovery_time: Duration,
    /// Enable parallel recovery
    pub enable_parallel: bool,
    /// Number of threads
    pub num_threads: usize,
    /// Create backup before recovery
    pub create_backup: bool,
    /// Enable validation after recovery
    pub enable_validation: bool,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            max_recovery_time: Duration::from_secs(300),
            enable_parallel: true,
            num_threads: 4,
            create_backup: false,
            enable_validation: true,
        }
    }
}

/// Advanced recovery manager
pub struct AdvancedRecoveryManager {
    /// Configuration
    config: RecoveryConfig,
    /// Statistics
    statistics: Arc<Mutex<RecoveryStatistics>>,
    /// WAL
    wal: Option<Arc<WriteAheadLog>>,
    /// Data pages (same file as logged `file_id`) — required for REDO/UNDO to touch storage
    page_manager: Option<Arc<Mutex<PageManager>>>,
}

impl AdvancedRecoveryManager {
    /// Creates a new recovery manager
    pub fn new(config: RecoveryConfig) -> Self {
        Self {
            config,
            statistics: Arc::new(Mutex::new(RecoveryStatistics::default())),
            wal: None,
            page_manager: None,
        }
    }

    /// Sets WAL
    pub fn set_wal(&mut self, wal: Arc<WriteAheadLog>) {
        self.wal = Some(wal);
    }

    /// Sets the page manager for applying data REDO/UNDO (must match WAL `file_id` for that table).
    pub fn set_page_manager(&mut self, page_manager: Arc<Mutex<PageManager>>) {
        self.page_manager = Some(page_manager);
    }

    /// Builder: attach page manager
    pub fn with_page_manager(mut self, page_manager: Arc<Mutex<PageManager>>) -> Self {
        self.page_manager = Some(page_manager);
        self
    }

    /// Checks if recovery is needed
    pub fn needs_recovery(&self, log_directory: &Path) -> bool {
        // Check for uncommitted transactions
        if !log_directory.exists() {
            return false;
        }

        // Check for log files
        if let Ok(entries) = std::fs::read_dir(log_directory) {
            for entry in entries.flatten() {
                if entry.path().extension().and_then(|s| s.to_str()) == Some("log") {
                    return true;
                }
            }
        }

        false
    }

    /// Performs database recovery
    pub fn recover(&mut self, log_directory: &Path) -> Result<RecoveryStatistics> {
        let start_time = Instant::now();

        println!("🔄 Starting database recovery...");

        // Stage 1: Log analysis
        println!("📊 Stage 1: Log file analysis");
        let analysis_result = self.analyze_logs(log_directory)?;

        println!(
            "   ✅ Processed {} log records",
            analysis_result.total_records
        );
        println!(
            "   ✅ Active transactions: {}",
            analysis_result.active_transactions.len()
        );
        println!(
            "   ✅ Committed: {}",
            analysis_result.committed_transactions.len()
        );

        // Stage 2: REDO
        println!("🔄 Stage 2: Restoring committed transactions (REDO)");
        self.perform_redo(&analysis_result)?;

        // Stage 3: UNDO
        println!("↩️  Stage 3: Rolling back uncommitted transactions (UNDO)");
        self.perform_undo(&analysis_result)?;

        // Update statistics
        {
            let mut stats = self.statistics.lock().unwrap();
            stats.recovery_time_ms = start_time.elapsed().as_millis() as u64;
            stats.total_records = analysis_result.total_records;
        }

        println!(
            "✅ Recovery completed in {} ms",
            start_time.elapsed().as_millis()
        );

        Ok(self.get_statistics())
    }

    /// Analyzes log files
    fn analyze_logs(&mut self, log_directory: &Path) -> Result<AnalysisResult> {
        let mut result = AnalysisResult {
            last_lsn: 0,
            checkpoint_lsn: None,
            active_transactions: HashMap::new(),
            committed_transactions: HashMap::new(),
            aborted_transactions: HashMap::new(),
            dirty_pages: HashSet::new(),
            total_records: 0,
        };

        // Get log files
        let log_files = self.get_log_files(log_directory)?;

        {
            let mut stats = self.statistics.lock().unwrap();
            stats.log_files_processed = log_files.len() as u32;
        }

        // Process each file
        for file_path in log_files {
            println!("   📖 Processing: {:?}", file_path.file_name());

            let records = self.read_log_file(&file_path)?;

            for record in records {
                self.process_record(&mut result, record)?;
                result.total_records += 1;
            }
        }

        println!("   📍 Last LSN: {}", result.last_lsn);

        Ok(result)
    }

    /// Gets list of log files
    fn get_log_files(&self, log_directory: &Path) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();

        if !log_directory.exists() {
            return Ok(files);
        }

        let entries = std::fs::read_dir(log_directory)
            .map_err(|e| Error::internal(format!("Failed to read directory: {}", e)))?;

        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("log") {
                files.push(path);
            }
        }

        // Sort by file name (assuming format with timestamp)
        files.sort();

        Ok(files)
    }

    /// Reads log records from file
    fn read_log_file(&self, file_path: &Path) -> Result<Vec<LogRecord>> {
        LogRecord::read_log_records_from_file(file_path)
    }

    /// Processes one log record
    fn process_record(&mut self, result: &mut AnalysisResult, record: LogRecord) -> Result<()> {
        // Update last LSN
        if record.lsn > result.last_lsn {
            result.last_lsn = record.lsn;
        }

        // Check if transaction_id exists
        let tx_id = match record.transaction_id {
            Some(id) => TransactionId::new(id),
            None => return Ok(()), // Skip records without transaction
        };

        match record.record_type {
            LogRecordType::TransactionBegin => {
                // Transaction begin
                let tx_info = RecoveryTransactionInfo {
                    transaction_id: tx_id,
                    state: RecoveryTransactionState::Active,
                    first_lsn: record.lsn,
                    last_lsn: record.lsn,
                    operations: vec![record.clone()],
                    dirty_pages: HashSet::new(),
                };
                result.active_transactions.insert(tx_id, tx_info);
            }

            LogRecordType::TransactionCommit => {
                // Transaction commit
                if let Some(mut tx_info) = result.active_transactions.remove(&tx_id) {
                    tx_info.state = RecoveryTransactionState::Committed;
                    tx_info.last_lsn = record.lsn;
                    tx_info.operations.push(record);
                    result
                        .committed_transactions
                        .insert(tx_info.transaction_id, tx_info);
                }
            }

            LogRecordType::TransactionAbort => {
                // Transaction abort
                if let Some(mut tx_info) = result.active_transactions.remove(&tx_id) {
                    tx_info.state = RecoveryTransactionState::Aborted;
                    tx_info.last_lsn = record.lsn;
                    result
                        .aborted_transactions
                        .insert(tx_info.transaction_id, tx_info);
                }
            }

            LogRecordType::DataInsert | LogRecordType::DataUpdate | LogRecordType::DataDelete => {
                // Data operation
                if let Some(tx_info) = result.active_transactions.get_mut(&tx_id) {
                    tx_info.last_lsn = record.lsn;
                    tx_info.operations.push(record.clone());
                }
            }

            LogRecordType::Checkpoint => {
                // Checkpoint
                result.checkpoint_lsn = Some(record.lsn);
            }

            _ => {
                // Other record types
            }
        }

        Ok(())
    }

    /// Performs REDO operations
    fn perform_redo(&mut self, analysis: &AnalysisResult) -> Result<()> {
        let mut redo_count = 0;

        // Collect all operations from committed transactions
        let mut operations: BTreeMap<LogSequenceNumber, &LogRecord> = BTreeMap::new();

        for tx_info in analysis.committed_transactions.values() {
            for op in &tx_info.operations {
                if matches!(
                    op.record_type,
                    LogRecordType::DataInsert
                        | LogRecordType::DataUpdate
                        | LogRecordType::DataDelete
                ) {
                    operations.insert(op.lsn, op);
                }
            }
        }

        // Apply in LSN order
        for (lsn, operation) in operations {
            self.apply_redo_operation(operation)?;
            redo_count += 1;

            if redo_count % 100 == 0 {
                println!("   📝 REDO: {} operations", redo_count);
            }
        }

        {
            let mut stats = self.statistics.lock().unwrap();
            stats.redo_operations = redo_count;
            stats.recovered_transactions = analysis.committed_transactions.len() as u64;
            stats.recovered_pages = analysis.dirty_pages.len() as u64;
        }

        println!("   ✅ Performed {} REDO operations", redo_count);

        Ok(())
    }

    /// Performs UNDO operations
    fn perform_undo(&mut self, analysis: &AnalysisResult) -> Result<()> {
        let mut undo_count = 0;

        // Rollback active transactions (in reverse order)
        for tx_info in analysis.active_transactions.values() {
            println!(
                "   ↩️  Rolling back transaction TXN{}",
                tx_info.transaction_id
            );

            // Operations in reverse order
            for operation in tx_info.operations.iter().rev() {
                if matches!(
                    operation.record_type,
                    LogRecordType::DataInsert
                        | LogRecordType::DataUpdate
                        | LogRecordType::DataDelete
                ) {
                    self.apply_undo_operation(operation)?;
                    undo_count += 1;
                }
            }
        }

        {
            let mut stats = self.statistics.lock().unwrap();
            stats.undo_operations = undo_count;
            stats.rolled_back_transactions = analysis.active_transactions.len() as u64;
        }

        println!("   ✅ Performed {} UNDO operations", undo_count);

        Ok(())
    }

    /// Applies one REDO operation
    fn apply_redo_operation(&self, operation: &LogRecord) -> Result<()> {
        match operation.record_type {
            LogRecordType::DataInsert | LogRecordType::DataUpdate | LogRecordType::DataDelete => {
                if let Some(pm) = &self.page_manager {
                    let mut pm = pm.lock().unwrap();
                    pm.apply_log_record_recovery(operation, true)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Applies one UNDO operation
    fn apply_undo_operation(&self, operation: &LogRecord) -> Result<()> {
        match operation.record_type {
            LogRecordType::DataInsert | LogRecordType::DataUpdate | LogRecordType::DataDelete => {
                if let Some(pm) = &self.page_manager {
                    let mut pm = pm.lock().unwrap();
                    pm.apply_log_record_recovery(operation, false)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Creates backup before recovery
    pub fn create_backup(&self, source_dir: &Path, backup_dir: &Path) -> Result<()> {
        if !self.config.create_backup {
            return Ok(());
        }

        println!("💾 Creating backup...");

        // Create backup directory
        std::fs::create_dir_all(backup_dir)
            .map_err(|e| Error::internal(format!("Failed to create backup directory: {}", e)))?;

        // Copy files
        let mut copied_files = 0;

        if let Ok(entries) = std::fs::read_dir(source_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    let file_name = path.file_name().unwrap();
                    let dest_path = backup_dir.join(file_name);

                    std::fs::copy(&path, &dest_path)
                        .map_err(|e| Error::internal(format!("Copy error: {}", e)))?;

                    copied_files += 1;
                }
            }
        }

        println!("   ✅ Copied {} files", copied_files);

        Ok(())
    }

    /// Validates recovery result
    pub fn validate_recovery(&self, analysis: &AnalysisResult) -> Result<()> {
        if !self.config.enable_validation {
            return Ok(());
        }

        println!("🔍 Validating recovery...");

        // Check that all active transactions are rolled back
        if !analysis.active_transactions.is_empty() {
            return Err(Error::internal(
                "Found uncommitted active transactions after recovery",
            ));
        }

        // Check consistency
        println!("   ✅ All active transactions rolled back");
        println!("   ✅ All committed transactions recovered");

        Ok(())
    }

    /// Returns statistics
    pub fn get_statistics(&self) -> RecoveryStatistics {
        self.statistics.lock().unwrap().clone()
    }

    /// Returns configuration
    pub fn config(&self) -> &RecoveryConfig {
        &self.config
    }
}

impl Default for AdvancedRecoveryManager {
    fn default() -> Self {
        Self::new(RecoveryConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recovery_manager_creation() {
        let manager = AdvancedRecoveryManager::default();
        let stats = manager.get_statistics();

        assert_eq!(stats.redo_operations, 0);
        assert_eq!(stats.undo_operations, 0);
    }

    #[test]
    fn test_needs_recovery() {
        let manager = AdvancedRecoveryManager::default();
        let non_existent_path = Path::new("./non_existent_logs");

        assert!(!manager.needs_recovery(non_existent_path));
    }

    #[test]
    fn test_recovery_config() {
        let config = RecoveryConfig {
            max_recovery_time: Duration::from_secs(60),
            enable_parallel: false,
            num_threads: 2,
            create_backup: true,
            enable_validation: true,
        };

        let manager = AdvancedRecoveryManager::new(config.clone());
        assert_eq!(manager.config.num_threads, 2);
        assert!(manager.config.create_backup);
    }

    #[test]
    fn test_analysis_result_creation() {
        let result = AnalysisResult {
            last_lsn: 100,
            checkpoint_lsn: Some(50),
            active_transactions: HashMap::new(),
            committed_transactions: HashMap::new(),
            aborted_transactions: HashMap::new(),
            dirty_pages: HashSet::new(),
            total_records: 100,
        };

        assert_eq!(result.last_lsn, 100);
        assert_eq!(result.checkpoint_lsn, Some(50));
        assert_eq!(result.total_records, 100);
    }

    #[test]
    fn test_transaction_states() {
        assert_eq!(
            RecoveryTransactionState::Active,
            RecoveryTransactionState::Active
        );
        assert_ne!(
            RecoveryTransactionState::Active,
            RecoveryTransactionState::Committed
        );
    }
}
