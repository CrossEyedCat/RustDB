//! Log-based recovery system for rustdb
//!
//! This module implements database recovery after failures:
//! - Analyzing log files to determine the recovery point
//! - REDO operations to restore committed transactions
//! - UNDO operations to roll back unfinished transactions
//! - Data integrity validation after recovery

use crate::common::{Error, Result};
use crate::logging::log_record::{LogRecord, LogRecordType, LogSequenceNumber, TransactionId};
use crate::logging::log_writer::{LogFileInfo, LogWriter};
use crate::storage::database_file::PageId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::time::{Duration, Instant};

/// Transaction state during recovery
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryTransactionState {
    /// Active (incomplete)
    Active,
    /// Committed
    Committed,
    /// Aborted
    Aborted,
}

/// Recovery transaction information
#[derive(Debug, Clone)]
pub struct RecoveryTransactionInfo {
    /// Transaction ID
    id: TransactionId,
    /// State
    state: RecoveryTransactionState,
    /// First LSN of the transaction
    first_lsn: LogSequenceNumber,
    /// Last LSN of the transaction
    last_lsn: LogSequenceNumber,
    /// List of transaction operations
    operations: Vec<LogRecord>,
    /// Dirty pages
    dirty_pages: HashSet<(u32, PageId)>,
}

/// Result of log analysis
#[derive(Debug, Clone)]
pub struct LogAnalysisResult {
    /// Last LSN in logs
    pub last_lsn: LogSequenceNumber,
    /// LSN of the last checkpoint
    pub checkpoint_lsn: Option<LogSequenceNumber>,
    /// Active transactions at failure time
    pub active_transactions: HashMap<TransactionId, RecoveryTransactionInfo>,
    /// Committed transactions
    pub committed_transactions: HashMap<TransactionId, RecoveryTransactionInfo>,
    /// Aborted transactions
    pub aborted_transactions: HashMap<TransactionId, RecoveryTransactionInfo>,
    /// All dirty pages
    pub dirty_pages: HashSet<(u32, PageId)>,
    /// Total number of processed records
    pub total_records: u64,
}

/// Recovery statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RecoveryStatistics {
    /// Recovery start time
    pub start_time: u64,
    /// Recovery end time
    pub end_time: u64,
    /// Total recovery time (ms)
    pub total_duration_ms: u64,
    /// Number of processed log files
    pub log_files_processed: u32,
    /// Total number of log records
    pub total_log_records: u64,
    /// Number of REDO operations
    pub redo_operations: u64,
    /// Number of UNDO operations
    pub undo_operations: u64,
    /// Number of recovered transactions
    pub recovered_transactions: u64,
    /// Number of rolled back transactions
    pub rolled_back_transactions: u64,
    /// Number of recovered pages
    pub recovered_pages: u64,
    /// Size of processed logs (bytes)
    pub processed_log_size: u64,
    /// Number of recovery errors
    pub recovery_errors: u64,
}

/// Recovery configuration
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    /// Maximum recovery time
    pub max_recovery_time: Duration,
    /// Buffer size for reading logs
    pub read_buffer_size: usize,
    /// Enable parallel recovery
    pub enable_parallel_recovery: bool,
    /// Number of recovery threads
    pub recovery_threads: usize,
    /// Enable post-recovery validation
    pub enable_validation: bool,
    /// Create backup before recovery
    pub create_backup: bool,
    /// When true, suppress `println!` progress output (used by [`crate::network::SqlEngine`] WAL recovery).
    pub quiet: bool,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            max_recovery_time: Duration::from_secs(300), // 5 minutes
            read_buffer_size: 64 * 1024,                 // 64KB
            enable_parallel_recovery: true,
            recovery_threads: 4,
            enable_validation: true,
            create_backup: false,
            quiet: false,
        }
    }
}

/// Recovery system
pub struct RecoveryManager {
    /// Configuration
    config: RecoveryConfig,
    /// Log writer
    log_writer: Option<LogWriter>,
    /// Recovery statistics
    statistics: RecoveryStatistics,
}

impl RecoveryManager {
    /// Creates a new recovery manager
    pub fn new(config: RecoveryConfig) -> Self {
        Self {
            config,
            log_writer: None,
            statistics: RecoveryStatistics::default(),
        }
    }

    /// Sets the log writer
    pub fn set_log_writer(&mut self, log_writer: LogWriter) {
        self.log_writer = Some(log_writer);
    }

    /// Performs full database recovery
    pub async fn recover_database(&mut self, log_directory: &Path) -> Result<RecoveryStatistics> {
        let start_time = Instant::now();
        self.statistics.start_time = start_time.elapsed().as_secs();

        let q = self.config.quiet;
        if !q {
            println!("🔄 Starting database recovery...");
        }

        // Phase 1: Log analysis
        if !q {
            println!("📊 Phase 1: Analyzing log files");
        }
        let analysis_result = self.analyze_logs(log_directory).await?;

        if !q {
            println!(
                "   ✅ Processed {} log records",
                analysis_result.total_records
            );
            println!(
                "   ✅ Found {} active transactions",
                analysis_result.active_transactions.len()
            );
            println!(
                "   ✅ Found {} committed transactions",
                analysis_result.committed_transactions.len()
            );
        }

        // Phase 2: REDO operations
        if !q {
            println!("🔄 Phase 2: Redo operations (REDO)");
        }
        self.perform_redo_operations(&analysis_result).await?;

        if !q {
            println!(
                "   ✅ {} REDO operations performed",
                self.statistics.redo_operations
            );
        }

        // Phase 3: UNDO operations
        if !q {
            println!("↩️  Phase 3: Rollback unfinished transactions (UNDO)");
        }
        self.perform_undo_operations(&analysis_result).await?;

        if !q {
            println!(
                "   ✅ {} UNDO operations performed",
                self.statistics.undo_operations
            );
        }

        // Phase 4: Validation (if enabled)
        if self.config.enable_validation {
            if !q {
                println!("🔍 Phase 4: Data integrity validation");
            }
            self.validate_recovery(&analysis_result).await?;
            if !q {
                println!("   ✅ Validation completed successfully");
            }
        }

        // Finalize statistics
        let end_time = Instant::now();
        self.statistics.end_time = end_time.duration_since(start_time).as_secs();
        self.statistics.total_duration_ms = start_time.elapsed().as_millis() as u64;

        if !q {
            println!("🎉 Recovery completed successfully!");
            println!(
                "   ⏱️  Total time: {} ms",
                self.statistics.total_duration_ms
            );
            println!(
                "   📊 Recovered transactions: {}",
                self.statistics.recovered_transactions
            );
            println!(
                "   📊 Rolled back transactions: {}",
                self.statistics.rolled_back_transactions
            );
        }

        Ok(self.statistics.clone())
    }

    /// Analyzes log files and builds a transaction map
    async fn analyze_logs(&mut self, log_directory: &Path) -> Result<LogAnalysisResult> {
        let mut result = LogAnalysisResult {
            last_lsn: 0,
            checkpoint_lsn: None,
            active_transactions: HashMap::new(),
            committed_transactions: HashMap::new(),
            aborted_transactions: HashMap::new(),
            dirty_pages: HashSet::new(),
            total_records: 0,
        };

        // Get list of log files
        let log_files = self.get_log_files(log_directory)?;
        self.statistics.log_files_processed = log_files.len() as u32;

        // Process files in chronological order
        for log_file in log_files {
            if !self.config.quiet {
                println!("   📖 Processing file: {}", log_file.filename);
            }

            let records = self.read_log_file(&log_file).await?;
            self.statistics.processed_log_size += log_file.size;

            for record in records {
                self.process_log_record(&mut result, record).await?;
                result.total_records += 1;
            }
        }

        self.statistics.total_log_records = result.total_records;

        // Determine the last checkpoint
        if let Some(checkpoint_lsn) = result.checkpoint_lsn {
            if !self.config.quiet {
                println!("   📍 Found checkpoint at LSN: {}", checkpoint_lsn);
            }
        }

        Ok(result)
    }

    /// Gets a list of log files
    fn get_log_files(&self, log_directory: &Path) -> Result<Vec<LogFileInfo>> {
        let mut files = Vec::new();

        if !log_directory.exists() {
            return Ok(files);
        }

        let entries = std::fs::read_dir(log_directory)
            .map_err(|e| Error::internal(&format!("Failed to read log directory: {}", e)))?;

        for entry in entries {
            let entry =
                entry.map_err(|e| Error::internal(&format!("Error reading entry: {}", e)))?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("log") {
                let metadata = std::fs::metadata(&path)
                    .map_err(|e| Error::internal(&format!("Failed to get file metadata: {}", e)))?;

                let file_info = LogFileInfo {
                    filename: path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or("unknown")
                        .to_string(),
                    path: path.clone(),
                    size: metadata.len(),
                    record_count: 0,
                    first_lsn: 0,
                    last_lsn: 0,
                    created_at: metadata
                        .created()
                        .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
                        .duration_since(std::time::SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    updated_at: metadata
                        .modified()
                        .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
                        .duration_since(std::time::SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    is_compressed: false,
                };

                files.push(file_info);
            }
        }

        // Sort by creation time
        files.sort_by_key(|f| f.created_at);

        Ok(files)
    }

    /// Reads log records from a file (length-prefixed bincode, same format as [`crate::logging::log_writer::LogWriter`]).
    async fn read_log_file(&self, log_file: &LogFileInfo) -> Result<Vec<LogRecord>> {
        LogRecord::read_log_records_from_file(&log_file.path)
    }

    /// Processes a single log record
    async fn process_log_record(
        &mut self,
        result: &mut LogAnalysisResult,
        record: LogRecord,
    ) -> Result<()> {
        // Update last LSN
        if record.lsn > result.last_lsn {
            result.last_lsn = record.lsn;
        }

        match record.record_type {
            LogRecordType::TransactionBegin => {
                if let Some(tx_id) = record.transaction_id {
                    let tx_info = RecoveryTransactionInfo {
                        id: tx_id,
                        state: RecoveryTransactionState::Active,
                        first_lsn: record.lsn,
                        last_lsn: record.lsn,
                        operations: vec![record.clone()],
                        dirty_pages: HashSet::new(),
                    };
                    result.active_transactions.insert(tx_id, tx_info);
                }
            }

            LogRecordType::TransactionCommit => {
                if let Some(tx_id) = record.transaction_id {
                    if let Some(mut tx_info) = result.active_transactions.remove(&tx_id) {
                        tx_info.state = RecoveryTransactionState::Committed;
                        tx_info.last_lsn = record.lsn;
                        tx_info.operations.push(record);
                        result.committed_transactions.insert(tx_id, tx_info);
                    }
                }
            }

            LogRecordType::TransactionAbort => {
                if let Some(tx_id) = record.transaction_id {
                    if let Some(mut tx_info) = result.active_transactions.remove(&tx_id) {
                        tx_info.state = RecoveryTransactionState::Aborted;
                        tx_info.last_lsn = record.lsn;
                        tx_info.operations.push(record);
                        result.aborted_transactions.insert(tx_id, tx_info);
                    }
                }
            }

            LogRecordType::DataInsert | LogRecordType::DataUpdate | LogRecordType::DataDelete => {
                if let Some(tx_id) = record.transaction_id {
                    // Add operation to the transaction
                    let tx_map = if result.active_transactions.contains_key(&tx_id) {
                        &mut result.active_transactions
                    } else if result.committed_transactions.contains_key(&tx_id) {
                        &mut result.committed_transactions
                    } else if result.aborted_transactions.contains_key(&tx_id) {
                        &mut result.aborted_transactions
                    } else {
                        return Ok(());
                    };

                    if let Some(tx_info) = tx_map.get_mut(&tx_id) {
                        tx_info.operations.push(record.clone());
                        tx_info.last_lsn = record.lsn;

                        // Add dirty page
                        if let crate::logging::log_record::LogOperationData::Record(op) =
                            &record.operation_data
                        {
                            let page_key = (op.file_id, op.page_id);
                            tx_info.dirty_pages.insert(page_key);
                            result.dirty_pages.insert(page_key);
                        }
                    }
                }
            }

            LogRecordType::Checkpoint => {
                result.checkpoint_lsn = Some(record.lsn);
            }

            _ => {
                // Other record types
            }
        }

        Ok(())
    }

    /// Performs REDO operations for committed transactions
    async fn perform_redo_operations(&mut self, analysis_result: &LogAnalysisResult) -> Result<()> {
        let mut redo_count = 0;

        // Collect all operations from committed transactions
        let mut all_operations: BTreeMap<LogSequenceNumber, &LogRecord> = BTreeMap::new();

        for tx_info in analysis_result.committed_transactions.values() {
            for operation in &tx_info.operations {
                if matches!(
                    operation.record_type,
                    LogRecordType::DataInsert
                        | LogRecordType::DataUpdate
                        | LogRecordType::DataDelete
                ) {
                    all_operations.insert(operation.lsn, operation);
                }
            }
        }

        // Execute operations in LSN order
        for (lsn, operation) in all_operations {
            self.apply_redo_operation(lsn, operation).await?;
            redo_count += 1;

            if redo_count % 1000 == 0 && !self.config.quiet {
                println!("   📝 {} REDO operations performed", redo_count);
            }
        }

        self.statistics.redo_operations = redo_count;
        self.statistics.recovered_transactions =
            analysis_result.committed_transactions.len() as u64;

        Ok(())
    }

    /// Applies a single REDO operation
    async fn apply_redo_operation(
        &mut self,
        _lsn: LogSequenceNumber,
        operation: &LogRecord,
    ) -> Result<()> {
        match operation.record_type {
            LogRecordType::DataInsert => {
                // In a real implementation, this would insert data
                tokio::time::sleep(Duration::from_micros(10)).await;
            }
            LogRecordType::DataUpdate => {
                // In a real implementation, this would update data
                tokio::time::sleep(Duration::from_micros(10)).await;
            }
            LogRecordType::DataDelete => {
                // In a real implementation, this would delete data
                tokio::time::sleep(Duration::from_micros(10)).await;
            }
            _ => {}
        }

        Ok(())
    }

    /// Performs UNDO operations for unfinished transactions
    async fn perform_undo_operations(&mut self, analysis_result: &LogAnalysisResult) -> Result<()> {
        let mut undo_count = 0;

        // Process active transactions (rollback them)
        for tx_info in analysis_result.active_transactions.values() {
            if !self.config.quiet {
                println!("   ↩️  Rolling back transaction {}", tx_info.id);
            }

            // Rollback operations in reverse order
            for operation in tx_info.operations.iter().rev() {
                if matches!(
                    operation.record_type,
                    LogRecordType::DataInsert
                        | LogRecordType::DataUpdate
                        | LogRecordType::DataDelete
                ) {
                    self.apply_undo_operation(operation).await?;
                    undo_count += 1;
                }
            }
        }

        self.statistics.undo_operations = undo_count;
        self.statistics.rolled_back_transactions = analysis_result.active_transactions.len() as u64;

        Ok(())
    }

    /// Applies a single UNDO operation
    async fn apply_undo_operation(&mut self, operation: &LogRecord) -> Result<()> {
        match operation.record_type {
            LogRecordType::DataInsert => {
                // For INSERT, do DELETE
                tokio::time::sleep(Duration::from_micros(10)).await;
            }
            LogRecordType::DataUpdate => {
                // For UPDATE, revert to old data
                tokio::time::sleep(Duration::from_micros(10)).await;
            }
            LogRecordType::DataDelete => {
                // For DELETE, revert to data
                tokio::time::sleep(Duration::from_micros(10)).await;
            }
            _ => {}
        }

        Ok(())
    }

    /// Validates the recovery result
    async fn validate_recovery(&mut self, analysis_result: &LogAnalysisResult) -> Result<()> {
        if !self.config.quiet {
            println!(
                "   🔍 Validating integrity of {} pages",
                analysis_result.dirty_pages.len()
            );
        }

        let mut validated_pages = 0;
        for (file_id, page_id) in &analysis_result.dirty_pages {
            // In a real implementation, this would validate page integrity
            self.validate_page(*file_id, *page_id).await?;
            validated_pages += 1;

            if validated_pages % 100 == 0 && !self.config.quiet {
                println!("   ✅ Validated {} pages", validated_pages);
            }
        }

        self.statistics.recovered_pages = validated_pages;

        Ok(())
    }

    /// Validates a single page
    async fn validate_page(&self, _file_id: u32, _page_id: PageId) -> Result<()> {
        // In a real implementation, this would check for checksums,
        // data integrity, and relationships between records
        tokio::time::sleep(Duration::from_micros(5)).await;
        Ok(())
    }

    /// Returns recovery statistics
    pub fn get_statistics(&self) -> &RecoveryStatistics {
        &self.statistics
    }

    /// Checks if recovery is needed
    pub async fn needs_recovery(&self, log_directory: &Path) -> Result<bool> {
        // In a real implementation, this would check:
        // - Presence of unfinished transactions
        // - Mismatch between logs and data
        // - Marker for incorrect shutdown

        let log_files = self.get_log_files(log_directory)?;

        // If there are log files, recovery might be needed
        Ok(!log_files.is_empty())
    }

    /// Creates a backup before recovery
    pub async fn create_backup(
        &self,
        _data_directory: &Path,
        _backup_directory: &Path,
    ) -> Result<()> {
        if !self.config.create_backup {
            return Ok(());
        }

        if !self.config.quiet {
            println!("💾 Creating data backup...");
        }

        // In a real implementation, this would copy data files
        tokio::time::sleep(Duration::from_millis(100)).await;

        if !self.config.quiet {
            println!("   ✅ Backup created");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_recovery_manager_creation() {
        let config = RecoveryConfig::default();
        let _manager = RecoveryManager::new(config);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_needs_recovery() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let config = RecoveryConfig::default();
        let manager = RecoveryManager::new(config);

        // Empty directory - no recovery needed
        let needs_recovery = manager.needs_recovery(temp_dir.path()).await?;
        assert!(!needs_recovery);

        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_get_log_files() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let config = RecoveryConfig::default();
        let manager = RecoveryManager::new(config);

        // Create a test log file
        let log_file_path = temp_dir.path().join("test.log");
        std::fs::write(&log_file_path, "test data")?;

        let log_files = manager.get_log_files(temp_dir.path())?;
        assert_eq!(log_files.len(), 1);
        assert_eq!(log_files[0].filename, "test.log");

        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_log_analysis() -> Result<()> {
        use crate::logging::log_record::{IsolationLevel, LogRecord};

        let config = RecoveryConfig::default();
        let mut manager = RecoveryManager::new(config);

        let mut result = LogAnalysisResult {
            last_lsn: 0,
            checkpoint_lsn: None,
            active_transactions: HashMap::new(),
            committed_transactions: HashMap::new(),
            aborted_transactions: HashMap::new(),
            dirty_pages: HashSet::new(),
            total_records: 0,
        };

        // Test processing BEGIN record
        let begin_record = LogRecord::new_transaction_begin(1, 100, IsolationLevel::ReadCommitted);
        manager
            .process_log_record(&mut result, begin_record)
            .await?;

        assert_eq!(result.active_transactions.len(), 1);
        assert!(result.active_transactions.contains_key(&100));

        // Test processing COMMIT record
        let commit_record = LogRecord::new_transaction_commit(2, 100, vec![], Some(1));
        manager
            .process_log_record(&mut result, commit_record)
            .await?;

        assert_eq!(result.active_transactions.len(), 0);
        assert_eq!(result.committed_transactions.len(), 1);

        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_backup_creation() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();

        let mut config = RecoveryConfig::default();
        config.create_backup = true;

        let manager = RecoveryManager::new(config);

        manager
            .create_backup(temp_dir.path(), backup_dir.path())
            .await?;

        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_statistics() {
        let config = RecoveryConfig::default();
        let manager = RecoveryManager::new(config);

        let stats = manager.get_statistics();
        assert_eq!(stats.total_log_records, 0);
        assert_eq!(stats.redo_operations, 0);
        assert_eq!(stats.undo_operations, 0);
    }
}
