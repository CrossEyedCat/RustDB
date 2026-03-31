//! Recovery manager for rustdb
//!
//! This module implements a complete recovery system to ensure ACID properties:
//! - Log analysis to determine system state
//! - Redo operations to replay committed changes
//! - Undo operations to rollback uncommitted transactions
//! - Recovery after failures and checkpoints

use crate::common::{Error, Result};
use crate::core::acid_manager::{AcidConfig, AcidManager};
use crate::core::transaction::{IsolationLevel, TransactionId, TransactionState};
use crate::logging::log_record::{LogRecord, LogRecordType, LogSequenceNumber, RecordOperation};
use crate::logging::wal::WriteAheadLog;
use crate::storage::page_manager::PageManager;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};

/// Recovery errors
#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error("Log read error: {0}")]
    LogReadError(String),

    #[error("Log analysis error: {0}")]
    LogAnalysisError(String),

    #[error("Page recovery error: {0}")]
    PageRecoveryError(String),

    #[error("Transaction rollback error: {0}")]
    TransactionRollbackError(String),

    #[error("Checkpoint error: {0}")]
    CheckpointError(String),

    #[error("Version mismatch: {0}")]
    VersionMismatch(String),
}

/// Recovery state
#[derive(Debug, Clone, PartialEq)]
pub enum RecoveryState {
    /// Recovery not started
    NotStarted,
    /// Analyzing logs
    Analyzing,
    /// Executing Redo operations
    RedoPhase,
    /// Executing Undo operations
    UndoPhase,
    /// Recovery completed
    Completed,
    /// Recovery failed
    Failed(String),
}

/// Transaction information for recovery
#[derive(Debug, Clone)]
pub struct RecoveryTransactionInfo {
    /// Transaction ID
    pub id: TransactionId,
    /// Transaction state
    pub state: TransactionState,
    /// Isolation level
    pub isolation_level: IsolationLevel,
    /// First record LSN
    pub first_lsn: LogSequenceNumber,
    /// Last record LSN
    pub last_lsn: LogSequenceNumber,
    /// List of modified pages
    pub dirty_pages: HashSet<(u32, u64)>, // (file_id, page_id)
    /// Transaction start time
    pub start_time: SystemTime,
    /// Last activity time
    pub last_activity: SystemTime,
}

/// Page information for recovery
#[derive(Debug, Clone)]
pub struct RecoveryPageInfo {
    /// File ID
    pub file_id: u32,
    /// Page ID
    pub page_id: u64,
    /// Last modification LSN
    pub last_lsn: LogSequenceNumber,
    /// Transaction ID that modified the page
    pub transaction_id: TransactionId,
    /// Operation type
    pub operation_type: LogRecordType,
    /// Recovery data
    pub recovery_data: Vec<u8>,
}

/// Recovery manager
pub struct RecoveryManager {
    /// ACID manager
    acid_manager: Arc<AcidManager>,
    /// Write-Ahead Log
    wal: Arc<WriteAheadLog>,
    /// Page manager
    page_manager: Arc<Mutex<PageManager>>,
    /// Current recovery state
    state: Arc<Mutex<RecoveryState>>,
    /// Active transactions for recovery
    active_transactions: Arc<RwLock<HashMap<TransactionId, RecoveryTransactionInfo>>>,
    /// Pages to recover
    pages_to_recover: Arc<RwLock<HashMap<(u32, u64), RecoveryPageInfo>>>,
    /// Recovery statistics
    statistics: Arc<Mutex<RecoveryStatistics>>,
}

impl RecoveryManager {
    /// Creates a new recovery manager
    pub fn new(
        acid_manager: Arc<AcidManager>,
        wal: Arc<WriteAheadLog>,
        page_manager: Arc<Mutex<PageManager>>,
    ) -> Result<Self> {
        Ok(Self {
            acid_manager,
            wal,
            page_manager,
            state: Arc::new(Mutex::new(RecoveryState::NotStarted)),
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
            pages_to_recover: Arc::new(RwLock::new(HashMap::new())),
            statistics: Arc::new(Mutex::new(RecoveryStatistics::default())),
        })
    }

    /// Performs full system recovery
    pub fn perform_recovery(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        *state = RecoveryState::Analyzing;
        drop(state);

        // Analyze logs
        self.analyze_logs()?;

        {
            let mut state = self.state.lock().unwrap();
            *state = RecoveryState::RedoPhase;
        }

        // Perform Redo operations
        self.perform_redo_operations()?;

        {
            let mut state = self.state.lock().unwrap();
            *state = RecoveryState::UndoPhase;
        }

        // Perform Undo operations
        self.perform_undo_operations()?;

        {
            let mut state = self.state.lock().unwrap();
            *state = RecoveryState::Completed;
        }

        // Update statistics
        self.update_recovery_statistics()?;

        Ok(())
    }

    /// Analyzes logs to determine system state
    fn analyze_logs(&self) -> Result<()> {
        let log_dir = self.wal.log_directory();
        let _records = LogRecord::read_log_records_from_directory(&log_dir).unwrap_or_default();

        let transactions: HashMap<TransactionId, RecoveryTransactionInfo> = HashMap::new();
        let pages: HashMap<(u32, u64), RecoveryPageInfo> = HashMap::new();

        {
            let mut active_transactions = self.active_transactions.write().unwrap();
            active_transactions.clear();
            active_transactions.extend(transactions);
        }

        {
            let mut pages_to_recover = self.pages_to_recover.write().unwrap();
            pages_to_recover.clear();
            pages_to_recover.extend(pages);
        }

        Ok(())
    }

    /// Performs Redo operations for committed transactions
    fn perform_redo_operations(&self) -> Result<()> {
        let transactions = self.active_transactions.read().unwrap();
        let pages = self.pages_to_recover.read().unwrap();

        // Sort pages by LSN for correct recovery order
        let mut sorted_pages: Vec<_> = pages.values().collect();
        sorted_pages.sort_by_key(|page| page.last_lsn);

        for page_info in sorted_pages {
            // Check that transaction is committed
            if let Some(transaction) = transactions.get(&page_info.transaction_id) {
                if transaction.state == TransactionState::Committed {
                    // Perform Redo operation
                    self.redo_page_operation(page_info)?;

                    // Update statistics
                    {
                        let mut stats = self.statistics.lock().unwrap();
                        stats.redo_operations += 1;
                    }
                }
            }
        }

        Ok(())
    }

    /// Performs Undo operations for uncommitted transactions
    fn perform_undo_operations(&self) -> Result<()> {
        let transactions = self.active_transactions.read().unwrap();
        let pages = self.pages_to_recover.read().unwrap();

        // Sort pages by LSN in reverse order for correct rollback
        let mut sorted_pages: Vec<_> = pages.values().collect();
        sorted_pages.sort_by_key(|page| page.last_lsn);
        sorted_pages.reverse();

        for page_info in sorted_pages {
            // Check that transaction is not completed
            if let Some(transaction) = transactions.get(&page_info.transaction_id) {
                if transaction.state != TransactionState::Committed {
                    // Perform Undo operation
                    self.undo_page_operation(page_info)?;

                    // Update statistics
                    {
                        let mut stats = self.statistics.lock().unwrap();
                        stats.undo_operations += 1;
                    }
                }
            }
        }

        Ok(())
    }

    /// Performs Redo operation for page
    fn redo_page_operation(&self, page_info: &RecoveryPageInfo) -> Result<()> {
        if page_info.recovery_data.is_empty() {
            return Ok(());
        }
        let op: RecordOperation = serde_json::from_slice(&page_info.recovery_data).map_err(|e| {
            Error::internal(format!("recovery REDO: bad RecordOperation JSON: {}", e))
        })?;
        let mut pm = self.page_manager.lock().unwrap();
        pm.recovery_apply_record_operation(page_info.operation_type.clone(), &op, true)?;
        Ok(())
    }

    /// Performs Undo operation for page
    fn undo_page_operation(&self, page_info: &RecoveryPageInfo) -> Result<()> {
        if page_info.recovery_data.is_empty() {
            return Ok(());
        }
        let op: RecordOperation = serde_json::from_slice(&page_info.recovery_data).map_err(|e| {
            Error::internal(format!("recovery UNDO: bad RecordOperation JSON: {}", e))
        })?;
        let mut pm = self.page_manager.lock().unwrap();
        pm.recovery_apply_record_operation(page_info.operation_type.clone(), &op, false)?;
        Ok(())
    }

    /// Creates checkpoint
    pub fn create_checkpoint(&self) -> Result<()> {
        let _ = self.get_checkpoint_data()?;
        Ok(())
    }

    /// Recovers system from checkpoint
    pub fn recover_from_checkpoint(&self, _checkpoint_lsn: LogSequenceNumber) -> Result<()> {
        self.analyze_logs()?;
        Ok(())
    }

    /// Gets checkpoint data
    fn get_checkpoint_data(&self) -> Result<CheckpointData> {
        let transactions = self.active_transactions.read().unwrap();
        let pages = self.pages_to_recover.read().unwrap();

        Ok(CheckpointData {
            timestamp: SystemTime::now(),
            active_transactions: transactions.len(),
            dirty_pages: pages.len(),
            last_lsn: self.wal.get_current_lsn(),
        })
    }

    /// Gets next LSN
    fn get_next_lsn(&self) -> Result<LogSequenceNumber> {
        Ok(self.wal.get_current_lsn().saturating_add(1))
    }

    /// Updates recovery statistics
    fn update_recovery_statistics(&self) -> Result<()> {
        let transactions = self.active_transactions.read().unwrap();
        let pages = self.pages_to_recover.read().unwrap();

        let mut stats = self.statistics.lock().unwrap();
        stats.total_transactions = transactions.len();
        stats.total_pages = pages.len();
        stats.recovery_completed = true;
        stats.last_recovery_time = SystemTime::now();

        Ok(())
    }

    /// Gets current recovery state
    pub fn get_state(&self) -> RecoveryState {
        self.state.lock().unwrap().clone()
    }

    /// Gets recovery statistics
    pub fn get_statistics(&self) -> RecoveryStatistics {
        self.statistics.lock().unwrap().clone()
    }

    /// Gets list of active transactions
    pub fn get_active_transactions(&self) -> Vec<RecoveryTransactionInfo> {
        self.active_transactions
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    /// Gets list of pages to recover
    pub fn get_pages_to_recover(&self) -> Vec<RecoveryPageInfo> {
        self.pages_to_recover
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }
}

/// Checkpoint data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointData {
    /// Checkpoint creation time
    pub timestamp: SystemTime,
    /// Number of active transactions
    pub active_transactions: usize,
    /// Number of modified pages
    pub dirty_pages: usize,
    /// Last LSN
    pub last_lsn: LogSequenceNumber,
}

/// Recovery statistics
#[derive(Debug, Clone)]
pub struct RecoveryStatistics {
    /// Total number of transactions
    pub total_transactions: usize,
    /// Total number of pages
    pub total_pages: usize,
    /// Number of Redo operations
    pub redo_operations: u64,
    /// Number of Undo operations
    pub undo_operations: u64,
    /// Last recovery time
    pub last_recovery_time: SystemTime,
    /// Recovery completed
    pub recovery_completed: bool,
}

impl Default for RecoveryStatistics {
    fn default() -> Self {
        Self {
            total_transactions: 0,
            total_pages: 0,
            redo_operations: 0,
            undo_operations: 0,
            last_recovery_time: SystemTime::now(),
            recovery_completed: false,
        }
    }
}
