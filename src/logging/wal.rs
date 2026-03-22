//! Write-Ahead Logging (WAL) system for rustdb
//!
//! This module implements WAL - a key component for ensuring ACID properties:
//! - Ensures that changes are written to the log first, then to data
//! - Ensures transaction atomicity and durability
//! - Supports recovery after failures
//! - Integrates with transaction and locking systems

use crate::common::{Error, Result};
use crate::logging::log_record::{IsolationLevel, LogRecord, LogSequenceNumber, TransactionId};
use crate::logging::log_writer::{LogWriter, LogWriterConfig};
use crate::storage::database_file::PageId;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;

/// WAL system configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    /// Log writer configuration
    pub log_writer_config: LogWriterConfig,
    /// When true, commit waits for WAL fsync (durable). When false, higher throughput but risk of data loss on crash.
    pub synchronous_commit: bool,
    /// Enable strict WAL mode (all changes are logged)
    pub strict_mode: bool,
    /// Maximum lock wait time (ms)
    pub lock_timeout_ms: u64,
    /// Transaction pool size
    pub transaction_pool_size: usize,
    /// Automatic checkpoint creation
    pub auto_checkpoint: bool,
    /// Checkpoint creation interval
    pub checkpoint_interval: Duration,
    /// Maximum number of active transactions
    pub max_active_transactions: usize,
    /// Enable integrity validation
    pub enable_integrity_validation: bool,
}

impl WalConfig {
    /// Preset for maximum throughput (group commit + synchronous_commit=off).
    pub fn high_throughput(log_directory: PathBuf) -> Self {
        let mut c = Self::default();
        c.log_writer_config = LogWriterConfig::high_throughput(log_directory);
        c.synchronous_commit = false;
        c
    }

    /// Preset for maximum durability (immediate fsync on each commit).
    pub fn durable(log_directory: PathBuf) -> Self {
        let mut c = Self::default();
        c.log_writer_config = LogWriterConfig::durable(log_directory);
        c.synchronous_commit = true;
        c
    }
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            log_writer_config: LogWriterConfig::default(),
            synchronous_commit: true,
            strict_mode: true,
            lock_timeout_ms: 5000,
            transaction_pool_size: 100,
            auto_checkpoint: true,
            checkpoint_interval: Duration::from_secs(60),
            max_active_transactions: 1000,
            enable_integrity_validation: true,
        }
    }
}

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionState {
    /// Active
    Active,
    /// Preparing to commit
    Preparing,
    /// Committed
    Committed,
    /// Aborted
    Aborted,
    /// Finished (can be deleted)
    Finished,
}

/// Transaction information
#[derive(Debug, Clone)]
pub struct TransactionInfo {
    /// Transaction ID
    pub id: TransactionId,
    /// State
    pub state: TransactionState,
    /// Isolation level
    pub isolation_level: IsolationLevel,
    /// Start time
    pub start_time: u64,
    /// Last activity time
    pub last_activity: u64,
    /// LSN of first transaction record
    pub first_lsn: Option<LogSequenceNumber>,
    /// LSN of last transaction record
    pub last_lsn: Option<LogSequenceNumber>,
    /// List of modified pages
    pub dirty_pages: HashSet<(u32, PageId)>,
    /// List of locked resources
    pub locks: HashSet<String>,
    /// Number of operations in transaction
    pub operation_count: u64,
}

impl TransactionInfo {
    /// Create new transaction information
    pub fn new(id: TransactionId, isolation_level: IsolationLevel) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            id,
            state: TransactionState::Active,
            isolation_level,
            start_time: now,
            last_activity: now,
            first_lsn: None,
            last_lsn: None,
            dirty_pages: HashSet::new(),
            locks: HashSet::new(),
            operation_count: 0,
        }
    }

    /// Update last activity time
    pub fn update_activity(&mut self) {
        self.last_activity = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    /// Add modified page
    pub fn add_dirty_page(&mut self, file_id: u32, page_id: PageId) {
        self.dirty_pages.insert((file_id, page_id));
        self.update_activity();
    }

    /// Add resource lock
    pub fn add_lock(&mut self, resource: String) {
        self.locks.insert(resource);
        self.update_activity();
    }

    /// Set record LSN
    pub fn set_lsn(&mut self, lsn: LogSequenceNumber) {
        if self.first_lsn.is_none() {
            self.first_lsn = Some(lsn);
        }
        self.last_lsn = Some(lsn);
        self.operation_count += 1;
        self.update_activity();
    }

    /// Return transaction duration
    pub fn duration(&self) -> Duration {
        Duration::from_secs(self.last_activity.saturating_sub(self.start_time))
    }

    /// Check if transaction has timed out
    pub fn is_timed_out(&self, timeout: Duration) -> bool {
        self.duration() > timeout
    }
}

/// WAL system statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WalStatistics {
    /// Total number of transactions
    pub total_transactions: u64,
    /// Active transactions
    pub active_transactions: u64,
    /// Committed transactions
    pub committed_transactions: u64,
    /// Aborted transactions
    pub aborted_transactions: u64,
    /// Total number of log records
    pub total_log_records: u64,
    /// Average transaction duration (ms)
    pub average_transaction_duration_ms: u64,
    /// Number of deadlocks
    pub deadlock_count: u64,
    /// Number of timeouts
    pub timeout_count: u64,
    /// Current LSN
    pub current_lsn: LogSequenceNumber,
    /// Last checkpoint LSN
    pub last_checkpoint_lsn: LogSequenceNumber,
    /// Number of forced syncs
    pub forced_syncs: u64,
}

/// Write-Ahead Logging system
pub struct WriteAheadLog {
    /// Configuration
    config: WalConfig,
    /// Log writer system
    log_writer: Arc<LogWriter>,
    /// Active transactions
    transactions: Arc<RwLock<HashMap<TransactionId, TransactionInfo>>>,
    /// Transaction ID generator
    transaction_id_generator: Arc<Mutex<TransactionId>>,
    /// Statistics
    statistics: Arc<RwLock<WalStatistics>>,
    /// Transaction completion notifications
    commit_notify: Arc<Notify>,
    /// Background tasks
    background_handle: Option<JoinHandle<()>>,
    /// Command channel
    command_tx: mpsc::UnboundedSender<WalCommand>,
}

/// WAL management commands
#[derive(Debug)]
enum WalCommand {
    /// Create checkpoint
    CreateCheckpoint,
    /// Cleanup finished transactions
    CleanupTransactions,
    /// Check timeouts
    CheckTimeouts,
}

impl WriteAheadLog {
    /// Create new WAL system
    pub async fn new(mut config: WalConfig) -> Result<Self> {
        config.log_writer_config.synchronous_commit = config.synchronous_commit;
        let log_writer = Arc::new(LogWriter::new(config.log_writer_config.clone())?);
        let (command_tx, command_rx) = mpsc::unbounded_channel();

        let mut wal = Self {
            config: config.clone(),
            log_writer,
            transactions: Arc::new(RwLock::new(HashMap::new())),
            transaction_id_generator: Arc::new(Mutex::new(1)),
            statistics: Arc::new(RwLock::new(WalStatistics::default())),
            commit_notify: Arc::new(Notify::new()),
            background_handle: None,
            command_tx,
        };

        // Start background tasks
        wal.start_background_tasks(command_rx).await;

        Ok(wal)
    }

    /// Start background tasks
    async fn start_background_tasks(
        &mut self,
        mut command_rx: mpsc::UnboundedReceiver<WalCommand>,
    ) {
        let transactions = self.transactions.clone();
        let statistics = self.statistics.clone();
        let config = self.config.clone();
        let log_writer = self.log_writer.clone();
        let command_sender = self.command_tx.clone();

        self.background_handle = Some(tokio::spawn(async move {
            let mut checkpoint_interval = tokio::time::interval(config.checkpoint_interval);
            let mut cleanup_interval = tokio::time::interval(Duration::from_secs(30));

            loop {
                tokio::select! {
                    // Command processing
                    Some(command) = command_rx.recv() => {
                        Self::handle_command(command, &transactions, &statistics, &log_writer).await;
                    }

                    // Automatic checkpoints
                    _ = checkpoint_interval.tick() => {
                        if config.auto_checkpoint {
                            let _ = command_sender.send(WalCommand::CreateCheckpoint);
                        }
                    }

                    // Periodic cleanup
                    _ = cleanup_interval.tick() => {
                        let _ = command_sender.send(WalCommand::CleanupTransactions);
                        let _ = command_sender.send(WalCommand::CheckTimeouts);
                    }
                }
            }
        }));
    }

    /// Handle management command
    async fn handle_command(
        command: WalCommand,
        transactions: &Arc<RwLock<HashMap<TransactionId, TransactionInfo>>>,
        statistics: &Arc<RwLock<WalStatistics>>,
        log_writer: &Arc<LogWriter>,
    ) {
        match command {
            WalCommand::CreateCheckpoint => {
                Self::create_checkpoint_internal(transactions, statistics, log_writer).await;
            }
            WalCommand::CleanupTransactions => {
                Self::cleanup_finished_transactions(transactions, statistics).await;
            }
            WalCommand::CheckTimeouts => {
                Self::check_transaction_timeouts(transactions, statistics).await;
            }
        }
    }

    /// Begin new transaction
    pub async fn begin_transaction(
        &self,
        isolation_level: IsolationLevel,
    ) -> Result<TransactionId> {
        // Check active transaction limit
        {
            let transactions = self.transactions.read().unwrap();
            if transactions.len() >= self.config.max_active_transactions {
                return Err(Error::database("Active transaction limit exceeded"));
            }
        }

        // Generate transaction ID
        let transaction_id = {
            let mut generator = self.transaction_id_generator.lock().unwrap();
            let id = *generator;
            *generator += 1;
            id
        };

        // Create transaction information
        let transaction_info = TransactionInfo::new(transaction_id, isolation_level);

        // Write BEGIN log record
        let begin_record = LogRecord::new_transaction_begin(0, transaction_id, isolation_level);
        let lsn = self.log_writer.write_log(begin_record).await?;

        // Update transaction information
        {
            let mut transactions = self.transactions.write().unwrap();
            let mut tx_info = transaction_info;
            tx_info.set_lsn(lsn);
            transactions.insert(transaction_id, tx_info);
        }

        // Update statistics
        {
            let mut stats = self.statistics.write().unwrap();
            stats.total_transactions += 1;
            stats.active_transactions += 1;
            stats.total_log_records += 1; // Count BEGIN record
            stats.current_lsn = lsn;
        }

        Ok(transaction_id)
    }

    /// Commit transaction
    pub async fn commit_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        // Get transaction information
        let (dirty_pages, last_lsn) = {
            let mut transactions = self.transactions.write().unwrap();
            if let Some(tx_info) = transactions.get_mut(&transaction_id) {
                if tx_info.state != TransactionState::Active {
                    return Err(Error::database("Transaction is not active"));
                }

                tx_info.state = TransactionState::Preparing;
                let dirty_pages: Vec<_> = tx_info.dirty_pages.iter().copied().collect();
                let last_lsn = tx_info.last_lsn;

                (dirty_pages, last_lsn)
            } else {
                return Err(Error::database("Transaction not found"));
            }
        };

        // Write COMMIT log record with forced sync
        let commit_record =
            LogRecord::new_transaction_commit(0, transaction_id, dirty_pages, last_lsn);
        let commit_lsn = self.log_writer.write_log_sync(commit_record).await?;

        // Update transaction state
        {
            let mut transactions = self.transactions.write().unwrap();
            if let Some(tx_info) = transactions.get_mut(&transaction_id) {
                tx_info.state = TransactionState::Committed;
                tx_info.set_lsn(commit_lsn);
            }
        }

        // Update statistics
        {
            let mut stats = self.statistics.write().unwrap();
            stats.committed_transactions += 1;
            stats.active_transactions = stats.active_transactions.saturating_sub(1);
            stats.total_log_records += 1; // Count COMMIT record
            stats.current_lsn = commit_lsn;
            stats.forced_syncs += 1;
        }

        // Notify about transaction completion
        self.commit_notify.notify_waiters();

        Ok(())
    }

    /// Abort transaction
    pub async fn abort_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        // Get transaction information
        let last_lsn = {
            let mut transactions = self.transactions.write().unwrap();
            if let Some(tx_info) = transactions.get_mut(&transaction_id) {
                if tx_info.state == TransactionState::Committed {
                    return Err(Error::database("Cannot abort committed transaction"));
                }

                tx_info.state = TransactionState::Aborted;
                tx_info.last_lsn
            } else {
                return Err(Error::database("Transaction not found"));
            }
        };

        // Write ABORT log record with forced sync
        let abort_record = LogRecord::new_transaction_abort(0, transaction_id, last_lsn);
        let abort_lsn = self.log_writer.write_log_sync(abort_record).await?;

        // Update transaction information
        {
            let mut transactions = self.transactions.write().unwrap();
            if let Some(tx_info) = transactions.get_mut(&transaction_id) {
                tx_info.set_lsn(abort_lsn);
            }
        }

        // Update statistics
        {
            let mut stats = self.statistics.write().unwrap();
            stats.aborted_transactions += 1;
            stats.active_transactions = stats.active_transactions.saturating_sub(1);
            stats.total_log_records += 1; // Count ABORT record
            stats.current_lsn = abort_lsn;
            stats.forced_syncs += 1;
        }

        Ok(())
    }

    /// Log data insert operation
    pub async fn log_insert(
        &self,
        transaction_id: TransactionId,
        file_id: u32,
        page_id: PageId,
        record_offset: u16,
        data: Vec<u8>,
    ) -> Result<LogSequenceNumber> {
        self.validate_transaction(transaction_id)?;

        // Get previous transaction LSN
        let prev_lsn = {
            let transactions = self.transactions.read().unwrap();
            transactions.get(&transaction_id).and_then(|tx| tx.last_lsn)
        };

        // Write log record
        let insert_record = LogRecord::new_data_insert(
            0,
            transaction_id,
            file_id,
            page_id,
            record_offset,
            data,
            prev_lsn,
        );
        let lsn = self.log_writer.write_log(insert_record).await?;

        // Update transaction information
        {
            let mut transactions = self.transactions.write().unwrap();
            if let Some(tx_info) = transactions.get_mut(&transaction_id) {
                tx_info.set_lsn(lsn);
                tx_info.add_dirty_page(file_id, page_id);
            }
        }

        // Update statistics
        {
            let mut stats = self.statistics.write().unwrap();
            stats.total_log_records += 1;
            stats.current_lsn = lsn;
        }

        Ok(lsn)
    }

    /// Log data update operation
    pub async fn log_update(
        &self,
        transaction_id: TransactionId,
        file_id: u32,
        page_id: PageId,
        record_offset: u16,
        old_data: Vec<u8>,
        new_data: Vec<u8>,
    ) -> Result<LogSequenceNumber> {
        self.validate_transaction(transaction_id)?;

        let prev_lsn = {
            let transactions = self.transactions.read().unwrap();
            transactions.get(&transaction_id).and_then(|tx| tx.last_lsn)
        };

        let update_record = LogRecord::new_data_update(
            0,
            transaction_id,
            file_id,
            page_id,
            record_offset,
            old_data,
            new_data,
            prev_lsn,
        );
        let lsn = self.log_writer.write_log(update_record).await?;

        {
            let mut transactions = self.transactions.write().unwrap();
            if let Some(tx_info) = transactions.get_mut(&transaction_id) {
                tx_info.set_lsn(lsn);
                tx_info.add_dirty_page(file_id, page_id);
            }
        }

        {
            let mut stats = self.statistics.write().unwrap();
            stats.total_log_records += 1;
            stats.current_lsn = lsn;
        }

        Ok(lsn)
    }

    /// Log data delete operation
    pub async fn log_delete(
        &self,
        transaction_id: TransactionId,
        file_id: u32,
        page_id: PageId,
        record_offset: u16,
        old_data: Vec<u8>,
    ) -> Result<LogSequenceNumber> {
        self.validate_transaction(transaction_id)?;

        let prev_lsn = {
            let transactions = self.transactions.read().unwrap();
            transactions.get(&transaction_id).and_then(|tx| tx.last_lsn)
        };

        let delete_record = LogRecord::new_data_delete(
            0,
            transaction_id,
            file_id,
            page_id,
            record_offset,
            old_data,
            prev_lsn,
        );
        let lsn = self.log_writer.write_log(delete_record).await?;

        {
            let mut transactions = self.transactions.write().unwrap();
            if let Some(tx_info) = transactions.get_mut(&transaction_id) {
                tx_info.set_lsn(lsn);
                tx_info.add_dirty_page(file_id, page_id);
            }
        }

        {
            let mut stats = self.statistics.write().unwrap();
            stats.total_log_records += 1;
            stats.current_lsn = lsn;
        }

        Ok(lsn)
    }

    /// Create checkpoint
    pub async fn create_checkpoint(&self) -> Result<LogSequenceNumber> {
        let _ = self.command_tx.send(WalCommand::CreateCheckpoint);

        // Wait for checkpoint creation to complete
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(self.get_current_lsn())
    }

    /// Internal checkpoint creation implementation
    async fn create_checkpoint_internal(
        transactions: &Arc<RwLock<HashMap<TransactionId, TransactionInfo>>>,
        statistics: &Arc<RwLock<WalStatistics>>,
        log_writer: &Arc<LogWriter>,
    ) {
        let (active_txs, dirty_pages, checkpoint_id) = {
            let txs = transactions.read().unwrap();
            let stats = statistics.read().unwrap();

            let active_txs: Vec<_> = txs
                .values()
                .filter(|tx| tx.state == TransactionState::Active)
                .map(|tx| tx.id)
                .collect();

            let dirty_pages: Vec<_> = txs
                .values()
                .flat_map(|tx| tx.dirty_pages.iter())
                .copied()
                .collect();

            let checkpoint_id = stats.last_checkpoint_lsn + 1;

            (active_txs, dirty_pages, checkpoint_id)
        };

        // Write checkpoint record
        let current_lsn = log_writer.current_lsn();
        let checkpoint_record =
            LogRecord::new_checkpoint(0, checkpoint_id, active_txs, dirty_pages, current_lsn);

        if let Ok(lsn) = log_writer.write_log_sync(checkpoint_record).await {
            let mut stats = statistics.write().unwrap();
            stats.last_checkpoint_lsn = lsn;
            stats.current_lsn = lsn;
        }
    }

    /// Cleanup finished transactions
    async fn cleanup_finished_transactions(
        transactions: &Arc<RwLock<HashMap<TransactionId, TransactionInfo>>>,
        _statistics: &Arc<RwLock<WalStatistics>>,
    ) {
        let mut txs = transactions.write().unwrap();
        txs.retain(|_, tx| {
            tx.state == TransactionState::Active || tx.state == TransactionState::Preparing
        });
    }

    /// Check transaction timeouts
    async fn check_transaction_timeouts(
        transactions: &Arc<RwLock<HashMap<TransactionId, TransactionInfo>>>,
        statistics: &Arc<RwLock<WalStatistics>>,
    ) {
        let timeout = Duration::from_secs(300); // 5 minutes
        let mut timed_out_txs = Vec::new();

        {
            let txs = transactions.read().unwrap();
            for (id, tx) in txs.iter() {
                if tx.state == TransactionState::Active && tx.is_timed_out(timeout) {
                    timed_out_txs.push(*id);
                }
            }
        }

        if !timed_out_txs.is_empty() {
            let mut stats = statistics.write().unwrap();
            stats.timeout_count += timed_out_txs.len() as u64;
        }
    }

    /// Validate transaction
    fn validate_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        let transactions = self.transactions.read().unwrap();
        if let Some(tx_info) = transactions.get(&transaction_id) {
            if tx_info.state != TransactionState::Active {
                return Err(Error::database("Transaction is not active"));
            }
            Ok(())
        } else {
            Err(Error::database("Transaction not found"))
        }
    }

    /// Get transaction information
    pub fn get_transaction_info(&self, transaction_id: TransactionId) -> Option<TransactionInfo> {
        let transactions = self.transactions.read().unwrap();
        transactions.get(&transaction_id).cloned()
    }

    /// Get list of active transactions
    pub fn get_active_transactions(&self) -> Vec<TransactionInfo> {
        let transactions = self.transactions.read().unwrap();
        transactions
            .values()
            .filter(|tx| tx.state == TransactionState::Active)
            .cloned()
            .collect()
    }

    /// Get current LSN
    pub fn get_current_lsn(&self) -> LogSequenceNumber {
        self.log_writer.current_lsn()
    }

    /// Get WAL statistics
    pub fn get_statistics(&self) -> WalStatistics {
        let mut stats = self.statistics.read().unwrap().clone();

        // Update current values
        let transactions = self.transactions.read().unwrap();
        stats.active_transactions = transactions
            .values()
            .filter(|tx| tx.state == TransactionState::Active)
            .count() as u64;

        // Calculate average transaction duration
        let total_duration: u64 = transactions
            .values()
            .filter(|tx| tx.state != TransactionState::Active)
            .map(|tx| tx.duration().as_millis() as u64)
            .sum();

        let completed_count = stats.committed_transactions + stats.aborted_transactions;
        if completed_count > 0 {
            stats.average_transaction_duration_ms = total_duration / completed_count;
        }

        stats.current_lsn = self.get_current_lsn();

        stats
    }

    /// Force sync logs
    pub async fn force_sync(&self) -> Result<()> {
        self.log_writer.flush().await?;

        {
            let mut stats = self.statistics.write().unwrap();
            stats.forced_syncs += 1;
        }

        Ok(())
    }

    /// Wait for all active transactions to complete
    pub async fn wait_for_transactions(&self, timeout: Duration) -> Result<()> {
        let start = tokio::time::Instant::now();

        while start.elapsed() < timeout {
            {
                let transactions = self.transactions.read().unwrap();
                if transactions
                    .values()
                    .all(|tx| tx.state != TransactionState::Active)
                {
                    return Ok(());
                }
            }

            // Wait for transaction completion notification or timeout
            tokio::select! {
                _ = self.commit_notify.notified() => {
                    // Check again
                    continue;
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    // Periodic check
                    continue;
                }
            }
        }

        Err(Error::database(
            "Timeout waiting for transaction completion",
        ))
    }
}

impl Drop for WriteAheadLog {
    fn drop(&mut self) {
        if let Some(handle) = self.background_handle.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_test_wal() -> Result<WriteAheadLog> {
        let temp_dir = TempDir::new().unwrap();
        let mut config = WalConfig::default();
        config.log_writer_config.log_directory = temp_dir.path().to_path_buf();
        config.auto_checkpoint = false; // Disable for tests

        WriteAheadLog::new(config).await
    }

    #[tokio::test]
    async fn test_transaction_lifecycle() -> Result<()> {
        let wal = create_test_wal().await?;

        // Begin transaction
        let tx_id = wal.begin_transaction(IsolationLevel::ReadCommitted).await?;
        assert!(tx_id > 0);

        // Check that transaction is active
        let tx_info = wal.get_transaction_info(tx_id).unwrap();
        assert_eq!(tx_info.state, TransactionState::Active);

        // Execute operations
        wal.log_insert(tx_id, 1, 10, 0, vec![1, 2, 3]).await?;
        wal.log_update(tx_id, 1, 10, 0, vec![1, 2, 3], vec![4, 5, 6])
            .await?;

        // Commit transaction
        wal.commit_transaction(tx_id).await?;

        // Check statistics
        let stats = wal.get_statistics();
        assert_eq!(stats.total_transactions, 1);
        assert_eq!(stats.committed_transactions, 1);
        assert!(stats.total_log_records >= 3); // BEGIN, INSERT, UPDATE, COMMIT

        Ok(())
    }

    #[tokio::test]
    async fn test_transaction_abort() -> Result<()> {
        let wal = create_test_wal().await?;

        let tx_id = wal.begin_transaction(IsolationLevel::Serializable).await?;

        // Execute operations
        wal.log_insert(tx_id, 1, 20, 0, vec![7, 8, 9]).await?;
        wal.log_delete(tx_id, 1, 20, 0, vec![7, 8, 9]).await?;

        // Abort transaction
        wal.abort_transaction(tx_id).await?;

        let stats = wal.get_statistics();
        assert_eq!(stats.aborted_transactions, 1);
        assert_eq!(stats.committed_transactions, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_transactions() -> Result<()> {
        let wal = create_test_wal().await?;

        // Begin multiple transactions
        let tx1 = wal.begin_transaction(IsolationLevel::ReadCommitted).await?;
        let tx2 = wal
            .begin_transaction(IsolationLevel::RepeatableRead)
            .await?;
        let tx3 = wal.begin_transaction(IsolationLevel::Serializable).await?;

        // Check active transactions
        let active_txs = wal.get_active_transactions();
        assert_eq!(active_txs.len(), 3);

        // Execute operations in different transactions
        wal.log_insert(tx1, 1, 10, 0, vec![1]).await?;
        wal.log_insert(tx2, 2, 20, 0, vec![2]).await?;
        wal.log_insert(tx3, 3, 30, 0, vec![3]).await?;

        // Commit two transactions
        wal.commit_transaction(tx1).await?;
        wal.commit_transaction(tx2).await?;

        // Abort third
        wal.abort_transaction(tx3).await?;

        let stats = wal.get_statistics();
        assert_eq!(stats.total_transactions, 3);
        assert_eq!(stats.committed_transactions, 2);
        assert_eq!(stats.aborted_transactions, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_checkpoint() -> Result<()> {
        let wal = create_test_wal().await?;

        // Begin transaction
        let tx_id = wal.begin_transaction(IsolationLevel::ReadCommitted).await?;
        wal.log_insert(tx_id, 1, 10, 0, vec![1, 2, 3]).await?;

        // Create checkpoint
        let checkpoint_lsn = wal.create_checkpoint().await?;
        assert!(checkpoint_lsn > 0);

        // Commit transaction
        wal.commit_transaction(tx_id).await?;

        let stats = wal.get_statistics();
        assert!(stats.last_checkpoint_lsn > 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_lsn_ordering() -> Result<()> {
        let wal = create_test_wal().await?;

        let tx_id = wal.begin_transaction(IsolationLevel::ReadCommitted).await?;

        let lsn1 = wal.log_insert(tx_id, 1, 10, 0, vec![1]).await?;
        let lsn2 = wal.log_update(tx_id, 1, 10, 0, vec![1], vec![2]).await?;
        let lsn3 = wal.log_delete(tx_id, 1, 10, 0, vec![2]).await?;

        // LSNs should increase
        assert!(lsn1 < lsn2);
        assert!(lsn2 < lsn3);

        wal.commit_transaction(tx_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_transaction_validation() -> Result<()> {
        let wal = create_test_wal().await?;

        // Try to execute operation with non-existent transaction
        let result = wal.log_insert(999, 1, 10, 0, vec![1]).await;
        assert!(result.is_err());

        // Begin transaction and abort it
        let tx_id = wal.begin_transaction(IsolationLevel::ReadCommitted).await?;
        wal.abort_transaction(tx_id).await?;

        // Try to execute operation with aborted transaction
        let result = wal.log_insert(tx_id, 1, 10, 0, vec![1]).await;
        assert!(result.is_err());

        Ok(())
    }
}
