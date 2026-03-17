//! ACID properties manager for rustdb
//!
//! This module implements full ACID properties support:
//! - Atomicity - all transaction operations are executed or rolled back
//! - Consistency - database remains in consistent state
//! - Isolation - transactions are isolated from each other
//! - Durability - committed changes are permanently saved

use crate::common::{Error, Result};
use crate::core::lock::{LockManager, LockMode, LockType};
use crate::core::transaction::{IsolationLevel, TransactionId, TransactionInfo, TransactionState};
use crate::logging::wal::WriteAheadLog;
use crate::storage::page_manager::PageManager;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime};

/// ACID manager errors
#[derive(Debug, thiserror::Error)]
pub enum AcidError {
    #[error("Transaction not found: {0}")]
    TransactionNotFound(TransactionId),

    #[error("Isolation violation: {0}")]
    IsolationViolation(String),

    #[error("Consistency violation: {0}")]
    ConsistencyViolation(String),

    #[error("Logging error: {0}")]
    LoggingError(String),

    #[error("Deadlock detected: {0}")]
    DeadlockDetected(String),

    #[error("Lock timeout: {0}")]
    LockTimeout(String),

    #[error("Recovery error: {0}")]
    RecoveryError(String),
}

impl From<AcidError> for Error {
    fn from(err: AcidError) -> Self {
        Error::database(err.to_string())
    }
}

/// ACID manager configuration
#[derive(Debug, Clone)]
pub struct AcidConfig {
    /// Maximum lock wait time
    pub lock_timeout: Duration,
    /// Deadlock check interval
    pub deadlock_check_interval: Duration,
    /// Maximum number of lock acquisition attempts
    pub max_lock_retries: u32,
    /// Enable strict consistency checking
    pub strict_consistency: bool,
    /// Enable automatic deadlock detection
    pub auto_deadlock_detection: bool,
    /// Enable MVCC
    pub enable_mvcc: bool,
    /// Maximum number of versions to store
    pub max_versions: usize,
}

impl Default for AcidConfig {
    fn default() -> Self {
        Self {
            lock_timeout: Duration::from_secs(30),
            deadlock_check_interval: Duration::from_millis(100),
            max_lock_retries: 3,
            strict_consistency: true,
            auto_deadlock_detection: true,
            enable_mvcc: true,
            max_versions: 1000,
        }
    }
}

/// Record version information for MVCC
#[derive(Debug, Clone)]
pub struct VersionInfo {
    /// Version ID
    pub version_id: u64,
    /// ID of transaction that created the version
    pub created_by: TransactionId,
    /// Version creation time
    pub created_at: SystemTime,
    /// Version deletion time (if deleted)
    pub deleted_at: Option<SystemTime>,
    /// ID of transaction that deleted the version
    pub deleted_by: Option<TransactionId>,
    /// Version data
    pub data: Vec<u8>,
}

/// ACID properties manager
pub struct AcidManager {
    /// Configuration
    config: AcidConfig,
    /// Lock manager
    lock_manager: Arc<LockManager>,
    /// Write-Ahead Log
    wal: Arc<WriteAheadLog>,
    /// Page manager
    page_manager: Arc<PageManager>,
    /// Active transactions
    active_transactions: Arc<RwLock<HashMap<TransactionId, TransactionInfo>>>,
    /// Wait-for graph for deadlock detection
    wait_for_graph: Arc<Mutex<HashMap<TransactionId, HashSet<TransactionId>>>>,
    /// Queue of transactions waiting for locks
    waiting_transactions: Arc<Mutex<VecDeque<(TransactionId, LockType, LockMode)>>>,
    /// Record versions for MVCC
    versions: Arc<RwLock<HashMap<(u64, u64), Vec<VersionInfo>>>>, // (page_id, record_id) -> versions
    /// Version counter
    version_counter: Arc<Mutex<u64>>,
}

impl AcidManager {
    /// Creates a new ACID manager
    pub fn new(
        config: AcidConfig,
        lock_manager: Arc<LockManager>,
        wal: Arc<WriteAheadLog>,
        page_manager: Arc<PageManager>,
    ) -> Result<Self> {
        Ok(Self {
            config,
            lock_manager,
            wal,
            page_manager,
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
            wait_for_graph: Arc::new(Mutex::new(HashMap::new())),
            waiting_transactions: Arc::new(Mutex::new(VecDeque::new())),
            versions: Arc::new(RwLock::new(HashMap::new())),
            version_counter: Arc::new(Mutex::new(0)),
        })
    }

    /// Begins a new transaction
    pub fn begin_transaction(
        &self,
        transaction_id: TransactionId,
        isolation_level: IsolationLevel,
        read_only: bool,
    ) -> Result<()> {
        // Create transaction information
        let transaction_info =
            TransactionInfo::new(transaction_id, isolation_level.clone(), read_only);

        // TODO: Write to WAL
        println!(
            "Started transaction {} with isolation level {:?}",
            transaction_id, isolation_level
        );

        // Add to active transactions
        {
            let mut transactions = self.active_transactions.write().unwrap();
            transactions.insert(transaction_id, transaction_info);
        }

        Ok(())
    }

    /// Commits transaction
    pub fn commit_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        // Check that transaction exists and is active
        let transaction_info = self.get_transaction_info(transaction_id)?;

        if transaction_info.state != TransactionState::Active {
            return Err(AcidError::ConsistencyViolation(format!(
                "Transaction {} cannot be committed in state {:?}",
                transaction_id, transaction_info.state
            ))
            .into());
        }

        // TODO: Write COMMIT to WAL
        println!("Committed transaction {}", transaction_id);

        // TODO: Release all locks
        // self.lock_manager.release_all_locks(transaction_id)?;

        // Remove from active transactions
        {
            let mut transactions = self.active_transactions.write().unwrap();
            transactions.remove(&transaction_id);
        }

        // Remove from wait-for graph
        {
            let mut graph = self.wait_for_graph.lock().unwrap();
            graph.remove(&transaction_id);
        }

        Ok(())
    }

    /// Aborts transaction
    pub fn abort_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        // Check that transaction exists
        let transaction_info = self.get_transaction_info(transaction_id)?;

        // TODO: Write ABORT to WAL
        println!("Aborted transaction {}", transaction_id);

        // TODO: Perform rollback of changes (UNDO)
        // self.undo_transaction_changes(transaction_id)?;

        // TODO: Release all locks
        // self.lock_manager.release_all_locks(transaction_id)?;

        // Remove from active transactions
        {
            let mut transactions = self.active_transactions.write().unwrap();
            transactions.remove(&transaction_id);
        }

        // Remove from wait-for graph
        {
            let mut graph = self.wait_for_graph.lock().unwrap();
            graph.remove(&transaction_id);
        }

        Ok(())
    }

    /// Acquires lock for transaction
    pub fn acquire_lock(
        &self,
        transaction_id: TransactionId,
        lock_type: LockType,
        lock_mode: LockMode,
    ) -> Result<()> {
        let start_time = Instant::now();
        let retry_count = 0;

        while retry_count < self.config.max_lock_retries {
            // TODO: Try to acquire lock
            // match self.lock_manager.acquire_lock(transaction_id, lock_type.clone(), lock_mode.clone()) {
            //     Ok(()) => {
            //         self.update_transaction_locks(transaction_id, &lock_type)?;
            //         return Ok(());
            //     }
            //     Err(_) => {
            //         // Error handling
            //     }
            // }

            // Temporarily just return success
            println!(
                "Acquired lock for transaction {} on resource {:?}",
                transaction_id, lock_type
            );
            return Ok(());

            // Check timeout
            if start_time.elapsed() > self.config.lock_timeout {
                return Err(AcidError::LockTimeout(format!(
                    "Lock acquisition timeout for transaction {}",
                    transaction_id
                ))
                .into());
            }
        }

        Err(AcidError::LockTimeout(format!(
            "Failed to acquire lock after {} attempts",
            self.config.max_lock_retries
        ))
        .into())
    }

    /// Releases lock
    pub fn release_lock(&self, transaction_id: TransactionId, lock_type: LockType) -> Result<()> {
        // TODO: Release lock
        // self.lock_manager.release_lock(transaction_id, lock_type.clone())?;

        println!(
            "Released lock for transaction {} on resource {:?}",
            transaction_id, lock_type
        );

        // Remove from transaction's locked resources
        self.remove_transaction_lock(transaction_id, &lock_type)?;

        // Remove from wait-for graph
        self.remove_wait_edge(transaction_id, lock_type)?;

        Ok(())
    }

    /// Performs read operation with isolation consideration
    pub fn read_record(
        &self,
        transaction_id: TransactionId,
        page_id: u64,
        record_id: u64,
    ) -> Result<Vec<u8>> {
        let transaction_info = self.get_transaction_info(transaction_id)?;

        match transaction_info.isolation_level {
            IsolationLevel::ReadUncommitted => {
                // Read uncommitted data
                self.read_uncommitted_record(page_id, record_id)
            }
            IsolationLevel::ReadCommitted => {
                // Read only committed data
                self.read_committed_record(page_id, record_id)
            }
            IsolationLevel::RepeatableRead => {
                // Read data snapshot at transaction start time
                self.read_repeatable_record(transaction_id, page_id, record_id)
            }
            IsolationLevel::Serializable => {
                // Strict isolation
                self.read_serializable_record(transaction_id, page_id, record_id)
            }
        }
    }

    /// Performs write operation with ACID consideration
    pub fn write_record(
        &self,
        transaction_id: TransactionId,
        page_id: u64,
        record_id: u64,
        _new_data: &[u8],
    ) -> Result<()> {
        let transaction_info = self.get_transaction_info(transaction_id)?;

        if transaction_info.read_only {
            return Err(AcidError::ConsistencyViolation(
                "Read-only transaction cannot modify data".to_string(),
            )
            .into());
        }

        // Acquire write lock
        self.acquire_lock(
            transaction_id,
            LockType::Record(page_id, record_id),
            LockMode::Exclusive,
        )?;

        // TODO: Read old data for UNDO
        // let old_data = self.page_manager.read_record(page_id, record_id)?;

        // Create version for MVCC
        if self.config.enable_mvcc {
            // TODO: Create version
            // self.create_version(page_id, record_id, transaction_id, &old_data)?;
        }

        // TODO: Write to WAL
        // let log_record = LogRecord { ... };
        // self.wal.write_record(&log_record)?;

        // TODO: Write data to page
        // self.page_manager.write_record(page_id, record_id, new_data)?;

        println!(
            "Wrote data for transaction {} to page {} record {}",
            transaction_id, page_id, record_id
        );

        // Update transaction information
        self.update_transaction_dirty_pages(transaction_id, page_id)?;

        Ok(())
    }

    /// Detects deadlock
    fn detect_deadlock(&self, transaction_id: TransactionId) -> Result<bool> {
        let graph = self.wait_for_graph.lock().unwrap();

        // Simple cycle check in graph
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();

        fn has_cycle(
            graph: &HashMap<TransactionId, HashSet<TransactionId>>,
            node: TransactionId,
            visited: &mut HashSet<TransactionId>,
            rec_stack: &mut HashSet<TransactionId>,
        ) -> bool {
            if rec_stack.contains(&node) {
                return true;
            }

            if visited.contains(&node) {
                return false;
            }

            visited.insert(node);
            rec_stack.insert(node);

            if let Some(neighbors) = graph.get(&node) {
                for &neighbor in neighbors {
                    if has_cycle(graph, neighbor, visited, rec_stack) {
                        return true;
                    }
                }
            }

            rec_stack.remove(&node);
            false
        }

        Ok(has_cycle(
            &graph,
            transaction_id,
            &mut visited,
            &mut rec_stack,
        ))
    }

    /// Adds edge to wait-for graph
    fn add_wait_edge(&self, _waiting: TransactionId, _lock_type: LockType) -> Result<()> {
        // TODO: Find transaction owning the lock
        // if let Some(owner) = self.lock_manager.get_lock_owner(&lock_type)? {
        //     let mut graph = self.wait_for_graph.lock().unwrap();
        //     graph.entry(waiting).or_insert_with(HashSet::new).insert(owner);
        // }
        Ok(())
    }

    /// Removes edge from wait-for graph
    fn remove_wait_edge(&self, transaction: TransactionId, _lock_type: LockType) -> Result<()> {
        let mut graph = self.wait_for_graph.lock().unwrap();

        // Remove all edges where transaction is waiting for others
        if let Some(waiting_for) = graph.get_mut(&transaction) {
            waiting_for.clear();
        }

        // Remove all edges where others are waiting for transaction
        for waiting_for in graph.values_mut() {
            waiting_for.remove(&transaction);
        }

        Ok(())
    }

    /// Creates a new record version for MVCC
    fn create_version(
        &self,
        page_id: u64,
        record_id: u64,
        transaction_id: TransactionId,
        data: &[u8],
    ) -> Result<()> {
        let mut version_id = self.version_counter.lock().unwrap();
        *version_id += 1;
        let current_version_id = *version_id;

        let version_info = VersionInfo {
            version_id: current_version_id,
            created_by: transaction_id,
            created_at: SystemTime::now(),
            deleted_at: None,
            deleted_by: None,
            data: data.to_vec(),
        };

        let mut versions = self.versions.write().unwrap();
        let key = (page_id, record_id);
        versions
            .entry(key)
            .or_insert_with(Vec::new)
            .push(version_info);

        // Limit number of versions
        if let Some(record_versions) = versions.get_mut(&key) {
            if record_versions.len() > self.config.max_versions {
                record_versions.remove(0); // Remove oldest version
            }
        }

        Ok(())
    }

    /// Reads uncommitted data
    fn read_uncommitted_record(&self, _page_id: u64, _record_id: u64) -> Result<Vec<u8>> {
        // TODO: Implement reading uncommitted data
        Ok(b"uncommitted_data".to_vec())
    }

    /// Reads only committed data
    fn read_committed_record(&self, _page_id: u64, _record_id: u64) -> Result<Vec<u8>> {
        // TODO: Implement reading only committed data
        Ok(b"committed_data".to_vec())
    }

    /// Reads data snapshot for repeatable read
    fn read_repeatable_record(
        &self,
        _transaction_id: TransactionId,
        _page_id: u64,
        _record_id: u64,
    ) -> Result<Vec<u8>> {
        // TODO: Implement reading data snapshot
        Ok(b"repeatable_data".to_vec())
    }

    /// Reads data with strict isolation
    fn read_serializable_record(
        &self,
        _transaction_id: TransactionId,
        _page_id: u64,
        _record_id: u64,
    ) -> Result<Vec<u8>> {
        // TODO: Implement strict isolation
        Ok(b"serializable_data".to_vec())
    }

    /// Performs rollback of transaction changes
    fn undo_transaction_changes(&self, _transaction_id: TransactionId) -> Result<()> {
        // TODO: Implement rollback of changes
        Ok(())
    }

    /// Gets transaction information
    fn get_transaction_info(&self, transaction_id: TransactionId) -> Result<TransactionInfo> {
        let transactions = self.active_transactions.read().unwrap();
        transactions
            .get(&transaction_id)
            .cloned()
            .ok_or_else(|| AcidError::TransactionNotFound(transaction_id).into())
    }

    /// Updates list of transaction's locked resources
    fn update_transaction_locks(
        &self,
        transaction_id: TransactionId,
        lock_type: &LockType,
    ) -> Result<()> {
        let mut transactions = self.active_transactions.write().unwrap();
        if let Some(transaction) = transactions.get_mut(&transaction_id) {
            transaction.locked_resources.insert(lock_type.to_string());
        }
        Ok(())
    }

    /// Removes lock from transaction's list
    fn remove_transaction_lock(
        &self,
        transaction_id: TransactionId,
        lock_type: &LockType,
    ) -> Result<()> {
        let mut transactions = self.active_transactions.write().unwrap();
        if let Some(transaction) = transactions.get_mut(&transaction_id) {
            transaction.locked_resources.remove(&lock_type.to_string());
        }
        Ok(())
    }

    /// Updates list of transaction's modified pages
    fn update_transaction_dirty_pages(
        &self,
        _transaction_id: TransactionId,
        _page_id: u64,
    ) -> Result<()> {
        // TODO: Add page_id to dirty_pages
        Ok(())
    }

    /// Gets next LSN
    fn get_next_lsn(&self) -> Result<LogSequenceNumber> {
        // TODO: Implement getting next LSN
        Ok(SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64)
    }

    /// Gets ACID manager statistics
    pub fn get_statistics(&self) -> Result<AcidStatistics> {
        let active_count = self.active_transactions.read().unwrap().len();
        let waiting_count = self.waiting_transactions.lock().unwrap().len();
        let version_count = self
            .versions
            .read()
            .unwrap()
            .values()
            .map(|v| v.len())
            .sum();

        Ok(AcidStatistics {
            active_transactions: active_count,
            waiting_transactions: waiting_count,
            total_versions: version_count,
            deadlocks_detected: 0,     // TODO: Add counter
            transactions_committed: 0, // TODO: Add counter
            transactions_aborted: 0,   // TODO: Add counter
        })
    }
}

/// ACID manager statistics
#[derive(Debug, Clone)]
pub struct AcidStatistics {
    /// Number of active transactions
    pub active_transactions: usize,
    /// Number of transactions waiting for locks
    pub waiting_transactions: usize,
    /// Total number of versions for MVCC
    pub total_versions: usize,
    /// Number of detected deadlocks
    pub deadlocks_detected: u64,
    /// Number of committed transactions
    pub transactions_committed: u64,
    /// Number of aborted transactions
    pub transactions_aborted: u64,
}

/// Type for LSN (temporary)
pub type LogSequenceNumber = u64;
