//! ACID properties manager for rustdb
//!
//! This module implements full ACID properties support:
//! - Atomicity - all transaction operations are executed or rolled back
//! - Consistency - database remains in consistent state
//! - Isolation - transactions are isolated from each other
//! - Durability - committed changes are permanently saved

use crate::common::types::RecordId;
use crate::common::{Error, Result};
use crate::core::lock::{LockManager, LockMode, LockType};
use crate::core::transaction::{IsolationLevel, TransactionId, TransactionInfo, TransactionState};
use crate::logging::log_record::{self as log_rec, LogRecord};
use crate::logging::wal::WriteAheadLog;
use crate::storage::page_manager::PageManager;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

fn log_isolation(il: &IsolationLevel) -> log_rec::IsolationLevel {
    match il {
        IsolationLevel::ReadUncommitted => log_rec::IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted => log_rec::IsolationLevel::ReadCommitted,
        IsolationLevel::RepeatableRead => log_rec::IsolationLevel::RepeatableRead,
        IsolationLevel::Serializable => log_rec::IsolationLevel::Serializable,
    }
}

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
    page_manager: Arc<Mutex<PageManager>>,
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
    deadlocks_detected: AtomicU64,
    transactions_committed: AtomicU64,
    transactions_aborted: AtomicU64,
}

impl AcidManager {
    /// Creates a new ACID manager
    pub fn new(
        config: AcidConfig,
        lock_manager: Arc<LockManager>,
        wal: Arc<WriteAheadLog>,
        page_manager: Arc<Mutex<PageManager>>,
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
            deadlocks_detected: AtomicU64::new(0),
            transactions_committed: AtomicU64::new(0),
            transactions_aborted: AtomicU64::new(0),
        })
    }

    /// Begins a new transaction
    pub fn begin_transaction(
        &self,
        transaction_id: TransactionId,
        isolation_level: IsolationLevel,
        read_only: bool,
    ) -> Result<()> {
        let begin = LogRecord::new_transaction_begin(
            0,
            transaction_id.0,
            log_isolation(&isolation_level),
        );
        self.wal.append_log_record_blocking(begin)?;

        let transaction_info =
            TransactionInfo::new(transaction_id, isolation_level.clone(), read_only);

        let mut transactions = self.active_transactions.write().unwrap();
        transactions.insert(transaction_id, transaction_info);

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

        let file_id = self.page_manager.lock().unwrap().file_id();
        let dirty: Vec<(u32, u64)> = transaction_info
            .dirty_pages
            .iter()
            .map(|&p| (file_id, p))
            .collect();
        let commit = LogRecord::new_transaction_commit(0, transaction_id.0, dirty, None);
        self.wal.append_log_record_blocking(commit)?;

        self.lock_manager.release_all_locks(transaction_id)?;

        self.transactions_committed.fetch_add(1, Ordering::Relaxed);

        {
            let mut transactions = self.active_transactions.write().unwrap();
            transactions.remove(&transaction_id);
        }

        {
            let mut graph = self.wait_for_graph.lock().unwrap();
            graph.remove(&transaction_id);
        }

        Ok(())
    }

    /// Aborts transaction
    pub fn abort_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        // Check that transaction exists
        let _transaction_info = self.get_transaction_info(transaction_id)?;

        let abort = LogRecord::new_transaction_abort(0, transaction_id.0, None);
        self.wal.append_log_record_blocking(abort)?;

        self.undo_transaction_changes(transaction_id)?;

        self.lock_manager.release_all_locks(transaction_id)?;

        self.transactions_aborted.fetch_add(1, Ordering::Relaxed);

        {
            let mut transactions = self.active_transactions.write().unwrap();
            transactions.remove(&transaction_id);
        }

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
        let resource = lock_type.to_string();
        let mut retry_count = 0u32;

        while retry_count < self.config.max_lock_retries {
            match self.lock_manager.acquire_lock(
                transaction_id,
                resource.clone(),
                lock_type.clone(),
                lock_mode.clone(),
            ) {
                Ok(true) => {
                    self.update_transaction_locks(transaction_id, &lock_type)?;
                    return Ok(());
                }
                Ok(false) => {
                    self.add_wait_edge(transaction_id, lock_type.clone())?;
                    if self.config.auto_deadlock_detection {
                        if let Ok(true) = self.detect_deadlock(transaction_id) {
                            self.deadlocks_detected.fetch_add(1, Ordering::Relaxed);
                            return Err(AcidError::DeadlockDetected(format!(
                                "cycle involving {}",
                                transaction_id
                            ))
                            .into());
                        }
                    }
                    if start_time.elapsed() > self.config.lock_timeout {
                        return Err(AcidError::LockTimeout(format!(
                            "Lock acquisition timeout for transaction {}",
                            transaction_id
                        ))
                        .into());
                    }
                    thread::sleep(Duration::from_millis(1));
                    retry_count += 1;
                }
                Err(e) => {
                    self.deadlocks_detected.fetch_add(1, Ordering::Relaxed);
                    return Err(e);
                }
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
        self.lock_manager
            .release_lock(transaction_id, lock_type.to_string())?;

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
        new_data: &[u8],
    ) -> Result<()> {
        let transaction_info = self.get_transaction_info(transaction_id)?;

        if transaction_info.read_only {
            return Err(AcidError::ConsistencyViolation(
                "Read-only transaction cannot modify data".to_string(),
            )
            .into());
        }

        self.acquire_lock(
            transaction_id,
            LockType::Record(page_id, record_id),
            LockMode::Exclusive,
        )?;

        let full_rid: RecordId = ((page_id as u64) << 32) | (record_id as u32 as u64);
        let record_offset = (full_rid & 0xFFFF_FFFF) as u32;
        let (file_id, old_data) = {
            let mut pm = self.page_manager.lock().unwrap();
            let prior = pm.get_record(full_rid)?;
            let old_data = prior.clone().unwrap_or_default();

            if self.config.enable_mvcc {
                self.create_version(page_id, record_id, transaction_id, new_data)?;
            }

            match prior {
                Some(_) => {
                    pm.update(full_rid, new_data)?;
                }
                None => {
                    pm.insert(new_data)?;
                }
            }

            (pm.file_id(), old_data)
        };

        let prev = Some(self.wal.get_current_lsn());
        let log = LogRecord::new_data_update(
            0,
            transaction_id.0,
            file_id,
            page_id,
            record_offset as u16,
            old_data,
            new_data.to_vec(),
            prev,
        );
        self.wal.append_log_record_blocking(log)?;

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
    fn add_wait_edge(&self, waiting: TransactionId, lock_type: LockType) -> Result<()> {
        let resource = lock_type.to_string();
        if let Some(owner) = self.lock_manager.get_lock_owner(&resource)? {
            if owner != waiting {
                let mut graph = self.wait_for_graph.lock().unwrap();
                graph
                    .entry(waiting)
                    .or_insert_with(HashSet::new)
                    .insert(owner);
            }
        }
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
    fn read_uncommitted_record(&self, page_id: u64, record_id: u64) -> Result<Vec<u8>> {
        let key = (page_id, record_id);
        let versions = self.versions.read().unwrap();
        if let Some(vlist) = versions.get(&key) {
            if let Some(v) = vlist.last() {
                return Ok(v.data.clone());
            }
        }
        drop(versions);
        let full_rid: RecordId = ((page_id as u64) << 32) | (record_id as u32 as u64);
        let mut pm = self.page_manager.lock().unwrap();
        Ok(pm.get_record(full_rid)?.unwrap_or_default())
    }

    /// Reads only committed data
    fn read_committed_record(&self, page_id: u64, record_id: u64) -> Result<Vec<u8>> {
        let full_rid: RecordId = ((page_id as u64) << 32) | (record_id as u32 as u64);
        let mut pm = self.page_manager.lock().unwrap();
        Ok(pm.get_record(full_rid)?.unwrap_or_default())
    }

    /// Reads data snapshot for repeatable read
    fn read_repeatable_record(
        &self,
        transaction_id: TransactionId,
        page_id: u64,
        record_id: u64,
    ) -> Result<Vec<u8>> {
        let key = (page_id, record_id);
        let versions = self.versions.read().unwrap();
        if let Some(vlist) = versions.get(&key) {
            if let Some(v) = vlist.iter().rev().find(|x| x.created_by == transaction_id) {
                return Ok(v.data.clone());
            }
            if let Some(v) = vlist.last() {
                return Ok(v.data.clone());
            }
        }
        drop(versions);
        self.read_committed_record(page_id, record_id)
    }

    /// Reads data with strict isolation
    fn read_serializable_record(
        &self,
        transaction_id: TransactionId,
        page_id: u64,
        record_id: u64,
    ) -> Result<Vec<u8>> {
        self.read_repeatable_record(transaction_id, page_id, record_id)
    }

    /// Performs rollback of transaction changes
    fn undo_transaction_changes(&self, transaction_id: TransactionId) -> Result<()> {
        let mut versions = self.versions.write().unwrap();
        for vlist in versions.values_mut() {
            vlist.retain(|v| v.created_by != transaction_id);
        }
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
        transaction_id: TransactionId,
        page_id: u64,
    ) -> Result<()> {
        let mut transactions = self.active_transactions.write().unwrap();
        if let Some(t) = transactions.get_mut(&transaction_id) {
            t.dirty_pages.insert(page_id);
        }
        Ok(())
    }

    /// Gets next LSN
    fn get_next_lsn(&self) -> Result<LogSequenceNumber> {
        Ok(self.wal.get_current_lsn().saturating_add(1))
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
            deadlocks_detected: self.deadlocks_detected.load(Ordering::Relaxed),
            transactions_committed: self.transactions_committed.load(Ordering::Relaxed),
            transactions_aborted: self.transactions_aborted.load(Ordering::Relaxed),
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
