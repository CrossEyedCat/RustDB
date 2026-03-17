//! Transaction manager for rustdb
//!
//! This module implements a full transaction management system
//! with ACID properties support, two-phase locking (2PL) and
//! deadlock detection.

use crate::common::{Error, Result};
use crate::core::lock::{LockManager, LockMode, LockType};
use crate::logging::log_record::{LogRecord, LogRecordType};
use crate::logging::wal::WriteAheadLog;
use std::collections::{HashMap, HashSet};
use std::sync::{atomic::AtomicU64, Arc, Mutex, RwLock};
use std::time::SystemTime;

/// Unique transaction identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TransactionId(pub u64);

impl TransactionId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn value(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for TransactionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TXN{}", self.0)
    }
}

/// Transaction states according to DBMS state model
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionState {
    /// Transaction is active and performing operations
    Active,
    /// Transaction completed all operations but not yet committed
    PartiallyCommitted,
    /// Transaction successfully committed
    Committed,
    /// Transaction was aborted
    Aborted,
    /// Transaction is in rollback process
    Aborting,
}

/// Transaction isolation level
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Read uncommitted data
    ReadUncommitted,
    /// Read committed data
    ReadCommitted,
    /// Repeatable read
    RepeatableRead,
    /// Serializable
    Serializable,
}

/// Transaction information
#[derive(Debug, Clone)]
pub struct TransactionInfo {
    /// Transaction identifier
    pub id: TransactionId,
    /// Current state
    pub state: TransactionState,
    /// Transaction start time
    pub start_time: SystemTime,
    /// Last activity time
    pub last_activity: SystemTime,
    /// Isolation level
    pub isolation_level: IsolationLevel,
    /// List of locked resources
    pub locked_resources: HashSet<String>,
    /// List of waiting locks
    pub waiting_for: Option<String>,
    /// Read-only flag
    pub read_only: bool,
}

impl TransactionInfo {
    pub fn new(id: TransactionId, isolation_level: IsolationLevel, read_only: bool) -> Self {
        let now = SystemTime::now();
        Self {
            id,
            state: TransactionState::Active,
            start_time: now,
            last_activity: now,
            isolation_level,
            locked_resources: HashSet::new(),
            waiting_for: None,
            read_only,
        }
    }

    pub fn update_activity(&mut self) {
        self.last_activity = SystemTime::now();
    }

    pub fn duration(&self) -> Result<std::time::Duration> {
        SystemTime::now()
            .duration_since(self.start_time)
            .map_err(|e| Error::internal(format!("Time calculation error: {}", e)))
    }
}

/// Transaction manager statistics
#[derive(Debug, Clone, Default)]
pub struct TransactionManagerStats {
    /// Total number of started transactions
    pub total_transactions: u64,
    /// Number of active transactions
    pub active_transactions: u64,
    /// Number of committed transactions
    pub committed_transactions: u64,
    /// Number of aborted transactions
    pub aborted_transactions: u64,
    /// Number of detected deadlocks
    pub deadlocks_detected: u64,
    /// Average transaction execution time (in milliseconds)
    pub average_transaction_time: f64,
    /// Number of lock operations
    pub lock_operations: u64,
    /// Number of unlock operations
    pub unlock_operations: u64,
}

/// Transaction manager configuration
#[derive(Debug, Clone)]
pub struct TransactionManagerConfig {
    /// Maximum number of concurrent transactions
    pub max_concurrent_transactions: usize,
    /// Lock timeout (in milliseconds)
    pub lock_timeout_ms: u64,
    /// Deadlock detection interval (in milliseconds)
    pub deadlock_detection_interval_ms: u64,
    /// Maximum idle transaction lifetime (in seconds)
    pub max_idle_time_seconds: u64,
    /// Enable automatic deadlock detection
    pub enable_deadlock_detection: bool,
}

impl Default for TransactionManagerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_transactions: 1000,
            lock_timeout_ms: 30000,               // 30 seconds
            deadlock_detection_interval_ms: 1000, // 1 second
            max_idle_time_seconds: 3600,          // 1 hour
            enable_deadlock_detection: true,
        }
    }
}

/// Transaction manager
///
/// Responsible for managing transaction lifecycle, coordination
/// with lock manager and ensuring ACID properties.
pub struct TransactionManager {
    /// Manager configuration
    config: TransactionManagerConfig,
    /// Counter for generating unique transaction IDs
    next_transaction_id: AtomicU64,
    /// Active transactions
    active_transactions: Arc<RwLock<HashMap<TransactionId, TransactionInfo>>>,
    /// Lock manager
    lock_manager: Arc<LockManager>,
    /// Write-Ahead Log for operation logging
    wal: Option<Arc<Mutex<WriteAheadLog>>>,
    /// Statistics
    stats: Arc<Mutex<TransactionManagerStats>>,
}

impl TransactionManager {
    /// Creates a new transaction manager with default configuration
    pub fn new() -> Result<Self> {
        Self::with_config(TransactionManagerConfig::default())
    }

    /// Creates a new transaction manager with specified configuration
    pub fn with_config(config: TransactionManagerConfig) -> Result<Self> {
        let lock_manager = Arc::new(LockManager::new()?);

        Ok(Self {
            config,
            next_transaction_id: AtomicU64::new(1),
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
            lock_manager,
            wal: None,
            stats: Arc::new(Mutex::new(TransactionManagerStats::default())),
        })
    }

    /// Sets Write-Ahead Log
    pub fn set_wal(&mut self, wal: Arc<Mutex<WriteAheadLog>>) {
        self.wal = Some(wal);
    }

    /// Gets manager configuration
    pub fn get_config(&self) -> &TransactionManagerConfig {
        &self.config
    }

    /// Gets transaction manager statistics
    pub fn get_statistics(&self) -> Result<TransactionManagerStats> {
        let stats = self
            .stats
            .lock()
            .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
        Ok(stats.clone())
    }

    /// Begins a new transaction
    pub fn begin_transaction(
        &self,
        isolation_level: IsolationLevel,
        read_only: bool,
    ) -> Result<TransactionId> {
        // Check concurrent transaction limit
        {
            let active = self.active_transactions.read().map_err(|_| {
                Error::internal("Failed to acquire read lock on active transactions".to_string())
            })?;

            if active.len() >= self.config.max_concurrent_transactions {
                return Err(Error::TransactionError(
                    "Maximum number of concurrent transactions reached".to_string(),
                ));
            }
        }

        // Generate new ID
        let transaction_id = TransactionId(
            self.next_transaction_id
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        );

        // Create transaction information
        let transaction_info = TransactionInfo::new(transaction_id, isolation_level, read_only);

        // Add to active transactions
        {
            let mut active = self.active_transactions.write().map_err(|_| {
                Error::internal("Failed to acquire write lock on active transactions".to_string())
            })?;
            active.insert(transaction_id, transaction_info);
        }

        // Update statistics
        {
            let mut stats = self
                .stats
                .lock()
                .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
            stats.total_transactions += 1;
            stats.active_transactions += 1;
        }

        Ok(transaction_id)
    }

    /// Commits transaction
    pub fn commit_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        // Get transaction information
        let transaction_info = {
            let active = self.active_transactions.read().map_err(|_| {
                Error::internal("Failed to acquire read lock on active transactions".to_string())
            })?;

            active.get(&transaction_id).cloned().ok_or_else(|| {
                Error::TransactionError(format!("Transaction {} not found", transaction_id))
            })?
        };

        // Check transaction state
        if transaction_info.state != TransactionState::Active {
            return Err(Error::TransactionError(format!(
                "Cannot commit transaction {} in state {:?}",
                transaction_id, transaction_info.state
            )));
        }

        // Transition to PartiallyCommitted state
        self.update_transaction_state(transaction_id, TransactionState::PartiallyCommitted)?;

        // Release all locks (shrinking phase in 2PL)
        self.release_all_locks(transaction_id)?;

        // Transition to Committed state and remove from active
        {
            let mut active = self.active_transactions.write().map_err(|_| {
                Error::internal("Failed to acquire write lock on active transactions".to_string())
            })?;

            if let Some(mut info) = active.remove(&transaction_id) {
                info.state = TransactionState::Committed;
                info.update_activity();
            }
        }

        // Update statistics
        {
            let mut stats = self
                .stats
                .lock()
                .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
            stats.active_transactions -= 1;
            stats.committed_transactions += 1;
        }

        Ok(())
    }

    /// Aborts transaction
    pub fn abort_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        // Get transaction information
        let _transaction_info = {
            let active = self.active_transactions.read().map_err(|_| {
                Error::internal("Failed to acquire read lock on active transactions".to_string())
            })?;

            active.get(&transaction_id).cloned().ok_or_else(|| {
                Error::TransactionError(format!("Transaction {} not found", transaction_id))
            })?
        };

        // Transition to Aborting state
        self.update_transaction_state(transaction_id, TransactionState::Aborting)?;

        // Release all locks
        self.release_all_locks(transaction_id)?;

        // Transition to Aborted state and remove from active
        {
            let mut active = self.active_transactions.write().map_err(|_| {
                Error::internal("Failed to acquire write lock on active transactions".to_string())
            })?;

            if let Some(mut info) = active.remove(&transaction_id) {
                info.state = TransactionState::Aborted;
                info.update_activity();
            }
        }

        // Update statistics
        {
            let mut stats = self
                .stats
                .lock()
                .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
            stats.active_transactions -= 1;
            stats.aborted_transactions += 1;
        }

        Ok(())
    }

    /// Gets transaction information
    pub fn get_transaction_info(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<TransactionInfo>> {
        let active = self.active_transactions.read().map_err(|_| {
            Error::internal("Failed to acquire read lock on active transactions".to_string())
        })?;

        Ok(active.get(&transaction_id).cloned())
    }

    /// Gets list of all active transactions
    pub fn get_active_transactions(&self) -> Result<Vec<TransactionInfo>> {
        let active = self.active_transactions.read().map_err(|_| {
            Error::internal("Failed to acquire read lock on active transactions".to_string())
        })?;

        Ok(active.values().cloned().collect())
    }

    /// Acquires lock for transaction
    pub fn acquire_lock(
        &self,
        transaction_id: TransactionId,
        resource: String,
        lock_type: LockType,
        lock_mode: LockMode,
    ) -> Result<()> {
        // Check that transaction is active
        self.ensure_transaction_active(transaction_id)?;

        // Try to acquire lock
        let acquired = self.lock_manager.acquire_lock(
            transaction_id,
            resource.clone(),
            lock_type,
            lock_mode,
        )?;

        if acquired {
            // Add resource to locked resources list
            let mut active = self.active_transactions.write().map_err(|_| {
                Error::internal("Failed to acquire write lock on active transactions".to_string())
            })?;

            if let Some(info) = active.get_mut(&transaction_id) {
                info.locked_resources.insert(resource);
                info.update_activity();
            }

            // Update statistics
            let mut stats = self
                .stats
                .lock()
                .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
            stats.lock_operations += 1;
        } else {
            // Lock not acquired - possible deadlock or timeout
            return Err(Error::TransactionError(format!(
                "Failed to acquire lock on resource {} for transaction {}",
                resource, transaction_id
            )));
        }

        Ok(())
    }

    /// Releases lock
    pub fn release_lock(&self, transaction_id: TransactionId, resource: String) -> Result<()> {
        // Release lock
        self.lock_manager
            .release_lock(transaction_id, resource.clone())?;

        // Remove resource from locked resources list
        let mut active = self.active_transactions.write().map_err(|_| {
            Error::internal("Failed to acquire write lock on active transactions".to_string())
        })?;

        if let Some(info) = active.get_mut(&transaction_id) {
            info.locked_resources.remove(&resource);
            info.update_activity();
        }

        // Update statistics
        let mut stats = self
            .stats
            .lock()
            .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
        stats.unlock_operations += 1;

        Ok(())
    }

    /// Releases all transaction locks
    fn release_all_locks(&self, transaction_id: TransactionId) -> Result<()> {
        // Get list of locked resources
        let locked_resources = {
            let active = self.active_transactions.read().map_err(|_| {
                Error::internal("Failed to acquire read lock on active transactions".to_string())
            })?;

            active
                .get(&transaction_id)
                .map(|info| info.locked_resources.clone())
                .unwrap_or_default()
        };

        // Release all locks
        for resource in locked_resources {
            self.lock_manager.release_lock(transaction_id, resource)?;
        }

        // Clear locked resources list
        let mut active = self.active_transactions.write().map_err(|_| {
            Error::internal("Failed to acquire write lock on active transactions".to_string())
        })?;

        if let Some(info) = active.get_mut(&transaction_id) {
            let unlock_count = info.locked_resources.len();
            info.locked_resources.clear();
            info.update_activity();

            // Update statistics
            let mut stats = self
                .stats
                .lock()
                .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
            stats.unlock_operations += unlock_count as u64;
        }

        Ok(())
    }

    /// Updates transaction state
    fn update_transaction_state(
        &self,
        transaction_id: TransactionId,
        new_state: TransactionState,
    ) -> Result<()> {
        let mut active = self.active_transactions.write().map_err(|_| {
            Error::internal("Failed to acquire write lock on active transactions".to_string())
        })?;

        if let Some(info) = active.get_mut(&transaction_id) {
            info.state = new_state;
            info.update_activity();
        }

        Ok(())
    }

    /// Checks that transaction is active
    fn ensure_transaction_active(&self, transaction_id: TransactionId) -> Result<()> {
        let active = self.active_transactions.read().map_err(|_| {
            Error::internal("Failed to acquire read lock on active transactions".to_string())
        })?;

        match active.get(&transaction_id) {
            Some(info) if info.state == TransactionState::Active => Ok(()),
            Some(info) => Err(Error::TransactionError(format!(
                "Transaction {} is not active (state: {:?})",
                transaction_id, info.state
            ))),
            None => Err(Error::TransactionError(format!(
                "Transaction {} not found",
                transaction_id
            ))),
        }
    }
}
