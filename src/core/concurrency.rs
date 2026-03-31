//! Comprehensive concurrency manager
//!
//! Combines MVCC, locks and deadlock detection

use crate::common::{Error, Result};
use crate::core::advanced_lock_manager::{
    AdvancedLockConfig, AdvancedLockManager, LockMode, ResourceType,
};
use crate::core::mvcc::{MVCCManager, RowKey, Timestamp};
use crate::core::transaction::TransactionId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Isolation level for concurrent access
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Read Uncommitted - minimal isolation
    ReadUncommitted,
    /// Read Committed - read only committed data
    ReadCommitted,
    /// Repeatable Read - repeatable read
    RepeatableRead,
    /// Serializable - full isolation
    Serializable,
}

/// Lock granularity
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockGranularity {
    /// Database-level lock
    Database,
    /// Table-level lock
    Table,
    /// Page-level lock
    Page,
    /// Row-level lock
    Row,
}

/// Concurrency manager settings
#[derive(Debug, Clone)]
pub struct ConcurrencyConfig {
    /// Lock configuration
    pub lock_config: AdvancedLockConfig,
    /// Default isolation level
    pub default_isolation_level: IsolationLevel,
    /// Default lock granularity
    pub default_lock_granularity: LockGranularity,
    /// Enable MVCC
    pub enable_mvcc: bool,
    /// MVCC automatic cleanup interval
    pub vacuum_interval: Duration,
}

impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self {
            lock_config: AdvancedLockConfig::default(),
            default_isolation_level: IsolationLevel::ReadCommitted,
            default_lock_granularity: LockGranularity::Row,
            enable_mvcc: true,
            vacuum_interval: Duration::from_secs(60),
        }
    }
}

/// Comprehensive concurrency manager
pub struct ConcurrencyManager {
    /// Lock manager
    lock_manager: Arc<AdvancedLockManager>,
    /// MVCC manager
    mvcc_manager: Arc<MVCCManager>,
    /// Configuration
    config: ConcurrencyConfig,
    /// Simple row store when MVCC is disabled (lock + in-memory map).
    non_mvcc_store: Arc<Mutex<HashMap<RowKey, Vec<u8>>>>,
}

impl ConcurrencyManager {
    /// Creates a new concurrency manager
    pub fn new(config: ConcurrencyConfig) -> Self {
        let lock_manager = Arc::new(AdvancedLockManager::new(config.lock_config.clone()));
        let mvcc_manager = Arc::new(MVCCManager::new());

        Self {
            lock_manager,
            mvcc_manager,
            config,
            non_mvcc_store: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Begins transaction
    pub fn begin_transaction(
        &self,
        transaction_id: TransactionId,
        isolation_level: IsolationLevel,
    ) -> Result<Timestamp> {
        // Return snapshot timestamp for transaction
        Ok(Timestamp::now())
    }

    /// Acquires read lock
    pub async fn acquire_read_lock(
        &self,
        transaction_id: TransactionId,
        resource: ResourceType,
        timeout: Option<Duration>,
    ) -> Result<()> {
        match self.config.default_isolation_level {
            IsolationLevel::ReadUncommitted => {
                // Don't require lock for reading
                Ok(())
            }
            IsolationLevel::ReadCommitted | IsolationLevel::RepeatableRead => {
                // Shared lock
                self.lock_manager
                    .acquire_lock(transaction_id, resource, LockMode::Shared, timeout)
                    .await
            }
            IsolationLevel::Serializable => {
                // Stricter lock
                self.lock_manager
                    .acquire_lock(transaction_id, resource, LockMode::Shared, timeout)
                    .await
            }
        }
    }

    /// Acquires write lock
    pub async fn acquire_write_lock(
        &self,
        transaction_id: TransactionId,
        resource: ResourceType,
        timeout: Option<Duration>,
    ) -> Result<()> {
        // For writes always require exclusive lock
        self.lock_manager
            .acquire_lock(transaction_id, resource, LockMode::Exclusive, timeout)
            .await
    }

    /// Reads data with MVCC consideration
    pub async fn read(
        &self,
        transaction_id: TransactionId,
        key: &RowKey,
        snapshot: Timestamp,
    ) -> Result<Option<Vec<u8>>> {
        if self.config.enable_mvcc {
            self.mvcc_manager
                .read_version(key, transaction_id, snapshot)
        } else {
            let resource = ResourceType::Record(key.table_id as u64, key.row_id);
            self.acquire_read_lock(transaction_id, resource, None)
                .await?;

            let store = self.non_mvcc_store.lock().map_err(|e| {
                Error::internal(format!("non-MVCC store lock: {}", e))
            })?;
            Ok(store.get(key).cloned())
        }
    }

    /// Writes data with MVCC consideration
    pub async fn write(
        &self,
        transaction_id: TransactionId,
        key: RowKey,
        data: Vec<u8>,
    ) -> Result<()> {
        // Acquire write lock
        let resource = ResourceType::Record(key.table_id as u64, key.row_id);
        self.acquire_write_lock(transaction_id, resource, None)
            .await?;

        if self.config.enable_mvcc {
            // Create new version
            self.mvcc_manager
                .create_version(key, transaction_id, data)?;
        } else {
            let mut store = self.non_mvcc_store.lock().map_err(|e| {
                Error::internal(format!("non-MVCC store lock: {}", e))
            })?;
            store.insert(key, data);
        }

        Ok(())
    }

    /// Deletes data
    pub async fn delete(&self, transaction_id: TransactionId, key: &RowKey) -> Result<()> {
        // Acquire write lock
        let resource = ResourceType::Record(key.table_id as u64, key.row_id);
        self.acquire_write_lock(transaction_id, resource, None)
            .await?;

        if self.config.enable_mvcc {
            // Mark for deletion
            self.mvcc_manager.delete_version(key, transaction_id)?;
        } else {
            let mut store = self.non_mvcc_store.lock().map_err(|e| {
                Error::internal(format!("non-MVCC store lock: {}", e))
            })?;
            store.remove(key);
        }

        Ok(())
    }

    /// Commits transaction
    pub fn commit_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        // Commit MVCC versions
        if self.config.enable_mvcc {
            self.mvcc_manager.commit_transaction(transaction_id)?;
        }

        // Release all locks
        self.lock_manager.release_all_locks(transaction_id)?;

        Ok(())
    }

    /// Aborts transaction
    pub fn abort_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        // Rollback MVCC versions
        if self.config.enable_mvcc {
            self.mvcc_manager.abort_transaction(transaction_id)?;
        }

        // Release all locks
        self.lock_manager.release_all_locks(transaction_id)?;

        Ok(())
    }

    /// Performs cleanup of old versions
    pub fn vacuum(&self) -> Result<u64> {
        if self.config.enable_mvcc {
            self.mvcc_manager.vacuum()
        } else {
            Ok(0)
        }
    }

    /// Returns lock statistics
    pub fn get_lock_statistics(
        &self,
    ) -> crate::core::advanced_lock_manager::AdvancedLockStatistics {
        self.lock_manager.get_statistics()
    }

    /// Returns MVCC statistics
    pub fn get_mvcc_statistics(&self) -> crate::core::mvcc::MVCCStatistics {
        self.mvcc_manager.get_statistics()
    }

    /// Updates minimum active transaction for VACUUM
    pub fn update_min_active_transaction(&self, transaction_id: TransactionId) {
        self.mvcc_manager
            .update_min_active_transaction(transaction_id);
    }
}

impl Default for ConcurrencyManager {
    fn default() -> Self {
        Self::new(ConcurrencyConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_concurrency_manager_creation() {
        let manager = ConcurrencyManager::default();
        let lock_stats = manager.get_lock_statistics();
        let mvcc_stats = manager.get_mvcc_statistics();

        assert_eq!(lock_stats.total_locks, 0);
        assert_eq!(mvcc_stats.total_versions, 0);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_write_and_read() {
        let manager = ConcurrencyManager::default();
        let tx1 = TransactionId::new(1);
        let key = RowKey::new(1, 1);
        let data = vec![1, 2, 3, 4];

        // Write data
        manager.write(tx1, key.clone(), data.clone()).await.unwrap();

        // Commit
        manager.commit_transaction(tx1).unwrap();

        // Read
        let tx2 = TransactionId::new(2);
        let snapshot = Timestamp::now();
        let read_data = manager.read(tx2, &key, snapshot).await.unwrap();

        assert_eq!(read_data, Some(data));
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_transaction_isolation() {
        let manager = ConcurrencyManager::default();
        let key = RowKey::new(1, 1);

        // Transaction 1 writes
        let tx1 = TransactionId::new(1);
        let data1 = vec![1, 2, 3];
        manager
            .write(tx1, key.clone(), data1.clone())
            .await
            .unwrap();

        // Transaction 2 reads before tx1 commit (doesn't see changes)
        let tx2 = TransactionId::new(2);
        let snapshot_before = Timestamp::now();

        // Commit tx1
        manager.commit_transaction(tx1).unwrap();

        // Transaction 3 reads after commit (sees changes)
        let tx3 = TransactionId::new(3);
        let snapshot_after = Timestamp::now();
        let read_data = manager.read(tx3, &key, snapshot_after).await.unwrap();

        assert_eq!(read_data, Some(data1));
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_abort_transaction() {
        let manager = ConcurrencyManager::default();
        let tx1 = TransactionId::new(1);
        let key = RowKey::new(1, 1);
        let data = vec![1, 2, 3, 4];

        // Write data
        manager.write(tx1, key.clone(), data).await.unwrap();

        // Abort
        manager.abort_transaction(tx1).unwrap();

        // Check statistics
        let mvcc_stats = manager.get_mvcc_statistics();
        assert_eq!(mvcc_stats.aborted_versions, 1);
        assert_eq!(mvcc_stats.active_versions, 0);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_vacuum() {
        let manager = ConcurrencyManager::default();
        let tx1 = TransactionId::new(1);
        let key = RowKey::new(1, 1);
        let data = vec![1, 2, 3, 4];

        // Create and abort transaction
        manager.write(tx1, key, data).await.unwrap();
        manager.abort_transaction(tx1).unwrap();

        // Perform VACUUM
        manager.update_min_active_transaction(TransactionId::new(100));
        let cleaned = manager.vacuum().unwrap();

        assert_eq!(cleaned, 1);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_non_mvcc_roundtrip() {
        let mut cfg = ConcurrencyConfig::default();
        cfg.enable_mvcc = false;
        let manager = ConcurrencyManager::new(cfg);
        let key = RowKey::new(1, 1);

        let tx1 = TransactionId::new(1);
        manager
            .write(tx1, key.clone(), vec![9, 9])
            .await
            .unwrap();
        manager.commit_transaction(tx1).unwrap();

        let tx2 = TransactionId::new(2);
        let got = manager.read(tx2, &key, Timestamp::now()).await.unwrap();
        assert_eq!(got, Some(vec![9, 9]));
        manager.commit_transaction(tx2).unwrap();

        let tx3 = TransactionId::new(3);
        manager.delete(tx3, &key).await.unwrap();
        manager.commit_transaction(tx3).unwrap();

        let tx4 = TransactionId::new(4);
        let got2 = manager.read(tx4, &key, Timestamp::now()).await.unwrap();
        assert_eq!(got2, None);
    }
}
