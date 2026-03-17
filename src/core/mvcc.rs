//! Multi-Version Concurrency Control (MVCC) system
//!
//! Provides transaction isolation through data versioning

use crate::common::{Error, Result};
use crate::core::transaction::TransactionId;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Version identifier
pub type VersionId = u64;

/// Timestamp for version
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp(u64);

impl Timestamp {
    /// Creates a new timestamp
    pub fn now() -> Self {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        Self(duration.as_secs() * 1_000_000 + duration.subsec_micros() as u64)
    }

    /// Returns timestamp value
    pub fn value(&self) -> u64 {
        self.0
    }
}

/// Version state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionState {
    /// Version is active
    Active,
    /// Version is committed
    Committed,
    /// Version is aborted
    Aborted,
    /// Version is marked for deletion
    MarkedForDeletion,
}

/// Row version
#[derive(Debug, Clone)]
pub struct RowVersion {
    /// Version identifier
    pub version_id: VersionId,
    /// Transaction that created the version
    pub created_by: TransactionId,
    /// Transaction that deleted the version (if any)
    pub deleted_by: Option<TransactionId>,
    /// Creation timestamp
    pub created_at: Timestamp,
    /// Deletion timestamp (if any)
    pub deleted_at: Option<Timestamp>,
    /// Version state
    pub state: VersionState,
    /// Version data
    pub data: Vec<u8>,
    /// Reference to previous version
    pub prev_version: Option<VersionId>,
}

impl RowVersion {
    /// Creates a new version
    pub fn new(
        version_id: VersionId,
        transaction_id: TransactionId,
        data: Vec<u8>,
        prev_version: Option<VersionId>,
    ) -> Self {
        Self {
            version_id,
            created_by: transaction_id,
            deleted_by: None,
            created_at: Timestamp::now(),
            deleted_at: None,
            state: VersionState::Active,
            data,
            prev_version,
        }
    }

    /// Checks version visibility for transaction
    pub fn is_visible(&self, transaction_id: TransactionId, snapshot_timestamp: Timestamp) -> bool {
        // Version is visible if:
        // 1. It was created before snapshot_timestamp
        if self.created_at > snapshot_timestamp {
            return false;
        }

        // 2. It is not deleted or deleted after snapshot_timestamp
        if let Some(deleted_at) = self.deleted_at {
            if deleted_at <= snapshot_timestamp {
                return false;
            }
        }

        // 3. Version is committed or created by current transaction
        match self.state {
            VersionState::Committed => true,
            VersionState::Active => self.created_by == transaction_id,
            _ => false,
        }
    }

    /// Marks version as committed
    pub fn commit(&mut self) {
        self.state = VersionState::Committed;
    }

    /// Marks version as aborted
    pub fn abort(&mut self) {
        self.state = VersionState::Aborted;
    }

    /// Marks version as deleted
    pub fn mark_deleted(&mut self, transaction_id: TransactionId) {
        self.deleted_by = Some(transaction_id);
        self.deleted_at = Some(Timestamp::now());
    }
}

/// Row key (table + row ID)
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct RowKey {
    pub table_id: u32,
    pub row_id: u64,
}

impl RowKey {
    pub fn new(table_id: u32, row_id: u64) -> Self {
        Self { table_id, row_id }
    }
}

/// Version manager
pub struct MVCCManager {
    /// Version storage (row key -> list of versions)
    versions: Arc<RwLock<HashMap<RowKey, Vec<RowVersion>>>>,
    /// Version counter
    version_counter: Arc<Mutex<VersionId>>,
    /// Minimum active transaction (for cleaning old versions)
    min_active_transaction: Arc<RwLock<TransactionId>>,
    /// MVCC statistics
    statistics: Arc<Mutex<MVCCStatistics>>,
}

/// MVCC statistics
#[derive(Debug, Clone)]
pub struct MVCCStatistics {
    /// Total versions
    pub total_versions: u64,
    /// Active versions
    pub active_versions: u64,
    /// Committed versions
    pub committed_versions: u64,
    /// Aborted versions
    pub aborted_versions: u64,
    /// Versions marked for deletion
    pub marked_for_deletion: u64,
    /// VACUUM operations
    pub vacuum_operations: u64,
    /// Versions cleaned during VACUUM
    pub versions_cleaned: u64,
    /// Last update
    pub last_updated: Instant,
}

impl MVCCStatistics {
    fn new() -> Self {
        Self {
            total_versions: 0,
            active_versions: 0,
            committed_versions: 0,
            aborted_versions: 0,
            marked_for_deletion: 0,
            vacuum_operations: 0,
            versions_cleaned: 0,
            last_updated: Instant::now(),
        }
    }
}

impl MVCCManager {
    /// Creates a new MVCC manager
    pub fn new() -> Self {
        Self {
            versions: Arc::new(RwLock::new(HashMap::new())),
            version_counter: Arc::new(Mutex::new(1)),
            min_active_transaction: Arc::new(RwLock::new(TransactionId::new(0))),
            statistics: Arc::new(Mutex::new(MVCCStatistics::new())),
        }
    }

    /// Creates a new row version
    pub fn create_version(
        &self,
        key: RowKey,
        transaction_id: TransactionId,
        data: Vec<u8>,
    ) -> Result<VersionId> {
        // Generate version ID
        let version_id = {
            let mut counter = self.version_counter.lock().unwrap();
            let id = *counter;
            *counter += 1;
            id
        };

        let mut versions = self.versions.write().unwrap();
        let row_versions = versions.entry(key.clone()).or_insert_with(Vec::new);

        // Find previous version
        let prev_version = row_versions.last().map(|v| v.version_id);

        // Create new version
        let version = RowVersion::new(version_id, transaction_id, data, prev_version);
        row_versions.push(version);

        // Update statistics
        {
            let mut stats = self.statistics.lock().unwrap();
            stats.total_versions += 1;
            stats.active_versions += 1;
            stats.last_updated = Instant::now();
        }

        Ok(version_id)
    }

    /// Reads row version visible for transaction
    pub fn read_version(
        &self,
        key: &RowKey,
        transaction_id: TransactionId,
        snapshot_timestamp: Timestamp,
    ) -> Result<Option<Vec<u8>>> {
        let versions = self.versions.read().unwrap();

        if let Some(row_versions) = versions.get(key) {
            // Find last visible version
            for version in row_versions.iter().rev() {
                if version.is_visible(transaction_id, snapshot_timestamp) {
                    return Ok(Some(version.data.clone()));
                }
            }
        }

        Ok(None)
    }

    /// Deletes row (creates new version with deletion mark)
    pub fn delete_version(&self, key: &RowKey, transaction_id: TransactionId) -> Result<()> {
        let mut versions = self.versions.write().unwrap();

        if let Some(row_versions) = versions.get_mut(key) {
            if let Some(last_version) = row_versions.last_mut() {
                last_version.mark_deleted(transaction_id);

                // Update statistics
                {
                    let mut stats = self.statistics.lock().unwrap();
                    stats.marked_for_deletion += 1;
                    stats.last_updated = Instant::now();
                }

                return Ok(());
            }
        }

        Err(Error::database("Row version not found"))
    }

    /// Commits transaction versions
    pub fn commit_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        let mut versions = self.versions.write().unwrap();
        let mut committed_count = 0;

        for row_versions in versions.values_mut() {
            for version in row_versions.iter_mut() {
                if version.created_by == transaction_id && version.state == VersionState::Active {
                    version.commit();
                    committed_count += 1;
                }
            }
        }

        // Update statistics
        {
            let mut stats = self.statistics.lock().unwrap();
            stats.active_versions = stats.active_versions.saturating_sub(committed_count);
            stats.committed_versions += committed_count;
            stats.last_updated = Instant::now();
        }

        Ok(())
    }

    /// Aborts transaction versions
    pub fn abort_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        let mut versions = self.versions.write().unwrap();
        let mut aborted_count = 0;

        for row_versions in versions.values_mut() {
            for version in row_versions.iter_mut() {
                if version.created_by == transaction_id && version.state == VersionState::Active {
                    version.abort();
                    aborted_count += 1;
                }
            }
        }

        // Update statistics
        {
            let mut stats = self.statistics.lock().unwrap();
            stats.active_versions = stats.active_versions.saturating_sub(aborted_count);
            stats.aborted_versions += aborted_count;
            stats.last_updated = Instant::now();
        }

        Ok(())
    }

    /// Cleans old versions (VACUUM)
    pub fn vacuum(&self) -> Result<u64> {
        let min_active = *self.min_active_transaction.read().unwrap();
        let mut versions = self.versions.write().unwrap();
        let mut cleaned_count = 0;

        // Clean each version chain
        for row_versions in versions.values_mut() {
            row_versions.retain(|version| {
                // Keep versions that:
                // 1. Are active
                // 2. Created by active transactions
                // 3. Committed and can be visible
                let should_keep = match version.state {
                    VersionState::Active => version.created_by >= min_active,
                    VersionState::Committed => {
                        // Keep last committed version
                        true
                    }
                    VersionState::Aborted | VersionState::MarkedForDeletion => {
                        // Remove aborted and marked for deletion
                        false
                    }
                };

                if !should_keep {
                    cleaned_count += 1;
                }

                should_keep
            });
        }

        // Remove empty chains
        versions.retain(|_, row_versions| !row_versions.is_empty());

        // Update statistics
        {
            let mut stats = self.statistics.lock().unwrap();
            stats.vacuum_operations += 1;
            stats.versions_cleaned += cleaned_count;
            stats.total_versions = stats.total_versions.saturating_sub(cleaned_count);
            stats.last_updated = Instant::now();
        }

        Ok(cleaned_count)
    }

    /// Updates minimum active transaction
    pub fn update_min_active_transaction(&self, transaction_id: TransactionId) {
        let mut min_active = self.min_active_transaction.write().unwrap();
        *min_active = transaction_id;
    }

    /// Returns statistics
    pub fn get_statistics(&self) -> MVCCStatistics {
        self.statistics.lock().unwrap().clone()
    }

    /// Returns version count for row
    pub fn get_version_count(&self, key: &RowKey) -> usize {
        let versions = self.versions.read().unwrap();
        versions.get(key).map(|v| v.len()).unwrap_or(0)
    }
}

impl Default for MVCCManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mvcc_manager_creation() {
        let manager = MVCCManager::new();
        let stats = manager.get_statistics();
        assert_eq!(stats.total_versions, 0);
        assert_eq!(stats.active_versions, 0);
    }

    #[test]
    fn test_create_version() {
        let manager = MVCCManager::new();
        let key = RowKey::new(1, 1);
        let transaction_id = TransactionId::new(1);
        let data = vec![1, 2, 3, 4];

        let version_id = manager
            .create_version(key.clone(), transaction_id, data.clone())
            .unwrap();
        assert_eq!(version_id, 1);

        let stats = manager.get_statistics();
        assert_eq!(stats.total_versions, 1);
        assert_eq!(stats.active_versions, 1);
    }

    #[test]
    fn test_read_version() {
        let manager = MVCCManager::new();
        let key = RowKey::new(1, 1);
        let transaction_id = TransactionId::new(1);
        let data = vec![1, 2, 3, 4];

        // Create version
        manager
            .create_version(key.clone(), transaction_id, data.clone())
            .unwrap();

        // Commit transaction
        manager.commit_transaction(transaction_id).unwrap();

        // Read version (snapshot after creation)
        let snapshot = Timestamp::now();
        let read_data = manager
            .read_version(&key, transaction_id, snapshot)
            .unwrap();
        assert_eq!(read_data, Some(data));
    }

    #[test]
    fn test_commit_transaction() {
        let manager = MVCCManager::new();
        let key = RowKey::new(1, 1);
        let transaction_id = TransactionId::new(1);
        let data = vec![1, 2, 3, 4];

        manager.create_version(key, transaction_id, data).unwrap();
        manager.commit_transaction(transaction_id).unwrap();

        let stats = manager.get_statistics();
        assert_eq!(stats.active_versions, 0);
        assert_eq!(stats.committed_versions, 1);
    }

    #[test]
    fn test_abort_transaction() {
        let manager = MVCCManager::new();
        let key = RowKey::new(1, 1);
        let transaction_id = TransactionId::new(1);
        let data = vec![1, 2, 3, 4];

        manager.create_version(key, transaction_id, data).unwrap();
        manager.abort_transaction(transaction_id).unwrap();

        let stats = manager.get_statistics();
        assert_eq!(stats.active_versions, 0);
        assert_eq!(stats.aborted_versions, 1);
    }

    #[test]
    fn test_vacuum() {
        let manager = MVCCManager::new();
        let key = RowKey::new(1, 1);
        let transaction_id = TransactionId::new(1);
        let data = vec![1, 2, 3, 4];

        // Create and abort version
        manager
            .create_version(key.clone(), transaction_id, data)
            .unwrap();
        manager.abort_transaction(transaction_id).unwrap();

        // Clean
        let cleaned = manager.vacuum().unwrap();
        assert_eq!(cleaned, 1);

        let stats = manager.get_statistics();
        assert_eq!(stats.total_versions, 0);
        assert_eq!(stats.versions_cleaned, 1);
    }

    #[test]
    fn test_multiple_versions() {
        let manager = MVCCManager::new();
        let key = RowKey::new(1, 1);
        let data1 = vec![1, 2, 3];
        let data2 = vec![4, 5, 6];

        // Create first version
        let tx1 = TransactionId::new(1);
        manager.create_version(key.clone(), tx1, data1).unwrap();
        manager.commit_transaction(tx1).unwrap();

        // Create second version
        let tx2 = TransactionId::new(2);
        manager
            .create_version(key.clone(), tx2, data2.clone())
            .unwrap();
        manager.commit_transaction(tx2).unwrap();

        // Check version count
        assert_eq!(manager.get_version_count(&key), 2);

        // Read last version
        let snapshot = Timestamp::now();
        let read_data = manager.read_version(&key, tx2, snapshot).unwrap();
        assert_eq!(read_data, Some(data2));
    }
}
