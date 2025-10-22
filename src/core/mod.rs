//! Ядро базы данных rustdb

pub mod acid_manager;
pub mod advanced_lock_manager;
pub mod buffer;
pub mod concurrency;
pub mod lock;
pub mod mvcc;
pub mod recovery;
pub mod recovery_manager;
pub mod transaction;

// Переэкспортируем основные типы
pub use acid_manager::{AcidConfig, AcidManager, AcidStatistics, VersionInfo};
pub use advanced_lock_manager::{
    AdvancedLockConfig, AdvancedLockInfo, AdvancedLockManager, AdvancedLockStatistics,
    LockMode as AdvancedLockMode, ResourceType,
};
pub use concurrency::{
    ConcurrencyConfig, ConcurrencyManager, IsolationLevel as ConcurrencyIsolationLevel,
    LockGranularity,
};
pub use lock::{
    LockInfo, LockManager, LockManagerStats, LockMode, LockRequest, LockType, WaitForGraph,
};
pub use mvcc::{
    MVCCManager, MVCCStatistics, RowKey, RowVersion, Timestamp, VersionId, VersionState,
};
pub use recovery_manager::{
    AdvancedRecoveryManager, AnalysisResult, RecoveryConfig, RecoveryStatistics,
    RecoveryTransactionInfo, RecoveryTransactionState,
};
pub use transaction::{
    IsolationLevel, TransactionId, TransactionInfo, TransactionManager, TransactionManagerConfig,
    TransactionManagerStats, TransactionState,
};

#[cfg(test)]
pub mod tests;
