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
pub use acid_manager::{
    AcidManager, AcidConfig, AcidStatistics, VersionInfo
};
pub use advanced_lock_manager::{
    AdvancedLockManager, AdvancedLockConfig, AdvancedLockStatistics,
    ResourceType, LockMode as AdvancedLockMode, AdvancedLockInfo
};
pub use transaction::{
    TransactionManager, TransactionId, TransactionState, IsolationLevel,
    TransactionInfo, TransactionManagerStats, TransactionManagerConfig
};
pub use lock::{
    LockManager, LockType, LockMode, LockInfo, LockRequest,
    LockManagerStats, WaitForGraph
};
pub use mvcc::{
    MVCCManager, MVCCStatistics, RowVersion, RowKey, Timestamp,
    VersionId, VersionState
};
pub use concurrency::{
    ConcurrencyManager, ConcurrencyConfig, IsolationLevel as ConcurrencyIsolationLevel,
    LockGranularity
};
pub use recovery_manager::{
    AdvancedRecoveryManager, RecoveryConfig, RecoveryStatistics,
    RecoveryTransactionInfo, RecoveryTransactionState, AnalysisResult
};

#[cfg(test)]
pub mod tests;
