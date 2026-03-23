//! Tests for `core::recovery::RecoveryManager` (WAL-based recovery path)

use crate::core::acid_manager::{AcidConfig, AcidManager};
use crate::core::lock::LockManager;
use crate::core::recovery::{RecoveryManager, RecoveryState, RecoveryStatistics};
use crate::logging::wal::{WalConfig, WriteAheadLog};
use crate::storage::page_manager::{PageManager, PageManagerConfig};
use std::sync::Arc;

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn test_recovery_manager_perform_recovery() -> crate::common::Result<()> {
    let temp = tempfile::tempdir()
        .map_err(|e| crate::common::Error::internal(format!("tempdir: {}", e)))?;
    let wal_dir = temp.path().join("wal");
    std::fs::create_dir_all(&wal_dir)
        .map_err(|e| crate::common::Error::internal(format!("mkdir: {}", e)))?;

    let mut wal_config = WalConfig::default();
    wal_config.log_writer_config.log_directory = wal_dir;

    let wal = Arc::new(WriteAheadLog::new(wal_config).await?);
    let lock_mgr = Arc::new(LockManager::new()?);
    let page_mgr = Arc::new(PageManager::new(
        temp.path().to_path_buf(),
        "recovery_test_tbl",
        PageManagerConfig::default(),
    )?);

    let acid = Arc::new(AcidManager::new(
        AcidConfig::default(),
        lock_mgr,
        wal.clone(),
        page_mgr.clone(),
    )?);

    let rm = RecoveryManager::new(acid, wal, page_mgr)?;
    rm.perform_recovery()?;

    assert_eq!(rm.get_state(), RecoveryState::Completed);
    let stats = rm.get_statistics();
    assert!(stats.recovery_completed);
    Ok(())
}

#[test]
fn test_recovery_statistics_default_core() {
    let s = RecoveryStatistics::default();
    assert_eq!(s.total_transactions, 0);
    assert_eq!(s.redo_operations, 0);
    assert_eq!(s.undo_operations, 0);
}

#[test]
fn test_recovery_getters_empty() {
    let temp = tempfile::TempDir::new().unwrap();
    let wal_dir = temp.path().join("wal2");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let mut wal_config = WalConfig::default();
        wal_config.log_writer_config.log_directory = wal_dir;
        let wal = Arc::new(WriteAheadLog::new(wal_config).await.unwrap());
        let lock_mgr = Arc::new(LockManager::new().unwrap());
        let page_mgr = Arc::new(
            PageManager::new(
                temp.path().to_path_buf(),
                "t2",
                PageManagerConfig::default(),
            )
            .unwrap(),
        );
        let acid = Arc::new(
            AcidManager::new(
                AcidConfig::default(),
                lock_mgr,
                wal.clone(),
                page_mgr.clone(),
            )
            .unwrap(),
        );
        let rm = RecoveryManager::new(acid, wal, page_mgr).unwrap();
        assert!(rm.get_active_transactions().is_empty());
        assert!(rm.get_pages_to_recover().is_empty());
        let _ = rm.create_checkpoint();
        let _ = rm.recover_from_checkpoint(0);
    });
}
