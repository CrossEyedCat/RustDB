//! Common test utilities for executor tests

use crate::common::types::{ColumnValue, DataType};
use crate::storage::page_manager::{PageManager, PageManagerConfig, PageManagerMutex};
use crate::storage::tuple::Tuple;
use std::sync::Arc;
use tempfile::TempDir;

/// Creates a PageManager in a temporary directory for testing
pub fn create_test_page_manager() -> (TempDir, Arc<PageManagerMutex>) {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();
    let pm = PageManager::new(data_path, "test_table", PageManagerConfig::default()).unwrap();
    (temp_dir, Arc::new(PageManagerMutex::new(pm)))
}

/// Inserts `n` tuples with columns `id` (Integer) and `data` (Varchar), for table-scan executor tests.
pub fn seed_id_data_rows(pm: &Arc<PageManagerMutex>, n: usize) {
    let mut pm = pm.lock();
    for i in 0..n {
        let mut t = Tuple::new((i + 1) as u64);
        t.set_value("id", ColumnValue::new(DataType::Integer(i as i32 + 1)));
        t.set_value(
            "data",
            ColumnValue::new(DataType::Varchar(format!("v{}", i))),
        );
        let bytes = t.to_bytes().expect("tuple bytes");
        pm.insert(&bytes).expect("insert");
    }
}
