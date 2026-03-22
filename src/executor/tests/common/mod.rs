//! Common test utilities for executor tests

use crate::storage::page_manager::{PageManager, PageManagerConfig};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

/// Creates a PageManager in a temporary directory for testing
pub fn create_test_page_manager() -> (TempDir, Arc<Mutex<PageManager>>) {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();
    let pm = PageManager::new(data_path, "test_table", PageManagerConfig::default()).unwrap();
    (temp_dir, Arc::new(Mutex::new(pm)))
}
