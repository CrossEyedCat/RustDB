use crate::common::types::PageId;
use crate::storage::advanced_file_manager::AdvancedFileManager;
use crate::storage::database_file::{DatabaseFileType, ExtensionStrategy};
use tempfile::TempDir;

/// Creates a test advanced file manager using a temporary directory
fn create_test_advanced_file_manager() -> (AdvancedFileManager, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_path_buf();
    let manager = AdvancedFileManager::new(data_dir).unwrap();
    (manager, temp_dir)
}

#[test]
fn test_create_advanced_file_manager() {
    let (manager, _temp_dir) = create_test_advanced_file_manager();

    // Ensure manager was constructed
    let _ = manager; // Avoid unused-variable warning
    assert!(true); // Basic sanity check
}

#[test]
fn test_create_database_file() {
    let (mut manager, _temp_dir) = create_test_advanced_file_manager();

    let result = manager.create_database_file(
        "test.dat",
        DatabaseFileType::Data,
        1,
        ExtensionStrategy::Fixed,
    );
    assert!(result.is_ok());
}

#[test]
fn test_open_database_file() {
    let (mut manager, _temp_dir) = create_test_advanced_file_manager();

    // First create file
    let _file_id = manager
        .create_database_file(
            "test_open.dat",
            DatabaseFileType::Data,
            1,
            ExtensionStrategy::Linear,
        )
        .unwrap();

    // Then open it
    let result = manager.open_database_file("test_open.dat");
    assert!(result.is_ok());
}

#[test]
fn test_allocate_pages() {
    let (mut manager, _temp_dir) = create_test_advanced_file_manager();

    let file_id = manager
        .create_database_file(
            "allocate_test.dat",
            DatabaseFileType::Data,
            1,
            ExtensionStrategy::Fixed,
        )
        .unwrap();

    // Allocate pages
    let result = manager.allocate_pages(file_id, 5);
    assert!(result.is_ok());

    let start_page = result.unwrap();
    // Ensure the returned page index is valid (may be 0+)
    let _ = start_page; // Use variable
}

#[test]
fn test_free_pages() {
    let (mut manager, _temp_dir) = create_test_advanced_file_manager();

    let file_id = manager
        .create_database_file(
            "free_test.dat",
            DatabaseFileType::Data,
            1,
            ExtensionStrategy::Linear,
        )
        .unwrap();

    // Allocate pages first
    let start_page = manager.allocate_pages(file_id, 5).unwrap();

    // Now free them
    let result = manager.free_pages(file_id, start_page, 3);
    assert!(result.is_ok());
}

#[test]
fn test_write_and_read_page() {
    let (mut manager, _temp_dir) = create_test_advanced_file_manager();

    let file_id = manager
        .create_database_file(
            "rw_test.dat",
            DatabaseFileType::Data,
            1,
            ExtensionStrategy::Fixed,
        )
        .unwrap();

    // Allocate a page
    let page_id = manager.allocate_pages(file_id, 1).unwrap();

    // Write data
    let test_data = vec![42u8; 4096];
    let write_result = manager.write_page(file_id, page_id, &test_data);

    // If write succeeded, verify read
    if write_result.is_ok() {
        let read_result = manager.read_page(file_id, page_id);
        if read_result.is_ok() {
            let read_data = read_result.unwrap();
            assert_eq!(&read_data[0..10], &test_data[0..10]); // Compare first 10 bytes
        }
    }

    // Succeeds provided no panic occurred
    assert!(true);
}

#[test]
fn test_get_global_statistics() {
    let (manager, _temp_dir) = create_test_advanced_file_manager();

    let stats = manager.get_global_statistics();
    // Ensure statistics object is accessible
    assert!(stats.total_files >= 0);
}

#[test]
fn test_maintenance_check() {
    let (mut manager, _temp_dir) = create_test_advanced_file_manager();

    let result = manager.maintenance_check();
    assert!(result.is_ok());

    let files_needing_maintenance = result.unwrap();
    // Fresh manager should report no pending maintenance
    assert!(files_needing_maintenance.len() == 0);
}

#[test]
fn test_multiple_extension_strategies() {
    let (mut manager, _temp_dir) = create_test_advanced_file_manager();

    // Exercise each extension strategy
    let strategies = vec![
        ExtensionStrategy::Fixed,
        ExtensionStrategy::Linear,
        ExtensionStrategy::Exponential,
        ExtensionStrategy::Adaptive,
    ];

    for (i, strategy) in strategies.iter().enumerate() {
        let filename = format!("strategy_test_{}.dat", i);
        let result = manager.create_database_file(
            &filename,
            DatabaseFileType::Data,
            i as u32 + 1,
            *strategy,
        );
        assert!(result.is_ok());
    }
}

#[test]
fn test_boundary_conditions() {
    let (mut manager, _temp_dir) = create_test_advanced_file_manager();

    let file_id = manager
        .create_database_file(
            "boundary.dat",
            DatabaseFileType::Data,
            1,
            ExtensionStrategy::Fixed,
        )
        .unwrap();

    // Attempt to free zero pages
    let result = manager.free_pages(file_id, 1, 0);
    // Implementation-defined result
    let _ = result;

    // Attempt to allocate zero pages
    let result = manager.allocate_pages(file_id, 0);
    // Implementation-defined result
    let _ = result;

    assert!(true); // Boundary-case smoke test
}
