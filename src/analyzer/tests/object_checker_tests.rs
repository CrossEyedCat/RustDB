//! Tests for the Object Existence Checker

use crate::analyzer::object_checker::{ObjectMetadata, ObjectType};
use crate::analyzer::ObjectChecker;
use crate::common::Result;

#[test]
fn test_object_checker_creation() {
    let checker = ObjectChecker::new();
    let (cache_size, cache_enabled) = checker.cache_statistics();
    assert_eq!(cache_size, 0);
    assert!(cache_enabled);
}

#[test]
fn test_table_existence_check() -> Result<()> {
    let mut checker = ObjectChecker::new();
    let result = checker.check_table_exists("users", &())?;

    assert!(result.exists);
    assert_eq!(result.object_type, ObjectType::Table);
    assert_eq!(result.metadata.name, "users");

    Ok(())
}

#[test]
fn test_column_existence_check() -> Result<()> {
    let mut checker = ObjectChecker::new();
    let result = checker.check_column_exists("users", "name", &())?;

    assert!(result.exists);
    assert_eq!(result.object_type, ObjectType::Column);
    assert_eq!(result.metadata.name, "name");

    Ok(())
}

#[test]
fn test_index_existence_check() -> Result<()> {
    let mut checker = ObjectChecker::new();
    let result = checker.check_index_exists("idx_users_email", &())?;

    // In test mode, indexes do not exist
    assert!(!result.exists);
    assert_eq!(result.object_type, ObjectType::Index);

    Ok(())
}

#[test]
fn test_cache_functionality() -> Result<()> {
    let mut checker = ObjectChecker::new();

    // First call - adds to cache
    let result1 = checker.check_table_exists("test_table", &())?;
    assert!(result1.exists);

    // Second call - must use cache
    let result2 = checker.check_table_exists("test_table", &())?;
    assert!(result2.exists);

    let (cache_size, _) = checker.cache_statistics();
    assert_eq!(cache_size, 1);

    Ok(())
}

#[test]
fn test_cache_disable() -> Result<()> {
    let mut checker = ObjectChecker::new();

    // Disable cache
    checker.set_cache_enabled(false);

    // Performing a check
    let _result = checker.check_table_exists("test_table", &())?;

    // The cache must be empty
    let (cache_size, cache_enabled) = checker.cache_statistics();
    assert_eq!(cache_size, 0);
    assert!(!cache_enabled);

    Ok(())
}

#[test]
fn test_cache_clear() -> Result<()> {
    let mut checker = ObjectChecker::new();

    // Adding entries to the cache
    let _result1 = checker.check_table_exists("table1", &())?;
    let _result2 = checker.check_table_exists("table2", &())?;

    let (cache_size_before, _) = checker.cache_statistics();
    assert!(cache_size_before > 0);

    // Clearing the cache
    checker.clear_cache();

    let (cache_size_after, _) = checker.cache_statistics();
    assert_eq!(cache_size_after, 0);

    Ok(())
}

#[test]
fn test_object_metadata() {
    let metadata = ObjectMetadata::new("test_object".to_string())
        .with_schema("public".to_string())
        .with_property("type".to_string(), "table".to_string())
        .with_property("owner".to_string(), "admin".to_string());

    assert_eq!(metadata.name, "test_object");
    assert_eq!(metadata.schema_name, Some("public".to_string()));
    assert_eq!(metadata.properties.get("type"), Some(&"table".to_string()));
    assert_eq!(metadata.properties.get("owner"), Some(&"admin".to_string()));
}
