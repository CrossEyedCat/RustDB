//! Tests for IndexRegistry

use crate::storage::index_registry::IndexRegistry;
use crate::common::Result;

#[test]
fn test_index_registry_creation() {
    let _registry = IndexRegistry::new();
}

#[test]
fn test_index_registry_create_and_get() -> Result<()> {
    let mut registry = IndexRegistry::new();
    registry.create_index("users", "idx_id", vec!["id".to_string()])?;
    let index = registry.get_index("users", "idx_id");
    assert!(index.is_some());
    Ok(())
}

#[test]
fn test_index_registry_duplicate_create_fails() -> Result<()> {
    let mut registry = IndexRegistry::new();
    registry.create_index("users", "idx_id", vec!["id".to_string()])?;
    let result = registry.create_index("users", "idx_id", vec!["id".to_string()]);
    assert!(result.is_err());
    Ok(())
}

#[test]
fn test_index_registry_drop() -> Result<()> {
    let mut registry = IndexRegistry::new();
    registry.create_index("users", "idx_id", vec!["id".to_string()])?;
    registry.drop_index("users", "idx_id")?;
    assert!(registry.get_index("users", "idx_id").is_none());
    Ok(())
}

#[test]
fn test_index_registry_drop_nonexistent_fails() -> Result<()> {
    let mut registry = IndexRegistry::new();
    let result = registry.drop_index("users", "idx_nonexistent");
    assert!(result.is_err());
    Ok(())
}
