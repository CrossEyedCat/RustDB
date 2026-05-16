//! Tests for IndexRegistry

use crate::common::Result;
use crate::storage::index_registry::IndexRegistry;
use std::collections::HashMap;

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

#[test]
fn test_index_registry_list_and_entry() -> Result<()> {
    let mut registry = IndexRegistry::new();
    registry.create_index("users", "idx_id", vec!["id".to_string()])?;
    let list = registry.list_indexes_for_table("users");
    assert_eq!(list.len(), 1);
    assert!(registry.get_index_entry("users", "idx_id").is_some());
    Ok(())
}

#[test]
fn test_index_registry_insert_delete_update() -> Result<()> {
    let mut registry = IndexRegistry::new();
    registry.create_index("t", "idx", vec!["k".to_string()])?;
    let mut m = HashMap::new();
    m.insert("k".to_string(), "v1".to_string());
    registry.insert_into_indexes("t", 1, &m)?;
    registry.update_indexes("t", 1, &m, &m)?;
    registry.delete_from_indexes("t", 1, &m)?;
    Ok(())
}

#[test]
fn test_index_registry_composite_equality_lookup() -> Result<()> {
    let mut registry = IndexRegistry::new();
    registry.create_index(
        "district",
        "idx_d_w_id_d_id",
        vec!["d_w_id".to_string(), "d_id".to_string()],
    )?;
    let mut m = HashMap::new();
    m.insert("d_w_id".to_string(), "1".to_string());
    m.insert("d_id".to_string(), "4".to_string());
    registry.insert_into_indexes("district", 100, &m)?;
    m.insert("d_id".to_string(), "5".to_string());
    registry.insert_into_indexes("district", 101, &m)?;

    let mut eq = HashMap::new();
    eq.insert("d_w_id".to_string(), "1".to_string());
    eq.insert("d_id".to_string(), "4".to_string());
    let ids = registry
        .lookup_record_ids_by_equalities("district", &eq)?
        .expect("index should match");
    assert_eq!(ids, vec![100]);

    eq.remove("d_id");
    let ids = registry
        .lookup_record_ids_by_equalities("district", &eq)?
        .expect("prefix index match");
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&100));
    assert!(ids.contains(&101));
    Ok(())
}
