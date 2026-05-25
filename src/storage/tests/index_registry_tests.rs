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
fn test_index_keys_unchanged_skips_update() -> Result<()> {
    let mut registry = IndexRegistry::new();
    registry.create_index(
        "district",
        "idx_district_wd",
        vec!["d_w_id".to_string(), "d_id".to_string()],
    )?;
    let mut old_m = HashMap::new();
    old_m.insert("d_w_id".to_string(), "1".to_string());
    old_m.insert("d_id".to_string(), "2".to_string());
    old_m.insert("d_next_o_id".to_string(), "10".to_string());
    let mut new_m = old_m.clone();
    new_m.insert("d_next_o_id".to_string(), "11".to_string());
    assert!(registry.index_keys_unchanged("district", &old_m, &new_m));
    registry.insert_into_indexes("district", 1, &old_m)?;
    registry.update_indexes("district", 1, &old_m, &new_m)?;
    let (ids, _) = registry
        .lookup_record_ids_by_equalities(
            "district",
            &HashMap::from([
                ("d_w_id".to_string(), "1".to_string()),
                ("d_id".to_string(), "2".to_string()),
            ]),
        )?
        .expect("lookup");
    assert_eq!(ids, vec![1]);
    Ok(())
}

#[test]
fn test_index_registry_named_index_insert() -> Result<()> {
    let mut registry = IndexRegistry::new();
    registry.create_index("t", "idx_k", vec!["k".to_string()])?;
    let mut m = HashMap::new();
    m.insert("k".to_string(), "1".to_string());
    registry.insert_into_named_index("t", "idx_k", 7, &m)?;
    let (ids, exact) = registry
        .lookup_record_ids_by_equalities("t", &m)?
        .expect("lookup");
    assert!(exact);
    assert_eq!(ids, vec![7]);
    Ok(())
}

#[test]
fn test_index_registry_lookup_miss_no_index() -> Result<()> {
    let registry = IndexRegistry::new();
    let mut eq = HashMap::new();
    eq.insert("w_id".to_string(), "1".to_string());
    assert!(registry
        .lookup_record_ids_by_equalities("warehouse", &eq)?
        .is_none());
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
    let (ids, exact) = registry
        .lookup_record_ids_by_equalities("district", &eq)?
        .expect("index should match");
    assert!(exact);
    assert_eq!(ids, vec![100]);

    eq.remove("d_id");
    let (ids, exact) = registry
        .lookup_record_ids_by_equalities("district", &eq)?
        .expect("prefix index match");
    assert!(!exact);
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&100));
    assert!(ids.contains(&101));
    Ok(())
}
