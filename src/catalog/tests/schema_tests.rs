//! Tests for SchemaManager

use crate::catalog::schema::SchemaManager;
use crate::common::Result;

#[test]
fn test_schema_manager_creation() -> Result<()> {
    let _sm = SchemaManager::new()?;
    Ok(())
}

#[test]
fn test_schema_manager_register_and_lookup() -> Result<()> {
    let mut sm = SchemaManager::new()?;
    assert_eq!(sm.table_id("nope"), None);

    let id1 = sm.register_table("users");
    assert_eq!(id1, 1);
    assert_eq!(sm.table_id("users"), Some(1));
    let id_again = sm.register_table("users");
    assert_eq!(id_again, id1);

    let id2 = sm.register_table("orders");
    assert_eq!(id2, 2);
    assert_eq!(sm.table_id("orders"), Some(2));
    Ok(())
}
