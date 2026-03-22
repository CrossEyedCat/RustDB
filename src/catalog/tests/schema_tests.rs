//! Tests for SchemaManager

use crate::catalog::schema::SchemaManager;
use crate::common::Result;

#[test]
fn test_schema_manager_creation() -> Result<()> {
    let _sm = SchemaManager::new()?;
    Ok(())
}
