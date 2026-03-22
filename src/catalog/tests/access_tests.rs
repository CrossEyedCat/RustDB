//! Tests for AccessControl

use crate::catalog::access::AccessControl;
use crate::common::Result;

#[test]
fn test_access_control_creation() -> Result<()> {
    let _ac = AccessControl::new()?;
    Ok(())
}
