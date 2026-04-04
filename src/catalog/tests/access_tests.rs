//! Tests for AccessControl

use crate::catalog::access::AccessControl;
use crate::common::Result;

#[test]
fn test_access_control_creation() -> Result<()> {
    let _ac = AccessControl::new()?;
    Ok(())
}

#[test]
fn test_access_control_grant_revoke_and_check() -> Result<()> {
    let mut ac = AccessControl::new()?;
    ac.grant("admin", "table.users.select");
    assert!(ac.role_has("admin", "table.users.select"));
    assert!(!ac.role_has("admin", "table.users.insert"));
    assert!(!ac.role_has("guest", "table.users.select"));

    assert!(ac.revoke("admin", "table.users.select"));
    assert!(!ac.role_has("admin", "table.users.select"));
    assert!(!ac.revoke("admin", "table.users.select"));
    assert!(!ac.revoke("missing_role", "any"));

    ac.grant("r1", "p1");
    ac.grant("r1", "p2");
    assert!(ac.role_has("r1", "p1") && ac.role_has("r1", "p2"));
    Ok(())
}
