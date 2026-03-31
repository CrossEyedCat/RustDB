//! Access control for rustdb

use crate::common::Result;
use std::collections::{HashMap, HashSet};

/// Role name → set of permission strings (e.g. `table.users.select`).
#[derive(Debug, Clone, Default)]
pub struct AccessControl {
    roles: HashMap<String, HashSet<String>>,
}

impl AccessControl {
    pub fn new() -> Result<Self> {
        Ok(Self {
            roles: HashMap::new(),
        })
    }

    pub fn grant(&mut self, role: &str, permission: &str) {
        self.roles
            .entry(role.to_string())
            .or_insert_with(HashSet::new)
            .insert(permission.to_string());
    }

    pub fn revoke(&mut self, role: &str, permission: &str) -> bool {
        self.roles
            .get_mut(role)
            .map(|p| p.remove(permission))
            .unwrap_or(false)
    }

    pub fn role_has(&self, role: &str, permission: &str) -> bool {
        self.roles
            .get(role)
            .map(|p| p.contains(permission))
            .unwrap_or(false)
    }
}
