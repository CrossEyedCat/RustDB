//! Prepared statement cache for RustDB
//!
//! Stores prepared statements (PREPARE name AS ...) and resolves EXECUTE name (params).

use crate::common::{Error, Result};
use crate::parser::ast::{ExecuteStatement, Expression, PrepareStatement, SqlStatement};
use std::collections::HashMap;
use std::sync::RwLock;

/// Cached prepared statement with optional parameter slots
#[derive(Clone)]
pub struct CachedPreparedStatement {
    /// The parsed statement (SELECT, INSERT, UPDATE, DELETE)
    pub statement: SqlStatement,
    /// Number of parameter placeholders ($1, $2, ...)
    pub param_count: usize,
}

/// Cache for prepared statements
pub struct PreparedStatementCache {
    statements: RwLock<HashMap<String, CachedPreparedStatement>>,
}

impl PreparedStatementCache {
    /// Creates a new empty cache
    pub fn new() -> Self {
        Self {
            statements: RwLock::new(HashMap::new()),
        }
    }

    /// Registers a prepared statement from PREPARE
    pub fn prepare(&self, stmt: PrepareStatement) -> Result<()> {
        let param_count = Self::count_params(&stmt.statement);
        let cached = CachedPreparedStatement {
            statement: *stmt.statement,
            param_count,
        };
        let mut map = self.statements.write().map_err(|e| {
            Error::internal(format!("PreparedStatementCache lock poisoned: {}", e))
        })?;
        map.insert(stmt.name, cached);
        Ok(())
    }

    /// Resolves EXECUTE to the bound statement (params substituted)
    pub fn execute(&self, stmt: ExecuteStatement) -> Result<SqlStatement> {
        let map = self.statements.read().map_err(|e| {
            Error::internal(format!("PreparedStatementCache lock poisoned: {}", e))
        })?;
        let cached = map.get(&stmt.name).ok_or_else(|| {
            Error::internal(format!("Prepared statement '{}' not found", stmt.name))
        })?;

        if stmt.params.len() != cached.param_count {
            return Err(Error::internal(format!(
                "Parameter count mismatch: expected {}, got {}",
                cached.param_count,
                stmt.params.len()
            )));
        }

        Self::bind_params(&cached.statement, &stmt.params)
    }

    /// Removes a prepared statement (for DEALLOCATE)
    pub fn deallocate(&self, name: &str) -> Result<()> {
        let mut map = self.statements.write().map_err(|e| {
            Error::internal(format!("PreparedStatementCache lock poisoned: {}", e))
        })?;
        map.remove(name);
        Ok(())
    }

    /// Returns the number of statements in the cache
    pub fn len(&self) -> usize {
        self.statements.read().map(|m| m.len()).unwrap_or(0)
    }

    fn count_params(stmt: &SqlStatement) -> usize {
        match stmt {
            SqlStatement::Select(s) => Self::count_params_in_select(s),
            SqlStatement::Insert(_) | SqlStatement::Update(_) | SqlStatement::Delete(_) => 0,
            _ => 0,
        }
    }

    fn count_params_in_select(_stmt: &crate::parser::ast::SelectStatement) -> usize {
        // Simplified: no $1, $2 placeholders in parser yet - use 0
        0
    }

    fn bind_params(stmt: &SqlStatement, params: &[Expression]) -> Result<SqlStatement> {
        if params.is_empty() {
            return Ok(stmt.clone());
        }
        // Simplified: no placeholder substitution yet - return as-is
        Ok(stmt.clone())
    }
}

impl Default for PreparedStatementCache {
    fn default() -> Self {
        Self::new()
    }
}
