//! Stable embedded API (in-process usage).
//!
//! This module provides a small, coherent surface area for embedders:
//! - [`Db`] owns the engine
//! - [`Connection`] carries per-session state
//! - [`Transaction`] is a safe RAII wrapper around `BEGIN/COMMIT/ROLLBACK`
//! - [`Config`] controls durability defaults (safe-by-default)

use crate::common::{DurabilityMode, Result};
use crate::network::engine::{EngineError, EngineHandle, EngineOutput, SessionContext};
use crate::network::sql_engine::SqlEngineConfig;
use crate::network::SqlEngine;
use std::path::Path;
use std::sync::Arc;

/// Embedded configuration for opening a database.
#[derive(Debug, Clone)]
pub struct Config {
    /// Durability policy (defaults to [`DurabilityMode::Safe`]).
    pub durability: DurabilityMode,
    /// Enable structured WAL + recovery (defaults to `true`).
    pub wal_enabled: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            durability: DurabilityMode::Safe,
            wal_enabled: true,
        }
    }
}

/// Embedded database handle (owns an in-process [`SqlEngine`]).
#[derive(Clone)]
pub struct Db {
    engine: Arc<SqlEngine>,
}

impl Db {
    /// Open or create a database rooted at `data_dir`.
    pub fn open(data_dir: impl AsRef<Path>, config: Config) -> Result<Self> {
        let dir = data_dir.as_ref().to_path_buf();
        let engine = SqlEngine::open_with_config(
            dir,
            SqlEngineConfig {
                wal_enabled: config.wal_enabled,
                durability: config.durability,
            },
        )?;
        Ok(Self {
            engine: Arc::new(engine),
        })
    }

    /// Borrow the underlying engine.
    pub fn engine(&self) -> &SqlEngine {
        self.engine.as_ref()
    }

    /// Create a new embedded session/connection.
    pub fn connect(&self) -> Connection {
        Connection {
            engine: self.engine.clone(),
            ctx: SessionContext::default(),
        }
    }
}

/// Per-session SQL connection for embedded usage.
pub struct Connection {
    engine: Arc<SqlEngine>,
    ctx: SessionContext,
}

impl Connection {
    /// Execute one SQL statement in this session.
    pub fn execute(&mut self, sql: &str) -> std::result::Result<EngineOutput, EngineError> {
        self.engine.execute_sql(sql, &mut self.ctx)
    }

    /// Start a transaction (issues `BEGIN`).
    pub fn begin(&mut self) -> std::result::Result<Transaction<'_>, EngineError> {
        self.execute("BEGIN TRANSACTION")?;
        Ok(Transaction {
            conn: self,
            active: true,
        })
    }
}

/// RAII transaction wrapper. If dropped while still active, it attempts to roll back.
pub struct Transaction<'a> {
    conn: &'a mut Connection,
    active: bool,
}

impl<'a> Transaction<'a> {
    /// Execute a statement inside this transaction (same as [`Connection::execute`]).
    pub fn execute(&mut self, sql: &str) -> std::result::Result<EngineOutput, EngineError> {
        self.conn.execute(sql)
    }

    /// Commit this transaction (issues `COMMIT`).
    pub fn commit(mut self) -> std::result::Result<(), EngineError> {
        self.conn.execute("COMMIT")?;
        self.active = false;
        Ok(())
    }

    /// Roll back this transaction (issues `ROLLBACK`).
    pub fn rollback(mut self) -> std::result::Result<(), EngineError> {
        self.conn.execute("ROLLBACK")?;
        self.active = false;
        Ok(())
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if self.active {
            let _ = self.conn.execute("ROLLBACK");
            self.active = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn embedded_db_connect_and_select() -> Result<()> {
        let dir = TempDir::new().unwrap();
        let db = Db::open(dir.path(), Config::default())?;
        let mut conn = db.connect();
        let out = conn.execute("SELECT 1, 2").unwrap();
        assert!(matches!(out, EngineOutput::ResultSet { .. }));
        Ok(())
    }

    #[test]
    fn embedded_transaction_raii_rollback_on_drop() -> Result<()> {
        let dir = TempDir::new().unwrap();
        let db = Db::open(dir.path(), Config::default())?;
        let mut conn = db.connect();
        {
            let mut tx = conn.begin().unwrap();
            let _ = tx.execute("INSERT INTO t (a, b) VALUES (1, 'x')").unwrap();
            // Drop without commit should rollback.
        }
        let out = conn.execute("SELECT a FROM t").unwrap();
        match out {
            EngineOutput::ResultSet { rows, .. } => {
                assert_eq!(rows.len(), 0);
            }
            _ => panic!("expected result set"),
        }
        Ok(())
    }

    #[test]
    fn embedded_transaction_commit_persists_in_session() -> Result<()> {
        let dir = TempDir::new().unwrap();
        let db = Db::open(dir.path(), Config::default())?;
        let mut conn = db.connect();
        let mut tx = conn.begin().unwrap();
        tx.execute("INSERT INTO t (a, b) VALUES (42, 'hi')")
            .unwrap();
        tx.commit().unwrap();
        let out = conn.execute("SELECT a FROM t").unwrap();
        match out {
            EngineOutput::ResultSet { rows, .. } => {
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("expected result set"),
        }
        Ok(())
    }
}
