//! Stable embedded API (in-process usage).
//!
//! Surface area for embedders:
//! - [`Db`] owns the engine
//! - [`Connection`] carries per-session state
//! - [`Transaction`] is a safe RAII wrapper around `BEGIN/COMMIT/ROLLBACK`
//! - [`Config`] controls durability / WAL / checkpoint defaults (safe-by-default for embedded)
//!
//! # Defaults
//! [`Config::default`] is **safe-by-default**:
//! - [`DurabilityMode::Safe`] (commit waits for `fsync`)
//! - WAL on
//! - Checkpoint manager wired
//!
//! Use [`Config::fast`] or the `with_*` builders to opt into a faster but less durable mode.
//!
//! # Lifecycle safety
//! - [`Transaction`] rolls back on drop if it was not committed.
//! - [`Connection`] also rolls back any *pending* transaction on drop, so a panic between
//!   `BEGIN` and the first DML still leaves the heap consistent (no orphaned uncommitted data).

use crate::common::{DurabilityMode, Result};
use crate::network::engine::{EngineError, EngineHandle, EngineOutput, SessionContext};
use crate::network::sql_engine::SqlEngineConfig;
use crate::network::SqlEngine;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Embedded configuration for opening a database.
///
/// Safe-by-default. New fields are added with sensible defaults; prefer the
/// [`Config::default`] + `with_*` builders or [`Config::safe`] / [`Config::fast`] presets
/// over struct-literal construction so future fields don't break callers.
#[derive(Debug, Clone)]
pub struct Config {
    /// Durability policy for commit points (defaults to [`DurabilityMode::Safe`]).
    pub durability: DurabilityMode,
    /// Enable structured WAL + recovery (defaults to `true`).
    pub wal_enabled: bool,
    /// Wire a checkpoint manager when WAL is enabled (defaults to `true`).
    ///
    /// Set to `false` for read-mostly tooling that does not need background
    /// checkpoints. The legacy `RUSTDB_DISABLE_CHECKPOINT=1` env var still wins.
    pub checkpoints_enabled: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            durability: DurabilityMode::Safe,
            wal_enabled: true,
            checkpoints_enabled: true,
        }
    }
}

impl Config {
    /// Safe-by-default preset: `Safe` durability, WAL on, checkpoints on.
    pub fn safe() -> Self {
        Self::default()
    }

    /// Fast preset: `Fast` durability (no fsync on commit), WAL on, checkpoints on.
    ///
    /// Suitable for benchmarks and CI; not recommended for production data you care about.
    pub fn fast() -> Self {
        Self {
            durability: DurabilityMode::Fast,
            wal_enabled: true,
            checkpoints_enabled: true,
        }
    }

    /// Builder: override durability policy.
    pub fn with_durability(mut self, durability: DurabilityMode) -> Self {
        self.durability = durability;
        self
    }

    /// Builder: enable/disable WAL.
    pub fn with_wal(mut self, enabled: bool) -> Self {
        self.wal_enabled = enabled;
        self
    }

    /// Builder: enable/disable checkpoint manager.
    pub fn with_checkpoints(mut self, enabled: bool) -> Self {
        self.checkpoints_enabled = enabled;
        self
    }
}

impl From<Config> for SqlEngineConfig {
    fn from(c: Config) -> Self {
        SqlEngineConfig {
            wal_enabled: c.wal_enabled,
            durability: c.durability,
            checkpoints_enabled: c.checkpoints_enabled,
        }
    }
}

/// Embedded database handle (owns an in-process [`SqlEngine`]).
#[derive(Clone)]
pub struct Db {
    engine: Arc<SqlEngine>,
    data_dir: PathBuf,
    config: Config,
}

impl Db {
    /// Open or create a database rooted at `data_dir` using [`Config::default`] (safe-by-default).
    pub fn open_default(data_dir: impl AsRef<Path>) -> Result<Self> {
        Self::open(data_dir, Config::default())
    }

    /// Open or create a database rooted at `data_dir` with the given [`Config`].
    pub fn open(data_dir: impl AsRef<Path>, config: Config) -> Result<Self> {
        let dir = data_dir.as_ref().to_path_buf();
        let engine = SqlEngine::open_with_config(dir.clone(), config.clone().into())?;
        Ok(Self {
            engine: Arc::new(engine),
            data_dir: dir,
            config,
        })
    }

    /// Borrow the underlying engine.
    pub fn engine(&self) -> &SqlEngine {
        self.engine.as_ref()
    }

    /// Active configuration this database was opened with.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Active durability policy (mirrors `engine().durability()`).
    pub fn durability(&self) -> DurabilityMode {
        self.engine.durability()
    }

    /// Whether WAL + recovery is wired for this engine.
    pub fn wal_enabled(&self) -> bool {
        self.engine.wal_enabled()
    }

    /// Root data directory the database was opened against.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Run a manual checkpoint: flush heaps and append a `Checkpoint` WAL record.
    ///
    /// Returns an error when WAL is disabled or [`Config::checkpoints_enabled`] is `false`.
    pub fn checkpoint(&self) -> Result<()> {
        self.engine.checkpoint()
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
///
/// On drop, if a transaction is still open in this session's context the connection
/// issues a `ROLLBACK` so the engine applies undo and persists a consistent state.
pub struct Connection {
    engine: Arc<SqlEngine>,
    ctx: SessionContext,
}

impl Connection {
    /// Execute one SQL statement in this session.
    pub fn execute(&mut self, sql: &str) -> std::result::Result<EngineOutput, EngineError> {
        self.engine.execute_sql(sql, &mut self.ctx)
    }

    /// Whether this session currently has an open transaction.
    pub fn in_transaction(&self) -> bool {
        self.ctx.transaction.is_some()
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

impl Drop for Connection {
    fn drop(&mut self) {
        // If a transaction is still recorded for this session (e.g. user dropped the connection
        // mid-transaction without going through Transaction's RAII), issue ROLLBACK so the engine
        // applies the undo log instead of just leaking the locks via SqlTransaction's Drop.
        if self.ctx.transaction.is_some() {
            let _ = self.engine.execute_sql("ROLLBACK", &mut self.ctx);
        }
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

    #[test]
    fn embedded_config_presets_and_builders() {
        let safe = Config::safe();
        assert_eq!(safe.durability, DurabilityMode::Safe);
        assert!(safe.wal_enabled);
        assert!(safe.checkpoints_enabled);

        let fast = Config::fast();
        assert_eq!(fast.durability, DurabilityMode::Fast);

        let custom = Config::default()
            .with_durability(DurabilityMode::Fast)
            .with_wal(false)
            .with_checkpoints(false);
        assert_eq!(custom.durability, DurabilityMode::Fast);
        assert!(!custom.wal_enabled);
        assert!(!custom.checkpoints_enabled);
    }

    #[test]
    fn embedded_db_inspectors_match_config() -> Result<()> {
        let dir = TempDir::new().unwrap();
        let db = Db::open(dir.path(), Config::fast().with_checkpoints(false))?;
        assert_eq!(db.durability(), DurabilityMode::Fast);
        assert!(db.wal_enabled());
        assert_eq!(db.data_dir(), dir.path());
        // Checkpoint must error out when disabled.
        assert!(db.checkpoint().is_err());
        Ok(())
    }

    #[test]
    fn embedded_connection_drop_rolls_back_open_tx() -> Result<()> {
        let dir = TempDir::new().unwrap();
        let db = Db::open(dir.path(), Config::default())?;
        // Open a connection, BEGIN + INSERT, then drop the connection without commit/rollback.
        {
            let mut conn = db.connect();
            conn.execute("BEGIN TRANSACTION").unwrap();
            conn.execute("INSERT INTO t (a, b) VALUES (7, 'x')")
                .unwrap();
            assert!(conn.in_transaction());
            // Drop connection here – the Drop impl must issue ROLLBACK.
        }
        let mut q = db.connect();
        let out = q.execute("SELECT a FROM t").unwrap();
        match out {
            EngineOutput::ResultSet { rows, .. } => {
                assert_eq!(
                    rows.len(),
                    0,
                    "Connection drop should have rolled back the open transaction"
                );
            }
            _ => panic!("expected result set"),
        }
        Ok(())
    }

    #[test]
    fn embedded_open_default_uses_safe_defaults() -> Result<()> {
        let dir = TempDir::new().unwrap();
        let db = Db::open_default(dir.path())?;
        assert_eq!(db.durability(), DurabilityMode::Safe);
        assert!(db.wal_enabled());
        Ok(())
    }
}
