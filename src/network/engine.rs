//! Engine boundary: thin contract between the network layer and SQL execution ([`EngineHandle`]).
//!
//! See `docs/network/engine-boundary.md`. Wire mapping uses [`crate::network::framing`] payloads.

use crate::network::framing::{ErrorPayload, ExecutionOkPayload, ResultSetPayload, ServerMessage};

/// Stable numeric codes for [`EngineError::code`], for mapping to [`ErrorPayload`] on the wire.
pub mod engine_error_code {
    /// Reserved stub / placeholder (tests, not yet classified).
    pub const STUB: u32 = 1000;
    /// Generic internal failure (logged server-side; message may be sanitized before wire).
    pub const INTERNAL: u32 = 1001;
    /// Malformed frame, wrong message kind on a query stream, etc.
    pub const PROTOCOL: u32 = 2000;
    /// SQL text exceeds configured max length.
    pub const SQL_TOO_LONG: u32 = 2001;
    /// Engine did not finish within the per-query deadline.
    pub const QUERY_TIMEOUT: u32 = 2002;
    /// Result row count exceeds configured cap.
    pub const RESULT_ROWS_TOO_LARGE: u32 = 2003;
    /// Statement kind not implemented on the server engine (e.g. DDL or DML not wired).
    pub const UNSUPPORTED_SQL: u32 = 2004;
    /// Constraint violation (PRIMARY KEY, UNIQUE, FOREIGN KEY, etc.).
    pub const CONSTRAINT_VIOLATION: u32 = 2005;
    /// `COMMIT` / `ROLLBACK` with no open transaction.
    pub const NO_ACTIVE_TRANSACTION: u32 = 2006;
    /// `BEGIN` while a transaction is already open for this session.
    pub const ALREADY_IN_TRANSACTION: u32 = 2007;
    /// DDL is not allowed inside an explicit transaction (minimal Phase 6 rule).
    pub const DDL_IN_TRANSACTION: u32 = 2008;
}

use crate::common::types::RecordId;

/// Isolation level for the SQL engine session transaction (`BEGIN` … `COMMIT`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SqlIsolationLevel {
    /// Read committed semantics at the statement level (see `SqlEngine` docs).
    #[default]
    ReadCommitted,
    /// One active engine transaction at a time for this level (global lock); snapshot semantics are approximate.
    RepeatableRead,
    /// Same global serialization as [`Self::RepeatableRead`] in this implementation; stricter page-level rules may follow.
    Serializable,
}

/// Undo record for a single DML operation within a session transaction.
#[derive(Debug, Clone)]
pub enum UndoEntry {
    /// Reverse an `INSERT` by unregistering and deleting the row at `rid`.
    /// `payload` is the serialized row bytes right after insert (so rollback does not rely on `get_record`).
    Insert {
        table: String,
        rid: RecordId,
        payload: Vec<u8>,
    },
    /// Reverse a `DELETE` by re-inserting the previous row bytes.
    Delete {
        table: String,
        rid: RecordId,
        payload: Vec<u8>,
    },
    /// Reverse an `UPDATE` by restoring the previous row bytes.
    Update {
        table: String,
        rid: RecordId,
        old_payload: Vec<u8>,
    },
}

/// Open user transaction state (per [`SessionContext`]).
pub struct SqlTransaction {
    pub isolation: SqlIsolationLevel,
    pub undo: Vec<UndoEntry>,
    /// Global lock for [`SqlIsolationLevel::RepeatableRead`] / [`SqlIsolationLevel::Serializable`].
    strong_iso: Option<parking_lot::MutexGuard<'static, ()>>,
    /// Structured WAL transaction id (see `src/logging`), if WAL is enabled.
    pub wal_tx_id: Option<u64>,
    pub wal_begin_lsn: Option<crate::logging::log_record::LogSequenceNumber>,
    pub wal_last_lsn: Option<crate::logging::log_record::LogSequenceNumber>,
}

impl std::fmt::Debug for SqlTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlTransaction")
            .field("isolation", &self.isolation)
            .field("undo_len", &self.undo.len())
            .field("wal_tx_id", &self.wal_tx_id)
            .field("wal_last_lsn", &self.wal_last_lsn)
            .field("strong_iso_held", &self.strong_iso.is_some())
            .finish()
    }
}

impl SqlTransaction {
    pub fn new(
        isolation: SqlIsolationLevel,
        strong_iso: Option<parking_lot::MutexGuard<'static, ()>>,
    ) -> Self {
        Self {
            isolation,
            undo: Vec::new(),
            strong_iso,
            wal_tx_id: None,
            wal_begin_lsn: None,
            wal_last_lsn: None,
        }
    }
}

/// Session-scoped state for a single logical client connection.
#[derive(Debug)]
pub struct SessionContext {
    /// Opaque session identifier when the server assigns one.
    pub session_id: Option<u64>,
    /// User transaction (`BEGIN` … `COMMIT` / `ROLLBACK`), if any.
    pub transaction: Option<SqlTransaction>,
}

impl Default for SessionContext {
    fn default() -> Self {
        Self {
            session_id: None,
            transaction: None,
        }
    }
}

/// Successful engine result: tabular data or a non-query completion without rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineOutput {
    /// Rows with column names (v1 uses strings everywhere; typed values can come later).
    ResultSet {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
    },
    /// DDL/DML or operation completed without a result set.
    ExecutionOk { rows_affected: u64 },
}

/// Engine-side failure with a **stable** `code` for mapping to wire [`ErrorPayload`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message} (code {code})")]
pub struct EngineError {
    pub code: u32,
    pub message: String,
}

impl EngineError {
    pub fn new(code: u32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl From<EngineError> for ErrorPayload {
    fn from(value: EngineError) -> Self {
        ErrorPayload {
            code: value.code,
            message: value.message,
        }
    }
}

impl From<&EngineError> for ErrorPayload {
    fn from(value: &EngineError) -> Self {
        ErrorPayload {
            code: value.code,
            message: value.message.clone(),
        }
    }
}

impl EngineOutput {
    /// Map engine output to a single [`ServerMessage`] for framing.
    pub fn into_server_message(self) -> ServerMessage {
        match self {
            EngineOutput::ResultSet { columns, rows } => {
                ServerMessage::ResultSet(ResultSetPayload { columns, rows })
            }
            EngineOutput::ExecutionOk { rows_affected } => {
                ServerMessage::ExecutionOk(ExecutionOkPayload { rows_affected })
            }
        }
    }
}

/// Abstraction implemented by the real database engine (or a stub for tests).
pub trait EngineHandle: Send + Sync {
    fn execute_sql(&self, sql: &str, ctx: &mut SessionContext)
        -> Result<EngineOutput, EngineError>;

    /// Whether the network layer may memoize and serve **pre-encoded** wire frames for deterministic
    /// `SELECT` queries without `FROM` (literal projections).
    ///
    /// Default is `false` (safe for tests/stubs). The real SQL engine can opt in.
    fn supports_select_no_from_wire_cache(&self) -> bool {
        false
    }
}

/// Configurable stub engine for tests and early server bring-up (no `Database` required).
#[derive(Debug, Clone)]
pub struct StubEngine {
    behavior: StubBehavior,
}

#[derive(Debug, Clone)]
enum StubBehavior {
    Ok(EngineOutput),
    Err(EngineError),
}

impl Default for StubEngine {
    fn default() -> Self {
        Self::empty_result_set()
    }
}

impl StubEngine {
    /// Returns an empty [`EngineOutput::ResultSet`] (zero columns, zero rows).
    pub fn empty_result_set() -> Self {
        Self {
            behavior: StubBehavior::Ok(EngineOutput::ResultSet {
                columns: vec![],
                rows: vec![],
            }),
        }
    }

    /// Returns a fixed successful [`EngineOutput`] on every call.
    pub fn fixed_ok(output: EngineOutput) -> Self {
        Self {
            behavior: StubBehavior::Ok(output),
        }
    }

    /// Always returns the same [`EngineError`].
    pub fn fixed_error(err: EngineError) -> Self {
        Self {
            behavior: StubBehavior::Err(err),
        }
    }
}

impl EngineHandle for StubEngine {
    fn execute_sql(
        &self,
        _sql: &str,
        _ctx: &mut SessionContext,
    ) -> Result<EngineOutput, EngineError> {
        match &self.behavior {
            StubBehavior::Ok(o) => Ok(o.clone()),
            StubBehavior::Err(e) => Err(e.clone()),
        }
    }
}

// Production path: [`crate::network::sql_engine::SqlEngine`] implements [`EngineHandle`] (parse → plan → execute).
