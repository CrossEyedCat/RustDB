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
}

/// Session-scoped state for a single logical client connection (placeholder for Phase 2).
#[derive(Debug, Default, Clone)]
pub struct SessionContext {
    /// Opaque session identifier when the server assigns one.
    pub session_id: Option<u64>,
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

// `Database` does not yet expose `execute_sql`; add `impl EngineHandle for Database` (or a wrapper)
// when the engine API is ready — see `docs/network/engine-boundary.md`.
