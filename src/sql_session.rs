//! High-level embedder API: owned [`SqlEngine`] + default [`SessionContext`].
//!
//! Prefer this wrapper when you want a single obvious entry point instead of
//! wiring [`crate::network::engine::EngineHandle`] yourself.

use crate::common::Result;
use crate::network::engine::{EngineHandle, EngineOutput, SessionContext};
use crate::network::SqlEngine;
use std::path::PathBuf;

/// SQL engine session with a default client context (transactions, undo, etc.).
pub struct SqlSession {
    engine: SqlEngine,
    context: SessionContext,
}

impl SqlSession {
    /// Opens storage under `data_dir` (see [`SqlEngine::open`]) and a fresh session context.
    pub fn open(data_dir: PathBuf) -> Result<Self> {
        Ok(Self {
            engine: SqlEngine::open(data_dir)?,
            context: SessionContext::default(),
        })
    }

    /// Underlying engine (immutable).
    pub fn engine(&self) -> &SqlEngine {
        &self.engine
    }

    /// Underlying engine (mutable) — rare; use [`Self::execute_sql`] for normal work.
    pub fn engine_mut(&mut self) -> &mut SqlEngine {
        &mut self.engine
    }

    /// Session context (transactions and per-session state).
    pub fn context(&self) -> &SessionContext {
        &self.context
    }

    pub fn context_mut(&mut self) -> &mut SessionContext {
        &mut self.context
    }

    /// Executes one SQL statement through the engine handle and this session's context.
    pub fn execute_sql(
        &mut self,
        sql: &str,
    ) -> std::result::Result<EngineOutput, crate::network::engine::EngineError> {
        self.engine.execute_sql(sql, &mut self.context)
    }
}
