//! Per-row write locks for index-backed DML (TPC-C hot path).
//!
//! Table-level locks remain for DDL, heap scan fallback, and `INSERT`.

use crate::common::types::RecordId;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::info;

/// Lazily allocates an exclusive lock per `(table, record_id)`.
#[derive(Debug, Default)]
pub struct RowLockManager {
    locks: Mutex<HashMap<(String, RecordId), Arc<RwLock<()>>>>,
}

impl RowLockManager {
    pub fn new() -> Self {
        Self {
            locks: Mutex::new(HashMap::new()),
        }
    }

    /// Runs `f` while holding exclusive row locks for `rids` (sorted, deduped).
    pub fn with_write_locks<R>(
        &self,
        table: &str,
        mut rids: Vec<RecordId>,
        f: impl FnOnce() -> R,
    ) -> R {
        let mut rids: Vec<RecordId> = {
            rids.sort_unstable();
            rids.dedup();
            rids
        };
        if rids.is_empty() {
            return f();
        }
        let wait_clock = row_lock_phase_log_enabled().then(Instant::now);
        let mut arcs: Vec<Arc<RwLock<()>>> = Vec::with_capacity(rids.len());
        {
            let mut map = self.locks.lock().expect("row lock map poisoned");
            for rid in rids {
                let key = (table.to_string(), rid);
                arcs.push(
                    map.entry(key)
                        .or_insert_with(|| Arc::new(RwLock::new(())))
                        .clone(),
                );
            }
        }
        let guards: Vec<_> = arcs.iter().map(|l| l.write()).collect();
        if let Some(t0) = wait_clock {
            info!(
                target: "rustdb::sql_phases",
                table = %table,
                row_count = guards.len(),
                lock_wait_us = t0.elapsed().as_micros() as u64,
                mode = "row_write",
                "row_storage_lock"
            );
        }
        let out = f();
        drop(guards);
        out
    }
}

fn row_lock_phase_log_enabled() -> bool {
    match std::env::var("RUSTDB_SQL_PHASE_LOG") {
        Ok(s) if s == "0" || s.eq_ignore_ascii_case("false") => false,
        Ok(_) => true,
        Err(_) => false,
    }
}
