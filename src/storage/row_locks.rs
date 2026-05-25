//! Per-row write locks for index-backed DML (TPC-C hot path).
//!
//! Table-level locks remain for DDL, heap scan fallback, and `INSERT`.

use crate::common::types::RecordId;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::info;

const ROW_LOCK_SHARD_COUNT: usize = 64;

#[derive(Debug)]
struct RowLockShard {
    locks: Mutex<HashMap<(String, RecordId), Arc<RwLock<()>>>>,
}

impl Default for RowLockShard {
    fn default() -> Self {
        Self {
            locks: Mutex::new(HashMap::new()),
        }
    }
}

fn shard_index(table: &str, rid: RecordId) -> usize {
    let mut h = table.len() as u64 ^ rid;
    h = h.wrapping_mul(0x9E3779B97F4A7C15);
    (h as usize) & (ROW_LOCK_SHARD_COUNT - 1)
}

/// Lazily allocates an exclusive lock per `(table, record_id)`.
#[derive(Debug)]
pub struct RowLockManager {
    shards: [RowLockShard; ROW_LOCK_SHARD_COUNT],
}

impl Default for RowLockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl RowLockManager {
    pub fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| RowLockShard::default()),
        }
    }

    /// Runs `f` while holding exclusive row locks for `rids` (sorted, deduped).
    pub fn with_write_locks<R>(
        &self,
        table: &str,
        mut rids: Vec<RecordId>,
        f: impl FnOnce() -> R,
    ) -> R {
        rids.sort_unstable();
        rids.dedup();
        if rids.is_empty() {
            return f();
        }
        let wait_clock = row_lock_phase_log_enabled().then(Instant::now);

        let mut shard_order: Vec<usize> = rids.iter().map(|rid| shard_index(table, *rid)).collect();
        shard_order.sort_unstable();
        shard_order.dedup();

        let mut arc_by_rid: HashMap<RecordId, Arc<RwLock<()>>> = HashMap::with_capacity(rids.len());
        for shard_idx in shard_order {
            let mut map = self.shards[shard_idx]
                .locks
                .lock()
                .expect("row lock shard map poisoned");
            for rid in rids
                .iter()
                .copied()
                .filter(|rid| shard_index(table, *rid) == shard_idx)
            {
                let key = (table.to_string(), rid);
                arc_by_rid.insert(
                    rid,
                    map.entry(key)
                        .or_insert_with(|| Arc::new(RwLock::new(())))
                        .clone(),
                );
            }
        }

        let arcs: Vec<Arc<RwLock<()>>> = rids.iter().map(|rid| arc_by_rid[rid].clone()).collect();
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

#[cfg(test)]
pub(crate) fn shard_index_for_test(table: &str, rid: RecordId) -> usize {
    shard_index(table, rid)
}

fn row_lock_phase_log_enabled() -> bool {
    match std::env::var("RUSTDB_SQL_PHASE_LOG") {
        Ok(s) if s == "0" || s.eq_ignore_ascii_case("false") => false,
        Ok(_) => true,
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Barrier;
    use std::thread;

    #[test]
    fn shard_index_distributes_keys() {
        let mut seen = [false; ROW_LOCK_SHARD_COUNT];
        for rid in 0..256u64 {
            seen[shard_index("district", rid)] = true;
        }
        assert!(
            seen.iter().filter(|&&b| b).count() > 8,
            "expected spread across shards"
        );
    }

    #[test]
    fn concurrent_distinct_row_locks_do_not_deadlock() {
        let mgr = Arc::new(RowLockManager::new());
        let barrier = Arc::new(Barrier::new(64));
        let done = Arc::new(AtomicU32::new(0));
        let mut handles = Vec::new();
        for rid in 0..64u64 {
            let mgr = Arc::clone(&mgr);
            let barrier = Arc::clone(&barrier);
            let done = Arc::clone(&done);
            handles.push(thread::spawn(move || {
                barrier.wait();
                mgr.with_write_locks("district", vec![rid], || {
                    done.fetch_add(1, Ordering::SeqCst);
                });
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(done.load(Ordering::SeqCst), 64);
    }

    #[test]
    fn same_row_lock_serializes_writers() {
        let mgr = Arc::new(RowLockManager::new());
        let counter = Arc::new(AtomicU32::new(0));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let mgr = Arc::clone(&mgr);
            let counter = Arc::clone(&counter);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    mgr.with_write_locks("district", vec![7], || {
                        let v = counter.load(Ordering::SeqCst);
                        thread::yield_now();
                        counter.store(v + 1, Ordering::SeqCst);
                    });
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(counter.load(Ordering::SeqCst), 800);
    }
}
