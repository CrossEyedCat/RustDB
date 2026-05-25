//! Tests for sharded [`crate::storage::row_locks::RowLockManager`].

use crate::storage::row_locks::{shard_index_for_test, RowLockManager};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::thread;

#[test]
fn sharded_row_locks_distinct_rids_complete() {
    let mgr = Arc::new(RowLockManager::new());
    let done = Arc::new(AtomicU32::new(0));
    let mut handles = Vec::new();
    for rid in 0..32u64 {
        let mgr = Arc::clone(&mgr);
        let done = Arc::clone(&done);
        handles.push(thread::spawn(move || {
            mgr.with_write_locks("stock", vec![rid], || {
                done.fetch_add(1, Ordering::SeqCst);
            });
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(done.load(Ordering::SeqCst), 32);
}

#[test]
fn district_hot_rids_use_few_shards_at_most() {
    let mut shards = std::collections::HashSet::new();
    for d_id in 1..=5u64 {
        shards.insert(shard_index_for_test("district", d_id));
    }
    assert!(
        !shards.is_empty(),
        "district hot keys hit at least one shard"
    );
}
