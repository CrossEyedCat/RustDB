//! Structured WAL integration for [`super::SqlEngine`] (`src/logging` log records + recovery on open).

use crate::common::Error as DbError;
use crate::common::Result as DbResult;
use crate::logging::checkpoint::{CheckpointConfig, CheckpointManager, DirtyPageFlusher};
use crate::logging::log_record::{
    IsolationLevel as LogIsolationLevel, LogOperationData, LogRecord, LogRecordType, TransactionId,
};
use crate::logging::log_writer::{LogWriter, LogWriterConfig};
use crate::logging::recovery::{RecoveryConfig, RecoveryManager};
use crate::network::engine::{engine_error_code, EngineError, SqlIsolationLevel, SqlTransaction};
use crate::storage::page_manager::PageManager;
use rayon::prelude::*;
use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

pub struct SqlEngineWal {
    runtime: Runtime,
    writer: Arc<LogWriter>,
    next_tx_id: AtomicU64,
    checkpoint: Mutex<Option<CheckpointManager>>,
}

impl SqlEngineWal {
    /// Opens WAL under `wal_dir` (e.g. `data_dir/.rustdb/wal`), after recovery has run.
    pub fn open(wal_dir: &Path, synchronous_commit: bool) -> DbResult<Self> {
        let mut cfg = LogWriterConfig::default();
        cfg.log_directory = wal_dir.to_path_buf();
        cfg.synchronous_commit = synchronous_commit;
        // Performance knobs (mainly for Safe mode). Defaults are tuned for throughput while
        // preserving durability when `synchronous_commit=true`.
        cfg.group_commit_enabled = std::env::var("RUSTDB_GROUP_COMMIT_ENABLED")
            .ok()
            .as_deref()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(true);
        if let Ok(v) = std::env::var("RUSTDB_GROUP_COMMIT_INTERVAL_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                cfg.group_commit_interval_ms = ms.max(1);
            }
        }
        if let Ok(v) = std::env::var("RUSTDB_GROUP_COMMIT_MAX_BATCH") {
            if let Ok(n) = v.parse::<usize>() {
                cfg.group_commit_max_batch = n.max(1);
            }
        }
        cfg.force_flush_immediately = std::env::var("RUSTDB_FORCE_FLUSH_IMMEDIATELY")
            .ok()
            .as_deref()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let runtime = Runtime::new().map_err(|e| DbError::database(e.to_string()))?;
        let writer = {
            let _guard = runtime.enter();
            Arc::new(LogWriter::new(cfg)?)
        };
        let max_tid =
            crate::logging::log_record::LogRecord::read_log_records_from_directory(wal_dir)
                .map(|v| v.iter().filter_map(|r| r.transaction_id).max().unwrap_or(0))
                .unwrap_or(0);
        let next = max_tid.saturating_add(1).max(1);
        Ok(Self {
            runtime,
            writer,
            next_tx_id: AtomicU64::new(next),
            checkpoint: Mutex::new(None),
        })
    }

    /// Wires [`CheckpointManager`] to the same [`LogWriter`] and flushes all table heaps on checkpoint.
    pub fn setup_checkpoint(
        &self,
        state: Arc<crate::network::sql_engine::SqlEngineState>,
    ) -> DbResult<()> {
        if std::env::var_os("RUSTDB_DISABLE_CHECKPOINT").is_some() {
            return Ok(());
        }
        let _guard = self.runtime.enter();
        let mut cfg = CheckpointConfig::default();
        cfg.quiet = true;
        cfg.enable_auto_checkpoint =
            matches!(std::env::var("RUSTDB_AUTO_CHECKPOINT").as_deref(), Ok("1"));
        if let Ok(s) = std::env::var("RUSTDB_CHECKPOINT_INTERVAL_SECS") {
            if let Ok(secs) = s.parse::<u64>() {
                cfg.checkpoint_interval = Duration::from_secs(secs.max(1));
            }
        }
        let mut cm = CheckpointManager::new(cfg, self.writer.clone());
        let st = state.clone();
        let flusher: DirtyPageFlusher = Arc::new(move || flush_all_page_managers(&st));
        cm.set_dirty_page_flusher(flusher);
        *self.checkpoint.lock().unwrap() = Some(cm);
        Ok(())
    }

    pub fn checkpoint(&self) -> DbResult<()> {
        let guard = self.checkpoint.lock().unwrap();
        let Some(mgr) = guard.as_ref() else {
            return Err(DbError::database(
                "checkpoints disabled (WAL off or RUSTDB_DISABLE_CHECKPOINT)",
            ));
        };
        self.runtime
            .block_on(async { mgr.create_checkpoint().await })
            .map_err(|e| DbError::database(e.to_string()))?;
        Ok(())
    }

    /// Snapshot of [`crate::logging::checkpoint::CheckpointStatistics`] when the manager is wired.
    pub fn checkpoint_statistics(
        &self,
    ) -> Option<crate::logging::checkpoint::CheckpointStatistics> {
        let guard = self.checkpoint.lock().ok()?;
        let mgr = guard.as_ref()?;
        Some(self.runtime.block_on(async { mgr.get_statistics().await }))
    }

    pub fn log_begin(
        &self,
        tx: &mut SqlTransaction,
        iso: SqlIsolationLevel,
    ) -> std::result::Result<(), EngineError> {
        let tid: TransactionId = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        tx.wal_tx_id = Some(tid);
        let log_iso = map_sql_isolation_to_log(iso);
        let record = LogRecord::new_transaction_begin(0, tid, log_iso);
        let lsn = self
            .runtime
            .block_on(self.writer.write_log_durable(record))
            .map_err(|e| EngineError::new(engine_error_code::INTERNAL, e.to_string()))?;
        tx.wal_begin_lsn = Some(lsn);
        tx.wal_last_lsn = Some(lsn);
        Ok(())
    }

    pub fn log_commit(&self, tx: &mut SqlTransaction) -> std::result::Result<(), EngineError> {
        let tid = tx.wal_tx_id.ok_or_else(|| {
            EngineError::new(
                engine_error_code::INTERNAL,
                "WAL transaction id missing at COMMIT",
            )
        })?;
        let prev = tx.wal_last_lsn.or(tx.wal_begin_lsn);
        let record = LogRecord::new_transaction_commit(0, tid, vec![], prev);
        let lsn = self
            .runtime
            .block_on(self.writer.write_log_durable(record))
            .map_err(|e| EngineError::new(engine_error_code::INTERNAL, e.to_string()))?;
        tx.wal_last_lsn = Some(lsn);
        Ok(())
    }

    pub fn log_abort(&self, tx: &mut SqlTransaction) -> std::result::Result<(), EngineError> {
        let tid = tx.wal_tx_id.ok_or_else(|| {
            EngineError::new(
                engine_error_code::INTERNAL,
                "WAL transaction id missing at ROLLBACK",
            )
        })?;
        let prev = tx.wal_last_lsn.or(tx.wal_begin_lsn);
        let record = LogRecord::new_transaction_abort(0, tid, prev);
        let lsn = self
            .runtime
            .block_on(self.writer.write_log_durable(record))
            .map_err(|e| EngineError::new(engine_error_code::INTERNAL, e.to_string()))?;
        tx.wal_last_lsn = Some(lsn);
        Ok(())
    }

    /// Recovery helper: mark a transaction as aborted after UNDO so future reopens don't
    /// repeatedly undo the same active transaction (idempotent recovery).
    pub fn log_abort_by_id(
        &self,
        tid: TransactionId,
        prev_lsn: Option<u64>,
    ) -> std::result::Result<(), EngineError> {
        let record = LogRecord::new_transaction_abort(0, tid, prev_lsn);
        self.runtime
            .block_on(self.writer.write_log_durable(record))
            .map_err(|e| EngineError::new(engine_error_code::INTERNAL, e.to_string()))?;
        Ok(())
    }

    pub fn log_data_insert(
        &self,
        tx: &mut SqlTransaction,
        file_id: u32,
        page_id: u64,
        record_offset: u16,
        new_data: &[u8],
    ) -> std::result::Result<(), EngineError> {
        let tid = tx.wal_tx_id.ok_or_else(|| {
            EngineError::new(
                engine_error_code::INTERNAL,
                "WAL transaction id missing at INSERT",
            )
        })?;
        let prev = tx.wal_last_lsn.or(tx.wal_begin_lsn);
        let record = LogRecord::new_data_insert(
            0,
            tid,
            file_id,
            page_id,
            record_offset,
            new_data.to_vec(),
            prev,
        );
        let lsn = self
            .runtime
            // Flush to the log file so crash recovery can reliably UNDO uncommitted writes,
            // without necessarily waiting for fsync on every statement.
            .block_on(self.writer.write_log_durable(record))
            .map_err(|e| EngineError::new(engine_error_code::INTERNAL, e.to_string()))?;
        tx.wal_last_lsn = Some(lsn);
        Ok(())
    }

    pub fn log_data_update(
        &self,
        tx: &mut SqlTransaction,
        file_id: u32,
        page_id: u64,
        record_offset: u16,
        old_data: Vec<u8>,
        new_data: Vec<u8>,
    ) -> std::result::Result<(), EngineError> {
        let tid = tx.wal_tx_id.ok_or_else(|| {
            EngineError::new(
                engine_error_code::INTERNAL,
                "WAL transaction id missing at UPDATE",
            )
        })?;
        let prev = tx.wal_last_lsn.or(tx.wal_begin_lsn);
        let record = LogRecord::new_data_update(
            0,
            tid,
            file_id,
            page_id,
            record_offset,
            old_data,
            new_data,
            prev,
        );
        let lsn = self
            .runtime
            .block_on(self.writer.write_log_durable(record))
            .map_err(|e| EngineError::new(engine_error_code::INTERNAL, e.to_string()))?;
        tx.wal_last_lsn = Some(lsn);
        Ok(())
    }

    pub fn log_data_delete(
        &self,
        tx: &mut SqlTransaction,
        file_id: u32,
        page_id: u64,
        record_offset: u16,
        old_data: Vec<u8>,
    ) -> std::result::Result<(), EngineError> {
        let tid = tx.wal_tx_id.ok_or_else(|| {
            EngineError::new(
                engine_error_code::INTERNAL,
                "WAL transaction id missing at DELETE",
            )
        })?;
        let prev = tx.wal_last_lsn.or(tx.wal_begin_lsn);
        let record =
            LogRecord::new_data_delete(0, tid, file_id, page_id, record_offset, old_data, prev);
        let lsn = self
            .runtime
            .block_on(self.writer.write_log_durable(record))
            .map_err(|e| EngineError::new(engine_error_code::INTERNAL, e.to_string()))?;
        tx.wal_last_lsn = Some(lsn);
        Ok(())
    }

    /// After [`crate::catalog::schema::SchemaManager::save_catalog_to_data_dir`], append a durable marker so the WAL
    /// sequence reflects catalog persistence (replay ignores this record; it is not tied to a user transaction).
    pub fn log_catalog_snapshot(&self) -> std::result::Result<(), EngineError> {
        let record = LogRecord::new_catalog_snapshot(0);
        self.runtime
            .block_on(self.writer.write_log_durable(record))
            .map_err(|e| EngineError::new(engine_error_code::INTERNAL, e.to_string()))?;
        Ok(())
    }
}

pub fn recover_sql_engine_wal(log_dir: &Path) -> DbResult<()> {
    if !log_dir.exists() {
        return Ok(());
    }
    let rt = Runtime::new().map_err(|e| DbError::database(e.to_string()))?;
    let mut rm = RecoveryManager::new(RecoveryConfig {
        quiet: true,
        enable_validation: false,
        ..Default::default()
    });
    rt.block_on(async move { rm.recover_database(log_dir).await })?;
    Ok(())
}

/// When set (non-`0`), `COMMIT` heap flush writes pages but skips per-table `fsync`.
///
/// Intended only for CI throughput benchmarks (`scripts/tpcc_throughput_ci.sh`); WAL and
/// `commits.log` still provide the durability path used by that job.
pub(crate) fn bench_defer_heap_fsync_enabled() -> bool {
    std::env::var_os("RUSTDB_BENCH_DEFER_HEAP_FSYNC").is_some_and(|v| v != "0")
}

/// Whether explicit `COMMIT` / no-op `ROLLBACK` should synchronously flush dirty heap pages.
///
/// When **`RUSTDB_DEFER_HEAP_FLUSH_ON_COMMIT=1`** (bench/CI only; see `scripts/tpcc_throughput_ci.sh`),
/// returns `false` so callers skip `flush_page_managers_*` while WAL is enabled. Checkpoint and
/// `ROLLBACK` with undo still flush regardless of this flag.
pub(crate) fn heap_flush_on_commit_enabled() -> bool {
    std::env::var_os("RUSTDB_DEFER_HEAP_FLUSH_ON_COMMIT").is_none_or(|v| v == "0")
}

/// Microsecond breakdown for `COMMIT` heap flush (logged on `rustdb::sql_phases`).
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct CommitFlushPhaseUs {
    pub table_map_lock_us: u64,
    pub pm_lock_wait_us: u64,
    pub heap_fsync_us: u64,
}

fn flush_pm_writes_only(pm: &Arc<Mutex<PageManager>>) -> DbResult<(usize, Option<u32>, u64)> {
    let lock_t0 = Instant::now();
    let mut guard = pm
        .lock()
        .map_err(|_| DbError::database("table page manager lock poisoned"))?;
    let pm_lock_wait_us = lock_t0.elapsed().as_micros() as u64;
    if guard.dirty_page_count() == 0 {
        return Ok((0, None, pm_lock_wait_us));
    }
    let file_id = guard.file_id();
    let n = guard
        .flush_dirty_pages_no_sync()
        .map_err(|e| DbError::database(e.to_string()))?;
    Ok((n, if n > 0 { Some(file_id) } else { None }, pm_lock_wait_us))
}

fn sync_heap_files_after_coalesced_flush(
    sync_targets: Vec<(u32, Arc<Mutex<PageManager>>)>,
) -> DbResult<u64> {
    if bench_defer_heap_fsync_enabled() {
        return Ok(0);
    }
    let fsync_t0 = Instant::now();
    let mut synced = HashSet::new();
    for (file_id, pm) in sync_targets {
        if !synced.insert(file_id) {
            continue;
        }
        let lock_t0 = Instant::now();
        pm.lock()
            .map_err(|_| DbError::database("table page manager lock poisoned"))?
            .sync_heap_file()
            .map_err(|e| DbError::database(e.to_string()))?;
        let _ = lock_t0;
    }
    Ok(fsync_t0.elapsed().as_micros() as u64)
}

fn coalesced_flush_page_managers(pms: Vec<Arc<Mutex<PageManager>>>) -> DbResult<(usize, u64, u64)> {
    if pms.is_empty() {
        return Ok((0, 0, 0));
    }
    let mut total = 0usize;
    let mut sync_targets = Vec::new();
    let mut pm_lock_wait_us = 0u64;

    if pms.len() <= 1 {
        for pm in pms {
            let (n, file_id, wait) = flush_pm_writes_only(&pm)?;
            pm_lock_wait_us += wait;
            total += n;
            if let Some(file_id) = file_id {
                sync_targets.push((file_id, pm));
            }
        }
    } else {
        let results: Vec<_> = pms
            .par_iter()
            .map(flush_pm_writes_only)
            .collect::<DbResult<Vec<_>>>()?;
        for (pm, (n, file_id, wait)) in pms.into_iter().zip(results) {
            pm_lock_wait_us += wait;
            total += n;
            if let Some(file_id) = file_id {
                sync_targets.push((file_id, pm));
            }
        }
    }

    let heap_fsync_us = sync_heap_files_after_coalesced_flush(sync_targets)?;
    Ok((total, pm_lock_wait_us, heap_fsync_us))
}

/// Flushes dirty pages for the given physical table heaps only.
///
/// When `tables` is empty, this is a no-op (e.g. `BEGIN` … `COMMIT` with no DML).
/// Skips page managers with no dirty pages and performs at most one `fsync` per heap file.
pub(crate) fn flush_page_managers_for_tables(
    state: &crate::network::sql_engine::SqlEngineState,
    tables: &HashSet<String>,
) -> DbResult<(usize, CommitFlushPhaseUs)> {
    if tables.is_empty() {
        return Ok((0, CommitFlushPhaseUs::default()));
    }
    let mut sorted: Vec<String> = tables.iter().cloned().collect();
    sorted.sort();
    let map_t0 = Instant::now();
    let map = state
        .table_page_managers
        .lock()
        .map_err(|_| DbError::database("table pm map lock poisoned"))?;
    let table_map_lock_us = map_t0.elapsed().as_micros() as u64;
    let mut pms: Vec<Arc<Mutex<PageManager>>> = Vec::with_capacity(sorted.len());
    let mut pm_lock_wait_us = 0u64;
    for name in &sorted {
        let Some(pm) = map.get(name) else {
            continue;
        };
        let lock_t0 = Instant::now();
        let dirty = pm.lock().map(|g| g.dirty_page_count() > 0).unwrap_or(false);
        pm_lock_wait_us += lock_t0.elapsed().as_micros() as u64;
        if dirty {
            pms.push(pm.clone());
        }
    }
    drop(map);
    let (flushed, flush_pm_wait, heap_fsync_us) = coalesced_flush_page_managers(pms)?;
    Ok((
        flushed,
        CommitFlushPhaseUs {
            table_map_lock_us,
            pm_lock_wait_us: pm_lock_wait_us.saturating_add(flush_pm_wait),
            heap_fsync_us,
        },
    ))
}

/// Flushes dirty pages for the given page managers without acquiring `table_page_managers`.
///
/// Skips managers with no dirty pages. `CommitFlushPhaseUs::table_map_lock_us` is always zero.
pub(crate) fn flush_page_managers_cached(
    pms: &[Arc<Mutex<PageManager>>],
) -> DbResult<(usize, CommitFlushPhaseUs)> {
    if pms.is_empty() {
        return Ok((0, CommitFlushPhaseUs::default()));
    }
    let mut dirty_pms: Vec<Arc<Mutex<PageManager>>> = Vec::new();
    let mut pm_lock_wait_us = 0u64;
    for pm in pms {
        let lock_t0 = Instant::now();
        let dirty = pm.lock().map(|g| g.dirty_page_count() > 0).unwrap_or(false);
        pm_lock_wait_us += lock_t0.elapsed().as_micros() as u64;
        if dirty {
            dirty_pms.push(pm.clone());
        }
    }
    dirty_pms.sort_by_key(|pm| pm.lock().map(|g| g.file_id()).unwrap_or(u32::MAX));
    let (flushed, flush_pm_wait, heap_fsync_us) = coalesced_flush_page_managers(dirty_pms)?;
    Ok((
        flushed,
        CommitFlushPhaseUs {
            table_map_lock_us: 0,
            pm_lock_wait_us: pm_lock_wait_us.saturating_add(flush_pm_wait),
            heap_fsync_us,
        },
    ))
}

pub(crate) fn flush_all_page_managers(
    state: &crate::network::sql_engine::SqlEngineState,
) -> DbResult<usize> {
    let mut pms: Vec<Arc<Mutex<PageManager>>> = Vec::new();
    pms.push(state.default_page_manager.clone());
    let map = state
        .table_page_managers
        .lock()
        .map_err(|_| DbError::database("table pm map lock poisoned"))?;
    for pm in map.values() {
        pms.push(pm.clone());
    }
    drop(map);
    let (n, _, _) = coalesced_flush_page_managers(pms)?;
    Ok(n)
}

pub fn replay_wal_into_engine(
    state: &crate::network::sql_engine::SqlEngineState,
    wal_dir: &Path,
    wal: Option<&SqlEngineWal>,
) -> DbResult<()> {
    use crate::logging::log_record::LogRecordType;
    use std::collections::HashMap;

    let (redo, undo_per_tx) = analyze_wal_for_replay(wal_dir)?;

    // Ensure table page managers exist for all catalog tables so WAL file_ids can match.
    let mut table_names = {
        let cat = state
            .catalog
            .lock()
            .map_err(|_| DbError::database("catalog lock poisoned"))?;
        cat.table_names()
    };
    // Ensure deterministic file_id allocation across opens (important for WAL replay), regardless of
    // underlying map iteration order.
    table_names.sort();
    for t in table_names {
        let _ = crate::network::sql_engine::table_page_manager(state, &t)
            .map_err(|e| DbError::database(e.message))?;
    }

    // Build a file_id -> page manager map so each record is applied exactly once.
    // Applying every record to every page manager relies on filtering inside PageManager and
    // becomes incorrect if multiple managers reference the same file_id (which can happen during
    // open + catalog/table PM wiring).
    let mut pm_by_file_id: HashMap<u32, Arc<Mutex<PageManager>>> = HashMap::new();
    {
        let default = state.default_page_manager.clone();
        let fid = default
            .lock()
            .map_err(|_| DbError::database("page manager lock poisoned"))?
            .file_id();
        pm_by_file_id.insert(fid, default);
    }
    {
        let map = state
            .table_page_managers
            .lock()
            .map_err(|_| DbError::database("table pm map lock poisoned"))?;
        for (_name, pm) in map.iter() {
            let fid = pm
                .lock()
                .map_err(|_| DbError::database("table page manager lock poisoned"))?
                .file_id();
            pm_by_file_id.entry(fid).or_insert_with(|| pm.clone());
        }
    }

    let record_file_id = |r: &LogRecord| -> Option<u32> {
        match &r.operation_data {
            LogOperationData::Record(op) => Some(op.file_id),
            LogOperationData::File(op) => Some(op.file_id),
            _ => None,
        }
    };

    // Apply REDO.
    {
        for r in &redo {
            if let Some(fid) = record_file_id(r) {
                if let Some(pm) = pm_by_file_id.get(&fid) {
                    let mut g = pm
                        .lock()
                        .map_err(|_| DbError::database("page manager lock poisoned"))?;
                    let _ = g.apply_log_record_recovery(r, true);
                }
            }
        }
    }

    // Apply UNDO for active txs (reverse order per tx).
    for tx_ops in undo_per_tx {
        let tx_id = tx_ops.first().and_then(|r| r.transaction_id);
        let last_lsn = tx_ops.first().map(|r| r.lsn);
        for r in &tx_ops {
            if !matches!(
                r.record_type,
                LogRecordType::DataInsert | LogRecordType::DataUpdate | LogRecordType::DataDelete
            ) {
                continue;
            }
            if let Some(fid) = record_file_id(r) {
                if let Some(pm) = pm_by_file_id.get(&fid) {
                    let mut g = pm
                        .lock()
                        .map_err(|_| DbError::database("page manager lock poisoned"))?;
                    let _ = g.apply_log_record_recovery(r, false);
                }
            }
        }

        // Critical for idempotence: persist the UNDO'ed heap state before marking the transaction
        // as aborted in the WAL. Otherwise, an ABORT marker could cause future reopens to skip UNDO
        // while uncommitted heap changes still linger on disk.
        flush_all_page_managers(state)?;

        // Make recovery idempotent: once we've undone an "active" transaction, append an ABORT
        // marker so subsequent opens don't keep applying UNDO (which could delete reused slots).
        if let (Some(tid), Some(wal)) = (tx_id, wal) {
            wal.log_abort_by_id(tid, last_lsn).map_err(|e| {
                DbError::database(format!("append recovery abort marker: {}", e.message))
            })?;
        }
    }

    Ok(())
}

/// Returns `(redo_records_in_lsn_order, undo_records_per_active_tx_in_reverse_lsn_order)`.
pub fn analyze_wal_for_replay(wal_dir: &Path) -> DbResult<(Vec<LogRecord>, Vec<Vec<LogRecord>>)> {
    let recs = LogRecord::read_log_records_from_directory(wal_dir)?;

    #[derive(Default)]
    struct TxBuf {
        committed: bool,
        aborted: bool,
        ops: Vec<LogRecord>,
    }
    use std::collections::HashMap;
    let mut txs: HashMap<TransactionId, TxBuf> = HashMap::new();

    for r in recs {
        let Some(tid) = r.transaction_id else {
            continue;
        };
        let entry = txs.entry(tid).or_default();
        match r.record_type {
            LogRecordType::TransactionCommit => entry.committed = true,
            LogRecordType::TransactionAbort => entry.aborted = true,
            _ => {}
        }
        if matches!(
            r.record_type,
            LogRecordType::DataInsert | LogRecordType::DataUpdate | LogRecordType::DataDelete
        ) {
            entry.ops.push(r);
        }
    }

    let mut redo: Vec<LogRecord> = Vec::new();
    let mut undo: Vec<Vec<LogRecord>> = Vec::new();

    for (_tid, buf) in txs {
        if buf.committed && !buf.aborted {
            redo.extend(buf.ops);
        } else if !buf.committed && !buf.aborted && !buf.ops.is_empty() {
            let mut ops = buf.ops;
            ops.sort_by_key(|r| r.lsn);
            ops.reverse();
            undo.push(ops);
        }
    }

    redo.sort_by_key(|r| r.lsn);
    Ok((redo, undo))
}

pub fn log_record_operation_parts(
    record: &LogRecord,
) -> Option<(LogRecordType, &crate::logging::log_record::RecordOperation)> {
    let op = match &record.operation_data {
        LogOperationData::Record(op) => op,
        _ => return None,
    };
    Some((record.record_type.clone(), op))
}

fn map_sql_isolation_to_log(iso: SqlIsolationLevel) -> LogIsolationLevel {
    match iso {
        SqlIsolationLevel::ReadCommitted => LogIsolationLevel::ReadCommitted,
        SqlIsolationLevel::RepeatableRead => LogIsolationLevel::RepeatableRead,
        SqlIsolationLevel::Serializable => LogIsolationLevel::Serializable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::engine::{EngineHandle, SessionContext};
    use crate::network::sql_engine::{table_page_manager, SqlEngine};
    use std::collections::HashSet;
    use tempfile::TempDir;

    #[test]
    fn bench_defer_heap_fsync_respects_env() {
        std::env::remove_var("RUSTDB_BENCH_DEFER_HEAP_FSYNC");
        assert!(!bench_defer_heap_fsync_enabled());
        std::env::set_var("RUSTDB_BENCH_DEFER_HEAP_FSYNC", "1");
        assert!(bench_defer_heap_fsync_enabled());
        std::env::set_var("RUSTDB_BENCH_DEFER_HEAP_FSYNC", "0");
        assert!(!bench_defer_heap_fsync_enabled());
        std::env::remove_var("RUSTDB_BENCH_DEFER_HEAP_FSYNC");
    }

    #[test]
    fn heap_flush_on_commit_respects_env() {
        std::env::remove_var("RUSTDB_DEFER_HEAP_FLUSH_ON_COMMIT");
        assert!(heap_flush_on_commit_enabled());
        std::env::set_var("RUSTDB_DEFER_HEAP_FLUSH_ON_COMMIT", "1");
        assert!(!heap_flush_on_commit_enabled());
        std::env::set_var("RUSTDB_DEFER_HEAP_FLUSH_ON_COMMIT", "0");
        assert!(heap_flush_on_commit_enabled());
        std::env::remove_var("RUSTDB_DEFER_HEAP_FLUSH_ON_COMMIT");
    }

    #[test]
    fn flush_page_managers_skips_clean_and_coalesces_sync() {
        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let state = eng.state_for_test();
        let mut ctx = SessionContext::default();
        eng.execute_sql("CREATE TABLE t_clean (k INT PRIMARY KEY)", &mut ctx)
            .unwrap();
        eng.execute_sql("CREATE TABLE t_dirty (k INT PRIMARY KEY)", &mut ctx)
            .unwrap();
        eng.execute_sql("INSERT INTO t_clean (k) VALUES (0)", &mut ctx)
            .unwrap();
        eng.execute_sql("BEGIN TRANSACTION", &mut ctx).unwrap();
        eng.execute_sql("INSERT INTO t_dirty (k) VALUES (1)", &mut ctx)
            .unwrap();

        let pm_clean = table_page_manager(state, "t_clean").unwrap();
        let pm_dirty = table_page_manager(state, "t_dirty").unwrap();
        assert_eq!(pm_clean.lock().unwrap().dirty_page_count(), 0);
        assert!(pm_dirty.lock().unwrap().dirty_page_count() > 0);

        let mut tables = HashSet::new();
        tables.insert("t_clean".to_string());
        tables.insert("t_dirty".to_string());
        let (flushed, _metrics) = flush_page_managers_for_tables(state, &tables).unwrap();
        assert!(flushed > 0);
        assert_eq!(pm_clean.lock().unwrap().dirty_page_count(), 0);
        assert_eq!(pm_dirty.lock().unwrap().dirty_page_count(), 0);

        eng.execute_sql("COMMIT", &mut ctx).unwrap();
    }

    #[test]
    fn flush_page_managers_empty_tables_is_noop() {
        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let (n, _) = flush_page_managers_for_tables(eng.state_for_test(), &HashSet::new()).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn flush_page_managers_cached_skips_clean_and_avoids_map_lock() {
        let dir = TempDir::new().unwrap();
        let eng = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let state = eng.state_for_test();
        let mut ctx = SessionContext::default();
        eng.execute_sql("CREATE TABLE t_clean (k INT PRIMARY KEY)", &mut ctx)
            .unwrap();
        eng.execute_sql("CREATE TABLE t_dirty (k INT PRIMARY KEY)", &mut ctx)
            .unwrap();
        eng.execute_sql("INSERT INTO t_clean (k) VALUES (0)", &mut ctx)
            .unwrap();
        eng.execute_sql("BEGIN TRANSACTION", &mut ctx).unwrap();
        eng.execute_sql("INSERT INTO t_dirty (k) VALUES (1)", &mut ctx)
            .unwrap();

        let pm_clean = table_page_manager(state, "t_clean").unwrap();
        let pm_dirty = table_page_manager(state, "t_dirty").unwrap();
        assert_eq!(pm_clean.lock().unwrap().dirty_page_count(), 0);
        assert!(pm_dirty.lock().unwrap().dirty_page_count() > 0);

        let pms = vec![pm_clean.clone(), pm_dirty.clone()];
        let (flushed, phases) = flush_page_managers_cached(&pms).unwrap();
        assert!(flushed > 0);
        assert_eq!(phases.table_map_lock_us, 0);
        assert_eq!(pm_clean.lock().unwrap().dirty_page_count(), 0);
        assert_eq!(pm_dirty.lock().unwrap().dirty_page_count(), 0);

        eng.execute_sql("COMMIT", &mut ctx).unwrap();
    }
}
