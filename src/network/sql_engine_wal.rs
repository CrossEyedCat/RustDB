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
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Runtime;

pub struct SqlEngineWal {
    runtime: Runtime,
    writer: Arc<LogWriter>,
    next_tx_id: AtomicU64,
    checkpoint: Mutex<Option<CheckpointManager>>,
}

impl SqlEngineWal {
    /// Opens WAL under `wal_dir` (e.g. `data_dir/.rustdb/wal`), after recovery has run.
    pub fn open(wal_dir: &Path) -> DbResult<Self> {
        let mut cfg = LogWriterConfig::default();
        cfg.log_directory = wal_dir.to_path_buf();
        // Deterministic visibility of records on disk for SqlEngine (avoid group-commit batching).
        cfg.group_commit_enabled = false;
        cfg.force_flush_immediately = true;
        if matches!(std::env::var("RUSTDB_FSYNC_COMMIT").as_deref(), Ok("1")) {
            cfg.synchronous_commit = true;
        }
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
            .block_on(self.writer.write_log_sync(record))
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
            .block_on(self.writer.write_log_sync(record))
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
            .block_on(self.writer.write_log_sync(record))
            .map_err(|e| EngineError::new(engine_error_code::INTERNAL, e.to_string()))?;
        tx.wal_last_lsn = Some(lsn);
        Ok(())
    }

    pub fn log_data_insert(
        &self,
        tx: &mut SqlTransaction,
        file_id: u32,
        page_id: u64,
        record_offset: u16,
        new_data: Vec<u8>,
    ) -> std::result::Result<(), EngineError> {
        let tid = tx.wal_tx_id.ok_or_else(|| {
            EngineError::new(
                engine_error_code::INTERNAL,
                "WAL transaction id missing at INSERT",
            )
        })?;
        let prev = tx.wal_last_lsn.or(tx.wal_begin_lsn);
        let record =
            LogRecord::new_data_insert(0, tid, file_id, page_id, record_offset, new_data, prev);
        let lsn = self
            .runtime
            // Use sync write so crash recovery can reliably UNDO uncommitted writes
            // even if the process stops between statements.
            .block_on(self.writer.write_log_sync(record))
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
            .block_on(self.writer.write_log_sync(record))
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
            .block_on(self.writer.write_log_sync(record))
            .map_err(|e| EngineError::new(engine_error_code::INTERNAL, e.to_string()))?;
        tx.wal_last_lsn = Some(lsn);
        Ok(())
    }

    /// After [`crate::catalog::schema::SchemaManager::save_catalog_to_data_dir`], append a durable marker so the WAL
    /// sequence reflects catalog persistence (replay ignores this record; it is not tied to a user transaction).
    pub fn log_catalog_snapshot(&self) -> std::result::Result<(), EngineError> {
        let record = LogRecord::new_catalog_snapshot(0);
        self.runtime
            .block_on(self.writer.write_log_sync(record))
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

pub(crate) fn flush_all_page_managers(
    state: &crate::network::sql_engine::SqlEngineState,
) -> DbResult<usize> {
    let mut n = 0usize;
    n += state
        .default_page_manager
        .lock()
        .map_err(|_| DbError::database("page manager lock poisoned"))?
        .flush_dirty_pages()
        .map_err(|e| DbError::database(e.to_string()))?;
    let map = state
        .table_page_managers
        .lock()
        .map_err(|_| DbError::database("table pm map lock poisoned"))?;
    for (_, pm) in map.iter() {
        n += pm
            .lock()
            .map_err(|_| DbError::database("table page manager lock poisoned"))?
            .flush_dirty_pages()
            .map_err(|e| DbError::database(e.to_string()))?;
    }
    Ok(n)
}

pub fn replay_wal_into_engine(
    state: &crate::network::sql_engine::SqlEngineState,
    wal_dir: &Path,
) -> DbResult<()> {
    use crate::logging::log_record::LogRecordType;

    let (redo, undo_per_tx) = analyze_wal_for_replay(wal_dir)?;

    // Ensure table page managers exist for all catalog tables so WAL file_ids can match.
    let table_names = {
        let cat = state
            .catalog
            .lock()
            .map_err(|_| DbError::database("catalog lock poisoned"))?;
        cat.table_names()
    };
    for t in table_names {
        let _ = crate::network::sql_engine::table_page_manager(state, &t)
            .map_err(|e| DbError::database(e.message))?;
    }

    // Apply REDO.
    {
        let mut pm = state
            .default_page_manager
            .lock()
            .map_err(|_| DbError::database("page manager lock poisoned"))?;
        for r in &redo {
            let _ = pm.apply_log_record_recovery(r, true);
        }
    }
    {
        let map = state
            .table_page_managers
            .lock()
            .map_err(|_| DbError::database("table pm map lock poisoned"))?;
        for (_name, pm) in map.iter() {
            let mut g = pm
                .lock()
                .map_err(|_| DbError::database("table page manager lock poisoned"))?;
            for r in &redo {
                let _ = g.apply_log_record_recovery(r, true);
            }
        }
    }

    // Apply UNDO for active txs (reverse order per tx).
    for tx_ops in undo_per_tx {
        {
            let mut pm = state
                .default_page_manager
                .lock()
                .map_err(|_| DbError::database("page manager lock poisoned"))?;
            for r in &tx_ops {
                if matches!(
                    r.record_type,
                    LogRecordType::DataInsert
                        | LogRecordType::DataUpdate
                        | LogRecordType::DataDelete
                ) {
                    let _ = pm.apply_log_record_recovery(r, false);
                }
            }
        }
        let map = state
            .table_page_managers
            .lock()
            .map_err(|_| DbError::database("table pm map lock poisoned"))?;
        for (_name, pm) in map.iter() {
            let mut g = pm
                .lock()
                .map_err(|_| DbError::database("table page manager lock poisoned"))?;
            for r in &tx_ops {
                if matches!(
                    r.record_type,
                    LogRecordType::DataInsert
                        | LogRecordType::DataUpdate
                        | LogRecordType::DataDelete
                ) {
                    let _ = g.apply_log_record_recovery(r, false);
                }
            }
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
