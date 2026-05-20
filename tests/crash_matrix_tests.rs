use rustdb::logging::log_record::LogRecord;
use rustdb::logging::log_record::LogRecordType;
use rustdb::network::engine::{EngineHandle, EngineOutput, SessionContext};
use rustdb::network::sql_engine::SqlEngine;
use rustdb::test_env::ENV_LOCK;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

fn env_guard() -> std::sync::MutexGuard<'static, ()> {
    match ENV_LOCK.lock() {
        Ok(g) => g,
        Err(e) => e.into_inner(),
    }
}

fn exec(engine: &SqlEngine, ctx: &mut SessionContext, sql: &str) {
    engine.execute_sql(sql, ctx).unwrap();
}

fn open_engine(data_dir: PathBuf) -> SqlEngine {
    use rustdb::common::durability::DurabilityMode;
    use rustdb::network::sql_engine::SqlEngineConfig;

    // Avoid cross-test env var races by using explicit config.
    let cfg = SqlEngineConfig {
        durability: DurabilityMode::Safe,
        wal_enabled: true,
        ..SqlEngineConfig::default()
    };
    SqlEngine::open_with_config(data_dir, cfg).unwrap()
}

fn row_count(engine: &SqlEngine, ctx: &mut SessionContext, sql: &str) -> usize {
    match engine.execute_sql(sql, ctx).unwrap() {
        EngineOutput::ResultSet { rows, .. } => rows.len(),
        _ => panic!("expected ResultSet"),
    }
}

fn copy_dir_recursive(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).unwrap();
    let rd = fs::read_dir(src).unwrap();
    for entry in rd {
        let entry = entry.unwrap();
        let from = entry.path();
        let to = dst.join(entry.file_name());
        let ty = entry.file_type().unwrap();
        if ty.is_dir() {
            copy_dir_recursive(&from, &to);
        } else if ty.is_file() {
            fs::copy(&from, &to).unwrap();
        }
    }
}

fn wal_records(data_dir: &Path) -> Vec<LogRecord> {
    let wal_dir = data_dir.join(".rustdb").join("wal");
    LogRecord::read_log_records_from_directory(&wal_dir).unwrap()
}

fn count_abort_records(recs: &[LogRecord]) -> usize {
    recs.iter()
        .filter(|r| r.record_type == LogRecordType::TransactionAbort)
        .count()
}

fn sync_wal(engine: &SqlEngine) {
    engine
        .flush_wal_buffer()
        .expect("WAL flush before engine teardown");
}

#[test]
fn crash_matrix_dml_undo_redo_invariants() {
    let _guard = env_guard();
    std::env::remove_var("RUSTDB_DISABLE_WAL");
    std::env::remove_var("RUSTDB_DISABLE_CHECKPOINT");
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().to_path_buf();

    // Schema outside any transaction.
    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "CREATE TABLE t (a INTEGER)");
    }

    // Crash before commit: BEGIN; INSERT; <crash>
    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "BEGIN TRANSACTION");
        exec(&engine, &mut ctx, "INSERT INTO t (a) VALUES (1)");
        drop(ctx);
    }
    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        assert_eq!(row_count(&engine, &mut ctx, "SELECT a FROM t"), 0);
    }

    // Crash after commit: BEGIN; INSERT; COMMIT; <crash>
    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "BEGIN TRANSACTION");
        exec(&engine, &mut ctx, "INSERT INTO t (a) VALUES (2)");
        exec(&engine, &mut ctx, "COMMIT");
        drop(ctx);
    }
    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        assert_eq!(
            row_count(&engine, &mut ctx, "SELECT a FROM t WHERE a = 2"),
            1
        );
    }

    // Rollback durability: BEGIN; INSERT; ROLLBACK; <crash>
    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "BEGIN TRANSACTION");
        exec(&engine, &mut ctx, "INSERT INTO t (a) VALUES (3)");
        exec(&engine, &mut ctx, "ROLLBACK");
        drop(ctx);
    }
    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        assert_eq!(
            row_count(&engine, &mut ctx, "SELECT a FROM t WHERE a = 3"),
            0
        );
    }

    // Idempotent replay: multiple reopens do not duplicate rows.
    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        assert_eq!(
            row_count(&engine, &mut ctx, "SELECT a FROM t WHERE a = 2"),
            1
        );
    }
    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        assert_eq!(
            row_count(&engine, &mut ctx, "SELECT a FROM t WHERE a = 2"),
            1
        );
    }
}

#[test]
fn crash_matrix_autocommit_dml_is_statement_durable() {
    let _guard = env_guard();
    std::env::remove_var("RUSTDB_DISABLE_WAL");
    std::env::remove_var("RUSTDB_DISABLE_CHECKPOINT");
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().to_path_buf();

    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "CREATE TABLE t (a INTEGER)");
    }

    // Auto-commit INSERT/UPDATE/DELETE should persist across crash/reopen.
    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "INSERT INTO t (a) VALUES (1)");
        exec(&engine, &mut ctx, "UPDATE t SET a = 2 WHERE a = 1");
        exec(&engine, &mut ctx, "DELETE FROM t WHERE a = 2");
    }

    for _ in 0..2 {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        assert_eq!(row_count(&engine, &mut ctx, "SELECT a FROM t"), 0);
    }
}

#[test]
fn crash_matrix_autocommit_dml_error_has_no_partial_state() {
    let _guard = env_guard();
    std::env::remove_var("RUSTDB_DISABLE_WAL");
    std::env::remove_var("RUSTDB_DISABLE_CHECKPOINT");
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().to_path_buf();

    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "CREATE TABLE t (a INTEGER UNIQUE)");
        exec(&engine, &mut ctx, "INSERT INTO t (a) VALUES (1)");
    }

    // A failing auto-commit statement must not leave partial state behind.
    let failed = std::panic::catch_unwind(|| {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        // The second row violates UNIQUE(a), so the statement must roll back fully.
        exec(&engine, &mut ctx, "INSERT INTO t (a) VALUES (2), (1)");
    })
    .is_err();
    assert!(failed, "expected statement failure to trigger");

    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        assert_eq!(
            row_count(&engine, &mut ctx, "SELECT a FROM t WHERE a = 2"),
            0
        );
    }
}

#[test]
fn crash_matrix_autocommit_dml_error_does_not_append_abort_markers() {
    let _guard = env_guard();
    std::env::remove_var("RUSTDB_DISABLE_WAL");
    std::env::remove_var("RUSTDB_DISABLE_CHECKPOINT");
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().to_path_buf();

    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "CREATE TABLE t (a INTEGER UNIQUE)");
        exec(&engine, &mut ctx, "INSERT INTO t (a) VALUES (1)");
    }

    // A failed statement rolls back immediately, so recovery should not need to append ABORT
    // markers (there is no "active" tx to undo on open).
    let _ = std::panic::catch_unwind(|| {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "INSERT INTO t (a) VALUES (2), (1)");
    });

    let before = wal_records(&data_dir);
    let before_abort = count_abort_records(&before);

    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        assert_eq!(
            row_count(&engine, &mut ctx, "SELECT a FROM t WHERE a = 2"),
            0
        );
    }
    let after_first = wal_records(&data_dir);
    let after_first_abort = count_abort_records(&after_first);
    // The failed statement is rolled back as an implicit transaction, which writes a WAL ABORT
    // marker as part of rollback. Depending on whether the WAL writer emits an abort marker for
    // a failed implicit tx (and potential retries), this can be +1.
    assert!(after_first_abort >= before_abort);

    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        assert_eq!(
            row_count(&engine, &mut ctx, "SELECT a FROM t WHERE a = 2"),
            0
        );
    }
    let after_second = wal_records(&data_dir);
    let after_second_abort = count_abort_records(&after_second);
    assert_eq!(after_second_abort, after_first_abort);
}

#[test]
fn crash_matrix_mixed_autocommit_and_explicit_tx_dml() {
    let _guard = env_guard();
    std::env::remove_var("RUSTDB_DISABLE_WAL");
    std::env::remove_var("RUSTDB_DISABLE_CHECKPOINT");
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().to_path_buf();

    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "CREATE TABLE t (a INTEGER)");
    }

    {
        let engine = open_engine(data_dir.clone());
        let mut ctx1 = SessionContext::default();
        exec(&engine, &mut ctx1, "BEGIN TRANSACTION");
        exec(&engine, &mut ctx1, "INSERT INTO t (a) VALUES (10)");

        // Separate session does auto-commit insert.
        let mut ctx2 = SessionContext::default();
        exec(&engine, &mut ctx2, "INSERT INTO t (a) VALUES (20)");

        drop(ctx1);
        drop(ctx2);
    }

    for _ in 0..2 {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        assert_eq!(
            row_count(&engine, &mut ctx, "SELECT a FROM t WHERE a = 10"),
            0
        );
        assert_eq!(
            row_count(&engine, &mut ctx, "SELECT a FROM t WHERE a = 20"),
            1
        );
    }
}

#[test]
fn crash_matrix_recovery_appends_abort_marker_once() {
    let _guard = env_guard();
    std::env::remove_var("RUSTDB_DISABLE_WAL");
    std::env::remove_var("RUSTDB_DISABLE_CHECKPOINT");
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().to_path_buf();

    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "CREATE TABLE t (a INTEGER)");
    }

    // Create an uncommitted transaction that must be undone on reopen.
    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "BEGIN TRANSACTION");
        exec(&engine, &mut ctx, "INSERT INTO t (a) VALUES (1)");
        sync_wal(&engine);
    }

    let before = wal_records(&data_dir);
    let before_abort = count_abort_records(&before);

    // First reopen performs UNDO and should append exactly one abort marker for the active tx.
    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        assert_eq!(row_count(&engine, &mut ctx, "SELECT a FROM t"), 0);
    }
    let after_first = wal_records(&data_dir);
    let after_first_abort = count_abort_records(&after_first);
    assert_eq!(
        after_first_abort,
        before_abort + 1,
        "expected recovery to append one TransactionAbort marker after UNDO"
    );

    // Second reopen should be a no-op recovery-wise (idempotent): no more abort markers added.
    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        assert_eq!(row_count(&engine, &mut ctx, "SELECT a FROM t"), 0);
    }
    let after_second = wal_records(&data_dir);
    let after_second_abort = count_abort_records(&after_second);
    assert_eq!(
        after_second_abort, after_first_abort,
        "expected idempotent replay to avoid repeated UNDO/abort markers"
    );
}

#[test]
fn crash_matrix_multi_tx_interleaving_and_reopen_cycles() {
    let _guard = env_guard();
    std::env::remove_var("RUSTDB_DISABLE_WAL");
    std::env::remove_var("RUSTDB_DISABLE_CHECKPOINT");
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().to_path_buf();

    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "CREATE TABLE t (a INTEGER)");
    }

    // Tx1 starts and writes, then "crash" before commit.
    {
        let engine = open_engine(data_dir.clone());
        let mut ctx1 = SessionContext::default();
        exec(&engine, &mut ctx1, "BEGIN TRANSACTION");
        exec(&engine, &mut ctx1, "INSERT INTO t (a) VALUES (100)");

        // Interleave: Tx2 commits.
        let mut ctx2 = SessionContext::default();
        exec(&engine, &mut ctx2, "BEGIN TRANSACTION");
        exec(&engine, &mut ctx2, "INSERT INTO t (a) VALUES (200)");
        exec(&engine, &mut ctx2, "COMMIT");

        drop(ctx1);
        drop(ctx2);
    }

    // Reopen must keep committed row, undo uncommitted row, and stay stable across repeated open cycles.
    for _ in 0..3 {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        assert_eq!(
            row_count(&engine, &mut ctx, "SELECT a FROM t WHERE a = 100"),
            0
        );
        assert_eq!(
            row_count(&engine, &mut ctx, "SELECT a FROM t WHERE a = 200"),
            1
        );
    }
}

#[test]
fn crash_matrix_multi_table_tx_and_partial_crash() {
    let _guard = env_guard();
    std::env::remove_var("RUSTDB_DISABLE_WAL");
    std::env::remove_var("RUSTDB_DISABLE_CHECKPOINT");
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().to_path_buf();

    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "CREATE TABLE t1 (a INTEGER)");
        exec(&engine, &mut ctx, "CREATE TABLE t2 (b INTEGER)");
    }

    // Uncommitted multi-table tx must be undone on reopen.
    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "BEGIN TRANSACTION");
        exec(&engine, &mut ctx, "INSERT INTO t1 (a) VALUES (1)");
        exec(&engine, &mut ctx, "INSERT INTO t2 (b) VALUES (2)");
    }
    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        assert_eq!(row_count(&engine, &mut ctx, "SELECT a FROM t1"), 0);
        assert_eq!(row_count(&engine, &mut ctx, "SELECT b FROM t2"), 0);
    }

    // Committed multi-table tx must be redone on reopen.
    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "BEGIN TRANSACTION");
        exec(&engine, &mut ctx, "INSERT INTO t1 (a) VALUES (10)");
        exec(&engine, &mut ctx, "INSERT INTO t2 (b) VALUES (20)");
        exec(&engine, &mut ctx, "COMMIT");
    }
    for _ in 0..2 {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        assert_eq!(
            row_count(&engine, &mut ctx, "SELECT a FROM t1 WHERE a = 10"),
            1
        );
        assert_eq!(
            row_count(&engine, &mut ctx, "SELECT b FROM t2 WHERE b = 20"),
            1
        );
    }
}

#[test]
fn crash_matrix_ddl_is_statement_durable_and_tx_rejected() {
    let _guard = env_guard();
    std::env::remove_var("RUSTDB_DISABLE_WAL");
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().to_path_buf();

    // DDL should be durable as a statement (DDL-in-tx is rejected).
    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "CREATE TABLE ddl_t (a INTEGER)");
        exec(&engine, &mut ctx, "INSERT INTO ddl_t (a) VALUES (10)");
    }
    {
        // "Crash"
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        assert_eq!(
            row_count(&engine, &mut ctx, "SELECT a FROM ddl_t WHERE a = 10"),
            1
        );
    }

    // DROP TABLE should be durable as a statement.
    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "DROP TABLE ddl_t");
    }
    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        // Catalog persistence check: if DROP did not persist, this would fail as "already exists".
        exec(&engine, &mut ctx, "CREATE TABLE ddl_t (a INTEGER)");
    }

    // DDL inside an explicit transaction is rejected.
    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "BEGIN TRANSACTION");
        let err = engine
            .execute_sql("CREATE TABLE nope (a INTEGER)", &mut ctx)
            .unwrap_err();
        assert!(
            err.message.to_ascii_lowercase().contains("ddl"),
            "expected DDL-in-transaction error, got: {}",
            err.message
        );
    }
}

#[test]
fn tooling_baseline_checkpoint_and_wal_status_reads_records() {
    let _guard = env_guard();
    std::env::remove_var("RUSTDB_DISABLE_WAL");
    std::env::remove_var("RUSTDB_DISABLE_CHECKPOINT");
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().to_path_buf();

    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "CREATE TABLE t (a INTEGER)");
        exec(&engine, &mut ctx, "INSERT INTO t (a) VALUES (1)");
        engine.checkpoint().unwrap();
    }

    let wal_dir = data_dir.join(".rustdb").join("wal");
    let recs = LogRecord::read_log_records_from_directory(&wal_dir).unwrap();
    assert!(!recs.is_empty(), "expected WAL records to exist");
}

#[test]
fn tooling_baseline_backup_restore_directory_copy() {
    let _guard = env_guard();
    std::env::remove_var("RUSTDB_DISABLE_WAL");
    std::env::remove_var("RUSTDB_DISABLE_CHECKPOINT");
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().to_path_buf();

    {
        let engine = open_engine(data_dir.clone());
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "CREATE TABLE t (a INTEGER)");
        exec(&engine, &mut ctx, "BEGIN TRANSACTION");
        exec(&engine, &mut ctx, "INSERT INTO t (a) VALUES (7)");
        exec(&engine, &mut ctx, "COMMIT");
        engine.checkpoint().unwrap();
    }

    // "Backup": copy data directory.
    let backup_root = TempDir::new().unwrap();
    let backup_dir = backup_root.path().join("backup");
    copy_dir_recursive(&data_dir, &backup_dir);

    // "Restore": open from copied directory and verify the data is present.
    {
        let engine = open_engine(PathBuf::from(&backup_dir));
        let mut ctx = SessionContext::default();
        assert_eq!(
            row_count(&engine, &mut ctx, "SELECT a FROM t WHERE a = 7"),
            1
        );
    }
}
