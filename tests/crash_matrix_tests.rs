use rustdb::logging::log_record::LogRecord;
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
