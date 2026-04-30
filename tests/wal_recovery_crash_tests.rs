use rustdb::network::engine::EngineHandle;
use rustdb::network::engine::SessionContext;
use rustdb::network::sql_engine::SqlEngine;
use rustdb::test_env::ENV_LOCK;
use tempfile::TempDir;

fn exec(engine: &SqlEngine, ctx: &mut SessionContext, sql: &str) {
    engine.execute_sql(sql, ctx).unwrap();
}

fn row_count(engine: &SqlEngine, ctx: &mut SessionContext, sql: &str) -> usize {
    match engine.execute_sql(sql, ctx).unwrap() {
        rustdb::network::engine::EngineOutput::ResultSet { rows, .. } => rows.len(),
        _ => panic!("expected ResultSet"),
    }
}

#[test]
fn wal_replay_undo_uncommitted_insert_on_reopen() {
    let _guard = ENV_LOCK.lock().unwrap();
    // Ensure WAL isn't disabled by some other test running in parallel.
    std::env::remove_var("RUSTDB_DISABLE_WAL");
    let dir = TempDir::new().unwrap();

    // Setup schema outside transaction.
    {
        let engine = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "CREATE TABLE t (a INTEGER)");
    }

    // Begin tx, insert, then drop the SessionContext without COMMIT/ROLLBACK.
    {
        let engine = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "BEGIN TRANSACTION");
        exec(&engine, &mut ctx, "INSERT INTO t (a) VALUES (1)");

        // Simulate a crash: the process disappears and the session transaction is lost.
        // We intentionally do NOT call COMMIT/ROLLBACK.
        drop(ctx);
    }

    // Reopen: WAL should UNDO the uncommitted insert.
    {
        let engine = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        let n = row_count(&engine, &mut ctx, "SELECT a FROM t");
        assert_eq!(n, 0);
    }
}

#[test]
fn wal_replay_redo_committed_insert_on_reopen() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::remove_var("RUSTDB_DISABLE_WAL");
    let dir = TempDir::new().unwrap();

    {
        let engine = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "CREATE TABLE t (a INTEGER)");
    }

    {
        let engine = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "BEGIN TRANSACTION");
        exec(&engine, &mut ctx, "INSERT INTO t (a) VALUES (1)");
        exec(&engine, &mut ctx, "COMMIT");
    }

    {
        let engine = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        let n = row_count(&engine, &mut ctx, "SELECT a FROM t");
        assert_eq!(n, 1);
    }
}

#[test]
fn wal_replay_keeps_rollback_invisible_on_reopen() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::remove_var("RUSTDB_DISABLE_WAL");
    let dir = TempDir::new().unwrap();

    {
        let engine = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "CREATE TABLE t (a INTEGER)");
    }

    {
        let engine = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "BEGIN TRANSACTION");
        exec(&engine, &mut ctx, "INSERT INTO t (a) VALUES (1)");
        exec(&engine, &mut ctx, "ROLLBACK");
    }

    {
        let engine = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        let n = row_count(&engine, &mut ctx, "SELECT a FROM t");
        assert_eq!(n, 0);
    }
}

#[test]
fn wal_replay_is_idempotent_on_multiple_reopens() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::remove_var("RUSTDB_DISABLE_WAL");
    let dir = TempDir::new().unwrap();

    {
        let engine = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "CREATE TABLE t (a INTEGER)");
        exec(&engine, &mut ctx, "BEGIN TRANSACTION");
        exec(&engine, &mut ctx, "INSERT INTO t (a) VALUES (1)");
        exec(&engine, &mut ctx, "COMMIT");
    }

    // Reopen #1
    {
        let engine = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        let n = row_count(&engine, &mut ctx, "SELECT a FROM t");
        assert_eq!(n, 1);
    }

    // Reopen #2 (should not duplicate the row)
    {
        let engine = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        let n = row_count(&engine, &mut ctx, "SELECT a FROM t");
        assert_eq!(n, 1);
    }
}

#[test]
fn wal_replay_undo_uncommitted_mixed_dml_on_reopen() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::remove_var("RUSTDB_DISABLE_WAL");
    let dir = TempDir::new().unwrap();

    {
        let engine = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "CREATE TABLE t (a INTEGER)");
        exec(&engine, &mut ctx, "INSERT INTO t (a) VALUES (1)");
        exec(&engine, &mut ctx, "INSERT INTO t (a) VALUES (2)");
    }

    // Start a tx, update + delete, then lose the session (no COMMIT/ROLLBACK).
    {
        let engine = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "BEGIN TRANSACTION");
        exec(&engine, &mut ctx, "UPDATE t SET a = 42 WHERE a = 1");
        exec(&engine, &mut ctx, "DELETE FROM t WHERE a = 2");
        drop(ctx);
    }

    // Reopen: should undo the uncommitted UPDATE/DELETE.
    {
        let engine = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        let n42 = row_count(&engine, &mut ctx, "SELECT a FROM t WHERE a = 42");
        assert_eq!(n42, 0);
        let n1 = row_count(&engine, &mut ctx, "SELECT a FROM t WHERE a = 1");
        assert_eq!(n1, 1);
        let n2 = row_count(&engine, &mut ctx, "SELECT a FROM t WHERE a = 2");
        assert_eq!(n2, 1);
    }
}
