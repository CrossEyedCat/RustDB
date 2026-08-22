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
    // Ensure durable commit markers when tests explicitly COMMIT.
    std::env::set_var("RUSTDB_FSYNC_COMMIT", "1");
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
    std::env::set_var("RUSTDB_FSYNC_COMMIT", "1");
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
    std::env::set_var("RUSTDB_FSYNC_COMMIT", "1");
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
    std::env::set_var("RUSTDB_FSYNC_COMMIT", "1");
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
    std::env::set_var("RUSTDB_FSYNC_COMMIT", "1");
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

/// Known defect (reproduction kept as documentation, not yet fixed).
///
/// With the bench durability preset the heap file stays many pages shorter than the WAL, and WAL
/// REDO is physical: it addresses `(file_id, page_id, offset)`. Records pointing at a page the
/// file never materialised fail in `PageManager::recovery_apply_record_operation` with
/// "Block N does not exist" (`FileManager::read_block` rejects `block_id >= header.total_blocks`,
/// while `write_block` would have extended the file), so recovery truncates the table at its
/// first page: 500 committed TPC-C new_orders come back as 171 `oorder` rows, 400 committed rows
/// here come back as ~171, independently of how many were committed.
///
/// `replay_wal_into_engine` now counts and logs those drops (`redo_lost`), so the loss is at least
/// visible; set `RUSTDB_STRICT_RECOVERY=1` to make it fail the open.
///
/// Fixing it is not just "materialise the missing page". A prototype that did so recovered all
/// 400 rows, but the pages it created are not registered in the file's free-page map, so the very
/// next `AdvancedFileManager::allocate_pages` hands those ids straight back out and post-recovery
/// inserts overwrite recovered rows: inserting 50 more rows after recovery moved the table from
/// 394 rows to 400 instead of 450. A real fix has to reserve the recovered pages in the free-page
/// map (and advance `header.total_pages`) as part of replay.
#[test]
#[ignore = "known defect: WAL replay truncates a table at its first page when heap flush is deferred"]
fn wal_replay_restores_every_committed_row() {
    let (before, after) = crash_with_deferred_heap_flush(400);
    assert_eq!(before, 400, "rows missing before the crash");
    assert_eq!(after, before, "WAL replay lost committed rows");
}

/// Commits `rows` wide rows with the bench durability preset (nothing reaches the heap file), then
/// drops the engine with every heap page still dirty — the crash — and reopens.
/// Returns `(rows visible before the crash, rows visible after recovery)`.
fn crash_with_deferred_heap_flush(rows: usize) -> (usize, usize) {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::remove_var("RUSTDB_DISABLE_WAL");
    std::env::set_var("RUSTDB_FSYNC_COMMIT", "1");
    std::env::set_var("RUSTDB_DEFER_HEAP_FLUSH_ON_COMMIT", "1");
    std::env::set_var("RUSTDB_DEFER_HEAP_FLUSH_AFTER_DML", "1");
    std::env::set_var("RUSTDB_BENCH_DEFER_HEAP_FSYNC", "1");

    let dir = TempDir::new().unwrap();
    // Wide rows so a few hundred inserts run well past the first page.
    let pad = "x".repeat(180);
    let before = {
        let engine = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        exec(
            &engine,
            &mut ctx,
            "CREATE TABLE t (a INTEGER, pad VARCHAR(200))",
        );
        for i in 0..rows {
            exec(
                &engine,
                &mut ctx,
                &format!("INSERT INTO t (a, pad) VALUES ({i}, '{pad}')"),
            );
        }
        row_count(&engine, &mut ctx, "SELECT * FROM t")
    };
    let after = {
        let engine = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        row_count(&engine, &mut ctx, "SELECT * FROM t")
    };

    for var in [
        "RUSTDB_DEFER_HEAP_FLUSH_ON_COMMIT",
        "RUSTDB_DEFER_HEAP_FLUSH_AFTER_DML",
        "RUSTDB_BENCH_DEFER_HEAP_FSYNC",
    ] {
        std::env::remove_var(var);
    }
    (before, after)
}

/// `redo_unmapped` must not read as data loss. `file_id` is derived from the heap filename and the
/// WAL directory is replayed whole with no checkpoint trim, so records belonging to a table that
/// was later dropped or renamed stay unmapped forever — the table is intentionally gone. Counting
/// that as a failed apply made every open after an ordinary `DROP TABLE` log an ERROR-level
/// "committed data is missing", and under `RUSTDB_STRICT_RECOVERY=1` it made the directory
/// permanently un-openable.
#[test]
fn strict_recovery_tolerates_dropped_and_renamed_tables() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::remove_var("RUSTDB_DISABLE_WAL");
    std::env::set_var("RUSTDB_FSYNC_COMMIT", "1");
    let dir = TempDir::new().unwrap();
    {
        let engine = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        exec(&engine, &mut ctx, "CREATE TABLE t (a INTEGER)");
        exec(&engine, &mut ctx, "INSERT INTO t (a) VALUES (1)");
        exec(&engine, &mut ctx, "INSERT INTO t (a) VALUES (2)");
        exec(&engine, &mut ctx, "DROP TABLE t");
        exec(&engine, &mut ctx, "CREATE TABLE u (b INTEGER)");
        exec(&engine, &mut ctx, "INSERT INTO u (b) VALUES (3)");
    }

    std::env::set_var("RUSTDB_STRICT_RECOVERY", "1");
    let opened = SqlEngine::open(dir.path().to_path_buf());
    std::env::remove_var("RUSTDB_STRICT_RECOVERY");

    let engine = opened.expect("strict recovery must not fail over a dropped table's WAL records");
    let mut ctx = SessionContext::default();
    assert_eq!(row_count(&engine, &mut ctx, "SELECT * FROM u"), 1);
}
