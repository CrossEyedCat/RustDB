//! End-to-end SQL through [`crate::network::SqlEngine`]: WHERE, ORDER BY, LIMIT, OFFSET.
//! Written in a TDD style: behavior is asserted at the engine boundary.

use crate::network::engine::{EngineHandle, EngineOutput, SessionContext};
use crate::network::SqlEngine;
use tempfile::TempDir;

#[test]
fn engine_select_where_filters_by_column_equality() {
    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
    let mut ctx = SessionContext::default();
    eng.execute_sql("INSERT INTO r (a) VALUES (1)", &mut ctx)
        .expect("i1");
    eng.execute_sql("INSERT INTO r (a) VALUES (2)", &mut ctx)
        .expect("i2");
    eng.execute_sql("INSERT INTO r (a) VALUES (2)", &mut ctx)
        .expect("i3");

    let out = eng
        .execute_sql("SELECT a FROM r WHERE a = 2", &mut ctx)
        .expect("select");
    match out {
        EngineOutput::ResultSet { columns, rows } => {
            assert_eq!(columns, vec!["a"]);
            assert_eq!(rows.len(), 2);
            assert!(rows.iter().all(|r| r[0] == "Integer(2)"));
        }
        _ => panic!("expected ResultSet"),
    }
}

#[test]
fn engine_select_where_boolean_and_or_not() {
    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
    let mut ctx = SessionContext::default();
    eng.execute_sql(
        "INSERT INTO t (a, b) VALUES (1, 1), (2, 1), (2, 0)",
        &mut ctx,
    )
    .unwrap();

    let out = eng
        .execute_sql("SELECT a FROM t WHERE a = 2 AND NOT (b = 0)", &mut ctx)
        .expect("select");
    match out {
        EngineOutput::ResultSet { columns, rows } => {
            assert!(columns.contains(&"a".to_string()));
            let a_idx = columns.iter().position(|c| c == "a").expect("a column");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][a_idx], "Integer(2)");
        }
        _ => panic!("expected ResultSet"),
    }

    let out2 = eng
        .execute_sql("SELECT a FROM t WHERE a = 1 OR b = 0", &mut ctx)
        .expect("select2");
    match out2 {
        EngineOutput::ResultSet { rows, .. } => assert_eq!(rows.len(), 2),
        _ => panic!("expected ResultSet"),
    }
}

#[test]
fn engine_select_order_by_sorts_rows() {
    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
    let mut ctx = SessionContext::default();
    eng.execute_sql("INSERT INTO s (n) VALUES (30)", &mut ctx)
        .unwrap();
    eng.execute_sql("INSERT INTO s (n) VALUES (10)", &mut ctx)
        .unwrap();
    eng.execute_sql("INSERT INTO s (n) VALUES (20)", &mut ctx)
        .unwrap();

    let out = eng
        .execute_sql("SELECT n FROM s ORDER BY n ASC", &mut ctx)
        .expect("select");
    match out {
        EngineOutput::ResultSet { columns, rows } => {
            assert_eq!(columns, vec!["n"]);
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0][0], "Integer(10)");
            assert_eq!(rows[1][0], "Integer(20)");
            assert_eq!(rows[2][0], "Integer(30)");
        }
        _ => panic!("expected ResultSet"),
    }
}

#[test]
fn engine_select_order_by_sorts_strings() {
    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
    let mut ctx = SessionContext::default();
    eng.execute_sql("INSERT INTO s (v) VALUES ('c')", &mut ctx)
        .unwrap();
    eng.execute_sql("INSERT INTO s (v) VALUES ('a')", &mut ctx)
        .unwrap();
    eng.execute_sql("INSERT INTO s (v) VALUES ('b')", &mut ctx)
        .unwrap();

    let out = eng
        .execute_sql("SELECT v FROM s ORDER BY v ASC", &mut ctx)
        .expect("select");
    match out {
        EngineOutput::ResultSet { columns, rows } => {
            assert_eq!(columns, vec!["v"]);
            assert_eq!(rows.len(), 3);
            assert!(rows[0][0].contains("a"), "{:?}", rows[0][0]);
            assert!(rows[1][0].contains("b"), "{:?}", rows[1][0]);
            assert!(rows[2][0].contains("c"), "{:?}", rows[2][0]);
        }
        _ => panic!("expected ResultSet"),
    }
}

#[test]
fn engine_select_limit_and_limit_offset() {
    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
    let mut ctx = SessionContext::default();
    for v in [1, 2, 3, 4] {
        eng.execute_sql(&format!("INSERT INTO p (v) VALUES ({v})"), &mut ctx)
            .unwrap();
    }

    let lim = eng
        .execute_sql("SELECT v FROM p ORDER BY v ASC LIMIT 2", &mut ctx)
        .expect("limit");
    match lim {
        EngineOutput::ResultSet { rows, .. } => assert_eq!(rows.len(), 2),
        _ => panic!("expected ResultSet"),
    }

    let off = eng
        .execute_sql("SELECT v FROM p ORDER BY v ASC LIMIT 2 OFFSET 1", &mut ctx)
        .expect("offset");
    match off {
        EngineOutput::ResultSet { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][0], "Integer(2)");
            assert_eq!(rows[1][0], "Integer(3)");
        }
        _ => panic!("expected ResultSet"),
    }
}

#[test]
fn engine_projection_computes_expressions_and_aliases() {
    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
    let mut ctx = SessionContext::default();
    eng.execute_sql("INSERT INTO t (a) VALUES (2)", &mut ctx)
        .unwrap();

    let out = eng
        .execute_sql("SELECT a, a + 1 AS b FROM t", &mut ctx)
        .expect("select");
    match out {
        EngineOutput::ResultSet { columns, rows } => {
            assert_eq!(columns, vec!["a", "b"]);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], "Integer(2)");
            // arithmetic in executor uses Float; still acceptable if it prints as Double(3.0)
            assert!(
                rows[0][1] == "Double(3)"
                    || rows[0][1] == "Double(3.0)"
                    || rows[0][1] == "Float(3)"
                    || rows[0][1] == "Integer(3)"
                    || rows[0][1] == "BigInt(3)",
                "unexpected b cell: {:?}",
                rows[0][1]
            );
        }
        _ => panic!("expected ResultSet"),
    }
}

#[test]
fn engine_join_inner_on_equality_self_join() {
    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
    let mut ctx = SessionContext::default();
    eng.execute_sql("INSERT INTO t1 (a) VALUES (2), (2), (1)", &mut ctx)
        .unwrap();
    eng.execute_sql("INSERT INTO t2 (a) VALUES (2), (2), (1)", &mut ctx)
        .unwrap();

    let out = eng
        .execute_sql(
            "SELECT a FROM t1 INNER JOIN t2 ON t1.a = t2.a WHERE a = 2",
            &mut ctx,
        )
        .expect("join select");
    match out {
        EngineOutput::ResultSet { columns, rows } => {
            assert!(columns.contains(&"a".to_string()));
            assert_eq!(rows.len(), 4, "expected 2x2 matches for a=2 self join");
        }
        _ => panic!("expected ResultSet"),
    }
}

#[test]
fn engine_group_by_count_having() {
    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
    let mut ctx = SessionContext::default();
    eng.execute_sql("INSERT INTO t (a) VALUES (2), (2), (1)", &mut ctx)
        .unwrap();

    let out = eng
        .execute_sql(
            "SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1 ORDER BY a ASC",
            &mut ctx,
        )
        .expect("group by");
    match out {
        EngineOutput::ResultSet { columns, rows } => {
            assert!(columns.contains(&"a".to_string()));
            assert!(
                columns.iter().any(|c| c.to_uppercase().contains("COUNT")),
                "expected COUNT column, got {columns:?}"
            );
            assert_eq!(rows.len(), 1);
        }
        _ => panic!("expected ResultSet"),
    }
}

#[test]
fn engine_create_drop_table_roundtrip() {
    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
    let mut ctx = SessionContext::default();
    eng.execute_sql("CREATE TABLE users (a INT)", &mut ctx)
        .expect("create");
    eng.execute_sql("INSERT INTO users (a) VALUES (7)", &mut ctx)
        .expect("insert");
    let out = eng
        .execute_sql("SELECT a FROM users", &mut ctx)
        .expect("select");
    match out {
        EngineOutput::ResultSet { rows, .. } => assert_eq!(rows.len(), 1),
        _ => panic!("expected ResultSet"),
    }
    eng.execute_sql("DROP TABLE users", &mut ctx).expect("drop");
}

#[test]
fn engine_enforces_not_null_default_and_check() {
    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
    let mut ctx = SessionContext::default();

    // NOT NULL + DEFAULT + CHECK.
    eng.execute_sql(
        "CREATE TABLE c (a INT NOT NULL DEFAULT 7, b INT CHECK (b > 0))",
        &mut ctx,
    )
    .expect("create");

    // DEFAULT applied for missing a.
    eng.execute_sql("INSERT INTO c (b) VALUES (1)", &mut ctx)
        .expect("insert default");

    // NOT NULL violation: missing a and no default.
    eng.execute_sql("CREATE TABLE nn (a INT NOT NULL)", &mut ctx)
        .expect("create nn");
    assert!(eng
        .execute_sql("INSERT INTO nn VALUES (NULL)", &mut ctx)
        .is_err());

    // CHECK violation.
    assert!(eng
        .execute_sql("INSERT INTO c (a,b) VALUES (1,0)", &mut ctx)
        .is_err());
}

#[test]
fn engine_insert_select_roundtrip() {
    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
    let mut ctx = SessionContext::default();
    eng.execute_sql("INSERT INTO src (a) VALUES (1), (2), (2), (3)", &mut ctx)
        .unwrap();
    let out = eng
        .execute_sql(
            "INSERT INTO dst (a) SELECT a FROM src WHERE a = 2",
            &mut ctx,
        )
        .expect("insert select");
    assert_eq!(out, EngineOutput::ExecutionOk { rows_affected: 2 });

    let sel = eng
        .execute_sql("SELECT a FROM dst ORDER BY a ASC", &mut ctx)
        .expect("select dst");
    match sel {
        EngineOutput::ResultSet { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][0], "Integer(2)");
            assert_eq!(rows[1][0], "Integer(2)");
        }
        _ => panic!("expected ResultSet"),
    }
}

#[test]
fn engine_transactions_begin_commit_rollback_and_errors() {
    use crate::network::engine::engine_error_code;

    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
    let mut ctx = SessionContext::default();
    assert_eq!(
        eng.execute_sql("BEGIN TRANSACTION", &mut ctx).unwrap(),
        EngineOutput::ExecutionOk { rows_affected: 0 }
    );
    assert_eq!(
        eng.execute_sql("COMMIT", &mut ctx).unwrap(),
        EngineOutput::ExecutionOk { rows_affected: 0 }
    );
    assert_eq!(
        eng.execute_sql("COMMIT", &mut ctx).unwrap_err().code,
        engine_error_code::NO_ACTIVE_TRANSACTION
    );
    assert_eq!(
        eng.execute_sql("ROLLBACK", &mut ctx).unwrap_err().code,
        engine_error_code::NO_ACTIVE_TRANSACTION
    );
    eng.execute_sql("BEGIN TRANSACTION", &mut ctx).unwrap();
    assert_eq!(
        eng.execute_sql("BEGIN TRANSACTION", &mut ctx)
            .unwrap_err()
            .code,
        engine_error_code::ALREADY_IN_TRANSACTION
    );
    eng.execute_sql("ROLLBACK", &mut ctx).unwrap();
}

#[test]
fn engine_transaction_insert_rollback_removes_row() {
    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
    let mut ctx = SessionContext::default();
    eng.execute_sql("CREATE TABLE txr (k INT PRIMARY KEY)", &mut ctx)
        .unwrap();
    eng.execute_sql("BEGIN TRANSACTION", &mut ctx).unwrap();
    eng.execute_sql("INSERT INTO txr (k) VALUES (1)", &mut ctx)
        .unwrap();
    eng.execute_sql("ROLLBACK", &mut ctx).unwrap();
    let out = eng
        .execute_sql("SELECT k FROM txr", &mut ctx)
        .expect("select");
    match out {
        EngineOutput::ResultSet { rows, .. } => assert!(rows.is_empty()),
        _ => panic!("expected ResultSet"),
    }
}

#[test]
fn engine_transaction_insert_rollback_survives_engine_reopen() {
    let dir = TempDir::new().expect("tempdir");
    {
        let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
        let mut ctx = SessionContext::default();
        eng.execute_sql("CREATE TABLE txre (k INT PRIMARY KEY)", &mut ctx)
            .unwrap();
        eng.execute_sql("BEGIN TRANSACTION", &mut ctx).unwrap();
        eng.execute_sql("INSERT INTO txre (k) VALUES (42)", &mut ctx)
            .unwrap();
        eng.execute_sql("ROLLBACK", &mut ctx).unwrap();
    }
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("reopen");
    let mut ctx = SessionContext::default();
    let out = eng
        .execute_sql("SELECT k FROM txre WHERE k = 42", &mut ctx)
        .expect("select");
    match out {
        EngineOutput::ResultSet { rows, .. } => assert!(rows.is_empty()),
        _ => panic!("expected ResultSet"),
    }
}

#[test]
fn engine_reopen_constraint_rebuild_insert_values_without_column_list() {
    let dir = TempDir::new().expect("tempdir");
    {
        let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
        let mut ctx = SessionContext::default();
        eng.execute_sql("CREATE TABLE t_vals (id INT PRIMARY KEY)", &mut ctx)
            .expect("create");
        eng.execute_sql("INSERT INTO t_vals VALUES (42)", &mut ctx)
            .expect("implicit column names → col1 in tuple");
    }
    SqlEngine::open(dir.path().to_path_buf()).expect("reopen rebuilds PK maps from heap");
}

#[test]
fn engine_transaction_insert_commit_keeps_row() {
    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
    let mut ctx = SessionContext::default();
    eng.execute_sql("CREATE TABLE txc (k INT PRIMARY KEY)", &mut ctx)
        .unwrap();
    eng.execute_sql("BEGIN TRANSACTION", &mut ctx).unwrap();
    eng.execute_sql("INSERT INTO txc (k) VALUES (7)", &mut ctx)
        .unwrap();
    eng.execute_sql("COMMIT", &mut ctx).unwrap();
    let out = eng
        .execute_sql("SELECT k FROM txc", &mut ctx)
        .expect("select");
    match out {
        EngineOutput::ResultSet { rows, .. } => assert_eq!(rows.len(), 1),
        _ => panic!("expected ResultSet"),
    }
}

#[test]
fn engine_ddl_rejected_inside_transaction() {
    use crate::network::engine::engine_error_code;

    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
    let mut ctx = SessionContext::default();
    eng.execute_sql("BEGIN TRANSACTION", &mut ctx).unwrap();
    assert_eq!(
        eng.execute_sql("CREATE TABLE n (x INT)", &mut ctx)
            .unwrap_err()
            .code,
        engine_error_code::DDL_IN_TRANSACTION
    );
    eng.execute_sql("ROLLBACK", &mut ctx).unwrap();
}

/// Eight threads contend on `INSERT … VALUES (1)`; exactly one succeeds.
#[cfg(not(rustdb_loom))]
#[test]
fn engine_concurrent_inserts_only_one_wins_same_pk() {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use std::thread;

    let dir = TempDir::new().expect("tempdir");
    let eng = Arc::new(SqlEngine::open(dir.path().to_path_buf()).expect("open"));
    let mut setup = SessionContext::default();
    eng.execute_sql("CREATE TABLE conc (id INT PRIMARY KEY)", &mut setup)
        .unwrap();

    let barrier = Arc::new(std::sync::Barrier::new(8));
    let ok_count = Arc::new(AtomicU32::new(0));
    let mut handles = vec![];
    for _ in 0..8 {
        let eng = eng.clone();
        let barrier = barrier.clone();
        let ok_count = ok_count.clone();
        handles.push(thread::spawn(move || {
            let mut ctx = SessionContext::default();
            barrier.wait();
            if eng
                .execute_sql("INSERT INTO conc (id) VALUES (1)", &mut ctx)
                .is_ok()
            {
                ok_count.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }
    for h in handles {
        h.join().expect("join");
    }
    assert_eq!(ok_count.load(Ordering::SeqCst), 1);
}

/// Same property under [loom](https://github.com/tokio-rs/loom) permutation scheduling (`RUSTFLAGS="--cfg rustdb_loom"`).
#[cfg(rustdb_loom)]
#[test]
fn engine_concurrent_inserts_only_one_wins_same_pk() {
    use loom::sync::atomic::AtomicU32;
    use loom::sync::atomic::Ordering::SeqCst;
    use loom::sync::Arc;

    loom::model(|| {
        let dir = TempDir::new().expect("tempdir");
        let eng = Arc::new(SqlEngine::open(dir.path().to_path_buf()).expect("open"));
        let mut setup = SessionContext::default();
        eng.execute_sql("CREATE TABLE conc (id INT PRIMARY KEY)", &mut setup)
            .unwrap();

        let ok_count = Arc::new(AtomicU32::new(0));
        let ea = eng.clone();
        let eb = eng.clone();
        let oa = ok_count.clone();
        let ob = ok_count.clone();
        let h1 = loom::thread::spawn(move || {
            let mut ctx = SessionContext::default();
            if ea
                .execute_sql("INSERT INTO conc (id) VALUES (1)", &mut ctx)
                .is_ok()
            {
                oa.fetch_add(1, SeqCst);
            }
        });
        let h2 = loom::thread::spawn(move || {
            let mut ctx = SessionContext::default();
            if eb
                .execute_sql("INSERT INTO conc (id) VALUES (1)", &mut ctx)
                .is_ok()
            {
                ob.fetch_add(1, SeqCst);
            }
        });
        h1.join().unwrap();
        h2.join().unwrap();
        assert_eq!(ok_count.load(SeqCst), 1);
    });
}

#[test]
fn engine_select_named_table_after_engine_reopen() {
    let dir = TempDir::new().expect("tempdir");
    {
        let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
        let mut ctx = SessionContext::default();
        eng.execute_sql("INSERT INTO stateful_r (a) VALUES (100)", &mut ctx)
            .expect("insert");
    }
    let heap = dir.path().join("stateful_r.tbl");
    assert!(
        heap.exists() && heap.metadata().expect("meta").len() > 0,
        "expected persisted heap file with data"
    );
    {
        let eng = SqlEngine::open(dir.path().to_path_buf()).expect("reopen");
        let mut ctx = SessionContext::default();
        let out = eng
            .execute_sql("SELECT a FROM stateful_r", &mut ctx)
            .expect("select");
        match out {
            EngineOutput::ResultSet { columns, rows } => {
                assert_eq!(columns, vec!["a"]);
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0], vec!["Integer(100)"]);
            }
            _ => panic!("expected ResultSet"),
        }
    }
}

#[test]
fn engine_enforces_primary_key_and_unique() {
    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
    let mut ctx = SessionContext::default();
    eng.execute_sql(
        "CREATE TABLE pk (id INT PRIMARY KEY, a INT UNIQUE)",
        &mut ctx,
    )
    .expect("create");
    eng.execute_sql("INSERT INTO pk (id, a) VALUES (1, 10)", &mut ctx)
        .expect("i1");
    assert!(eng
        .execute_sql("INSERT INTO pk (id, a) VALUES (1, 20)", &mut ctx)
        .is_err());
    assert!(eng
        .execute_sql("INSERT INTO pk (id, a) VALUES (2, 10)", &mut ctx)
        .is_err());
    eng.execute_sql("INSERT INTO pk (id, a) VALUES (2, 20)", &mut ctx)
        .expect("i2");
}

#[test]
fn engine_enforces_foreign_key_and_parent_delete() {
    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
    let mut ctx = SessionContext::default();
    eng.execute_sql("CREATE TABLE parent (id INT PRIMARY KEY)", &mut ctx)
        .expect("p");
    eng.execute_sql(
        "CREATE TABLE child (pid INT REFERENCES parent(id))",
        &mut ctx,
    )
    .expect("c");
    assert!(eng
        .execute_sql("INSERT INTO child (pid) VALUES (5)", &mut ctx)
        .is_err());
    eng.execute_sql("INSERT INTO parent (id) VALUES (5)", &mut ctx)
        .expect("ip");
    eng.execute_sql("INSERT INTO child (pid) VALUES (5)", &mut ctx)
        .expect("ic");
    assert!(eng
        .execute_sql("DELETE FROM parent WHERE id = 5", &mut ctx)
        .is_err());
    eng.execute_sql("DELETE FROM child WHERE pid = 5", &mut ctx)
        .expect("del child");
    eng.execute_sql("DELETE FROM parent WHERE id = 5", &mut ctx)
        .expect("del parent");
}

#[test]
fn engine_drop_table_restrict_and_cascade() {
    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
    let mut ctx = SessionContext::default();
    eng.execute_sql("CREATE TABLE p (id INT PRIMARY KEY)", &mut ctx)
        .expect("p");
    eng.execute_sql("CREATE TABLE c (pid INT REFERENCES p(id))", &mut ctx)
        .expect("c");
    assert!(eng.execute_sql("DROP TABLE p", &mut ctx).is_err());
    eng.execute_sql("DROP TABLE p CASCADE", &mut ctx)
        .expect("drop cascade");
    eng.execute_sql("CREATE TABLE c (x INT)", &mut ctx)
        .expect("recreate c after cascade drop");
}

#[test]
fn engine_alter_add_unique_constraint() {
    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
    let mut ctx = SessionContext::default();
    eng.execute_sql("CREATE TABLE ac (a INT)", &mut ctx)
        .expect("c");
    eng.execute_sql("INSERT INTO ac (a) VALUES (1)", &mut ctx)
        .expect("i1");
    eng.execute_sql("INSERT INTO ac (a) VALUES (2)", &mut ctx)
        .expect("i2");
    eng.execute_sql("ALTER TABLE ac ADD CONSTRAINT uq UNIQUE (a)", &mut ctx)
        .expect("alter");
    assert!(eng
        .execute_sql("INSERT INTO ac (a) VALUES (1)", &mut ctx)
        .is_err());
}

#[test]
fn engine_transaction_rollback_rebuilds_fk_runtime() {
    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
    let mut ctx = SessionContext::default();

    eng.execute_sql("CREATE TABLE p (id INT PRIMARY KEY)", &mut ctx)
        .expect("p");
    eng.execute_sql("CREATE TABLE c (pid INT REFERENCES p(id))", &mut ctx)
        .expect("c");

    eng.execute_sql("BEGIN TRANSACTION", &mut ctx).unwrap();
    eng.execute_sql("INSERT INTO p (id) VALUES (10)", &mut ctx)
        .unwrap();
    eng.execute_sql("INSERT INTO c (pid) VALUES (10)", &mut ctx)
        .unwrap();
    eng.execute_sql("ROLLBACK", &mut ctx).unwrap();

    // After rollback, FK runtime should be rebuilt: parent row is gone, so child insert must fail.
    assert!(eng
        .execute_sql("INSERT INTO c (pid) VALUES (10)", &mut ctx)
        .is_err());

    // Parent delete should not be blocked (no child refcount).
    eng.execute_sql("INSERT INTO p (id) VALUES (10)", &mut ctx)
        .unwrap();
    eng.execute_sql("DELETE FROM p WHERE id = 10", &mut ctx)
        .unwrap();
}

#[test]
fn engine_alter_drop_constraint_allows_duplicates_again() {
    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
    let mut ctx = SessionContext::default();

    eng.execute_sql("CREATE TABLE ad (a INT)", &mut ctx)
        .expect("c");
    eng.execute_sql("ALTER TABLE ad ADD CONSTRAINT uq UNIQUE (a)", &mut ctx)
        .expect("add uq");
    eng.execute_sql("INSERT INTO ad (a) VALUES (1)", &mut ctx)
        .expect("i1");
    assert!(eng
        .execute_sql("INSERT INTO ad (a) VALUES (1)", &mut ctx)
        .is_err());

    eng.execute_sql("ALTER TABLE ad DROP CONSTRAINT uq", &mut ctx)
        .expect("drop uq");
    eng.execute_sql("INSERT INTO ad (a) VALUES (1)", &mut ctx)
        .expect("dup ok after drop");
}

#[test]
fn engine_drop_table_if_exists_is_ok() {
    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
    let mut ctx = SessionContext::default();
    eng.execute_sql("DROP TABLE IF EXISTS missing", &mut ctx)
        .expect("if exists");
}

#[cfg(not(rustdb_loom))]
#[test]
fn engine_alter_fk_many_inserts_under_contention() {
    use std::sync::{Arc, Mutex};

    let dir = TempDir::new().expect("tempdir");
    let eng = Arc::new(Mutex::new(
        SqlEngine::open(dir.path().to_path_buf()).expect("open"),
    ));
    {
        let g = eng.lock().expect("lock");
        let mut ctx = SessionContext::default();
        g.execute_sql("CREATE TABLE pp (id INT PRIMARY KEY)", &mut ctx)
            .expect("p");
        for i in 1..=64 {
            g.execute_sql(&format!("INSERT INTO pp (id) VALUES ({i})"), &mut ctx)
                .expect("ip");
        }
        g.execute_sql(
            "CREATE TABLE cc (k INT PRIMARY KEY, pid INT REFERENCES pp(id))",
            &mut ctx,
        )
        .expect("c");
    }
    std::thread::scope(|s| {
        for t in 0..4 {
            let e = eng.clone();
            s.spawn(move || {
                let mut ctx = SessionContext::default();
                for n in 0..32 {
                    let id = t * 10_000 + n;
                    let pid = (n % 64) + 1;
                    let sql = format!("INSERT INTO cc (k, pid) VALUES ({id}, {pid})");
                    e.lock().expect("lock").execute_sql(&sql, &mut ctx).unwrap();
                }
            });
        }
    });
    let g = eng.lock().expect("lock");
    let mut ctx = SessionContext::default();
    let out = g
        .execute_sql("SELECT k FROM cc", &mut ctx)
        .expect("all child rows");
    match out {
        EngineOutput::ResultSet { rows, .. } => assert_eq!(rows.len(), 4 * 32),
        _ => panic!("expected result set"),
    }
}

#[cfg(rustdb_loom)]
#[test]
fn engine_alter_fk_many_inserts_under_contention() {
    use loom::sync::{Arc, Mutex};

    loom::model(|| {
        let dir = TempDir::new().expect("tempdir");
        let eng = Arc::new(Mutex::new(
            SqlEngine::open(dir.path().to_path_buf()).expect("open"),
        ));
        {
            let g = eng.lock().unwrap();
            let mut ctx = SessionContext::default();
            g.execute_sql("CREATE TABLE pp (id INT PRIMARY KEY)", &mut ctx)
                .expect("p");
            for i in 1..=8 {
                g.execute_sql(&format!("INSERT INTO pp (id) VALUES ({i})"), &mut ctx)
                    .expect("ip");
            }
            g.execute_sql(
                "CREATE TABLE cc (k INT PRIMARY KEY, pid INT REFERENCES pp(id))",
                &mut ctx,
            )
            .expect("c");
        }
        let e0 = eng.clone();
        let e1 = eng.clone();
        let h0 = loom::thread::spawn(move || {
            let mut ctx = SessionContext::default();
            for n in 0..4 {
                let id = n;
                let pid = (n % 8) + 1;
                let sql = format!("INSERT INTO cc (k, pid) VALUES ({id}, {pid})");
                e0.lock().unwrap().execute_sql(&sql, &mut ctx).unwrap();
            }
        });
        let h1 = loom::thread::spawn(move || {
            let mut ctx = SessionContext::default();
            for n in 0..4 {
                let id = 100 + n;
                let pid = (n % 8) + 1;
                let sql = format!("INSERT INTO cc (k, pid) VALUES ({id}, {pid})");
                e1.lock().unwrap().execute_sql(&sql, &mut ctx).unwrap();
            }
        });
        h0.join().unwrap();
        h1.join().unwrap();
        let g = eng.lock().unwrap();
        let mut ctx = SessionContext::default();
        let out = g.execute_sql("SELECT k FROM cc", &mut ctx).expect("rows");
        match out {
            EngineOutput::ResultSet { rows, .. } => assert_eq!(rows.len(), 8),
            _ => panic!("expected result set"),
        }
    });
}
