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
fn engine_transactions_are_accepted_minimally() {
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
        eng.execute_sql("ROLLBACK", &mut ctx).unwrap(),
        EngineOutput::ExecutionOk { rows_affected: 0 }
    );
}
