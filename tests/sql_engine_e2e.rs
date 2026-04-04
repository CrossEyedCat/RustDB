//! End-to-end SQL tests against [`rustdb::network::SqlEngine`] (parser → plan → executor / heap).
//!
//! Contrasts with `tests/integration/common` (`IntegrationTestContext`), which simulates row counts
//! without reading the heap, and with network tests that use [`rustdb::network::StubEngine`].

use rustdb::network::engine::{engine_error_code, EngineHandle, EngineOutput, SessionContext};
use rustdb::network::SqlEngine;
use tempfile::TempDir;

fn open_engine() -> (TempDir, SqlEngine) {
    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("SqlEngine::open");
    (dir, eng)
}

#[test]
fn e2e_select_without_from() {
    let (_dir, eng) = open_engine();
    let mut ctx = SessionContext::default();
    let out = eng.execute_sql("SELECT 7, 8", &mut ctx).expect("execute");
    match out {
        EngineOutput::ResultSet { columns, rows } => {
            assert_eq!(columns.len(), 2);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0], vec!["7", "8"]);
        }
        _ => panic!("expected ResultSet"),
    }
}

#[test]
fn e2e_insert_then_select_returns_stored_tuple() {
    let (_dir, eng) = open_engine();
    let mut ctx = SessionContext::default();
    eng.execute_sql("INSERT INTO items (sku, qty) VALUES (100, 3)", &mut ctx)
        .expect("insert");
    let out = eng
        .execute_sql("SELECT sku, qty FROM items", &mut ctx)
        .expect("select");
    match out {
        EngineOutput::ResultSet { columns, rows } => {
            assert_eq!(columns, vec!["qty", "sku"]);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][1], "Integer(100)");
            assert_eq!(rows[0][0], "Integer(3)");
        }
        _ => panic!("expected ResultSet"),
    }
}

#[test]
fn e2e_multi_value_insert_and_count_rows() {
    let (_dir, eng) = open_engine();
    let mut ctx = SessionContext::default();
    let ins = eng
        .execute_sql("INSERT INTO batch (n) VALUES (1), (2), (3)", &mut ctx)
        .expect("insert");
    assert_eq!(ins, EngineOutput::ExecutionOk { rows_affected: 3 });
    let out = eng
        .execute_sql("SELECT n FROM batch", &mut ctx)
        .expect("select");
    match out {
        EngineOutput::ResultSet { rows, .. } => assert_eq!(rows.len(), 3),
        _ => panic!("expected ResultSet"),
    }
}

#[test]
fn e2e_update_and_delete_with_where_true() {
    let (_dir, eng) = open_engine();
    let mut ctx = SessionContext::default();
    eng.execute_sql("INSERT INTO rowz (k, v) VALUES (1, 10)", &mut ctx)
        .unwrap();
    let up = eng
        .execute_sql("UPDATE rowz SET v = 99 WHERE true", &mut ctx)
        .expect("update");
    assert_eq!(up, EngineOutput::ExecutionOk { rows_affected: 1 });
    let sel = eng
        .execute_sql("SELECT v FROM rowz", &mut ctx)
        .expect("select");
    match sel {
        EngineOutput::ResultSet { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], "Integer(99)");
        }
        _ => panic!("expected ResultSet"),
    }
    let del = eng
        .execute_sql("DELETE FROM rowz WHERE true", &mut ctx)
        .expect("delete");
    assert_eq!(del, EngineOutput::ExecutionOk { rows_affected: 1 });
    let empty = eng
        .execute_sql("SELECT k FROM rowz", &mut ctx)
        .expect("select");
    match empty {
        EngineOutput::ResultSet { rows, .. } => assert!(rows.is_empty()),
        _ => panic!("expected ResultSet"),
    }
}

#[test]
fn e2e_ddl_returns_unsupported() {
    let (_dir, eng) = open_engine();
    let mut ctx = SessionContext::default();
    let err = eng
        .execute_sql("CREATE TABLE x (id INT)", &mut ctx)
        .expect_err("ddl unsupported");
    assert_eq!(err.code, engine_error_code::UNSUPPORTED_SQL);
}

#[test]
fn e2e_multiple_statements_rejected() {
    let (_dir, eng) = open_engine();
    let mut ctx = SessionContext::default();
    let err = eng
        .execute_sql("SELECT 1; SELECT 2", &mut ctx)
        .expect_err("one statement only");
    assert_eq!(err.code, engine_error_code::PROTOCOL);
}

#[test]
fn e2e_insert_select_subquery_unsupported() {
    let (_dir, eng) = open_engine();
    let mut ctx = SessionContext::default();
    let err = eng
        .execute_sql("INSERT INTO t SELECT 1", &mut ctx)
        .expect_err("insert select");
    assert_eq!(err.code, engine_error_code::UNSUPPORTED_SQL);
}
