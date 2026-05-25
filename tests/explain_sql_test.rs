//! Integration tests for `EXPLAIN` / `EXPLAIN ANALYZE`.

use rustdb::network::engine::{EngineHandle, EngineOutput, SessionContext};
use rustdb::network::SqlEngine;
use tempfile::TempDir;

fn open_engine() -> (TempDir, SqlEngine) {
    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("SqlEngine::open");
    (dir, eng)
}

fn plan_lines(out: EngineOutput) -> Vec<String> {
    match out {
        EngineOutput::ResultSet { columns, rows } => {
            assert_eq!(columns, vec!["QUERY PLAN"]);
            rows.into_iter().map(|r| r[0].clone()).collect()
        }
        _ => panic!("expected ResultSet"),
    }
}

#[test]
fn explain_select_returns_plan_column() {
    let (_dir, eng) = open_engine();
    let mut ctx = SessionContext::default();
    eng.execute_sql("CREATE TABLE ex_t (id INTEGER)", &mut ctx)
        .expect("ddl");
    eng.execute_sql("INSERT INTO ex_t (id) VALUES (1), (2)", &mut ctx)
        .expect("insert");
    let out = eng
        .execute_sql("EXPLAIN SELECT id FROM ex_t WHERE id = 1", &mut ctx)
        .expect("explain");
    let lines = plan_lines(out);
    let text = lines.join("\n");
    assert!(text.contains("Table Scan") || text.contains("Index Scan"));
    assert!(text.contains("Planning:"));
}

#[test]
fn explain_insert_shows_insert_node() {
    let (_dir, eng) = open_engine();
    let mut ctx = SessionContext::default();
    eng.execute_sql("CREATE TABLE ex_i (n INTEGER)", &mut ctx)
        .expect("ddl");
    let out = eng
        .execute_sql("EXPLAIN INSERT INTO ex_i (n) VALUES (1)", &mut ctx)
        .expect("explain");
    let lines = plan_lines(out);
    assert!(lines.iter().any(|l| l.contains("Insert on ex_i")));
}

#[test]
fn explain_analyze_insert_executes_and_reports_rows_affected() {
    let (_dir, eng) = open_engine();
    let mut ctx = SessionContext::default();
    eng.execute_sql("CREATE TABLE ex_a (n INTEGER)", &mut ctx)
        .expect("ddl");
    let out = eng
        .execute_sql(
            "EXPLAIN ANALYZE INSERT INTO ex_a (n) VALUES (1), (2)",
            &mut ctx,
        )
        .expect("explain analyze");
    let lines = plan_lines(out);
    let text = lines.join("\n");
    assert!(text.contains("Execution Time:"));
    assert!(text.contains("Rows Affected: 2"));
    let count = eng
        .execute_sql("SELECT n FROM ex_a", &mut ctx)
        .expect("select");
    match count {
        EngineOutput::ResultSet { rows, .. } => assert_eq!(rows.len(), 2),
        _ => panic!("expected rows"),
    }
}

#[test]
fn explain_create_table_rejected_at_parse() {
    let (_dir, eng) = open_engine();
    let mut ctx = SessionContext::default();
    assert!(eng
        .execute_sql("EXPLAIN CREATE TABLE bad (id INT)", &mut ctx)
        .is_err());
}
