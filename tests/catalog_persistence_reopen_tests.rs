use rustdb::network::engine::{EngineHandle, SessionContext};
use rustdb::network::sql_engine::SqlEngine;
use tempfile::TempDir;

#[test]
fn catalog_json_persists_across_reopen() {
    let dir = TempDir::new().unwrap();

    {
        let engine = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        engine
            .execute_sql("CREATE TABLE t (a INTEGER)", &mut ctx)
            .unwrap();
    }

    {
        let engine = SqlEngine::open(dir.path().to_path_buf()).unwrap();
        let mut ctx = SessionContext::default();
        // If catalog did not persist, this would error (table missing).
        engine
            .execute_sql("INSERT INTO t (a) VALUES (1)", &mut ctx)
            .unwrap();
        let out = engine.execute_sql("SELECT a FROM t", &mut ctx).unwrap();
        match out {
            rustdb::network::engine::EngineOutput::ResultSet { rows, .. } => {
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("expected ResultSet"),
        }
    }
}
