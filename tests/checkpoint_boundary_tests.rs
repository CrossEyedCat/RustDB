use rustdb::logging::log_record::{LogRecord, LogRecordType};
use rustdb::network::engine::{EngineHandle, SessionContext};
use rustdb::network::sql_engine::SqlEngine;
use tempfile::TempDir;

#[test]
fn manual_checkpoint_writes_checkpoint_record() {
    std::env::remove_var("RUSTDB_DISABLE_WAL");
    std::env::remove_var("RUSTDB_DISABLE_CHECKPOINT");

    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().to_path_buf();

    {
        let engine = SqlEngine::open(data_dir.clone()).unwrap();
        let mut ctx = SessionContext::default();
        engine.execute_sql("CREATE TABLE t (a INTEGER)", &mut ctx).unwrap();
        engine
            .execute_sql("INSERT INTO t (a) VALUES (1)", &mut ctx)
            .unwrap();
        engine.checkpoint().unwrap();
    }

    let wal_dir = data_dir.join(".rustdb").join("wal");
    let recs = LogRecord::read_log_records_from_directory(&wal_dir).unwrap();
    assert!(
        recs.iter().any(|r| r.record_type == LogRecordType::Checkpoint),
        "expected at least one Checkpoint record in WAL"
    );
}

