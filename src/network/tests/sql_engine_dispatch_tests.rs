//! Query stream dispatch (`dispatch_client_frame`) with a real [`crate::network::SqlEngine`] instead of [`crate::network::StubEngine`].

use crate::network::engine::{EngineHandle, SessionContext};
use crate::network::framing::{
    decode_server_frame_v1, encode_client_message_v1, ClientMessage, QueryPayload, ServerMessage,
};
use crate::network::query_stream::{dispatch_client_frame, StreamPolicy};
use crate::network::SqlEngine;
use tempfile::TempDir;

#[test]
fn dispatch_sql_engine_inserts_and_returns_select() {
    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
    let mut ctx = SessionContext::default();
    eng.execute_sql("INSERT INTO t (a) VALUES (5)", &mut ctx)
        .expect("insert");

    let req = encode_client_message_v1(&ClientMessage::Query(QueryPayload {
        sql: "SELECT a FROM t".into(),
    }))
    .expect("encode");
    let resp = dispatch_client_frame(&req, &eng, &StreamPolicy::default()).expect("dispatch");
    match decode_server_frame_v1(resp.as_ref()).expect("decode") {
        ServerMessage::ResultSet(p) => {
            assert_eq!(p.columns, vec!["a"]);
            assert_eq!(p.rows.len(), 1);
            assert_eq!(p.rows[0][0], "Integer(5)");
        }
        _ => panic!("expected ResultSet"),
    }
}

#[test]
fn dispatch_sql_engine_execution_ok() {
    let dir = TempDir::new().expect("tempdir");
    let eng = SqlEngine::open(dir.path().to_path_buf()).expect("open");
    let mut ctx = SessionContext::default();
    eng.execute_sql("INSERT INTO u (x) VALUES (1)", &mut ctx)
        .expect("seed");

    let req = encode_client_message_v1(&ClientMessage::Query(QueryPayload {
        sql: "DELETE FROM u WHERE true".into(),
    }))
    .expect("encode");
    let resp = dispatch_client_frame(&req, &eng, &StreamPolicy::default()).expect("dispatch");
    match decode_server_frame_v1(resp.as_ref()).expect("decode") {
        ServerMessage::ExecutionOk(p) => assert_eq!(p.rows_affected, 1),
        _ => panic!("expected ExecutionOk"),
    }
}
