//! Variant A query/stream dispatch tests (no live UDP).

use crate::network::engine::{engine_error_code, EngineOutput, StubEngine};
use crate::network::framing::{
    decode_server_frame_v1, encode_client_message_v1, ClientHelloPayload, ClientMessage,
    QueryPayload, ServerMessage,
};
use crate::network::query_stream::{dispatch_client_frame, DispatchError, StreamPolicy};

#[test]
fn dispatch_query_returns_result_set() {
    let engine = StubEngine::fixed_ok(EngineOutput::ResultSet {
        columns: vec!["n".into()],
        rows: vec![vec!["1".into()]],
    });
    let req = encode_client_message_v1(&ClientMessage::Query(QueryPayload {
        sql: "SELECT 1".into(),
    }))
    .expect("encode");
    let policy = StreamPolicy::default();
    let resp = dispatch_client_frame(&req, &engine, &policy).expect("dispatch");
    match decode_server_frame_v1(resp.as_ref()).expect("decode") {
        ServerMessage::ResultSet(p) => {
            assert_eq!(p.columns, vec!["n"]);
            assert_eq!(p.rows, vec![vec!["1"]]);
        }
        _ => panic!("expected ResultSet"),
    }
}

#[test]
fn dispatch_rejects_sql_over_limit() {
    let engine = StubEngine::empty_result_set();
    let sql = "x".repeat(16);
    let req =
        encode_client_message_v1(&ClientMessage::Query(QueryPayload { sql })).expect("encode");
    let policy = StreamPolicy {
        max_sql_bytes: 8,
        ..Default::default()
    };
    let err = dispatch_client_frame(&req, &engine, &policy).expect_err("too long");
    match err {
        DispatchError::Engine(e) => {
            assert_eq!(e.code, engine_error_code::SQL_TOO_LONG);
        }
        _ => panic!("expected engine SQL_TOO_LONG"),
    }
}

#[test]
fn stream_policy_from_server_config() {
    let c = crate::network::server::ServerConfig::default();
    let p: StreamPolicy = (&c).into();
    assert_eq!(
        p.max_concurrent_streams_per_connection,
        c.max_concurrent_streams_per_connection
    );
}

#[test]
fn dispatch_rejects_result_rows_over_limit() {
    let engine = StubEngine::fixed_ok(EngineOutput::ResultSet {
        columns: vec!["c".into()],
        rows: vec![vec!["1".into()], vec!["2".into()]],
    });
    let req = encode_client_message_v1(&ClientMessage::Query(QueryPayload {
        sql: "SELECT 1".into(),
    }))
    .expect("encode");
    let policy = StreamPolicy {
        max_result_rows: 1,
        ..Default::default()
    };
    let err = dispatch_client_frame(&req, &engine, &policy).expect_err("too many rows");
    match err {
        DispatchError::Engine(e) => {
            assert_eq!(e.code, engine_error_code::RESULT_ROWS_TOO_LARGE);
        }
        _ => panic!("expected RESULT_ROWS_TOO_LARGE"),
    }
}

#[test]
fn dispatch_rejects_client_hello_on_query_stream() {
    let engine = StubEngine::empty_result_set();
    let req = encode_client_message_v1(&ClientMessage::ClientHello(ClientHelloPayload {
        client_version: "test".into(),
    }))
    .expect("encode");
    let policy = StreamPolicy::default();
    let err = dispatch_client_frame(&req, &engine, &policy).expect_err("hello not allowed");
    match err {
        DispatchError::Engine(e) => {
            assert_eq!(e.code, engine_error_code::PROTOCOL);
        }
        _ => panic!("expected protocol engine error"),
    }
}
