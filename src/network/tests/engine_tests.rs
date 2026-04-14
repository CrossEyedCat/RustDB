//! Tests for `network::engine` (Phase 2 — stub boundary).

use crate::network::engine::{
    engine_error_code, EngineError, EngineHandle, EngineOutput, SessionContext, StubEngine,
};
use crate::network::framing::{ErrorPayload, ServerMessage};

#[test]
fn stub_fixed_error() {
    let err = EngineError::new(engine_error_code::STUB, "stub failure");
    let engine = StubEngine::fixed_error(err.clone());
    let mut ctx = SessionContext::default();
    let got = engine.execute_sql("ANY", &mut ctx).unwrap_err();
    assert_eq!(got, err);
    let payload: ErrorPayload = got.into();
    assert_eq!(payload.code, engine_error_code::STUB);
}

#[test]
fn stub_returns_empty_rows() {
    let engine = StubEngine::empty_result_set();
    let mut ctx = SessionContext {
        session_id: Some(42),
        transaction: None,
    };
    let out = engine.execute_sql("SELECT * FROM t", &mut ctx).expect("ok");
    assert_eq!(
        out,
        EngineOutput::ResultSet {
            columns: vec![],
            rows: vec![]
        }
    );
}

#[test]
fn engine_output_maps_to_server_message() {
    let out = EngineOutput::ResultSet {
        columns: vec!["x".into()],
        rows: vec![vec!["1".into()]],
    };
    let msg = out.into_server_message();
    match msg {
        ServerMessage::ResultSet(p) => {
            assert_eq!(p.columns, vec!["x"]);
            assert_eq!(p.rows, vec![vec!["1"]]);
        }
        _ => panic!("expected ResultSet"),
    }

    let ok = EngineOutput::ExecutionOk { rows_affected: 3 };
    match ok.into_server_message() {
        ServerMessage::ExecutionOk(p) => assert_eq!(p.rows_affected, 3),
        _ => panic!("expected ExecutionOk"),
    }
}

#[test]
fn engine_error_maps_to_error_payload() {
    let e = EngineError::new(engine_error_code::INTERNAL, "boom");
    let p: ErrorPayload = e.clone().into();
    assert_eq!(p.code, engine_error_code::INTERNAL);
    assert_eq!(p.message, "boom");
    let p2: ErrorPayload = (&e).into();
    assert_eq!(p2.code, p.code);
    assert_eq!(p2.message, p.message);
}
