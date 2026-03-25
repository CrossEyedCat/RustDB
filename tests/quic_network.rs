//! QUIC integration tests: metrics for success, engine errors, SQL limits, timeouts, connection refuse.

use std::sync::Arc;
use std::time::Duration;

use rustdb::network::client::{build_quinn_client_config, connect, make_client_endpoint, query_once};
use rustdb::network::engine::{
    engine_error_code, EngineError, EngineHandle, EngineOutput, StubEngine,
};
use rustdb::network::framing::ServerMessage;
use rustdb::network::server::{QuicServer, ServerConfig};
use rustdb::network::QuicNetworkSnapshot;

async fn try_connect_loops(
    endpoint: &quinn::Endpoint,
    addr: std::net::SocketAddr,
) -> Result<quinn::Connection, rustdb::network::client::QuicClientError> {
    let mut last_err = None;
    for _ in 0..60 {
        match connect(endpoint, addr, "127.0.0.1").await {
            Ok(c) => return Ok(c),
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
    Err(last_err.expect("last_err"))
}

struct QuicTestHarness {
    srv: Arc<QuicServer>,
    cert: rustls::pki_types::CertificateDer<'static>,
    addr: std::net::SocketAddr,
    run_task: tokio::task::JoinHandle<()>,
}

impl QuicTestHarness {
    fn spawn(server_config: ServerConfig, engine: Arc<dyn EngineHandle>) -> Self {
        let srv = Arc::new(QuicServer::bind(server_config).expect("bind"));
        let cert = srv.pinned_certificate().clone();
        let addr = srv.local_addr().expect("local addr");
        let run_task = tokio::spawn({
            let srv = srv.clone();
            async move {
                let _ = srv.run(engine).await;
            }
        });
        Self {
            srv,
            cert,
            addr,
            run_task,
        }
    }

    fn metrics_snapshot(&self) -> QuicNetworkSnapshot {
        self.srv.metrics().snapshot()
    }

    fn client_endpoint(&self) -> quinn::Endpoint {
        let cfg = build_quinn_client_config(std::slice::from_ref(&self.cert)).expect("client cfg");
        make_client_endpoint(cfg).expect("client endpoint")
    }

    fn finish(self) {
        self.run_task.abort();
    }
}

#[tokio::test]
async fn quic_success_updates_ok_metrics() {
    let h = QuicTestHarness::spawn(
        ServerConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            query_timeout: Duration::from_secs(5),
            ..Default::default()
        },
        Arc::new(StubEngine::fixed_ok(EngineOutput::ResultSet {
            columns: vec!["n".into()],
            rows: vec![vec!["1".into()]],
        })),
    );
    let ep = h.client_endpoint();
    let conn = try_connect_loops(&ep, h.addr).await.expect("connect");
    let msg = query_once(&conn, "SELECT 1").await.expect("query");
    assert!(matches!(msg, ServerMessage::ResultSet(_)));

    let snap = h.metrics_snapshot();
    assert_eq!(snap.handshakes_ok, 1);
    assert_eq!(snap.queries_handled, 1);
    assert_eq!(snap.queries_ok, 1);
    assert_eq!(snap.queries_error_response, 0);
    assert_eq!(snap.queries_write_failed, 0);
    assert!(snap.query_latency_sum_ns > 0);
    h.finish();
}

#[tokio::test]
async fn quic_engine_error_counts_error_response() {
    let h = QuicTestHarness::spawn(
        ServerConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            ..Default::default()
        },
        Arc::new(StubEngine::fixed_error(EngineError::new(
            engine_error_code::INTERNAL,
            "test failure",
        ))),
    );
    let ep = h.client_endpoint();
    let conn = try_connect_loops(&ep, h.addr).await.expect("connect");
    let msg = query_once(&conn, "SELECT 1").await.expect("query");
    match msg {
        ServerMessage::Error(e) => {
            assert_eq!(e.code, engine_error_code::INTERNAL);
        }
        _ => panic!("expected Error frame, got {:?}", msg),
    }

    let snap = h.metrics_snapshot();
    assert_eq!(snap.queries_handled, 1);
    assert_eq!(snap.queries_ok, 0);
    assert_eq!(snap.queries_error_response, 1);
    assert_eq!(snap.queries_write_failed, 0);
    h.finish();
}

#[tokio::test]
async fn quic_sql_too_long_counts_error_response() {
    let h = QuicTestHarness::spawn(
        ServerConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            max_sql_bytes: 8,
            ..Default::default()
        },
        Arc::new(StubEngine::empty_result_set()),
    );
    let ep = h.client_endpoint();
    let conn = try_connect_loops(&ep, h.addr).await.expect("connect");
    let sql = "x".repeat(64);
    let msg = query_once(&conn, &sql).await.expect("query");
    assert!(matches!(msg, ServerMessage::Error(_)));

    let snap = h.metrics_snapshot();
    assert_eq!(snap.queries_error_response, 1);
    assert_eq!(snap.queries_ok, 0);
    h.finish();
}

#[tokio::test]
async fn quic_query_timeout_error_code_counts_error_response() {
    // Real wall-clock timeout is covered by `tokio::time::timeout` in `query_stream`; blocking
    // `execute_sql` would starve the runtime, so we assert the same metric path via QUERY_TIMEOUT.
    let h = QuicTestHarness::spawn(
        ServerConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            ..Default::default()
        },
        Arc::new(StubEngine::fixed_error(EngineError::new(
            engine_error_code::QUERY_TIMEOUT,
            "query exceeded per-query timeout",
        ))),
    );
    let ep = h.client_endpoint();
    let conn = try_connect_loops(&ep, h.addr).await.expect("connect");
    let msg = query_once(&conn, "SELECT 1").await.expect("query");
    match msg {
        ServerMessage::Error(e) => {
            assert_eq!(e.code, engine_error_code::QUERY_TIMEOUT);
        }
        _ => panic!("expected QUERY_TIMEOUT Error"),
    }

    let snap = h.metrics_snapshot();
    assert_eq!(snap.queries_error_response, 1);
    assert_eq!(snap.queries_ok, 0);
    h.finish();
}

#[tokio::test]
async fn quic_second_connection_refused_when_at_capacity() {
    let h = QuicTestHarness::spawn(
        ServerConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            max_connections: 1,
            query_timeout: Duration::from_secs(5),
            ..Default::default()
        },
        Arc::new(StubEngine::fixed_ok(EngineOutput::ResultSet {
            columns: vec!["a".into()],
            rows: vec![vec!["1".into()]],
        })),
    );

    let ep1 = h.client_endpoint();
    let c1 = try_connect_loops(&ep1, h.addr).await.expect("c1");
    query_once(&c1, "SELECT 1").await.expect("first query");

    tokio::time::sleep(Duration::from_millis(50)).await;

    let ep2 = h.client_endpoint();
    let _c2_result = connect(&ep2, h.addr, "127.0.0.1").await;

    tokio::time::sleep(Duration::from_millis(150)).await;

    let snap = h.metrics_snapshot();
    assert_eq!(snap.handshakes_ok, 1);
    assert!(
        snap.connections_refused >= 1,
        "expected connections_refused when max_connections=1 and a second client dials; snap={:?}",
        snap
    );
    h.finish();
}
