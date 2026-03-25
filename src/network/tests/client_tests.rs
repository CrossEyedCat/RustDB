//! QUIC client integration (loopback).

use std::sync::Arc;

use crate::network::client::{build_quinn_client_config, connect, make_client_endpoint, query_once};
use crate::network::engine::{EngineOutput, StubEngine};
use crate::network::framing::ServerMessage;
use crate::network::server::{QuicServer, ServerConfig};

#[tokio::test]
async fn quic_client_query_roundtrip_localhost() {
    let server_config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port: 0,
        ..Default::default()
    };
    let srv = Arc::new(QuicServer::bind(server_config).expect("bind server"));
    let addr = srv.local_addr().expect("local addr");
    let cert = srv.pinned_certificate().clone();
    let engine = Arc::new(StubEngine::fixed_ok(EngineOutput::ResultSet {
        columns: vec!["x".into()],
        rows: vec![vec!["42".into()]],
    }));
    let server = tokio::spawn({
        let srv = srv.clone();
        let engine = engine.clone();
        async move {
            let _ = srv.run(engine).await;
        }
    });

    let client_cfg = build_quinn_client_config(std::slice::from_ref(&cert)).expect("client cfg");
    let endpoint = make_client_endpoint(client_cfg).expect("client endpoint");

    let mut last_err = None;
    for _ in 0..30 {
        // Cert SAN matches the bind address (see `server::tls_subject_name`).
        match connect(&endpoint, addr, "127.0.0.1").await {
            Ok(c) => {
                let msg = query_once(&c, "SELECT 1").await.expect("query");
                match msg {
                    ServerMessage::ResultSet(p) => {
                        assert_eq!(p.columns, vec!["x"]);
                        assert_eq!(p.rows, vec![vec!["42"]]);
                    }
                    _ => panic!("unexpected message: {:?}", msg),
                }
                server.abort();
                return;
            }
            Err(e) => last_err = Some(e),
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    server.abort();
    panic!("connect failed: {:?}", last_err);
}
