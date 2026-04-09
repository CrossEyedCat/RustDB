//! Minimal QUIC client (TLS + Variant A: one bidirectional stream per query).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use quinn::Connection;
use rustls::pki_types::CertificateDer;
use thiserror::Error;

use crate::network::framing::{
    decode_server_frame_v1, encode_client_message_v1, ClientMessage, ProtocolError, QueryPayload,
    ServerMessage, MAX_FRAME_PAYLOAD_BYTES,
};
use crate::network::query_stream::read_application_frame;
use crate::network::server::{ensure_rustls_crypto_provider, ServerConfig, ALPN_RUSTDB_V1};
use crate::network::transport::transport_config_arc;

/// Build a [`quinn::ClientConfig`] that trusts the given certificates (e.g. dev server leaf) and uses [`ALPN_RUSTDB_V1`].
///
/// Transport limits match [`ServerConfig::default`] so benchmarks are not asymmetric vs the stock listener.
pub fn build_quinn_client_config(
    trusted_certs: &[CertificateDer<'_>],
) -> Result<quinn::ClientConfig, QuicClientError> {
    let d = ServerConfig::default();
    build_quinn_client_config_with_limits(
        trusted_certs,
        d.max_concurrent_streams_per_connection,
        d.connection_timeout,
    )
}

/// Same as [`build_quinn_client_config`], but with explicit stream and idle limits (mirror the server's [`ServerConfig`]).
pub fn build_quinn_client_config_with_limits(
    trusted_certs: &[CertificateDer<'_>],
    max_concurrent_streams: usize,
    idle_timeout: Duration,
) -> Result<quinn::ClientConfig, QuicClientError> {
    ensure_rustls_crypto_provider();
    let mut roots = rustls::RootCertStore::empty();
    for c in trusted_certs {
        roots.add(c.clone())?;
    }
    let mut rustls_client = rustls::ClientConfig::builder()
        .with_root_certificates(Arc::new(roots))
        .with_no_client_auth();
    rustls_client.alpn_protocols = vec![ALPN_RUSTDB_V1.to_vec()];
    let quic = quinn::crypto::rustls::QuicClientConfig::try_from(rustls_client)?;
    let mut cfg = quinn::ClientConfig::new(Arc::new(quic));
    let transport = transport_config_arc(max_concurrent_streams, idle_timeout)?;
    cfg.transport_config(transport);
    Ok(cfg)
}

/// Create a client [`quinn::Endpoint`] bound to an ephemeral UDP port with `client_config` as default.
pub fn make_client_endpoint(
    client_config: quinn::ClientConfig,
) -> Result<quinn::Endpoint, std::io::Error> {
    let mut endpoint = quinn::Endpoint::client((std::net::Ipv4Addr::UNSPECIFIED, 0).into())?;
    endpoint.set_default_client_config(client_config);
    Ok(endpoint)
}

/// Dial `addr` using the endpoint default client config; `server_name` must match the server cert SAN (e.g. `127.0.0.1` when the dev cert is issued for that address).
pub async fn connect(
    endpoint: &quinn::Endpoint,
    addr: SocketAddr,
    server_name: &str,
) -> Result<quinn::Connection, QuicClientError> {
    let conn = endpoint.connect(addr, server_name)?.await?;
    Ok(conn)
}

/// Variant A: open one bidirectional stream, send a single [`ClientMessage::Query`] frame, read one response frame.
pub async fn query_once(
    connection: &Connection,
    sql: &str,
) -> Result<ServerMessage, QuicClientError> {
    let (mut send, mut recv) = connection.open_bi().await?;
    let frame = encode_client_message_v1(&ClientMessage::Query(QueryPayload {
        sql: sql.to_string(),
    }))?;
    send.write_all(&frame).await?;
    let _ = send.finish();
    let response = read_application_frame(&mut recv, MAX_FRAME_PAYLOAD_BYTES)
        .await
        .map_err(QuicClientError::from)?;
    Ok(decode_server_frame_v1(&response)?)
}

#[derive(Debug, Error)]
pub enum QuicClientError {
    #[error("rustls: {0}")]
    Rustls(#[from] rustls::Error),
    #[error("QUIC connect: {0}")]
    Connect(#[from] quinn::ConnectError),
    #[error("QUIC connection: {0}")]
    Connection(#[from] quinn::ConnectionError),
    #[error("read stream: {0}")]
    Read(#[from] quinn::ReadExactError),
    #[error("write stream: {0}")]
    Write(#[from] quinn::WriteError),
    #[error("frame protocol: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("encode: {0}")]
    Encode(#[from] crate::network::framing::EncodeError),
    #[error("QUIC crypto: {0}")]
    QuicCrypto(#[from] quinn::crypto::rustls::NoInitialCipherSuite),
    #[error("read frame: {0}")]
    ReadFrame(String),
    #[error("QUIC transport parameter out of bounds: {0}")]
    QuicBounds(#[from] quinn::VarIntBoundsExceeded),
}

impl From<crate::network::query_stream::ReadFrameError> for QuicClientError {
    fn from(e: crate::network::query_stream::ReadFrameError) -> Self {
        QuicClientError::ReadFrame(e.to_string())
    }
}
