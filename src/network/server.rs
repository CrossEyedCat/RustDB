//! Network server for rustdb — configuration and a **QUIC** listener skeleton (quinn).

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Once};

use quinn::crypto::rustls::QuicServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use thiserror::Error;
use tracing::{info, warn};

use crate::common::Result;
use crate::network::engine::EngineHandle;
use crate::network::framing::MAX_FRAME_PAYLOAD_BYTES;
use crate::network::metrics::{QuicMetrics, QuicNetworkMetrics};
use crate::network::query_stream::{run_connection_streams, StreamPolicy};
use crate::network::transport::build_rustdb_transport_config;

/// ALPN token for RustDB over QUIC (must match the client). See `docs/network/quic-and-quinn.md`.
pub const ALPN_RUSTDB_V1: &[u8] = b"rustdb-v1";

/// Server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub max_connections: usize,
    pub connection_timeout: std::time::Duration,
    /// Legacy flag for non-QUIC transports; QUIC always uses TLS 1.3.
    pub enable_tls: bool,
    /// Variant A: max concurrent bidirectional streams being processed per QUIC connection.
    pub max_concurrent_streams_per_connection: usize,
    /// Per-query engine execution deadline (also used for `max_idle_timeout` baseline elsewhere).
    pub query_timeout: std::time::Duration,
    /// Max UTF-8 byte length of SQL text accepted on the wire.
    pub max_sql_bytes: usize,
    /// Max rows returned in a single `ResultSet` response.
    pub max_result_rows: usize,
    /// Max application payload per frame (cannot exceed [`MAX_FRAME_PAYLOAD_BYTES`]).
    pub max_frame_payload_bytes: u32,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 5432,
            max_connections: 100,
            connection_timeout: std::time::Duration::from_secs(30),
            enable_tls: false,
            max_concurrent_streams_per_connection: 256,
            query_timeout: std::time::Duration::from_secs(30),
            max_sql_bytes: 1024 * 1024,
            max_result_rows: 65_536,
            max_frame_payload_bytes: MAX_FRAME_PAYLOAD_BYTES,
        }
    }
}

impl From<&ServerConfig> for StreamPolicy {
    fn from(c: &ServerConfig) -> Self {
        StreamPolicy {
            max_concurrent_streams_per_connection: c.max_concurrent_streams_per_connection,
            query_timeout: c.query_timeout,
            max_sql_bytes: c.max_sql_bytes,
            max_result_rows: c.max_result_rows,
            max_frame_payload_bytes: c.max_frame_payload_bytes.min(MAX_FRAME_PAYLOAD_BYTES),
        }
    }
}

/// Errors building or running the QUIC server.
#[derive(Debug, Error)]
pub enum QuicServerError {
    #[error("no socket address resolved for {0}")]
    NoResolvedAddress(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("TLS (rustls): {0}")]
    Rustls(#[from] rustls::Error),
    #[error("certificate generation: {0}")]
    Rcgen(#[from] rcgen::Error),
    #[error("QUIC crypto layer: {0}")]
    QuicCrypto(#[from] quinn::crypto::rustls::NoInitialCipherSuite),
    #[error("QUIC transport parameter out of bounds: {0}")]
    QuicBounds(#[from] quinn::VarIntBoundsExceeded),
}

/// Result type for QUIC bind/config helpers (distinct from [`crate::common::Result`]).
pub type QuicResult<T> = std::result::Result<T, QuicServerError>;

static RUSTLS_CRYPTO_INIT: Once = Once::new();

/// Install rustls [`rustls::crypto::CryptoProvider`] (aws-lc-rs). Required before building TLS configs.
pub(crate) fn ensure_rustls_crypto_provider() {
    RUSTLS_CRYPTO_INIT.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("install rustls default crypto provider (aws-lc-rs)");
    });
}

/// Build a quinn [`quinn::ServerConfig`] from [`ServerConfig`]: dev self-signed cert, ALPN, idle timeout.
/// Returns the **leaf** [`CertificateDer`] so QUIC clients can pin the same dev certificate.
pub fn build_quinn_server_config(
    config: &ServerConfig,
) -> QuicResult<(quinn::ServerConfig, CertificateDer<'static>)> {
    ensure_rustls_crypto_provider();
    let subject = tls_subject_name(config);
    let certified = rcgen::generate_simple_self_signed(vec![subject])?;
    let cert_der = CertificateDer::from(certified.cert);
    let key_der = PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der());
    let pinned = cert_der.clone();

    let mut rustls_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], PrivateKeyDer::Pkcs8(key_der))?;

    rustls_config.alpn_protocols = vec![ALPN_RUSTDB_V1.to_vec()];

    let quic_crypto = QuicServerConfig::try_from(rustls_config)?;
    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_crypto));

    let transport = build_rustdb_transport_config(
        config.max_concurrent_streams_per_connection,
        config.connection_timeout,
    )?;
    server_config.transport_config(Arc::new(transport));

    Ok((server_config, pinned))
}

fn tls_subject_name(config: &ServerConfig) -> String {
    let h = config.host.trim();
    if h.is_empty() || h == "0.0.0.0" || h == "::" || h == "[::]" {
        "localhost".to_string()
    } else {
        config.host.clone()
    }
}

/// Resolve `ServerConfig` host/port to a [`SocketAddr`] for binding.
pub fn resolve_listen_addr(config: &ServerConfig) -> QuicResult<SocketAddr> {
    let mut addrs = (config.host.as_str(), config.port).to_socket_addrs()?;
    addrs.next().ok_or_else(|| {
        QuicServerError::NoResolvedAddress(format!("{}:{}", config.host, config.port))
    })
}

/// QUIC server handle: bound UDP [`quinn::Endpoint`] and accept loop (Variant A: bidi streams per query).
pub struct QuicServer {
    endpoint: quinn::Endpoint,
    config: ServerConfig,
    metrics: QuicMetrics,
    /// Leaf certificate (DER) presented to TLS clients — add to [`rustls::RootCertStore`] for dev.
    pinned_certificate: CertificateDer<'static>,
}

impl QuicServer {
    /// Bind UDP and install the TLS + transport configuration from `config`.
    pub fn bind(config: ServerConfig) -> QuicResult<Self> {
        let addr = resolve_listen_addr(&config)?;
        let (server_config, pinned_certificate) = build_quinn_server_config(&config)?;
        let endpoint = quinn::Endpoint::server(server_config, addr)?;
        info!(%addr, "QUIC endpoint bound");
        Ok(Self {
            endpoint,
            config,
            metrics: Arc::new(QuicNetworkMetrics::default()),
            pinned_certificate,
        })
    }

    /// DER-encoded leaf cert (same bytes clients should trust for this listener).
    pub fn pinned_certificate(&self) -> &CertificateDer<'static> {
        &self.pinned_certificate
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.endpoint.local_addr()
    }

    /// Returns the underlying quinn endpoint (e.g. for tests or advanced wiring).
    pub fn endpoint(&self) -> &quinn::Endpoint {
        &self.endpoint
    }

    /// Cumulative successful handshakes observed by this accept loop.
    pub fn total_connections_accepted(&self) -> u64 {
        self.metrics.handshakes_ok.load(Ordering::Relaxed)
    }

    /// Counters and latency aggregates for this listener.
    pub fn metrics(&self) -> &QuicNetworkMetrics {
        self.metrics.as_ref()
    }

    /// Stop accepting new connections and begin closing existing ones (clone [`Self::endpoint`] first if you need to call this while [`run`](Self::run) is active on another task).
    pub fn initiate_shutdown(endpoint: &quinn::Endpoint) {
        endpoint.close(quinn::VarInt::from_u32(0), b"shutdown");
    }

    /// Wait until all connections on this endpoint have finished closing.
    pub async fn wait_idle(endpoint: &quinn::Endpoint) {
        endpoint.wait_idle().await;
    }

    /// Accept incoming connections until the endpoint is closed. Spawns one task per connection
    /// (each runs [`run_connection_streams`] — Variant A).
    pub async fn run(&self, engine: Arc<dyn EngineHandle>) -> QuicResult<()> {
        let max = self.config.max_connections.max(1);
        let endpoint = self.endpoint.clone();
        let metrics = self.metrics.clone();
        let policy: Arc<StreamPolicy> = Arc::new((&self.config).into());

        while let Some(incoming) = endpoint.accept().await {
            if endpoint.open_connections() >= max {
                metrics.connections_refused.fetch_add(1, Ordering::Relaxed);
                warn!(
                    limit = max,
                    open = endpoint.open_connections(),
                    "refusing QUIC connection: max_connections reached"
                );
                incoming.refuse();
                continue;
            }

            let eng = engine.clone();
            let pol = policy.clone();
            let m = metrics.clone();
            tokio::spawn(handle_incoming_quic(incoming, m, eng, pol));
        }

        Ok(())
    }
}

async fn handle_incoming_quic(
    incoming: quinn::Incoming,
    metrics: QuicMetrics,
    engine: Arc<dyn EngineHandle>,
    policy: Arc<StreamPolicy>,
) {
    let conn = match incoming.await {
        Ok(c) => c,
        Err(e) => {
            metrics.handshake_failures.fetch_add(1, Ordering::Relaxed);
            warn!(error = %e, "QUIC incoming handshake failed");
            return;
        }
    };

    metrics.handshakes_ok.fetch_add(1, Ordering::Relaxed);
    metrics.active_connections.fetch_add(1, Ordering::Relaxed);
    info!(
        remote = %conn.remote_address(),
        "QUIC connection established (Variant A: bidi streams per query)"
    );

    run_connection_streams(conn.clone(), engine, policy, Some(metrics.clone())).await;
    metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
    info!(
        remote = %conn.remote_address(),
        "QUIC connection stream loop ended"
    );
}

/// Database server (pre-QUIC stub API).
pub struct Server {
    config: ServerConfig,
}

impl Server {
    pub fn new(config: ServerConfig) -> Result<Self> {
        Ok(Self { config })
    }

    pub fn get_statistics(&self) -> Result<ServerStatistics> {
        Ok(ServerStatistics {
            total_connections: 0,
            active_connections: 0,
        })
    }

    pub fn config(&self) -> &ServerConfig {
        &self.config
    }
}

/// Server statistics
pub struct ServerStatistics {
    pub total_connections: u64,
    pub active_connections: u64,
}

/// Legacy wrapper: same fields as [`Server`].
pub struct NetworkServer {
    inner: Server,
}

impl NetworkServer {
    pub fn new(config: ServerConfig) -> Result<Self> {
        Ok(Self {
            inner: Server::new(config)?,
        })
    }

    pub fn inner(&self) -> &Server {
        &self.inner
    }
}
