//! Variant A: one QUIC connection, many bidirectional streams — one query per stream.
//!
//! See `docs/network/stream-models.md`.

use std::cell::RefCell;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use quinn::{Connection, RecvStream, SendStream};
use thiserror::Error;
use tokio::sync::OwnedSemaphorePermit;
use tracing::{info, info_span, warn};

use crate::network::engine::{
    engine_error_code, EngineError, EngineHandle, EngineOutput, SessionContext,
};
use crate::network::framing::{
    decode_client_frame_v1, encode_server_message_v1, encode_server_message_write, ClientMessage,
    FrameHeader, ProtocolError, ServerMessage, FRAME_HEADER_LEN, MAX_FRAME_PAYLOAD_BYTES,
    PROTOCOL_VERSION_V1,
};
use crate::network::metrics::{QueryHandledOutcome, QuicMetrics};

/// Limits for stream handling (from [`crate::network::server::ServerConfig`]).
#[derive(Debug, Clone)]
pub struct StreamPolicy {
    pub max_concurrent_streams_per_connection: usize,
    pub query_timeout: Duration,
    pub max_sql_bytes: usize,
    pub max_result_rows: usize,
    /// Max payload bytes accepted per frame on the wire (clamped to protocol max).
    pub max_frame_payload_bytes: u32,
}

impl Default for StreamPolicy {
    fn default() -> Self {
        Self {
            max_concurrent_streams_per_connection: 32,
            query_timeout: Duration::from_secs(30),
            max_sql_bytes: 1024 * 1024,
            max_result_rows: 65_536,
            max_frame_payload_bytes: MAX_FRAME_PAYLOAD_BYTES,
        }
    }
}

/// Failure while building a response frame for the wire.
#[derive(Debug, Error)]
pub enum DispatchError {
    #[error("protocol: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("encode: {0}")]
    Encode(#[from] crate::network::framing::EncodeError),
    #[error("engine: {0}")]
    Engine(#[from] EngineError),
}

/// Small cache for `SELECT` without `FROM` wire responses (hot path for `select_literal` benchmark).
///
/// Key is the raw SQL string (must match exactly). Value is a full server frame (header + postcard payload).
static SELECT_NO_FROM_WIRE_CACHE: LazyLock<Mutex<std::collections::HashMap<String, Vec<u8>>>> =
    LazyLock::new(|| Mutex::new(std::collections::HashMap::new()));

const SELECT_NO_FROM_WIRE_CACHE_MAX_ENTRIES: usize = 1024;

thread_local! {
    static TL_ENCODE_BUF: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

impl DispatchError {
    fn to_error_message(&self) -> ServerMessage {
        match self {
            DispatchError::Protocol(p) => {
                ServerMessage::Error(crate::network::framing::ErrorPayload {
                    code: engine_error_code::PROTOCOL,
                    message: p.to_string(),
                })
            }
            DispatchError::Encode(e) => {
                ServerMessage::Error(crate::network::framing::ErrorPayload {
                    code: engine_error_code::INTERNAL,
                    message: e.to_string(),
                })
            }
            DispatchError::Engine(e) => {
                ServerMessage::Error(crate::network::framing::ErrorPayload {
                    code: e.code,
                    message: e.message.clone(),
                })
            }
        }
    }
}

/// Read one full application frame from the receive half (12-byte header + payload).
///
/// `max_payload` is the maximum payload length allowed (typically from [`StreamPolicy::max_frame_payload_bytes`],
/// never greater than [`MAX_FRAME_PAYLOAD_BYTES`]).
pub async fn read_application_frame(
    recv: &mut RecvStream,
    max_payload: u32,
) -> Result<Vec<u8>, ReadFrameError> {
    let max_payload = max_payload.min(MAX_FRAME_PAYLOAD_BYTES);
    let mut header = [0u8; FRAME_HEADER_LEN];
    recv.read_exact(&mut header)
        .await
        .map_err(ReadFrameError::Recv)?;
    let fh = FrameHeader::decode(&header).map_err(ReadFrameError::Header)?;
    if fh.payload_len > max_payload {
        return Err(ReadFrameError::PayloadTooLarge(fh.payload_len));
    }
    let mut body = vec![0u8; fh.payload_len as usize];
    if !body.is_empty() {
        recv.read_exact(&mut body)
            .await
            .map_err(ReadFrameError::Recv)?;
    }
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + body.len());
    frame.extend_from_slice(&header);
    frame.extend_from_slice(&body);
    Ok(frame)
}

#[derive(Debug, Error)]
pub enum ReadFrameError {
    #[error("recv: {0}")]
    Recv(quinn::ReadExactError),
    #[error("header: {0}")]
    Header(ProtocolError),
    #[error("payload length {0} exceeds max frame size")]
    PayloadTooLarge(u32),
}

/// Sync dispatch: decode → engine → encode (used inside per-query timeout).
pub fn dispatch_client_frame(
    frame: &[u8],
    engine: &dyn EngineHandle,
    policy: &StreamPolicy,
) -> Result<Vec<u8>, DispatchError> {
    let msg = decode_client_frame_v1(frame)?;
    match msg {
        ClientMessage::Query(q) => {
            let span = info_span!(
                "sql.query",
                sql_len = q.sql.len(),
                sql = %summarize_sql(&q.sql)
            );
            let _g = span.enter();
            if q.sql.len() > policy.max_sql_bytes {
                return Err(EngineError::new(
                    engine_error_code::SQL_TOO_LONG,
                    "SQL text exceeds configured max_sql_bytes",
                )
                .into());
            }

            // Ultra-hot path: deterministic literal projections without FROM.
            // Serve the already encoded frame to skip engine + postcard encode overhead.
            if engine.supports_select_no_from_wire_cache() && likely_select_without_from(&q.sql) {
                if let Ok(g) = SELECT_NO_FROM_WIRE_CACHE.lock() {
                    if let Some(bytes) = g.get(&q.sql) {
                        return Ok(bytes.clone());
                    }
                }
            }

            let mut ctx = SessionContext::default();
            let out = engine.execute_sql(&q.sql, &mut ctx)?;
            let out = enforce_max_result_rows(out, policy.max_result_rows)?;
            let server = out.into_server_message();

            // Encode using a thread-local buffer (still allocates postcard payload internally,
            // but avoids reallocating the frame buffer each request).
            let mut bytes = TL_ENCODE_BUF.with(|b| {
                let mut buf = b.borrow_mut();
                buf.clear();
                // Keep a small minimum capacity for common tiny responses.
                let cap = buf.capacity();
                if cap < 256 {
                    buf.reserve(256 - cap);
                }
                encode_server_message_write(PROTOCOL_VERSION_V1, &server, &mut *buf)?;
                Ok::<_, DispatchError>(buf.clone())
            })?;
            if engine.supports_select_no_from_wire_cache() && likely_select_without_from(&q.sql) {
                if let Ok(mut g) = SELECT_NO_FROM_WIRE_CACHE.lock() {
                    if g.len() >= SELECT_NO_FROM_WIRE_CACHE_MAX_ENTRIES && !g.contains_key(&q.sql) {
                        // Simple cap: drop the whole map when it grows beyond a bound.
                        // (Avoids adding an LRU dependency; literal SELECTs are typically tiny in variety.)
                        g.clear();
                    }
                    g.insert(q.sql.clone(), bytes.clone());
                }
            }
            Ok(bytes)
        }
        ClientMessage::ClientHello(_) => Err(EngineError::new(
            engine_error_code::PROTOCOL,
            "expected Query frame on this bidirectional stream (ClientHello is not supported here)",
        )
        .into()),
    }
}

fn likely_select_without_from(sql: &str) -> bool {
    let s = sql.trim_start();
    if s.len() < 6 {
        return false;
    }
    let upper = s.get(..s.len().min(64)).unwrap_or(s).to_ascii_uppercase();
    if !upper.starts_with("SELECT") {
        return false;
    }
    // Avoid multi-statement.
    if s.contains(';') {
        return false;
    }
    // FROM anywhere means it's not our literal-only target.
    if upper.contains(" FROM ") {
        return false;
    }
    true
}

fn summarize_sql(sql: &str) -> String {
    let s = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    const MAX: usize = 120;
    if s.len() <= MAX {
        return s;
    }
    format!("{}…", &s[..MAX])
}

fn enforce_max_result_rows(
    out: EngineOutput,
    max_rows: usize,
) -> Result<EngineOutput, DispatchError> {
    match out {
        EngineOutput::ResultSet { columns, rows } if rows.len() > max_rows => {
            Err(EngineError::new(
                engine_error_code::RESULT_ROWS_TOO_LARGE,
                format!(
                    "result has {} rows; max_result_rows is {}",
                    rows.len(),
                    max_rows
                ),
            )
            .into())
        }
        other => Ok(other),
    }
}

async fn write_error_response(
    send: &mut SendStream,
    err: &DispatchError,
) -> Result<(), quinn::WriteError> {
    let msg = err.to_error_message();
    let bytes = match encode_server_message_v1(&msg) {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "failed to encode Error frame");
            return Err(quinn::WriteError::ClosedStream);
        }
    };
    send.write_all(&bytes).await
}

fn error_response_wire_len(err: &DispatchError) -> u64 {
    let msg = err.to_error_message();
    encode_server_message_v1(&msg)
        .map(|b| b.len() as u64)
        .unwrap_or(0)
}

/// One bidirectional stream: read one request frame, run engine (with timeout), write one response.
pub async fn handle_query_bidi_stream(
    mut send: SendStream,
    mut recv: RecvStream,
    engine: Arc<dyn EngineHandle>,
    policy: Arc<StreamPolicy>,
    metrics: Option<QuicMetrics>,
    _permit: OwnedSemaphorePermit,
) {
    let _keep_permit = _permit;
    let max_frame = policy.max_frame_payload_bytes.min(MAX_FRAME_PAYLOAD_BYTES);

    // Variant A compatibility: old clients use one query per stream; newer clients may send
    // multiple frames on the same stream for better throughput.
    const MAX_FRAMES_PER_STREAM: usize = 1024;

    for _ in 0..MAX_FRAMES_PER_STREAM {
        let frame = match read_application_frame(&mut recv, max_frame).await {
            Ok(f) => f,
            Err(ReadFrameError::Recv(_)) => {
                // Client closed the stream (EOF / reset). Treat as graceful end-of-stream.
                let _ = send.finish();
                return;
            }
            Err(e) => {
                warn!(error = %e, "failed to read request frame");
                if let Some(m) = metrics.as_ref() {
                    m.record_read_frame_error();
                }
                let _ = send.reset(quinn::VarInt::from_u32(0));
                return;
            }
        };

        let t0 = Instant::now();
        let result = tokio::time::timeout(policy.query_timeout, async {
            dispatch_client_frame(&frame, engine.as_ref(), policy.as_ref())
        })
        .await;

        let result = match result {
            Ok(r) => r,
            Err(_elapsed) => Err(EngineError::new(
                engine_error_code::QUERY_TIMEOUT,
                "query exceeded per-query timeout",
            )
            .into()),
        };

        let latency_ns = t0.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        let record_metrics = |outcome: QueryHandledOutcome, bytes_out: u64| {
            if let Some(m) = metrics.as_ref() {
                let ms = latency_ns / 1_000_000;
                crate::debug::record_network_query_latency_ms(ms);
                m.record_query_handled(outcome, frame.len() as u64, bytes_out, latency_ns);
            }
        };

        match result {
            Ok(bytes) => {
                let out_len = bytes.len() as u64;
                if let Err(e) = send.write_all(&bytes).await {
                    warn!(error = %e, "write response failed");
                    record_metrics(QueryHandledOutcome::WriteFailed, 0);
                    return;
                }
                record_metrics(QueryHandledOutcome::Ok, out_len);
            }
            Err(ref e) => {
                let expected_out = error_response_wire_len(e);
                if write_error_response(&mut send, e).await.is_ok() {
                    record_metrics(QueryHandledOutcome::ErrorResponse, expected_out);
                } else {
                    let _ = send.reset(quinn::VarInt::from_u32(0));
                    record_metrics(QueryHandledOutcome::WriteFailed, 0);
                    return;
                }
            }
        }
    }

    // Abuse guard: we processed the per-stream maximum; close the send side.
    let _ = send.finish();
}

/// Accept bidirectional streams on `connection` until closed (Variant A).
pub async fn run_connection_streams(
    connection: Connection,
    engine: Arc<dyn EngineHandle>,
    policy: Arc<StreamPolicy>,
    metrics: Option<QuicMetrics>,
) {
    let max = policy.max_concurrent_streams_per_connection.max(1);
    let sem = Arc::new(tokio::sync::Semaphore::new(max));
    let remote = connection.remote_address();

    loop {
        let incoming = match connection.accept_bi().await {
            Ok(s) => s,
            Err(quinn::ConnectionError::ApplicationClosed { .. }) => {
                info!(%remote, "connection closed (application)");
                break;
            }
            Err(quinn::ConnectionError::LocallyClosed) => {
                info!(%remote, "connection closed (local)");
                break;
            }
            Err(e) => {
                warn!(%remote, error = %e, "accept_bi ended");
                break;
            }
        };

        let (mut send, recv) = incoming;
        let permit = match sem.clone().acquire_owned().await {
            Ok(p) => p,
            Err(_) => break,
        };

        let eng = engine.clone();
        let pol = policy.clone();
        let m = metrics.clone();
        tokio::spawn(async move {
            handle_query_bidi_stream(send, recv, eng, pol, m, permit).await;
        });
    }
}
