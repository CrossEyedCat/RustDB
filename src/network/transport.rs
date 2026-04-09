//! Shared QUIC [`quinn::TransportConfig`] for RustDB server and clients.
//!
//! Keeps flow-control and stream limits aligned so the application semaphore in
//! [`crate::network::query_stream::run_connection_streams`] is not stricter than the peer's QUIC
//! advertised limits (both are derived from the same `max_concurrent_streams_per_connection`).

use std::sync::Arc;
use std::time::Duration;

use quinn::{IdleTimeout, TransportConfig, VarInt};

/// Build transport parameters shared by the listener and load tools (`rustdb_load`, etc.).
///
/// - **Bidirectional streams:** `max_concurrent_bidi_streams` matches the application cap (clamped
///   to quinn's supported range). The app's per-connection semaphore uses the same cap, so the QUIC
///   limit never rejects streams before the app would throttle.
/// - **Keep-alive:** a fraction of `idle_timeout` so middleboxes are less likely to drop idle DB
///   sessions (negotiated idle is the minimum with the peer).
/// - **Send fairness:** disabled — quinn documents this as helpful when many small streams carry
///   request/response pairs (less scheduling overhead).
pub fn build_rustdb_transport_config(
    max_concurrent_streams_app: usize,
    idle_timeout: Duration,
) -> Result<TransportConfig, quinn::VarIntBoundsExceeded> {
    let idle = IdleTimeout::try_from(idle_timeout)?;
    let mut t = TransportConfig::default();
    t.max_idle_timeout(Some(idle));

    let n = (max_concurrent_streams_app.max(1) as u32).clamp(4, 10_000);
    t.max_concurrent_bidi_streams(VarInt::from(n));

    t.send_fairness(false);

    let ka_secs = idle_timeout.as_secs().max(6) / 3;
    let ka_secs = ka_secs.clamp(2, 60);
    t.keep_alive_interval(Some(Duration::from_secs(ka_secs)));

    Ok(t)
}

/// Wrap a [`TransportConfig`] for use on both sides of a connection.
pub fn transport_config_arc(
    max_concurrent_streams_app: usize,
    idle_timeout: Duration,
) -> Result<Arc<TransportConfig>, quinn::VarIntBoundsExceeded> {
    Ok(Arc::new(build_rustdb_transport_config(
        max_concurrent_streams_app,
        idle_timeout,
    )?))
}
