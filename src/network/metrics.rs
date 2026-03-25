//! Lightweight QUIC network counters for operations (no external metrics stack).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Result of handling one query stream after a full request frame was read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryHandledOutcome {
    /// Successful [`crate::network::framing::ServerMessage`] body written (ResultSet / ExecutionOk).
    Ok,
    /// An error frame was written (dispatch failure, timeout, etc.).
    ErrorResponse,
    /// The response could not be written (peer reset, closed stream, encode failure).
    WriteFailed,
}

/// Snapshot of [`QuicNetworkMetrics`] for tests and dashboards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuicNetworkSnapshot {
    pub active_connections: u64,
    pub handshakes_ok: u64,
    pub handshake_failures: u64,
    pub connections_refused: u64,
    pub read_frame_errors: u64,
    pub queries_handled: u64,
    pub queries_ok: u64,
    pub queries_error_response: u64,
    pub queries_write_failed: u64,
    pub bytes_received: u64,
    pub bytes_sent: u64,
    pub query_latency_sum_ns: u64,
}

/// Atomics updated from the accept loop and per-stream handlers.
#[derive(Debug)]
pub struct QuicNetworkMetrics {
    pub active_connections: AtomicU64,
    pub handshakes_ok: AtomicU64,
    pub handshake_failures: AtomicU64,
    pub connections_refused: AtomicU64,
    pub read_frame_errors: AtomicU64,
    pub queries_handled: AtomicU64,
    pub queries_ok: AtomicU64,
    pub queries_error_response: AtomicU64,
    pub queries_write_failed: AtomicU64,
    pub bytes_received: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub query_latency_sum_ns: AtomicU64,
}

impl Default for QuicNetworkMetrics {
    fn default() -> Self {
        Self {
            active_connections: AtomicU64::new(0),
            handshakes_ok: AtomicU64::new(0),
            handshake_failures: AtomicU64::new(0),
            connections_refused: AtomicU64::new(0),
            read_frame_errors: AtomicU64::new(0),
            queries_handled: AtomicU64::new(0),
            queries_ok: AtomicU64::new(0),
            queries_error_response: AtomicU64::new(0),
            queries_write_failed: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            query_latency_sum_ns: AtomicU64::new(0),
        }
    }
}

impl QuicNetworkMetrics {
    pub fn snapshot(&self) -> QuicNetworkSnapshot {
        QuicNetworkSnapshot {
            active_connections: self.active_connections.load(Ordering::Relaxed),
            handshakes_ok: self.handshakes_ok.load(Ordering::Relaxed),
            handshake_failures: self.handshake_failures.load(Ordering::Relaxed),
            connections_refused: self.connections_refused.load(Ordering::Relaxed),
            read_frame_errors: self.read_frame_errors.load(Ordering::Relaxed),
            queries_handled: self.queries_handled.load(Ordering::Relaxed),
            queries_ok: self.queries_ok.load(Ordering::Relaxed),
            queries_error_response: self.queries_error_response.load(Ordering::Relaxed),
            queries_write_failed: self.queries_write_failed.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            query_latency_sum_ns: self.query_latency_sum_ns.load(Ordering::Relaxed),
        }
    }

    pub fn record_read_frame_error(&self) {
        self.read_frame_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_query_handled(
        &self,
        outcome: QueryHandledOutcome,
        bytes_in: u64,
        bytes_out: u64,
        latency_ns: u64,
    ) {
        self.queries_handled.fetch_add(1, Ordering::Relaxed);
        match outcome {
            QueryHandledOutcome::Ok => {
                self.queries_ok.fetch_add(1, Ordering::Relaxed);
            }
            QueryHandledOutcome::ErrorResponse => {
                self.queries_error_response.fetch_add(1, Ordering::Relaxed);
            }
            QueryHandledOutcome::WriteFailed => {
                self.queries_write_failed.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.bytes_received.fetch_add(bytes_in, Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes_out, Ordering::Relaxed);
        self.query_latency_sum_ns
            .fetch_add(latency_ns, Ordering::Relaxed);
    }
}

/// Shared metrics handle (one per [`crate::network::server::QuicServer`]).
pub type QuicMetrics = Arc<QuicNetworkMetrics>;
