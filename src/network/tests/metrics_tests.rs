//! Unit tests for [`crate::network::metrics`] (no QUIC).

use crate::network::metrics::{QueryHandledOutcome, QuicNetworkMetrics};

#[test]
fn snapshot_starts_zero() {
    let m = QuicNetworkMetrics::default();
    let s = m.snapshot();
    assert_eq!(s.active_connections, 0);
    assert_eq!(s.handshakes_ok, 0);
    assert_eq!(s.handshake_failures, 0);
    assert_eq!(s.connections_refused, 0);
    assert_eq!(s.read_frame_errors, 0);
    assert_eq!(s.queries_handled, 0);
    assert_eq!(s.queries_ok, 0);
    assert_eq!(s.queries_error_response, 0);
    assert_eq!(s.queries_write_failed, 0);
    assert_eq!(s.bytes_received, 0);
    assert_eq!(s.bytes_sent, 0);
    assert_eq!(s.query_latency_sum_ns, 0);
}

#[test]
fn read_frame_errors_increment() {
    let m = QuicNetworkMetrics::default();
    m.record_read_frame_error();
    m.record_read_frame_error();
    assert_eq!(m.snapshot().read_frame_errors, 2);
}

#[test]
fn query_ok_updates_bytes_and_latency() {
    let m = QuicNetworkMetrics::default();
    m.record_query_handled(QueryHandledOutcome::Ok, 100, 200, 50);
    let s = m.snapshot();
    assert_eq!(s.queries_handled, 1);
    assert_eq!(s.queries_ok, 1);
    assert_eq!(s.queries_error_response, 0);
    assert_eq!(s.queries_write_failed, 0);
    assert_eq!(s.bytes_received, 100);
    assert_eq!(s.bytes_sent, 200);
    assert_eq!(s.query_latency_sum_ns, 50);
}

#[test]
fn query_error_response_counted_separately() {
    let m = QuicNetworkMetrics::default();
    m.record_query_handled(QueryHandledOutcome::ErrorResponse, 10, 30, 15);
    let s = m.snapshot();
    assert_eq!(s.queries_handled, 1);
    assert_eq!(s.queries_ok, 0);
    assert_eq!(s.queries_error_response, 1);
    assert_eq!(s.queries_write_failed, 0);
}

#[test]
fn query_write_failed_counted_separately() {
    let m = QuicNetworkMetrics::default();
    m.record_query_handled(QueryHandledOutcome::WriteFailed, 5, 0, 9);
    let s = m.snapshot();
    assert_eq!(s.queries_handled, 1);
    assert_eq!(s.queries_ok, 0);
    assert_eq!(s.queries_error_response, 0);
    assert_eq!(s.queries_write_failed, 1);
}

#[test]
fn multiple_queries_aggregate() {
    let m = QuicNetworkMetrics::default();
    m.record_query_handled(QueryHandledOutcome::Ok, 1, 2, 10);
    m.record_query_handled(QueryHandledOutcome::ErrorResponse, 3, 4, 20);
    m.record_query_handled(QueryHandledOutcome::WriteFailed, 5, 0, 30);
    let s = m.snapshot();
    assert_eq!(s.queries_handled, 3);
    assert_eq!(s.queries_ok, 1);
    assert_eq!(s.queries_error_response, 1);
    assert_eq!(s.queries_write_failed, 1);
    assert_eq!(s.bytes_received, 1 + 3 + 5);
    assert_eq!(s.bytes_sent, 2 + 4 + 0);
    assert_eq!(s.query_latency_sum_ns, 10 + 20 + 30);
}
