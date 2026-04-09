//! Network layer for rustdb

pub mod client;
pub mod connection;
pub mod engine;
pub mod framing;
pub mod metrics;
pub mod query_stream;
pub mod server;
pub mod sql_engine;
pub mod transport;

pub use metrics::{QueryHandledOutcome, QuicNetworkMetrics, QuicNetworkSnapshot};
pub use sql_engine::SqlEngine;

#[cfg(test)]
pub mod tests;

// QUIC server, framing, and engine hooks — see `docs/network/`.
