//! Network layer for rustdb

pub mod client;
pub mod connection;
pub mod engine;
pub mod framing;
pub mod metrics;
pub mod query_stream;
pub mod server;

pub use metrics::{QueryHandledOutcome, QuicNetworkMetrics, QuicNetworkSnapshot};

#[cfg(test)]
pub mod tests;

// TODO: Implement network layer
