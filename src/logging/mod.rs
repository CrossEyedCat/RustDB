//! Logging system for rustdb
//!
//! This module contains a complete logging system for the database:
//! - Write-Ahead Logging (WAL) to ensure ACID properties
//! - Structured log entries with various operation types
//! - Buffered log writing using optimized I/O
//! - Data recovery system from logs
//! - Checkpoints and log compression
//! - Performance monitoring and metrics

pub mod checkpoint;
pub mod compaction;
pub mod log_record;
pub mod log_writer;
pub mod metrics;
pub mod recovery;
pub mod wal;

#[cfg(test)]
pub mod tests;

pub use checkpoint::*;
pub use compaction::*;
pub use log_record::*;
pub use log_writer::*;
pub use metrics::*;
pub use recovery::*;
pub use wal::*;
