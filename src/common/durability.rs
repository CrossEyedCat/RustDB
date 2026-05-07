//! Durability policy shared across embedded and network APIs.
//!
//! Durability policy is **safe-by-default** for the embedded API, while the network/server
//! engine defaults to higher throughput unless explicitly overridden (see `SqlEngineConfig::default`).

use serde::{Deserialize, Serialize};

/// Durability policy for commit/recovery markers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DurabilityMode {
    /// Safe default: wait for durability at commit points (fsync / sync_all where applicable).
    Safe,
    /// Higher throughput: do not wait for fsync at commit points.
    Fast,
}

impl DurabilityMode {
    /// Whether commit points should wait for `fsync`/`sync_all`.
    pub fn fsync_on_commit(self) -> bool {
        matches!(self, Self::Safe)
    }
}

impl Default for DurabilityMode {
    fn default() -> Self {
        Self::Safe
    }
}
