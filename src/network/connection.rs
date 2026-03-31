//! Connection management for rustdb

use crate::common::{Error, Result};
use std::time::Duration;

/// Connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    New,
    Connected,
    Authenticated,
    Closed,
}

/// Connection configuration
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub buffer_size: usize,
    pub enable_keepalive: bool,
    pub keepalive_interval: Duration,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            buffer_size: 8192,
            enable_keepalive: false,
            keepalive_interval: Duration::from_secs(60),
        }
    }
}

/// Client connection
pub struct Connection {
    config: ConnectionConfig,
    state: ConnectionState,
}

impl Connection {
    pub fn new(config: ConnectionConfig) -> Result<Self> {
        Ok(Self {
            config,
            state: ConnectionState::New,
        })
    }

    pub fn state(&self) -> ConnectionState {
        self.state
    }
}

/// Simple bounded pool placeholder (same config for every slot).
pub struct ConnectionPool {
    config: ConnectionConfig,
    max_size: usize,
}

impl ConnectionPool {
    pub fn new() -> Result<Self> {
        Ok(Self {
            config: ConnectionConfig::default(),
            max_size: 8,
        })
    }

    pub fn with_config(config: ConnectionConfig, max_size: usize) -> Result<Self> {
        Ok(Self { config, max_size })
    }

    pub fn max_size(&self) -> usize {
        self.max_size
    }

    pub fn config(&self) -> &ConnectionConfig {
        &self.config
    }
}
