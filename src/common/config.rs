//! Configuration for rustdb
//!
//! Provides configuration structures for various system components

use crate::common::i18n::Language;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Main database configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Database name
    pub name: String,
    /// Data storage directory
    pub data_directory: String,
    /// Maximum number of connections
    pub max_connections: usize,
    /// Connection timeout (in seconds)
    pub connection_timeout: u64,
    /// Query timeout (in seconds)
    pub query_timeout: u64,
    /// Interface language
    pub language: Language,
    /// QUIC / network listener (used by `rustdb server`; see `docs/network/`).
    #[serde(default)]
    pub network: NetworkConfig,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            name: "rustdb".to_string(),
            data_directory: "./data".to_string(),
            max_connections: 100,
            connection_timeout: 30,
            query_timeout: 60,
            language: Language::English,
            network: NetworkConfig::default(),
        }
    }
}

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Page size in bytes
    pub page_size: usize,
    /// Buffer pool size
    pub buffer_pool_size: usize,
    /// Checkpoint creation interval
    pub checkpoint_interval: Duration,
    /// Enable WAL
    pub wal_enabled: bool,
    /// Enable compression
    pub compression_enabled: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            page_size: 8192,
            buffer_pool_size: 1000,
            checkpoint_interval: Duration::from_secs(300),
            wal_enabled: true,
            compression_enabled: false,
        }
    }
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Logging level
    pub level: String,
    /// Log file
    pub file: PathBuf,
    /// Maximum log file size
    pub max_file_size: String,
    /// Maximum number of log files
    pub max_files: usize,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            file: PathBuf::from("./logs/rustdb.log"),
            max_file_size: "100MB".to_string(),
            max_files: 10,
        }
    }
}

/// Network configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// Host to listen on
    pub host: String,
    /// Port to listen on
    pub port: u16,
    /// Maximum number of connections
    pub max_connections: usize,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 5432,
            max_connections: 100,
        }
    }
}

/// Performance configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Lock timeout
    pub lock_timeout: Duration,
    /// Deadlock detection interval
    pub deadlock_detection_interval: Duration,
    /// Maximum query plan cache size
    pub max_query_plan_cache_size: usize,
    /// Enable query optimization
    pub enable_query_optimization: bool,
    /// Enable parallel execution (parallel table scan, parallel join branches)
    pub enable_parallel_execution: bool,
    /// Number of worker threads for parallel execution
    pub num_worker_threads: usize,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            lock_timeout: Duration::from_secs(10),
            deadlock_detection_interval: Duration::from_millis(1000),
            max_query_plan_cache_size: 1000,
            enable_query_optimization: true,
            enable_parallel_execution: true,
            num_worker_threads: 4,
        }
    }
}

impl DatabaseConfig {
    /// Loads configuration from a TOML file
    pub fn from_file(path: &PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: DatabaseConfig = toml::from_str(&content)?;
        Ok(config)
    }

    /// Saves configuration to a TOML file
    pub fn to_file(&self, path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
        let content = toml::to_string_pretty(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    /// Loads configuration from environment variables
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let mut config = DatabaseConfig::default();

        if let Ok(name) = std::env::var("RUSTDB_NAME") {
            config.name = name;
        }

        if let Ok(data_dir) = std::env::var("RUSTDB_DATA_DIR") {
            config.data_directory = data_dir;
        }

        if let Ok(max_conn) = std::env::var("RUSTDB_MAX_CONNECTIONS") {
            config.max_connections = max_conn.parse()?;
        }

        if let Ok(lang) = std::env::var("RUSTDB_LANGUAGE") {
            config.language = lang.parse()?;
        }

        // if let Ok(host) = std::env::var("RUSTDB_HOST") {
        //     config.network.host = host;
        // }

        // if let Ok(port) = std::env::var("RUSTDB_PORT") {
        //     config.network.port = port.parse()?;
        // }

        // if let Ok(log_level) = std::env::var("RUSTDB_LOG_LEVEL") {
        //     config.logging.level = log_level;
        // }

        Ok(config)
    }

    /// Merges configuration with another
    pub fn merge(mut self, other: Self) -> Self {
        if other.name != "rustdb" {
            self.name = other.name;
        }
        if other.data_directory != "./data" {
            self.data_directory = other.data_directory;
        }
        if other.max_connections != 100 {
            self.max_connections = other.max_connections;
        }
        if other.connection_timeout != 30 {
            self.connection_timeout = other.connection_timeout;
        }
        if other.query_timeout != 60 {
            self.query_timeout = other.query_timeout;
        }
        if other.language != Language::English {
            self.language = other.language;
        }
        self.network = self.network.clone().merge(other.network.clone());

        // Merge nested configs
        // self.storage = self.storage.merge(other.storage);
        // self.logging = self.logging.merge(other.logging);
        // self.network = self.network.merge(other.network);
        // self.performance = self.performance.merge(other.performance);

        self
    }

    /// Validates the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() {
            return Err("Database name cannot be empty".to_string());
        }

        if self.max_connections == 0 {
            return Err("Max connections must be greater than 0".to_string());
        }

        // if self.storage.page_size == 0 {
        //     return Err("Page size must be greater than 0".to_string());
        // }

        // if self.storage.buffer_pool_size == 0 {
        //     return Err("Buffer pool size must be greater than 0".to_string());
        // }

        // if self.network.port == 0 {
        //     return Err("Port must be greater than 0".to_string());
        // }

        // if self.network.max_connections == 0 {
        //     return Err("Network max connections must be greater than 0".to_string());
        // }

        Ok(())
    }
}

impl StorageConfig {
    fn merge(mut self, other: Self) -> Self {
        if other.page_size != 8192 {
            self.page_size = other.page_size;
        }
        if other.buffer_pool_size != 1000 {
            self.buffer_pool_size = other.buffer_pool_size;
        }
        if other.checkpoint_interval != Duration::from_secs(300) {
            self.checkpoint_interval = other.checkpoint_interval;
        }
        if other.wal_enabled != true {
            self.wal_enabled = other.wal_enabled;
        }
        if other.compression_enabled != false {
            self.compression_enabled = other.compression_enabled;
        }
        self
    }
}

impl LoggingConfig {
    fn merge(mut self, other: Self) -> Self {
        let default = Self::default();
        if other.level != default.level {
            self.level = other.level;
        }
        if other.file != default.file {
            self.file = other.file;
        }
        if other.max_file_size != default.max_file_size {
            self.max_file_size = other.max_file_size;
        }
        if other.max_files != default.max_files {
            self.max_files = other.max_files;
        }
        self
    }
}

impl NetworkConfig {
    fn merge(mut self, other: Self) -> Self {
        let default = Self::default();
        if other.host != default.host {
            self.host = other.host;
        }
        if other.port != default.port {
            self.port = other.port;
        }
        if other.max_connections != default.max_connections {
            self.max_connections = other.max_connections;
        }
        self
    }
}

impl PerformanceConfig {
    fn merge(mut self, other: Self) -> Self {
        if other.lock_timeout != Duration::from_secs(10) {
            self.lock_timeout = other.lock_timeout;
        }
        if other.deadlock_detection_interval != Duration::from_millis(1000) {
            self.deadlock_detection_interval = other.deadlock_detection_interval;
        }
        if other.max_query_plan_cache_size != 1000 {
            self.max_query_plan_cache_size = other.max_query_plan_cache_size;
        }
        if other.enable_query_optimization != true {
            self.enable_query_optimization = other.enable_query_optimization;
        }
        if other.enable_parallel_execution != true {
            self.enable_parallel_execution = other.enable_parallel_execution;
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tempfile::TempDir;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn test_default_config() {
        let config = DatabaseConfig::default();
        assert_eq!(config.name, "rustdb");
        assert_eq!(config.max_connections, 100);
        assert_eq!(config.language, Language::English);
        assert_eq!(config.connection_timeout, 30);
        assert_eq!(config.network.port, 5432);
        assert_eq!(config.network.host, "127.0.0.1");
    }

    #[test]
    fn test_config_validation() {
        let mut config = DatabaseConfig::default();
        assert!(config.validate().is_ok());

        config.name = String::new();
        assert!(config.validate().is_err());

        config = DatabaseConfig::default();
        config.max_connections = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_merge() {
        let mut config1 = DatabaseConfig::default();
        let mut config2 = DatabaseConfig::default();

        config2.name = "testdb".to_string();
        config2.connection_timeout = 60;
        config2.language = Language::English;

        let merged = config1.merge(config2);
        assert_eq!(merged.name, "testdb");
        assert_eq!(merged.connection_timeout, 60);
        assert_eq!(merged.language, Language::English);
    }

    #[test]
    fn test_config_from_env() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("RUSTDB_NAME", "envdb");
        std::env::set_var("RUSTDB_DATA_DIR", "C:/data");
        std::env::set_var("RUSTDB_MAX_CONNECTIONS", "123");
        std::env::set_var("RUSTDB_LANGUAGE", "en");

        let c = DatabaseConfig::from_env().expect("from_env");
        assert_eq!(c.name, "envdb");
        assert_eq!(c.data_directory, "C:/data");
        assert_eq!(c.max_connections, 123);
        assert_eq!(c.language, Language::English);

        std::env::remove_var("RUSTDB_NAME");
        std::env::remove_var("RUSTDB_DATA_DIR");
        std::env::remove_var("RUSTDB_MAX_CONNECTIONS");
        std::env::remove_var("RUSTDB_LANGUAGE");
    }

    #[test]
    fn test_config_toml_roundtrip_file() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("cfg.toml");

        let mut c = DatabaseConfig::default();
        c.name = "filedb".to_string();
        c.max_connections = 321;
        c.network.port = 7777;
        c.to_file(&path).expect("to_file");

        let loaded = DatabaseConfig::from_file(&path).expect("from_file");
        assert_eq!(loaded.name, "filedb");
        assert_eq!(loaded.max_connections, 321);
        assert_eq!(loaded.network.port, 7777);
    }

    #[test]
    fn test_merge_helpers_storage_logging_network_performance() {
        let s1 = StorageConfig::default();
        let mut s2 = StorageConfig::default();
        s2.page_size = 4096;
        s2.buffer_pool_size = 1;
        s2.checkpoint_interval = Duration::from_secs(1);
        s2.wal_enabled = false;
        s2.compression_enabled = true;
        let sm = s1.merge(s2);
        assert_eq!(sm.page_size, 4096);
        assert_eq!(sm.buffer_pool_size, 1);
        assert_eq!(sm.checkpoint_interval, Duration::from_secs(1));
        assert!(!sm.wal_enabled);
        assert!(sm.compression_enabled);

        let l1 = LoggingConfig::default();
        let mut l2 = LoggingConfig::default();
        l2.level = "debug".to_string();
        l2.file = PathBuf::from("x.log");
        l2.max_file_size = "1KB".to_string();
        l2.max_files = 1;
        let lm = l1.merge(l2);
        assert_eq!(lm.level, "debug");
        assert_eq!(lm.file, PathBuf::from("x.log"));
        assert_eq!(lm.max_file_size, "1KB");
        assert_eq!(lm.max_files, 1);

        let n1 = NetworkConfig::default();
        let mut n2 = NetworkConfig::default();
        n2.host = "0.0.0.0".to_string();
        n2.port = 4242;
        n2.max_connections = 5;
        let nm = n1.merge(n2);
        assert_eq!(nm.host, "0.0.0.0");
        assert_eq!(nm.port, 4242);
        assert_eq!(nm.max_connections, 5);

        let p1 = PerformanceConfig::default();
        let mut p2 = PerformanceConfig::default();
        p2.lock_timeout = Duration::from_secs(1);
        p2.deadlock_detection_interval = Duration::from_millis(10);
        p2.max_query_plan_cache_size = 10;
        p2.enable_query_optimization = false;
        p2.enable_parallel_execution = false;
        let pm = p1.merge(p2);
        assert_eq!(pm.lock_timeout, Duration::from_secs(1));
        assert_eq!(pm.deadlock_detection_interval, Duration::from_millis(10));
        assert_eq!(pm.max_query_plan_cache_size, 10);
        assert!(!pm.enable_query_optimization);
        assert!(!pm.enable_parallel_execution);
    }
}
