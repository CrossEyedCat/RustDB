//! Detailed debug logger for rustdb
//!
//! Provides advanced logging of operations with multiple verbosity levels

use crate::debug::DebugConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;

/// Logging verbosity level
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LogLevel {
    /// Critical errors only
    Critical = 0,
    /// Errors
    Error = 1,
    /// Warnings
    Warning = 2,
    /// Informational messages
    Info = 3,
    /// Debug information
    Debug = 4,
    /// Maximum detail
    Trace = 5,
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogLevel::Critical => write!(f, "CRITICAL"),
            LogLevel::Error => write!(f, "ERROR"),
            LogLevel::Warning => write!(f, "WARNING"),
            LogLevel::Info => write!(f, "INFO"),
            LogLevel::Debug => write!(f, "DEBUG"),
            LogLevel::Trace => write!(f, "TRACE"),
        }
    }
}

/// Log entry category
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogCategory {
    /// Transaction operations
    Transaction,
    /// Data operations
    Data,
    /// Index operations
    Index,
    /// Buffer operations
    Buffer,
    /// File operations
    File,
    /// Network operations
    Network,
    /// Query parsing/planning
    Query,
    /// System operations
    System,
    /// Recovery operations
    Recovery,
    /// Logging operations
    Logging,
}

impl std::fmt::Display for LogCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogCategory::Transaction => write!(f, "TX"),
            LogCategory::Data => write!(f, "DATA"),
            LogCategory::Index => write!(f, "INDEX"),
            LogCategory::Buffer => write!(f, "BUFFER"),
            LogCategory::File => write!(f, "FILE"),
            LogCategory::Network => write!(f, "NET"),
            LogCategory::Query => write!(f, "QUERY"),
            LogCategory::System => write!(f, "SYSTEM"),
            LogCategory::Recovery => write!(f, "RECOVERY"),
            LogCategory::Logging => write!(f, "LOG"),
        }
    }
}

/// Debug log entry structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebugLogEntry {
    /// Timestamp (microseconds since Unix epoch)
    pub timestamp: u64,
    /// Log level
    pub level: LogLevel,
    /// Category
    pub category: LogCategory,
    /// System component
    pub component: String,
    /// Message
    pub message: String,
    /// Additional data (JSON)
    pub data: Option<serde_json::Value>,
    /// Thread ID
    pub thread_id: u64,
    /// Transaction ID (if applicable)
    pub transaction_id: Option<u64>,
    /// Query ID (if applicable)
    pub query_id: Option<String>,
    /// Operation duration (microseconds)
    pub duration_us: Option<u64>,
    /// Data size (bytes)
    pub data_size: Option<u64>,
}

impl DebugLogEntry {
    /// Creates a new log entry
    pub fn new(level: LogLevel, category: LogCategory, component: &str, message: &str) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        let thread_id = 0; // Temporary placeholder for compatibility

        Self {
            timestamp,
            level,
            category,
            component: component.to_string(),
            message: message.to_string(),
            data: None,
            thread_id,
            transaction_id: None,
            query_id: None,
            duration_us: None,
            data_size: None,
        }
    }

    /// Attaches structured data
    pub fn with_data(mut self, data: serde_json::Value) -> Self {
        self.data = Some(data);
        self
    }

    /// Sets transaction ID
    pub fn with_transaction_id(mut self, tx_id: u64) -> Self {
        self.transaction_id = Some(tx_id);
        self
    }

    /// Sets query ID
    pub fn with_query_id(mut self, query_id: &str) -> Self {
        self.query_id = Some(query_id.to_string());
        self
    }

    /// Sets operation duration
    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.duration_us = Some(duration.as_micros() as u64);
        self
    }

    /// Sets associated data size
    pub fn with_data_size(mut self, size: u64) -> Self {
        self.data_size = Some(size);
        self
    }

    /// Formats entry for output
    pub fn format(&self) -> String {
        let mut formatted = String::new();

        // Timestamp
        let datetime = SystemTime::UNIX_EPOCH + Duration::from_micros(self.timestamp);
        let datetime_str = format!("{:?}", datetime);
        formatted.push_str(&format!("[{}] ", datetime_str));

        // Level and category
        formatted.push_str(&format!("{}:{} ", self.level, self.category));

        // Component
        formatted.push_str(&format!("[{}] ", self.component));

        // Transaction and query identifiers
        if let Some(tx_id) = self.transaction_id {
            formatted.push_str(&format!("TX:{} ", tx_id));
        }
        if let Some(query_id) = &self.query_id {
            formatted.push_str(&format!("Q:{} ", query_id));
        }

        // Duration
        if let Some(duration) = self.duration_us {
            formatted.push_str(&format!("({}μs) ", duration));
        }

        // Data size
        if let Some(size) = self.data_size {
            formatted.push_str(&format!("[{}B] ", size));
        }

        // Message body
        formatted.push_str(&self.message);

        // Additional payload
        if let Some(data) = &self.data {
            formatted.push_str(&format!(" | Data: {}", data));
        }

        formatted
    }
}

/// Logging statistics snapshot
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LoggingStats {
    /// Total number of entries
    pub total_entries: u64,
    /// Entries per level
    pub entries_by_level: HashMap<String, u64>,
    /// Entries per category
    pub entries_by_category: HashMap<String, u64>,
    /// Entries per component
    pub entries_by_component: HashMap<String, u64>,
    /// Timestamp of last entry
    pub last_entry_time: u64,
    /// Log file size (bytes)
    pub log_file_size: u64,
    /// Number of write errors
    pub write_errors: u64,
}

/// Debug logger implementation
pub struct DebugLogger {
    config: DebugConfig,
    log_file: Arc<Mutex<Option<BufWriter<File>>>>,
    stats: Arc<RwLock<LoggingStats>>,
    background_handle: Option<JoinHandle<()>>,
    log_buffer: Arc<Mutex<Vec<DebugLogEntry>>>,
    buffer_size: usize,
}

impl DebugLogger {
    /// Creates new debug logger
    pub fn new(config: &DebugConfig) -> Self {
        let mut logger = Self {
            config: config.clone(),
            log_file: Arc::new(Mutex::new(None)),
            stats: Arc::new(RwLock::new(LoggingStats::default())),
            background_handle: None,
            log_buffer: Arc::new(Mutex::new(Vec::new())),
            buffer_size: 1000,
        };

        // Initialize log file
        logger.initialize_log_file();

        // Start background task
        logger.start_background_task();

        logger
    }

    /// Initializes the log file handle
    fn initialize_log_file(&mut self) {
        let log_path = Path::new("debug.log");

        match OpenOptions::new().create(true).append(true).open(log_path) {
            Ok(file) => {
                let writer = BufWriter::new(file);
                *self.log_file.lock().unwrap() = Some(writer);
            }
            Err(e) => {
                eprintln!("Failed to create log file: {}", e);
            }
        }
    }

    /// Launches background writer task
    fn start_background_task(&mut self) {
        let log_file = self.log_file.clone();
        let stats = self.stats.clone();
        let log_buffer = self.log_buffer.clone();
        let buffer_size = self.buffer_size;

        self.background_handle = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));

            loop {
                interval.tick().await;

                // Retrieve buffered entries
                let entries = {
                    let mut buffer = log_buffer.lock().unwrap();
                    if buffer.len() >= buffer_size || !buffer.is_empty() {
                        let entries = buffer.drain(..).collect::<Vec<_>>();
                        entries
                    } else {
                        continue;
                    }
                };

                // Write to file
                if let Some(writer) = log_file.lock().unwrap().as_mut() {
                    for entry in entries {
                        if let Err(e) = writeln!(writer, "{}", entry.format()) {
                            eprintln!("Failed to write to log: {}", e);
                            let mut stats = stats.write().unwrap();
                            stats.write_errors += 1;
                        }
                    }

                    if let Err(e) = writer.flush() {
                        eprintln!("Failed to flush log buffer: {}", e);
                    }
                }
            }
        }));
    }

    /// Writes a single entry
    pub fn log(&self, entry: DebugLogEntry) {
        // Respect configured detail level
        if entry.level as u8 > self.config.detail_level {
            return;
        }

        // Buffer entry
        {
            let mut buffer = self.log_buffer.lock().unwrap();
            buffer.push(entry.clone());
        }

        // Update statistics
        {
            let mut stats = self.stats.write().unwrap();
            stats.total_entries += 1;
            stats.last_entry_time = entry.timestamp;

            *stats
                .entries_by_level
                .entry(entry.level.to_string())
                .or_default() += 1;
            *stats
                .entries_by_category
                .entry(entry.category.to_string())
                .or_default() += 1;
            *stats
                .entries_by_component
                .entry(entry.component.clone())
                .or_default() += 1;
        }

        // Echo critical errors to stderr
        if entry.level == LogLevel::Critical || entry.level == LogLevel::Error {
            eprintln!("{}", entry.format());
        }
    }

    /// Creates an entry with specified parameters
    pub fn create_entry(
        &self,
        level: LogLevel,
        category: LogCategory,
        component: &str,
        message: &str,
    ) -> DebugLogEntry {
        DebugLogEntry::new(level, category, component, message)
    }

    /// Logs transaction-related operation
    pub fn log_transaction_operation(
        &self,
        level: LogLevel,
        operation: &str,
        transaction_id: u64,
        duration: Option<Duration>,
        data_size: Option<u64>,
    ) {
        let mut entry = self.create_entry(
            level,
            LogCategory::Transaction,
            "TransactionManager",
            &format!("Transaction {}: {}", transaction_id, operation),
        );

        entry = entry.with_transaction_id(transaction_id);

        if let Some(dur) = duration {
            entry = entry.with_duration(dur);
        }

        if let Some(size) = data_size {
            entry = entry.with_data_size(size);
        }

        self.log(entry);
    }

    /// Logs data operation
    pub fn log_data_operation(
        &self,
        level: LogLevel,
        operation: &str,
        table: &str,
        duration: Option<Duration>,
        data_size: Option<u64>,
    ) {
        let mut entry = self.create_entry(
            level,
            LogCategory::Data,
            "DataManager",
            &format!("Table '{}': {}", table, operation),
        );

        if let Some(dur) = duration {
            entry = entry.with_duration(dur);
        }

        if let Some(size) = data_size {
            entry = entry.with_data_size(size);
        }

        self.log(entry);
    }

    /// Logs query operation
    pub fn log_query_operation(
        &self,
        level: LogLevel,
        operation: &str,
        query_id: &str,
        duration: Option<Duration>,
        data_size: Option<u64>,
    ) {
        let mut entry = self.create_entry(
            level,
            LogCategory::Query,
            "QueryEngine",
            &format!("Query {}: {}", query_id, operation),
        );

        entry = entry.with_query_id(query_id);

        if let Some(dur) = duration {
            entry = entry.with_duration(dur);
        }

        if let Some(size) = data_size {
            entry = entry.with_data_size(size);
        }

        self.log(entry);
    }

    /// Logs system-level operation
    pub fn log_system_operation(
        &self,
        level: LogLevel,
        component: &str,
        operation: &str,
        duration: Option<Duration>,
    ) {
        let mut entry = self.create_entry(level, LogCategory::System, component, operation);

        if let Some(dur) = duration {
            entry = entry.with_duration(dur);
        }

        self.log(entry);
    }

    /// Returns logging statistics
    pub fn get_stats(&self) -> LoggingStats {
        self.stats.read().unwrap().clone()
    }

    /// Generates logger status report
    pub fn generate_status_report(&self) -> String {
        let stats = self.get_stats();
        let mut report = String::new();

        report.push_str(&format!("Total entries: {}\n", stats.total_entries));
        report.push_str(&format!("Log file size: {} bytes\n", stats.log_file_size));
        report.push_str(&format!("Write errors: {}\n", stats.write_errors));

        if !stats.entries_by_level.is_empty() {
            report.push_str("Entries by level:\n");
            for (level, count) in &stats.entries_by_level {
                report.push_str(&format!("  {}: {}\n", level, count));
            }
        }

        if !stats.entries_by_category.is_empty() {
            report.push_str("Entries by category:\n");
            for (category, count) in &stats.entries_by_category {
                report.push_str(&format!("  {}: {}\n", category, count));
            }
        }

        report
    }

    /// Shuts down logger
    pub fn shutdown(&mut self) {
        if let Some(handle) = self.background_handle.take() {
            handle.abort();
        }

        // Flush remaining entries
        if let Some(writer) = self.log_file.lock().unwrap().as_mut() {
            let _ = writer.flush();
        }
    }
}

impl Drop for DebugLogger {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_log_entry_creation() {
        let entry = DebugLogEntry::new(
            LogLevel::Info,
            LogCategory::Transaction,
            "TestComponent",
            "Test message",
        );

        assert_eq!(entry.level, LogLevel::Info);
        assert!(matches!(entry.category, LogCategory::Transaction));
        assert_eq!(entry.component, "TestComponent");
        assert_eq!(entry.message, "Test message");
        assert!(entry.timestamp > 0);
    }

    #[test]
    fn test_log_entry_with_data() {
        let data = serde_json::json!({"key": "value"});
        let entry = DebugLogEntry::new(
            LogLevel::Debug,
            LogCategory::Data,
            "TestComponent",
            "Test message",
        )
        .with_data(data.clone())
        .with_transaction_id(123)
        .with_duration(Duration::from_millis(100))
        .with_data_size(1024);

        assert_eq!(entry.data, Some(data));
        assert_eq!(entry.transaction_id, Some(123));
        assert_eq!(entry.duration_us, Some(100_000));
        assert_eq!(entry.data_size, Some(1024));
    }

    #[test]
    fn test_log_entry_formatting() {
        let entry = DebugLogEntry::new(
            LogLevel::Error,
            LogCategory::System,
            "TestComponent",
            "Test error message",
        )
        .with_transaction_id(456)
        .with_duration(Duration::from_micros(500))
        .with_data_size(2048);

        let formatted = entry.format();
        assert!(formatted.contains("ERROR:SYSTEM"));
        assert!(formatted.contains("TestComponent"));
        assert!(formatted.contains("TX:456"));
        assert!(formatted.contains("(500μs)"));
        assert!(formatted.contains("[2048B]"));
        assert!(formatted.contains("Test error message"));
    }

    #[tokio::test]
    async fn test_debug_logger() {
        let config = DebugConfig {
            enable_debug_logging: true,
            detail_level: 5,
            ..Default::default()
        };

        let logger = DebugLogger::new(&config);

        // Exercise various logging helpers
        logger.log_transaction_operation(
            LogLevel::Info,
            "BEGIN",
            123,
            Some(Duration::from_millis(10)),
            Some(256),
        );

        logger.log_data_operation(
            LogLevel::Debug,
            "INSERT",
            "users",
            Some(Duration::from_millis(5)),
            Some(512),
        );

        logger.log_query_operation(
            LogLevel::Info,
            "EXECUTE",
            "query_001",
            Some(Duration::from_millis(50)),
            Some(1024),
        );

        logger.log_system_operation(
            LogLevel::Warning,
            "BufferManager",
            "Buffer overflow detected",
            Some(Duration::from_millis(1)),
        );

        // Validate statistics
        let stats = logger.get_stats();
        assert!(stats.total_entries >= 4);
        assert!(stats.entries_by_level.contains_key("INFO"));
        assert!(stats.entries_by_level.contains_key("DEBUG"));
        assert!(stats.entries_by_level.contains_key("WARNING"));
        assert!(stats.entries_by_category.contains_key("TX"));
        assert!(stats.entries_by_category.contains_key("DATA"));
        assert!(stats.entries_by_category.contains_key("QUERY"));
        assert!(stats.entries_by_category.contains_key("SYSTEM"));

        // Validate status report
        let report = logger.generate_status_report();
        assert!(report.contains("Total entries"));
        assert!(report.contains("Entries by level"));
        assert!(report.contains("Entries by category"));
    }
}
