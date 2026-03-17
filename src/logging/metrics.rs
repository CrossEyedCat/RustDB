//! Metrics and monitoring for the logging subsystem of rustdb
//!
//! This module collects and exposes performance metrics for the
//! logging system:
//! - Operation counters and performance stats
//! - Timing metrics and latency
//! - Resource utilization and throughput monitoring
//! - Metric export for external monitoring systems

use crate::logging::log_record::LogRecordType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;

/// Operation counter
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OperationCounter {
    /// Total number of operations
    pub total: u64,
    /// Successful operations
    pub success: u64,
    /// Failed operations
    pub failed: u64,
    /// Operations per second (moving average)
    pub rate_per_second: f64,
    /// Timestamp of last update
    pub last_updated: u64,
}

impl OperationCounter {
    /// Increments successful operation counter
    pub fn increment_success(&mut self) {
        self.total += 1;
        self.success += 1;
        self.update_timestamp();
    }

    /// Increments failed operation counter
    pub fn increment_failed(&mut self) {
        self.total += 1;
        self.failed += 1;
        self.update_timestamp();
    }

    /// Updates the timestamp
    fn update_timestamp(&mut self) {
        self.last_updated = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    /// Returns success rate
    pub fn success_rate(&self) -> f64 {
        if self.total > 0 {
            self.success as f64 / self.total as f64
        } else {
            0.0
        }
    }
}

/// Timing metric
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TimingMetric {
    /// Total time (microseconds)
    pub total_time_us: u64,
    /// Number of samples
    pub count: u64,
    /// Minimum time
    pub min_time_us: u64,
    /// Maximum time
    pub max_time_us: u64,
    /// Average time
    pub avg_time_us: u64,
    /// 95th percentile
    pub p95_time_us: u64,
    /// 99th percentile
    pub p99_time_us: u64,
    /// Sample history (for percentile calculation)
    samples: Vec<u64>,
}

impl TimingMetric {
    /// Records a new timing sample
    pub fn record_time(&mut self, duration: Duration) {
        let time_us = duration.as_micros() as u64;

        self.total_time_us += time_us;
        self.count += 1;

        if self.count == 1 {
            self.min_time_us = time_us;
            self.max_time_us = time_us;
        } else {
            self.min_time_us = self.min_time_us.min(time_us);
            self.max_time_us = self.max_time_us.max(time_us);
        }

        self.avg_time_us = self.total_time_us / self.count;

        // Store sample for percentiles (limit size)
        self.samples.push(time_us);
        if self.samples.len() > 10000 {
            self.samples.remove(0);
        }

        self.update_percentiles();
    }

    /// Updates percentile values
    fn update_percentiles(&mut self) {
        if self.samples.is_empty() {
            return;
        }

        let mut sorted_samples = self.samples.clone();
        sorted_samples.sort_unstable();

        let len = sorted_samples.len();

        if len > 0 {
            let p95_index = ((len as f64) * 0.95) as usize;
            let p99_index = ((len as f64) * 0.99) as usize;

            self.p95_time_us = sorted_samples[p95_index.min(len - 1)];
            self.p99_time_us = sorted_samples[p99_index.min(len - 1)];
        }
    }

    /// Returns average time in milliseconds
    pub fn avg_time_ms(&self) -> f64 {
        self.avg_time_us as f64 / 1000.0
    }

    /// Returns throughput (operations per second)
    pub fn throughput_per_second(&self) -> f64 {
        if self.avg_time_us > 0 {
            1_000_000.0 / self.avg_time_us as f64
        } else {
            0.0
        }
    }
}

/// Size metric
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SizeMetric {
    /// Total size (bytes)
    pub total_bytes: u64,
    /// Number of items
    pub count: u64,
    /// Minimum size
    pub min_bytes: u64,
    /// Maximum size
    pub max_bytes: u64,
    /// Average size
    pub avg_bytes: u64,
}

impl SizeMetric {
    /// Records a new size sample
    pub fn record_size(&mut self, size_bytes: u64) {
        self.total_bytes += size_bytes;
        self.count += 1;

        if self.count == 1 {
            self.min_bytes = size_bytes;
            self.max_bytes = size_bytes;
        } else {
            self.min_bytes = self.min_bytes.min(size_bytes);
            self.max_bytes = self.max_bytes.max(size_bytes);
        }

        self.avg_bytes = self.total_bytes / self.count;
    }

    /// Returns total size in MB
    pub fn total_mb(&self) -> f64 {
        self.total_bytes as f64 / (1024.0 * 1024.0)
    }

    /// Returns average size in KB
    pub fn avg_kb(&self) -> f64 {
        self.avg_bytes as f64 / 1024.0
    }
}

/// Aggregated logging metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LoggingMetrics {
    /// Metrics collection start time
    pub start_time: u64,
    /// Last update timestamp
    pub last_updated: u64,
    /// System uptime in seconds
    pub uptime_seconds: u64,

    // Operation counters by category
    /// Transaction operations
    pub transaction_operations: HashMap<String, OperationCounter>,
    /// Data operations
    pub data_operations: HashMap<String, OperationCounter>,
    /// System operations
    pub system_operations: HashMap<String, OperationCounter>,

    // Timing metrics
    /// Log write time
    pub write_timing: TimingMetric,
    /// Sync time
    pub sync_timing: TimingMetric,
    /// Checkpoint creation time
    pub checkpoint_timing: TimingMetric,
    /// Recovery time
    pub recovery_timing: TimingMetric,

    // Size metrics
    /// Log record sizes
    pub log_record_size: SizeMetric,
    /// Log file sizes
    pub log_file_size: SizeMetric,

    // Specialized metrics
    /// Buffer utilization (%)
    pub buffer_utilization: f64,
    /// Cache hit ratio
    pub cache_hit_ratio: f64,
    /// Throughput (records/sec)
    pub throughput_records_per_sec: f64,
    /// Throughput (bytes/sec)
    pub throughput_bytes_per_sec: f64,
    /// Disk utilization (%)
    pub disk_utilization: f64,
    /// Log fragmentation (%)
    pub log_fragmentation: f64,
}

impl LoggingMetrics {
    /// Creates a new metrics collection
    pub fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            start_time: now,
            last_updated: now,
            uptime_seconds: 0,
            transaction_operations: HashMap::new(),
            data_operations: HashMap::new(),
            system_operations: HashMap::new(),
            write_timing: TimingMetric::default(),
            sync_timing: TimingMetric::default(),
            checkpoint_timing: TimingMetric::default(),
            recovery_timing: TimingMetric::default(),
            log_record_size: SizeMetric::default(),
            log_file_size: SizeMetric::default(),
            buffer_utilization: 0.0,
            cache_hit_ratio: 0.0,
            throughput_records_per_sec: 0.0,
            throughput_bytes_per_sec: 0.0,
            disk_utilization: 0.0,
            log_fragmentation: 0.0,
        }
    }

    /// Updates last update timestamp and uptime
    pub fn update_timestamp(&mut self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.last_updated = now;
        self.uptime_seconds = now.saturating_sub(self.start_time);
    }

    /// Records a log operation
    pub fn record_log_operation(
        &mut self,
        record_type: LogRecordType,
        success: bool,
        duration: Duration,
        size: u64,
    ) {
        self.update_timestamp();

        // Update counters grouped by operation type
        let operation_name = match record_type {
            LogRecordType::TransactionBegin => "transaction_begin",
            LogRecordType::TransactionCommit => "transaction_commit",
            LogRecordType::TransactionAbort => "transaction_abort",
            LogRecordType::DataInsert => "data_insert",
            LogRecordType::DataUpdate => "data_update",
            LogRecordType::DataDelete => "data_delete",
            LogRecordType::Checkpoint => "checkpoint",
            LogRecordType::CheckpointEnd => "checkpoint_end",
            LogRecordType::Compaction => "compaction",
            LogRecordType::FileCreate => "file_create",
            LogRecordType::FileDelete => "file_delete",
            LogRecordType::FileExtend => "file_extend",
            LogRecordType::MetadataUpdate => "metadata_update",
        };

        let counter = match record_type {
            LogRecordType::TransactionBegin
            | LogRecordType::TransactionCommit
            | LogRecordType::TransactionAbort => self
                .transaction_operations
                .entry(operation_name.to_string())
                .or_default(),
            LogRecordType::DataInsert | LogRecordType::DataUpdate | LogRecordType::DataDelete => {
                self.data_operations
                    .entry(operation_name.to_string())
                    .or_default()
            }
            _ => self
                .system_operations
                .entry(operation_name.to_string())
                .or_default(),
        };

        if success {
            counter.increment_success();
        } else {
            counter.increment_failed();
        }

        // Update timing metrics
        self.write_timing.record_time(duration);

        // Update size metrics
        self.log_record_size.record_size(size);

        // Recalculate throughput
        self.recalculate_throughput();
    }

    /// Records a sync operation
    pub fn record_sync_operation(&mut self, duration: Duration, success: bool) {
        self.update_timestamp();

        let counter = self
            .system_operations
            .entry("sync".to_string())
            .or_default();
        if success {
            counter.increment_success();
        } else {
            counter.increment_failed();
        }

        self.sync_timing.record_time(duration);
    }

    /// Records a checkpoint operation
    pub fn record_checkpoint_operation(&mut self, duration: Duration, success: bool) {
        self.update_timestamp();

        let counter = self
            .system_operations
            .entry("checkpoint".to_string())
            .or_default();
        if success {
            counter.increment_success();
        } else {
            counter.increment_failed();
        }

        self.checkpoint_timing.record_time(duration);
    }

    /// Records a recovery operation
    pub fn record_recovery_operation(&mut self, duration: Duration, success: bool) {
        self.update_timestamp();

        let counter = self
            .system_operations
            .entry("recovery".to_string())
            .or_default();
        if success {
            counter.increment_success();
        } else {
            counter.increment_failed();
        }

        self.recovery_timing.record_time(duration);
    }

    /// Updates buffer utilization
    pub fn update_buffer_utilization(&mut self, utilization: f64) {
        self.buffer_utilization = utilization.clamp(0.0, 100.0);
        self.update_timestamp();
    }

    /// Updates cache hit ratio
    pub fn update_cache_hit_ratio(&mut self, hit_ratio: f64) {
        self.cache_hit_ratio = hit_ratio.clamp(0.0, 1.0);
        self.update_timestamp();
    }

    /// Updates disk utilization
    pub fn update_disk_utilization(&mut self, utilization: f64) {
        self.disk_utilization = utilization.clamp(0.0, 100.0);
        self.update_timestamp();
    }

    /// Updates log fragmentation
    pub fn update_log_fragmentation(&mut self, fragmentation: f64) {
        self.log_fragmentation = fragmentation.clamp(0.0, 100.0);
        self.update_timestamp();
    }

    /// Recalculates throughput
    fn recalculate_throughput(&mut self) {
        if self.uptime_seconds > 0 {
            self.throughput_records_per_sec =
                self.log_record_size.count as f64 / self.uptime_seconds as f64;
            self.throughput_bytes_per_sec =
                self.log_record_size.total_bytes as f64 / self.uptime_seconds as f64;
        }
    }

    /// Returns total number of operations
    pub fn total_operations(&self) -> u64 {
        let tx_ops: u64 = self.transaction_operations.values().map(|c| c.total).sum();
        let data_ops: u64 = self.data_operations.values().map(|c| c.total).sum();
        let sys_ops: u64 = self.system_operations.values().map(|c| c.total).sum();
        tx_ops + data_ops + sys_ops
    }

    /// Returns overall success rate
    pub fn overall_success_rate(&self) -> f64 {
        let total_success: u64 = self
            .transaction_operations
            .values()
            .map(|c| c.success)
            .sum::<u64>()
            + self
                .data_operations
                .values()
                .map(|c| c.success)
                .sum::<u64>()
            + self
                .system_operations
                .values()
                .map(|c| c.success)
                .sum::<u64>();

        let total_ops = self.total_operations();

        if total_ops > 0 {
            total_success as f64 / total_ops as f64
        } else {
            0.0
        }
    }

    /// Returns top operations by count
    pub fn top_operations_by_count(&self, limit: usize) -> Vec<(String, u64)> {
        let mut all_operations = Vec::new();

        for (name, counter) in &self.transaction_operations {
            all_operations.push((format!("tx_{}", name), counter.total));
        }

        for (name, counter) in &self.data_operations {
            all_operations.push((format!("data_{}", name), counter.total));
        }

        for (name, counter) in &self.system_operations {
            all_operations.push((format!("sys_{}", name), counter.total));
        }

        all_operations.sort_by(|a, b| b.1.cmp(&a.1));
        all_operations.into_iter().take(limit).collect()
    }

    /// Exports metrics in Prometheus format
    pub fn export_prometheus(&self) -> String {
        let mut output = String::new();

        // Base metrics
        output.push_str(&format!(
            "# HELP rustdb_logging_uptime_seconds Uptime of the logging system\n"
        ));
        output.push_str(&format!("# TYPE rustdb_logging_uptime_seconds counter\n"));
        output.push_str(&format!(
            "rustdb_logging_uptime_seconds {}\n",
            self.uptime_seconds
        ));

        output.push_str(&format!(
            "# HELP rustdb_logging_operations_total Total number of logging operations\n"
        ));
        output.push_str(&format!("# TYPE rustdb_logging_operations_total counter\n"));
        output.push_str(&format!(
            "rustdb_logging_operations_total {}\n",
            self.total_operations()
        ));

        // Timing metrics
        output.push_str(&format!(
            "# HELP rustdb_logging_write_duration_microseconds Write operation duration\n"
        ));
        output.push_str(&format!(
            "# TYPE rustdb_logging_write_duration_microseconds histogram\n"
        ));
        output.push_str(&format!(
            "rustdb_logging_write_duration_microseconds_avg {}\n",
            self.write_timing.avg_time_us
        ));
        output.push_str(&format!(
            "rustdb_logging_write_duration_microseconds_p95 {}\n",
            self.write_timing.p95_time_us
        ));
        output.push_str(&format!(
            "rustdb_logging_write_duration_microseconds_p99 {}\n",
            self.write_timing.p99_time_us
        ));

        // Resource metrics
        output.push_str(&format!(
            "# HELP rustdb_logging_buffer_utilization_percent Buffer utilization percentage\n"
        ));
        output.push_str(&format!(
            "# TYPE rustdb_logging_buffer_utilization_percent gauge\n"
        ));
        output.push_str(&format!(
            "rustdb_logging_buffer_utilization_percent {}\n",
            self.buffer_utilization
        ));

        output.push_str(&format!(
            "# HELP rustdb_logging_cache_hit_ratio Cache hit ratio\n"
        ));
        output.push_str(&format!("# TYPE rustdb_logging_cache_hit_ratio gauge\n"));
        output.push_str(&format!(
            "rustdb_logging_cache_hit_ratio {}\n",
            self.cache_hit_ratio
        ));

        // Throughput metrics
        output.push_str(&format!(
            "# HELP rustdb_logging_throughput_records_per_second Records processed per second\n"
        ));
        output.push_str(&format!(
            "# TYPE rustdb_logging_throughput_records_per_second gauge\n"
        ));
        output.push_str(&format!(
            "rustdb_logging_throughput_records_per_second {}\n",
            self.throughput_records_per_sec
        ));

        output
    }

    /// Exports metrics in JSON format
    pub fn export_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Generates a performance report
    pub fn generate_performance_report(&self) -> String {
        let mut report = String::new();

        report.push_str("=== rustdb logging system performance report ===\n\n");

        // General information
        report.push_str(&format!(
            "Uptime: {} seconds ({:.1} hours)\n",
            self.uptime_seconds,
            self.uptime_seconds as f64 / 3600.0
        ));
        report.push_str(&format!("Total operations: {}\n", self.total_operations()));
        report.push_str(&format!(
            "Success rate: {:.2}%\n",
            self.overall_success_rate() * 100.0
        ));
        report.push_str("\n");

        // Performance
        report.push_str("=== Performance ===\n");
        report.push_str(&format!(
            "Throughput: {:.1} records/sec\n",
            self.throughput_records_per_sec
        ));
        report.push_str(&format!(
            "Throughput: {:.1} MB/sec\n",
            self.throughput_bytes_per_sec / (1024.0 * 1024.0)
        ));
        report.push_str(&format!(
            "Average write time: {:.2} ms\n",
            self.write_timing.avg_time_ms()
        ));
        report.push_str(&format!(
            "95th percentile write time: {:.2} ms\n",
            self.write_timing.p95_time_us as f64 / 1000.0
        ));
        report.push_str("\n");

        // Resources
        report.push_str("=== Resource utilization ===\n");
        report.push_str(&format!(
            "Buffer utilization: {:.1}%\n",
            self.buffer_utilization
        ));
        report.push_str(&format!(
            "Cache hit ratio: {:.2}%\n",
            self.cache_hit_ratio * 100.0
        ));
        report.push_str(&format!(
            "Disk utilization: {:.1}%\n",
            self.disk_utilization
        ));
        report.push_str(&format!(
            "Log fragmentation: {:.1}%\n",
            self.log_fragmentation
        ));
        report.push_str("\n");

        // Top operations
        report.push_str("=== Top operations ===\n");
        let top_ops = self.top_operations_by_count(5);
        for (i, (name, count)) in top_ops.iter().enumerate() {
            report.push_str(&format!("{}. {}: {} operations\n", i + 1, name, count));
        }
        report.push_str("\n");

        // Sizes
        report.push_str("=== Data sizes ===\n");
        report.push_str(&format!(
            "Average log record size: {:.1} KB\n",
            self.log_record_size.avg_kb()
        ));
        report.push_str(&format!(
            "Total log volume: {:.1} MB\n",
            self.log_record_size.total_mb()
        ));
        report.push_str(&format!(
            "Average log file size: {:.1} MB\n",
            self.log_file_size.total_mb() / self.log_file_size.count.max(1) as f64
        ));

        report
    }
}

/// Logging metrics manager
pub struct LoggingMetricsManager {
    /// Metrics
    metrics: Arc<RwLock<LoggingMetrics>>,
    /// Background update task
    background_handle: Option<JoinHandle<()>>,
}

impl LoggingMetricsManager {
    /// Creates a new metrics manager
    pub fn new() -> Self {
        let metrics = Arc::new(RwLock::new(LoggingMetrics::new()));

        let mut manager = Self {
            metrics: metrics.clone(),
            background_handle: None,
        };

        // Start background metrics update task
        manager.start_background_updates();

        manager
    }

    /// Starts background updates
    fn start_background_updates(&mut self) {
        let metrics = self.metrics.clone();

        self.background_handle = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));

            loop {
                interval.tick().await;

                {
                    let mut m = metrics.write().unwrap();
                    m.update_timestamp();
                }
            }
        }));
    }

    /// Records a log operation
    pub fn record_log_operation(
        &self,
        record_type: LogRecordType,
        success: bool,
        duration: Duration,
        size: u64,
    ) {
        let mut metrics = self.metrics.write().unwrap();
        metrics.record_log_operation(record_type, success, duration, size);
    }

    /// Records a sync operation
    pub fn record_sync_operation(&self, duration: Duration, success: bool) {
        let mut metrics = self.metrics.write().unwrap();
        metrics.record_sync_operation(duration, success);
    }

    /// Records a checkpoint operation
    pub fn record_checkpoint_operation(&self, duration: Duration, success: bool) {
        let mut metrics = self.metrics.write().unwrap();
        metrics.record_checkpoint_operation(duration, success);
    }

    /// Updates resource metrics
    pub fn update_resource_metrics(
        &self,
        buffer_util: f64,
        cache_hit_ratio: f64,
        disk_util: f64,
        fragmentation: f64,
    ) {
        let mut metrics = self.metrics.write().unwrap();
        metrics.update_buffer_utilization(buffer_util);
        metrics.update_cache_hit_ratio(cache_hit_ratio);
        metrics.update_disk_utilization(disk_util);
        metrics.update_log_fragmentation(fragmentation);
    }

    /// Returns a metrics snapshot
    pub fn get_metrics(&self) -> LoggingMetrics {
        self.metrics.read().unwrap().clone()
    }

    /// Exports metrics in Prometheus format
    pub fn export_prometheus(&self) -> String {
        let metrics = self.metrics.read().unwrap();
        metrics.export_prometheus()
    }

    /// Exports metrics in JSON format
    pub fn export_json(&self) -> Result<String, serde_json::Error> {
        let metrics = self.metrics.read().unwrap();
        metrics.export_json()
    }

    /// Generates a performance report
    pub fn generate_performance_report(&self) -> String {
        let metrics = self.metrics.read().unwrap();
        metrics.generate_performance_report()
    }

    /// Resets all metrics
    pub fn reset_metrics(&self) {
        let mut metrics = self.metrics.write().unwrap();
        *metrics = LoggingMetrics::new();
    }

    /// Shuts down the metrics manager
    pub fn shutdown(&mut self) {
        if let Some(handle) = self.background_handle.take() {
            handle.abort();
        }
    }
}

impl Drop for LoggingMetricsManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_counter() {
        let mut counter = OperationCounter::default();

        counter.increment_success();
        counter.increment_success();
        counter.increment_failed();

        assert_eq!(counter.total, 3);
        assert_eq!(counter.success, 2);
        assert_eq!(counter.failed, 1);
        assert!((counter.success_rate() - 0.6666666666666666).abs() < f64::EPSILON);
    }

    #[test]
    fn test_timing_metric() {
        let mut timing = TimingMetric::default();

        timing.record_time(Duration::from_millis(100));
        timing.record_time(Duration::from_millis(200));
        timing.record_time(Duration::from_millis(150));

        assert_eq!(timing.count, 3);
        assert_eq!(timing.min_time_us, 100_000);
        assert_eq!(timing.max_time_us, 200_000);
        assert_eq!(timing.avg_time_us, 150_000);
        assert_eq!(timing.avg_time_ms(), 150.0);
    }

    #[test]
    fn test_size_metric() {
        let mut size = SizeMetric::default();

        size.record_size(1024);
        size.record_size(2048);
        size.record_size(1536);

        assert_eq!(size.count, 3);
        assert_eq!(size.total_bytes, 4608);
        assert_eq!(size.avg_bytes, 1536);
        assert_eq!(size.min_bytes, 1024);
        assert_eq!(size.max_bytes, 2048);
    }

    #[tokio::test]
    async fn test_logging_metrics() {
        let mut metrics = LoggingMetrics::new();

        // Record several operations
        metrics.record_log_operation(
            LogRecordType::TransactionBegin,
            true,
            Duration::from_millis(10),
            256,
        );

        metrics.record_log_operation(
            LogRecordType::DataInsert,
            true,
            Duration::from_millis(5),
            512,
        );

        metrics.record_sync_operation(Duration::from_millis(50), true);

        assert_eq!(metrics.total_operations(), 3);
        assert_eq!(metrics.overall_success_rate(), 1.0);
        assert_eq!(metrics.log_record_size.count, 2);
        assert_eq!(metrics.log_record_size.total_bytes, 768);

        // Verify top operations
        let top_ops = metrics.top_operations_by_count(5);
        assert!(!top_ops.is_empty());
    }

    #[tokio::test]
    async fn test_metrics_manager() {
        let manager = LoggingMetricsManager::new();

        manager.record_log_operation(
            LogRecordType::TransactionCommit,
            true,
            Duration::from_millis(15),
            1024,
        );

        manager.update_resource_metrics(75.0, 0.85, 60.0, 5.0);

        let metrics = manager.get_metrics();
        assert!(metrics.total_operations() > 0);
        assert_eq!(metrics.buffer_utilization, 75.0);
        assert_eq!(metrics.cache_hit_ratio, 0.85);

        // Test export
        let prometheus_export = manager.export_prometheus();
        assert!(prometheus_export.contains("rustdb_logging"));

        let json_export = manager.export_json().unwrap();
        assert!(json_export.contains("uptime_seconds"));

        let report = manager.generate_performance_report();
        assert!(report.contains("performance report"));
    }

    #[test]
    fn test_prometheus_export() {
        let metrics = LoggingMetrics::new();
        let export = metrics.export_prometheus();

        assert!(export.contains("# HELP"));
        assert!(export.contains("# TYPE"));
        assert!(export.contains("rustdb_logging_uptime_seconds"));
        assert!(export.contains("rustdb_logging_operations_total"));
    }

    #[test]
    fn test_performance_report() {
        let mut metrics = LoggingMetrics::new();

        // Add test data
        metrics.record_log_operation(
            LogRecordType::DataUpdate,
            true,
            Duration::from_millis(20),
            2048,
        );

        let report = metrics.generate_performance_report();

        assert!(report.contains("performance report"));
        assert!(report.contains("Uptime"));
        assert!(report.contains("Total operations"));
        assert!(report.contains("Performance"));
        assert!(report.contains("Resource utilization"));
    }
}
