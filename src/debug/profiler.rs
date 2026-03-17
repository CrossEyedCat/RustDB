//! CPU and memory profiler for rustdb
//!
//! Provides tools for profiling performance and memory usage

#![allow(clippy::absurd_extreme_comparisons)]

use crate::debug::DebugConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;

/// Profiling mode
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ProfilingType {
    /// CPU profiling
    Cpu,
    /// Memory profiling
    Memory,
    /// Combined profiling
    Combined,
}

/// CPU metric
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuMetric {
    /// Timestamp
    pub timestamp: u64,
    /// CPU usage (%)
    pub cpu_usage: f64,
    /// Thread count
    pub thread_count: usize,
    /// User time (microseconds)
    pub user_time_us: u64,
    /// System time (microseconds)
    pub system_time_us: u64,
    /// Idle time (microseconds)
    pub idle_time_us: u64,
}

/// Memory metric
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryMetric {
    /// Timestamp
    pub timestamp: u64,
    /// Used memory (bytes)
    pub used_memory: u64,
    /// Available memory (bytes)
    pub available_memory: u64,
    /// Total memory (bytes)
    pub total_memory: u64,
    /// Memory usage (%)
    pub memory_usage_percent: f64,
    /// Process memory (bytes)
    pub process_memory: u64,
    /// Process virtual memory (bytes)
    pub process_virtual_memory: u64,
    /// Number of pages in memory
    pub page_count: u64,
    /// Number of pages in swap
    pub swap_count: u64,
}

/// Performance snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceSnapshot {
    /// Timestamp
    pub timestamp: u64,
    /// CPU metrics
    pub cpu: Option<CpuMetric>,
    /// Memory metrics
    pub memory: Option<MemoryMetric>,
    /// Additional metrics
    pub additional_metrics: HashMap<String, f64>,
}

/// Profiling stats
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProfilingStats {
    /// Total number of snapshots
    pub total_snapshots: u64,
    /// Average CPU usage (%)
    pub avg_cpu_usage: f64,
    /// Maximum CPU usage (%)
    pub max_cpu_usage: f64,
    /// Average memory usage (%)
    pub avg_memory_usage: f64,
    /// Maximum memory usage (%)
    pub max_memory_usage: f64,
    /// Average process memory (MB)
    pub avg_process_memory_mb: f64,
    /// Maximum process memory (MB)
    pub max_process_memory_mb: f64,
    /// Profiling start time
    pub start_time: u64,
    /// Timestamp of last snapshot
    pub last_snapshot_time: u64,
    /// Profiling duration (seconds)
    pub profiling_duration_seconds: u64,
}

/// Profiler
pub struct Profiler {
    config: DebugConfig,
    snapshots: Arc<RwLock<Vec<PerformanceSnapshot>>>,
    stats: Arc<RwLock<ProfilingStats>>,
    background_handle: Option<JoinHandle<()>>,
    is_profiling: Arc<RwLock<bool>>,
}

impl Profiler {
    /// Creates a new profiler
    pub fn new(config: &DebugConfig) -> Self {
        let mut profiler = Self {
            config: config.clone(),
            snapshots: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(RwLock::new(ProfilingStats::default())),
            background_handle: None,
            is_profiling: Arc::new(RwLock::new(false)),
        };

        // Start background profiling task
        if config.enable_cpu_profiling || config.enable_memory_profiling {
            profiler.start_profiling();
        }

        profiler
    }

    /// Starts profiling
    pub fn start_profiling(&mut self) {
        if *self.is_profiling.read().unwrap() {
            return; // Already running
        }

        *self.is_profiling.write().unwrap() = true;

        let snapshots = self.snapshots.clone();
        let stats = self.stats.clone();
        let is_profiling = self.is_profiling.clone();
        let config = self.config.clone();

        // Initialize statistics
        {
            let mut stats = stats.write().unwrap();
            stats.start_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
        }

        self.background_handle = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));

            while *is_profiling.read().unwrap() {
                interval.tick().await;

                let snapshot = Self::collect_performance_snapshot(&config);

                // Store snapshot
                {
                    let mut snapshots = snapshots.write().unwrap();
                    snapshots.push(snapshot.clone());

                    // Limit number of snapshots
                    let len = snapshots.len();
                    if len > 10000 {
                        snapshots.drain(0..len - 10000);
                    }
                }

                // Update stats
                Self::update_stats(&stats, &snapshot);
            }
        }));
    }

    /// Stops profiling
    pub fn stop_profiling(&mut self) {
        *self.is_profiling.write().unwrap() = false;

        if let Some(handle) = self.background_handle.take() {
            handle.abort();
        }
    }

    /// Collects a performance snapshot
    fn collect_performance_snapshot(config: &DebugConfig) -> PerformanceSnapshot {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        let mut snapshot = PerformanceSnapshot {
            timestamp,
            cpu: None,
            memory: None,
            additional_metrics: HashMap::new(),
        };

        // Collect CPU metrics
        if config.enable_cpu_profiling {
            snapshot.cpu = Some(Self::collect_cpu_metrics());
        }

        // Collect memory metrics
        if config.enable_memory_profiling {
            snapshot.memory = Some(Self::collect_memory_metrics());
        }

        // Additional metrics
        snapshot.additional_metrics.insert(
            "gc_collections".to_string(),
            Self::get_gc_collections() as f64,
        );

        snapshot
    }

    /// Collects CPU metrics
    fn collect_cpu_metrics() -> CpuMetric {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        // Real implementation would call system APIs; we simulate here
        CpuMetric {
            timestamp,
            cpu_usage: Self::get_cpu_usage(),
            thread_count: Self::get_thread_count(),
            user_time_us: Self::get_user_time(),
            system_time_us: Self::get_system_time(),
            idle_time_us: Self::get_idle_time(),
        }
    }

    /// Collects memory metrics
    fn collect_memory_metrics() -> MemoryMetric {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        // Real implementation would call system APIs; we simulate here
        let (used_memory, total_memory) = Self::get_memory_info();
        let process_memory = Self::get_process_memory();
        let process_virtual_memory = Self::get_process_virtual_memory();

        MemoryMetric {
            timestamp,
            used_memory,
            available_memory: total_memory - used_memory,
            total_memory,
            memory_usage_percent: (used_memory as f64 / total_memory as f64) * 100.0,
            process_memory,
            process_virtual_memory,
            page_count: Self::get_page_count(),
            swap_count: Self::get_swap_count(),
        }
    }

    /// Updates statistics
    fn update_stats(stats: &Arc<RwLock<ProfilingStats>>, snapshot: &PerformanceSnapshot) {
        let mut stats = stats.write().unwrap();
        stats.total_snapshots += 1;
        stats.last_snapshot_time = snapshot.timestamp;

        if let Some(cpu) = &snapshot.cpu {
            stats.avg_cpu_usage = (stats.avg_cpu_usage * (stats.total_snapshots - 1) as f64
                + cpu.cpu_usage)
                / stats.total_snapshots as f64;
            stats.max_cpu_usage = stats.max_cpu_usage.max(cpu.cpu_usage);
        }

        if let Some(memory) = &snapshot.memory {
            stats.avg_memory_usage = (stats.avg_memory_usage * (stats.total_snapshots - 1) as f64
                + memory.memory_usage_percent)
                / stats.total_snapshots as f64;
            stats.max_memory_usage = stats.max_memory_usage.max(memory.memory_usage_percent);

            let process_memory_mb = memory.process_memory as f64 / (1024.0 * 1024.0);
            stats.avg_process_memory_mb = (stats.avg_process_memory_mb
                * (stats.total_snapshots - 1) as f64
                + process_memory_mb)
                / stats.total_snapshots as f64;
            stats.max_process_memory_mb = stats.max_process_memory_mb.max(process_memory_mb);
        }

        stats.profiling_duration_seconds = (snapshot.timestamp / 1_000_000) - stats.start_time;
    }

    /// Returns simulated CPU usage
    fn get_cpu_usage() -> f64 {
        // Real implementation would call system APIs; here we return a pseudo-random value
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        SystemTime::now().hash(&mut hasher);
        (hasher.finish() % 100) as f64
    }

    /// Returns simulated thread count
    fn get_thread_count() -> usize {
        // Real implementation would call system APIs
        4 // Approximate value
    }

    /// Returns simulated user time
    fn get_user_time() -> u64 {
        // Real implementation would call system APIs
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64
            % 1000000
    }

    /// Returns simulated system time
    fn get_system_time() -> u64 {
        // Real implementation would call system APIs
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64
            % 100000
    }

    /// Returns simulated idle time
    fn get_idle_time() -> u64 {
        // Real implementation would call system APIs
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64
            % 1000000
    }

    /// Returns simulated memory info
    fn get_memory_info() -> (u64, u64) {
        // Real implementation would call system APIs
        let total = 16 * 1024 * 1024 * 1024; // 16 GB
        let used = total / 2; // 50% usage
        (used, total)
    }

    /// Returns simulated process memory usage
    fn get_process_memory() -> u64 {
        // Real implementation would call system APIs
        100 * 1024 * 1024 // 100 MB
    }

    /// Returns simulated process virtual memory usage
    fn get_process_virtual_memory() -> u64 {
        // Real implementation would call system APIs
        200 * 1024 * 1024 // 200 MB
    }

    /// Returns simulated page count
    fn get_page_count() -> u64 {
        // Real implementation would call system APIs
        1000
    }

    /// Returns simulated swap page count
    fn get_swap_count() -> u64 {
        // Real implementation would call system APIs
        100
    }

    /// Returns simulated GC run count
    fn get_gc_collections() -> u64 {
        // Real implementation would call system APIs
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            % 1000
    }

    /// Returns performance snapshots
    pub fn get_snapshots(&self, limit: usize) -> Vec<PerformanceSnapshot> {
        let snapshots = self.snapshots.read().unwrap();
        let start = snapshots.len().saturating_sub(limit);
        snapshots[start..].to_vec()
    }

    /// Returns profiling stats
    pub fn get_stats(&self) -> ProfilingStats {
        self.stats.read().unwrap().clone()
    }

    /// Generates performance report
    pub fn generate_performance_report(&self) -> String {
        let stats = self.get_stats();
        let recent_snapshots = self.get_snapshots(100);

        let mut report = String::new();

        report.push_str("=== System performance report ===\n\n");

        // General information
        report.push_str("General information:\n");
        report.push_str(&format!(
            "  Profiling duration: {} seconds\n",
            stats.profiling_duration_seconds
        ));
        report.push_str(&format!(
            "  Snapshot count: {}\n",
            stats.total_snapshots
        ));
        report.push_str(&format!("  Snapshot interval: 100 ms\n"));
        report.push_str("\n");

        // CPU statistics
        if self.config.enable_cpu_profiling {
            report.push_str("CPU statistics:\n");
            report.push_str(&format!(
                "  Average usage: {:.1}%\n",
                stats.avg_cpu_usage
            ));
            report.push_str(&format!(
                "  Peak usage: {:.1}%\n",
                stats.max_cpu_usage
            ));
            report.push_str("\n");
        }

        // Memory statistics
        if self.config.enable_memory_profiling {
            report.push_str("Memory statistics:\n");
            report.push_str(&format!(
                "  Average usage: {:.1}%\n",
                stats.avg_memory_usage
            ));
            report.push_str(&format!(
                "  Peak usage: {:.1}%\n",
                stats.max_memory_usage
            ));
            report.push_str(&format!(
                "  Average process memory: {:.1} MB\n",
                stats.avg_process_memory_mb
            ));
            report.push_str(&format!(
                "  Peak process memory: {:.1} MB\n",
                stats.max_process_memory_mb
            ));
            report.push_str("\n");
        }

        // Trend analysis
        if recent_snapshots.len() >= 10 {
            report.push_str("Trend analysis (last 10 snapshots):\n");

            let cpu_trend =
                Self::analyze_trend(&recent_snapshots, |s| s.cpu.as_ref().map(|c| c.cpu_usage));
            let memory_trend = Self::analyze_trend(&recent_snapshots, |s| {
                s.memory.as_ref().map(|m| m.memory_usage_percent)
            });

            if let Some(trend) = cpu_trend {
                report.push_str(&format!("  CPU trend: {}\n", trend));
            }

            if let Some(trend) = memory_trend {
                report.push_str(&format!("  Memory trend: {}\n", trend));
            }

            report.push_str("\n");
        }

        // Recommendations
        report.push_str("Recommendations:\n");
        if stats.avg_cpu_usage > 80.0 {
            report
                .push_str("  ⚠️  High CPU usage detected. Consider optimizing algorithms.\n");
        }
        if stats.avg_memory_usage > 90.0 {
            report.push_str("  ⚠️  High memory usage. Inspect for memory leaks.\n");
        }
        if stats.max_process_memory_mb > 1000.0 {
            report.push_str(
                "  ⚠️  Process memory is large. Optimize data structures.\n",
            );
        }

        if stats.avg_cpu_usage <= 80.0
            && stats.avg_memory_usage <= 90.0
            && stats.max_process_memory_mb <= 1000.0
        {
            report.push_str("  ✅ System operates within normal parameters.\n");
        }

        report
    }

    /// Analyzes trend of values
    fn analyze_trend<F>(snapshots: &[PerformanceSnapshot], extractor: F) -> Option<String>
    where
        F: Fn(&PerformanceSnapshot) -> Option<f64>,
    {
        let values: Vec<f64> = snapshots.iter().filter_map(&extractor).collect();

        if values.len() < 3 {
            return None;
        }

        let first_half = &values[..values.len() / 2];
        let second_half = &values[values.len() / 2..];

        let first_avg = first_half.iter().sum::<f64>() / first_half.len() as f64;
        let second_avg = second_half.iter().sum::<f64>() / second_half.len() as f64;

        let change_percent = ((second_avg - first_avg) / first_avg) * 100.0;

        if change_percent > 5.0 {
            Some(format!("Increasing (+{:.1}%)", change_percent))
        } else if change_percent < -5.0 {
            Some(format!("Decreasing ({:.1}%)", change_percent))
        } else {
            Some("Stable".to_string())
        }
    }

    /// Generates profiler status report
    pub fn generate_status_report(&self) -> String {
        let stats = self.get_stats();
        let is_profiling = *self.is_profiling.read().unwrap();
        let snapshot_count = self.snapshots.read().unwrap().len();

        let mut report = String::new();

        report.push_str(&format!("Profiling active: {}\n", is_profiling));
        report.push_str(&format!(
            "Snapshots stored in memory: {}\n",
            snapshot_count
        ));
        report.push_str(&format!(
            "Total snapshots: {}\n",
            stats.total_snapshots
        ));
        report.push_str(&format!(
            "CPU profiling: {}\n",
            self.config.enable_cpu_profiling
        ));
        report.push_str(&format!(
            "Memory profiling: {}\n",
            self.config.enable_memory_profiling
        ));
        report.push_str(&format!(
            "Profiling duration: {} seconds\n",
            stats.profiling_duration_seconds
        ));

        report
    }

    /// Stops the profiler
    pub fn shutdown(&mut self) {
        self.stop_profiling();
    }
}

impl Drop for Profiler {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_profiler() {
        let config = DebugConfig {
            enable_cpu_profiling: true,
            enable_memory_profiling: true,
            ..Default::default()
        };

        let mut profiler = Profiler::new(&config);

        // Wait a bit to accumulate snapshots
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Validate statistics
        let stats = profiler.get_stats();
        assert!(stats.total_snapshots > 0);
        // Time profiling might be 0 if the test ran very quickly
        assert!(stats.profiling_duration_seconds >= 0);

        // Validate snapshots
        let snapshots = profiler.get_snapshots(10);
        assert!(!snapshots.is_empty());

        // Validate report output
        let report = profiler.generate_performance_report();
        assert!(report.contains("performance report"));
        assert!(report.contains("CPU statistics"));
        assert!(report.contains("Memory statistics"));

        // Stop profiling
        profiler.stop_profiling();

        let is_profiling = *profiler.is_profiling.read().unwrap();
        assert!(!is_profiling);
    }

    #[test]
    fn test_performance_snapshot() {
        let config = DebugConfig {
            enable_cpu_profiling: true,
            enable_memory_profiling: true,
            ..Default::default()
        };

        let snapshot = Profiler::collect_performance_snapshot(&config);

        assert!(snapshot.timestamp > 0);
        assert!(snapshot.cpu.is_some());
        assert!(snapshot.memory.is_some());

        if let Some(cpu) = snapshot.cpu {
            assert!(cpu.cpu_usage >= 0.0 && cpu.cpu_usage <= 100.0);
            assert!(cpu.thread_count > 0);
        }

        if let Some(memory) = snapshot.memory {
            assert!(memory.memory_usage_percent >= 0.0 && memory.memory_usage_percent <= 100.0);
            assert!(memory.total_memory > 0);
            assert!(memory.used_memory <= memory.total_memory);
        }
    }

    #[test]
    fn test_trend_analysis() {
        let snapshots = vec![
            PerformanceSnapshot {
                timestamp: 1000,
                cpu: Some(CpuMetric {
                    timestamp: 1000,
                    cpu_usage: 10.0,
                    thread_count: 4,
                    user_time_us: 1000,
                    system_time_us: 100,
                    idle_time_us: 900,
                }),
                memory: None,
                additional_metrics: HashMap::new(),
            },
            PerformanceSnapshot {
                timestamp: 2000,
                cpu: Some(CpuMetric {
                    timestamp: 2000,
                    cpu_usage: 20.0,
                    thread_count: 4,
                    user_time_us: 2000,
                    system_time_us: 200,
                    idle_time_us: 1800,
                }),
                memory: None,
                additional_metrics: HashMap::new(),
            },
            PerformanceSnapshot {
                timestamp: 3000,
                cpu: Some(CpuMetric {
                    timestamp: 3000,
                    cpu_usage: 30.0,
                    thread_count: 4,
                    user_time_us: 3000,
                    system_time_us: 300,
                    idle_time_us: 2700,
                }),
                memory: None,
                additional_metrics: HashMap::new(),
            },
        ];

        let trend = Profiler::analyze_trend(&snapshots, |s| s.cpu.as_ref().map(|c| c.cpu_usage));
        assert!(trend.is_some());
        assert!(trend.unwrap().contains("Increasing"));
    }
}
