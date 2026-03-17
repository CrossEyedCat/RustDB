//! Performance analyzer for rustdb
//!
//! Provides tools for analyzing system performance and identifying bottlenecks

use crate::debug::DebugConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;

/// Bottleneck type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BottleneckType {
    /// CPU bottleneck
    Cpu,
    /// Memory bottleneck
    Memory,
    /// I/O bottleneck
    Io,
    /// Network bottleneck
    Network,
    /// Lock contention
    Locking,
    /// Cache
    Cache,
}

impl std::fmt::Display for BottleneckType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BottleneckType::Cpu => write!(f, "CPU"),
            BottleneckType::Memory => write!(f, "Memory"),
            BottleneckType::Io => write!(f, "I/O"),
            BottleneckType::Network => write!(f, "Network"),
            BottleneckType::Locking => write!(f, "Locking"),
            BottleneckType::Cache => write!(f, "Cache"),
        }
    }
}

/// Severity level of a bottleneck
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SeverityLevel {
    /// Informational
    Info,
    /// Warning
    Warning,
    /// Critical
    Critical,
}

impl std::fmt::Display for SeverityLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SeverityLevel::Info => write!(f, "INFO"),
            SeverityLevel::Warning => write!(f, "WARNING"),
            SeverityLevel::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// Detected bottleneck
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bottleneck {
    /// Bottleneck type
    pub bottleneck_type: BottleneckType,
    /// Severity level
    pub severity: SeverityLevel,
    /// Problem description
    pub description: String,
    /// Recommendations
    pub recommendations: Vec<String>,
    /// Detection timestamp
    pub detected_at: u64,
    /// System component
    pub component: String,
    /// Additional metrics
    pub metrics: HashMap<String, f64>,
    /// Performance impact (%)
    pub performance_impact: f64,
}

/// Performance metric
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetric {
    /// Metric name
    pub name: String,
    /// Value
    pub value: f64,
    /// Unit of measure
    pub unit: String,
    /// Timestamp
    pub timestamp: u64,
    /// Component name
    pub component: String,
    /// Threshold values
    pub thresholds: Thresholds,
}

/// Threshold configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Thresholds {
    /// Warning threshold
    pub warning: f64,
    /// Critical threshold
    pub critical: f64,
}

/// Performance analysis report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceAnalysis {
    /// Analysis timestamp
    pub timestamp: u64,
    /// Overall performance score (0-100)
    pub overall_score: f64,
    /// Detected bottlenecks
    pub bottlenecks: Vec<Bottleneck>,
    /// Performance metrics
    pub metrics: Vec<PerformanceMetric>,
    /// Recommendations
    pub recommendations: Vec<String>,
    /// Trends
    pub trends: HashMap<String, String>,
}

/// Analysis statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AnalysisStats {
    /// Total analyses executed
    pub total_analyses: u64,
    /// Total bottlenecks detected
    pub total_bottlenecks: u64,
    /// Critical issues found
    pub critical_issues: u64,
    /// Average performance score
    pub avg_performance_score: f64,
    /// Timestamp of last analysis
    pub last_analysis_time: u64,
}

/// Performance analyzer
pub struct PerformanceAnalyzer {
    config: DebugConfig,
    analyses: Arc<RwLock<Vec<PerformanceAnalysis>>>,
    stats: Arc<RwLock<AnalysisStats>>,
    background_handle: Option<JoinHandle<()>>,
    metrics_history: Arc<RwLock<Vec<PerformanceMetric>>>,
}

impl PerformanceAnalyzer {
    /// Creates a new performance analyzer
    pub fn new(config: &DebugConfig) -> Self {
        let mut analyzer = Self {
            config: config.clone(),
            analyses: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(RwLock::new(AnalysisStats::default())),
            background_handle: None,
            metrics_history: Arc::new(RwLock::new(Vec::new())),
        };

        // Start background analysis task
        analyzer.start_background_analysis();

        analyzer
    }

    /// Starts background analysis job
    fn start_background_analysis(&mut self) {
        let analyses = self.analyses.clone();
        let stats = self.stats.clone();
        let metrics_history = self.metrics_history.clone();
        let config = self.config.clone();

        self.background_handle = Some(tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(config.metrics_collection_interval));

            loop {
                interval.tick().await;

                // Collect metrics
                let metrics = Self::collect_performance_metrics();

                // Append to history
                {
                    let mut history = metrics_history.write().unwrap();
                    history.extend(metrics.clone());

                    // Limit history size
                    let len = history.len();
                    if len > 10000 {
                        history.drain(0..len - 10000);
                    }
                }

                // Perform analysis
                let analysis = Self::perform_analysis(&metrics);

                // Store analysis
                {
                    let mut analyses = analyses.write().unwrap();
                    analyses.push(analysis.clone());

                    // Limit number of saved analyses
                    let len = analyses.len();
                    if len > 1000 {
                        analyses.drain(0..len - 1000);
                    }
                }

                // Update statistics snapshot
                Self::update_analysis_stats(&stats, &analysis);
            }
        }));
    }

    /// Collects performance metrics
    fn collect_performance_metrics() -> Vec<PerformanceMetric> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        vec![
            PerformanceMetric {
                name: "cpu_usage".to_string(),
                value: Self::get_cpu_usage(),
                unit: "%".to_string(),
                timestamp,
                component: "System".to_string(),
                thresholds: Thresholds {
                    warning: 70.0,
                    critical: 90.0,
                },
            },
            PerformanceMetric {
                name: "memory_usage".to_string(),
                value: Self::get_memory_usage(),
                unit: "%".to_string(),
                timestamp,
                component: "System".to_string(),
                thresholds: Thresholds {
                    warning: 80.0,
                    critical: 95.0,
                },
            },
            PerformanceMetric {
                name: "disk_io".to_string(),
                value: Self::get_disk_io(),
                unit: "MB/s".to_string(),
                timestamp,
                component: "Storage".to_string(),
                thresholds: Thresholds {
                    warning: 100.0,
                    critical: 200.0,
                },
            },
            PerformanceMetric {
                name: "network_io".to_string(),
                value: Self::get_network_io(),
                unit: "MB/s".to_string(),
                timestamp,
                component: "Network".to_string(),
                thresholds: Thresholds {
                    warning: 50.0,
                    critical: 100.0,
                },
            },
            PerformanceMetric {
                name: "cache_hit_ratio".to_string(),
                value: Self::get_cache_hit_ratio(),
                unit: "%".to_string(),
                timestamp,
                component: "Cache".to_string(),
                thresholds: Thresholds {
                    warning: 80.0,
                    critical: 60.0,
                },
            },
            PerformanceMetric {
                name: "lock_contention".to_string(),
                value: Self::get_lock_contention(),
                unit: "%".to_string(),
                timestamp,
                component: "Concurrency".to_string(),
                thresholds: Thresholds {
                    warning: 20.0,
                    critical: 40.0,
                },
            },
        ]
    }

    /// Performs performance analysis
    fn perform_analysis(metrics: &[PerformanceMetric]) -> PerformanceAnalysis {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        let mut bottlenecks = Vec::new();
        let mut recommendations = Vec::new();
        let mut trends = HashMap::new();
        let mut overall_score = 100.0;

        // Analyze each metric
        for metric in metrics {
            let mut metric_impact = 0.0;
            let mut metric_bottlenecks = Vec::new();

            // Check critical thresholds
            if metric.value >= metric.thresholds.critical {
                metric_impact = 30.0;
                metric_bottlenecks.push(Bottleneck {
                    bottleneck_type: Self::get_bottleneck_type(&metric.name),
                    severity: SeverityLevel::Critical,
                    description: format!(
                        "{} exceeds critical threshold: {:.1}{} (threshold: {:.1}{})",
                        metric.name,
                        metric.value,
                        metric.unit,
                        metric.thresholds.critical,
                        metric.unit
                    ),
                    recommendations: Self::get_recommendations(
                        &metric.name,
                        SeverityLevel::Critical,
                    ),
                    detected_at: timestamp,
                    component: metric.component.clone(),
                    metrics: HashMap::from([(metric.name.clone(), metric.value)]),
                    performance_impact: metric_impact,
                });
            }
            // Check warning thresholds
            else if metric.value >= metric.thresholds.warning {
                metric_impact = 15.0;
                metric_bottlenecks.push(Bottleneck {
                    bottleneck_type: Self::get_bottleneck_type(&metric.name),
                    severity: SeverityLevel::Warning,
                    description: format!(
                        "{} exceeds warning threshold: {:.1}{} (threshold: {:.1}{})",
                        metric.name,
                        metric.value,
                        metric.unit,
                        metric.thresholds.warning,
                        metric.unit
                    ),
                    recommendations: Self::get_recommendations(
                        &metric.name,
                        SeverityLevel::Warning,
                    ),
                    detected_at: timestamp,
                    component: metric.component.clone(),
                    metrics: HashMap::from([(metric.name.clone(), metric.value)]),
                    performance_impact: metric_impact,
                });
            }

            overall_score -= metric_impact;
            bottlenecks.extend(metric_bottlenecks);

            // Analyze metric trend
            let trend = Self::analyze_metric_trend(metric);
            trends.insert(metric.name.clone(), trend);
        }

        // Generate overall recommendations
        if overall_score < 70.0 {
            recommendations.push(
                "Critical performance. Immediate action required.".to_string(),
            );
        } else if overall_score < 85.0 {
            recommendations.push(
                "Performance below optimal. Optimization recommended.".to_string(),
            );
        }

        if bottlenecks
            .iter()
            .any(|b| matches!(b.bottleneck_type, BottleneckType::Cpu))
        {
            recommendations
                .push("Optimize algorithms and consider scaling CPU resources.".to_string());
        }

        if bottlenecks
            .iter()
            .any(|b| matches!(b.bottleneck_type, BottleneckType::Memory))
        {
            recommendations
                .push("Check for memory leaks and optimize memory usage.".to_string());
        }

        if bottlenecks
            .iter()
            .any(|b| matches!(b.bottleneck_type, BottleneckType::Io))
        {
            recommendations
                .push("Optimize I/O operations and consider using SSDs.".to_string());
        }

        PerformanceAnalysis {
            timestamp,
            overall_score: overall_score.max(0.0),
            bottlenecks,
            metrics: metrics.to_vec(),
            recommendations,
            trends,
        }
    }

    /// Maps metric name to bottleneck type
    fn get_bottleneck_type(metric_name: &str) -> BottleneckType {
        match metric_name {
            "cpu_usage" => BottleneckType::Cpu,
            "memory_usage" => BottleneckType::Memory,
            "disk_io" | "disk_usage" => BottleneckType::Io,
            "network_io" | "network_usage" => BottleneckType::Network,
            "lock_contention" => BottleneckType::Locking,
            "cache_hit_ratio" => BottleneckType::Cache,
            _ => BottleneckType::Cpu,
        }
    }

    /// Provides recommendations for a given metric and severity
    fn get_recommendations(metric_name: &str, severity: SeverityLevel) -> Vec<String> {
        match (metric_name, severity) {
            ("cpu_usage", SeverityLevel::Critical) => vec![
                "Immediately optimize hot-path algorithms".to_string(),
                "Consider horizontal scaling".to_string(),
                "Inspect for runaway loops".to_string(),
            ],
            ("cpu_usage", SeverityLevel::Warning) => vec![
                "Optimize the most resource-heavy operations".to_string(),
                "Evaluate caching of repeated results".to_string(),
            ],
            ("memory_usage", SeverityLevel::Critical) => vec![
                "Investigate potential memory leaks".to_string(),
                "Increase available memory".to_string(),
                "Optimize data structures".to_string(),
            ],
            ("memory_usage", SeverityLevel::Warning) => vec![
                "Monitor memory usage closely".to_string(),
                "Consider object pooling".to_string(),
            ],
            ("cache_hit_ratio", _) => vec![
                "Increase cache size".to_string(),
                "Tune eviction strategy".to_string(),
                "Verify cache correctness".to_string(),
            ],
            ("lock_contention", _) => vec![
                "Optimize lock granularity".to_string(),
                "Consider lock-free data structures".to_string(),
                "Reduce lock hold times".to_string(),
            ],
            _ => vec!["Check system configuration".to_string()],
        }
    }

    /// Analyzes metric trend
    fn analyze_metric_trend(metric: &PerformanceMetric) -> String {
        // Real implementation would analyze history; for demo return static value
        if metric.value > metric.thresholds.warning {
            "Increasing".to_string()
        } else {
            "Stable".to_string()
        }
    }

    /// Updates analysis statistics
    fn update_analysis_stats(stats: &Arc<RwLock<AnalysisStats>>, analysis: &PerformanceAnalysis) {
        let mut stats = stats.write().unwrap();
        stats.total_analyses += 1;
        stats.last_analysis_time = analysis.timestamp;
        stats.total_bottlenecks += analysis.bottlenecks.len() as u64;
        stats.critical_issues += analysis
            .bottlenecks
            .iter()
            .filter(|b| matches!(b.severity, SeverityLevel::Critical))
            .count() as u64;

        stats.avg_performance_score = (stats.avg_performance_score
            * (stats.total_analyses - 1) as f64
            + analysis.overall_score)
            / stats.total_analyses as f64;
    }

    /// Returns simulated CPU usage
    fn get_cpu_usage() -> f64 {
        // In a real implementation, this would call system APIs
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        SystemTime::now().hash(&mut hasher);
        (hasher.finish() % 100) as f64
    }

    /// Returns simulated memory usage
    fn get_memory_usage() -> f64 {
        // In a real implementation, this would call system APIs
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        (SystemTime::now().hash(&mut hasher), "memory").hash(&mut hasher);
        (hasher.finish() % 100) as f64
    }

    /// Returns simulated disk I/O
    fn get_disk_io() -> f64 {
        // In a real implementation, this would call system APIs
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        (SystemTime::now().hash(&mut hasher), "disk").hash(&mut hasher);
        (hasher.finish() % 200) as f64
    }

    /// Returns simulated network I/O
    fn get_network_io() -> f64 {
        // In a real implementation, this would call system APIs
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        (SystemTime::now().hash(&mut hasher), "network").hash(&mut hasher);
        (hasher.finish() % 100) as f64
    }

    /// Returns simulated cache hit ratio
    fn get_cache_hit_ratio() -> f64 {
        // In a real implementation, this would call system APIs
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        (SystemTime::now().hash(&mut hasher), "cache").hash(&mut hasher);
        60.0 + (hasher.finish() % 40) as f64
    }

    /// Returns simulated lock contention
    fn get_lock_contention() -> f64 {
        // In a real implementation, this would call system APIs
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        (SystemTime::now().hash(&mut hasher), "lock").hash(&mut hasher);
        (hasher.finish() % 50) as f64
    }

    /// Returns most recent analysis
    pub fn get_latest_analysis(&self) -> Option<PerformanceAnalysis> {
        let analyses = self.analyses.read().unwrap();
        analyses.last().cloned()
    }

    /// Returns recent analyses
    pub fn get_analyses(&self, limit: usize) -> Vec<PerformanceAnalysis> {
        let analyses = self.analyses.read().unwrap();
        let start = analyses.len().saturating_sub(limit);
        analyses[start..].to_vec()
    }

    /// Returns analysis statistics
    pub fn get_stats(&self) -> AnalysisStats {
        self.stats.read().unwrap().clone()
    }

    /// Generates performance analysis report
    pub fn generate_performance_report(&self) -> String {
        let stats = self.get_stats();
        let latest_analysis = self.get_latest_analysis();

        let mut report = String::new();

        report.push_str("=== Performance analysis report ===\n\n");

        // Overall statistics
        report.push_str("Overall statistics:\n");
        report.push_str(&format!("  Total analyses: {}\n", stats.total_analyses));
        report.push_str(&format!(
            "  Bottlenecks detected: {}\n",
            stats.total_bottlenecks
        ));
        report.push_str(&format!(
            "  Critical issues: {}\n",
            stats.critical_issues
        ));
        report.push_str(&format!(
            "  Average performance score: {:.1}/100\n",
            stats.avg_performance_score
        ));
        report.push_str("\n");

        // Latest analysis
        if let Some(analysis) = latest_analysis {
            report.push_str("Most recent analysis:\n");
            report.push_str(&format!(
                "  Overall score: {:.1}/100\n",
                analysis.overall_score
            ));
            report.push_str(&format!(
                "  Issues detected: {}\n",
                analysis.bottlenecks.len()
            ));
            report.push_str("\n");

            // Bottlenecks
            if !analysis.bottlenecks.is_empty() {
                report.push_str("Identified bottlenecks:\n");
                for (i, bottleneck) in analysis.bottlenecks.iter().enumerate() {
                    report.push_str(&format!(
                        "  {}. [{}] {}: {}\n",
                        i + 1,
                        bottleneck.severity,
                        bottleneck.bottleneck_type,
                        bottleneck.description
                    ));

                    if !bottleneck.recommendations.is_empty() {
                        report.push_str("     Recommendations:\n");
                        for rec in &bottleneck.recommendations {
                            report.push_str(&format!("       - {}\n", rec));
                        }
                    }
                }
                report.push_str("\n");
            }

            // Overall recommendations
            if !analysis.recommendations.is_empty() {
                report.push_str("General recommendations:\n");
                for (i, rec) in analysis.recommendations.iter().enumerate() {
                    report.push_str(&format!("  {}. {}\n", i + 1, rec));
                }
                report.push_str("\n");
            }

            // Trends
            if !analysis.trends.is_empty() {
                report.push_str("Metric trends:\n");
                for (metric, trend) in &analysis.trends {
                    report.push_str(&format!("  {}: {}\n", metric, trend));
                }
                report.push_str("\n");
            }
        }

        // Improvement guidance
        report.push_str("Improvement guidance:\n");
        if stats.avg_performance_score < 70.0 {
            report.push_str(
                "  🔴 Critical performance degradation. Immediate action required.\n",
            );
        } else if stats.avg_performance_score < 85.0 {
            report
                .push_str("  🟡 Performance below optimal. Optimization recommended.\n");
        } else {
            report.push_str("  🟢 Performance within normal range.\n");
        }

        if stats.critical_issues > 0 {
            report.push_str(&format!(
                "  ⚠️  {} critical issues detected.\n",
                stats.critical_issues
            ));
        }

        report
    }

    /// Generates analyzer status report
    pub fn generate_status_report(&self) -> String {
        let stats = self.get_stats();
        let analyses_count = self.analyses.read().unwrap().len();
        let metrics_count = self.metrics_history.read().unwrap().len();

        let mut report = String::new();

        report.push_str(&format!("Analyses stored: {}\n", analyses_count));
        report.push_str(&format!("Metrics tracked: {}\n", metrics_count));
        report.push_str(&format!("Total analyses: {}\n", stats.total_analyses));
        report.push_str(&format!(
            "Bottlenecks detected: {}\n",
            stats.total_bottlenecks
        ));
        report.push_str(&format!("Critical issues: {}\n", stats.critical_issues));
        report.push_str(&format!(
            "Average score: {:.1}/100\n",
            stats.avg_performance_score
        ));
        report.push_str(&format!(
            "Metrics collection interval: {} sec\n",
            self.config.metrics_collection_interval
        ));

        report
    }

    /// Stops analyzer
    pub fn shutdown(&mut self) {
        if let Some(handle) = self.background_handle.take() {
            handle.abort();
        }
    }
}

impl Drop for PerformanceAnalyzer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_performance_analyzer() {
        let config = DebugConfig {
            metrics_collection_interval: 1,
            ..Default::default()
        };

        let analyzer = PerformanceAnalyzer::new(&config);

        // Wait for analyses to accumulate
        tokio::time::sleep(Duration::from_millis(1500)).await;

        // Validate statistics
        let stats = analyzer.get_stats();
        assert!(stats.total_analyses > 0);

        // Validate latest analysis
        let latest_analysis = analyzer.get_latest_analysis();
        assert!(latest_analysis.is_some());

        if let Some(analysis) = latest_analysis {
            assert!(analysis.overall_score >= 0.0 && analysis.overall_score <= 100.0);
            assert!(!analysis.metrics.is_empty());
        }

        // Validate report contents
        let report = analyzer.generate_performance_report();
        assert!(report.contains("Performance analysis report"));
        assert!(report.contains("Overall statistics"));
        assert!(report.contains("Improvement guidance"));
    }

    #[test]
    fn test_bottleneck_detection() {
        let metrics = vec![
            PerformanceMetric {
                name: "cpu_usage".to_string(),
                value: 95.0, // Critical value
                unit: "%".to_string(),
                timestamp: 1000,
                component: "System".to_string(),
                thresholds: Thresholds {
                    warning: 70.0,
                    critical: 90.0,
                },
            },
            PerformanceMetric {
                name: "memory_usage".to_string(),
                value: 75.0, // Warning value
                unit: "%".to_string(),
                timestamp: 1000,
                component: "System".to_string(),
                thresholds: Thresholds {
                    warning: 80.0,
                    critical: 95.0,
                },
            },
        ];

        let analysis = PerformanceAnalyzer::perform_analysis(&metrics);

        assert!(analysis.overall_score < 100.0);
        assert!(!analysis.bottlenecks.is_empty());

        let critical_bottlenecks: Vec<_> = analysis
            .bottlenecks
            .iter()
            .filter(|b| matches!(b.severity, SeverityLevel::Critical))
            .collect();

        assert!(!critical_bottlenecks.is_empty());
        assert!(critical_bottlenecks
            .iter()
            .any(|b| matches!(b.bottleneck_type, BottleneckType::Cpu)));
    }

    #[test]
    fn test_recommendations() {
        let cpu_recommendations =
            PerformanceAnalyzer::get_recommendations("cpu_usage", SeverityLevel::Critical);
        assert!(!cpu_recommendations.is_empty());
        assert!(cpu_recommendations
            .iter()
            .any(|r| r.to_lowercase().contains("optimize")));

        let memory_recommendations =
            PerformanceAnalyzer::get_recommendations("memory_usage", SeverityLevel::Warning);
        assert!(!memory_recommendations.is_empty());
        // Ensure recommendations contain relevant keywords
        assert!(memory_recommendations
            .iter()
            .any(|r| r.to_lowercase().contains("memory") || r.to_lowercase().contains("object")));
    }
}
