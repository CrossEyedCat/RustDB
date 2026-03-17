//! Debugging and profiling module for rustdb
//!
//! This module provides tools for:
//! - Detailed operation logging
//! - Query tracing
//! - CPU and Memory profiling
//! - Performance analysis
//! - Problem debugging

pub mod debug_logger;
pub mod performance_analyzer;
pub mod profiler;
pub mod query_tracer;

pub use debug_logger::DebugLogger;
pub use performance_analyzer::PerformanceAnalyzer;
pub use profiler::Profiler;
pub use query_tracer::QueryTracer;

/// Debug configuration
#[derive(Debug, Clone)]
pub struct DebugConfig {
    /// Enable detailed logging
    pub enable_debug_logging: bool,
    /// Enable query tracing
    pub enable_query_tracing: bool,
    /// Enable CPU profiling
    pub enable_cpu_profiling: bool,
    /// Enable Memory profiling
    pub enable_memory_profiling: bool,
    /// Detail level (0-5)
    pub detail_level: u8,
    /// Maximum log file size (MB)
    pub max_log_file_size_mb: u64,
    /// Number of rotated log files
    pub max_log_files: u32,
    /// Metrics collection interval (seconds)
    pub metrics_collection_interval: u64,
    /// Enable Prometheus export
    pub enable_prometheus_export: bool,
    /// Port for Prometheus metrics
    pub prometheus_port: u16,
}

impl Default for DebugConfig {
    fn default() -> Self {
        Self {
            enable_debug_logging: false,
            enable_query_tracing: false,
            enable_cpu_profiling: false,
            enable_memory_profiling: false,
            detail_level: 2,
            max_log_file_size_mb: 100,
            max_log_files: 5,
            metrics_collection_interval: 10,
            enable_prometheus_export: false,
            prometheus_port: 9090,
        }
    }
}

/// Debug manager
pub struct DebugManager {
    config: DebugConfig,
    debug_logger: Option<DebugLogger>,
    query_tracer: Option<QueryTracer>,
    profiler: Option<Profiler>,
    performance_analyzer: Option<PerformanceAnalyzer>,
}

impl DebugManager {
    /// Creates a new debug manager
    pub fn new(config: DebugConfig) -> Self {
        let mut manager = Self {
            config: config.clone(),
            debug_logger: None,
            query_tracer: None,
            profiler: None,
            performance_analyzer: None,
        };

        // Initialize components based on configuration
        if config.enable_debug_logging {
            manager.debug_logger = Some(DebugLogger::new(&config));
        }

        if config.enable_query_tracing {
            manager.query_tracer = Some(QueryTracer::new(&config));
        }

        if config.enable_cpu_profiling || config.enable_memory_profiling {
            manager.profiler = Some(Profiler::new(&config));
        }

        manager.performance_analyzer = Some(PerformanceAnalyzer::new(&config));

        manager
    }

    /// Gets configuration
    pub fn config(&self) -> &DebugConfig {
        &self.config
    }

    /// Gets debug logger
    pub fn debug_logger(&self) -> Option<&DebugLogger> {
        self.debug_logger.as_ref()
    }

    /// Gets query tracer
    pub fn query_tracer(&self) -> Option<&QueryTracer> {
        self.query_tracer.as_ref()
    }

    /// Gets profiler
    pub fn profiler(&self) -> Option<&Profiler> {
        self.profiler.as_ref()
    }

    /// Gets performance analyzer
    pub fn performance_analyzer(&self) -> Option<&PerformanceAnalyzer> {
        self.performance_analyzer.as_ref()
    }

    /// Updates configuration
    pub fn update_config(&mut self, new_config: DebugConfig) {
        self.config = new_config.clone();

        // Recreate components if necessary
        if self.config.enable_debug_logging && self.debug_logger.is_none() {
            self.debug_logger = Some(DebugLogger::new(&self.config));
        }

        if self.config.enable_query_tracing && self.query_tracer.is_none() {
            self.query_tracer = Some(QueryTracer::new(&self.config));
        }

        if (self.config.enable_cpu_profiling || self.config.enable_memory_profiling)
            && self.profiler.is_none()
        {
            self.profiler = Some(Profiler::new(&self.config));
        }
    }

    /// Generates debug status report
    pub fn generate_debug_report(&self) -> String {
        let mut report = String::new();

        report.push_str("=== rustdb Debug Status Report ===\n\n");

        report.push_str("Configuration:\n");
        report.push_str(&format!(
            "  - Detailed logging: {}\n",
            self.config.enable_debug_logging
        ));
        report.push_str(&format!(
            "  - Query tracing: {}\n",
            self.config.enable_query_tracing
        ));
        report.push_str(&format!(
            "  - CPU profiling: {}\n",
            self.config.enable_cpu_profiling
        ));
        report.push_str(&format!(
            "  - Memory profiling: {}\n",
            self.config.enable_memory_profiling
        ));
        report.push_str(&format!("  - Detail level: {}\n", self.config.detail_level));
        report.push_str(&format!(
            "  - Prometheus export: {}\n",
            self.config.enable_prometheus_export
        ));
        report.push_str("\n");

        report.push_str("Active components:\n");
        report.push_str(&format!(
            "  - Debug Logger: {}\n",
            self.debug_logger.is_some()
        ));
        report.push_str(&format!(
            "  - Query Tracer: {}\n",
            self.query_tracer.is_some()
        ));
        report.push_str(&format!("  - Profiler: {}\n", self.profiler.is_some()));
        report.push_str(&format!(
            "  - Performance Analyzer: {}\n",
            self.performance_analyzer.is_some()
        ));
        report.push_str("\n");

        // Add reports from active components
        if let Some(logger) = &self.debug_logger {
            report.push_str("=== Debug Logger ===\n");
            report.push_str(&logger.generate_status_report());
            report.push_str("\n");
        }

        if let Some(tracer) = &self.query_tracer {
            report.push_str("=== Query Tracer ===\n");
            report.push_str(&tracer.generate_status_report());
            report.push_str("\n");
        }

        if let Some(profiler) = &self.profiler {
            report.push_str("=== Profiler ===\n");
            report.push_str(&profiler.generate_status_report());
            report.push_str("\n");
        }

        if let Some(analyzer) = &self.performance_analyzer {
            report.push_str("=== Performance Analyzer ===\n");
            report.push_str(&analyzer.generate_status_report());
            report.push_str("\n");
        }

        report
    }

    /// Stops all debug components
    pub fn shutdown(&mut self) {
        if let Some(logger) = &mut self.debug_logger {
            logger.shutdown();
        }

        if let Some(tracer) = &mut self.query_tracer {
            tracer.shutdown();
        }

        if let Some(profiler) = &mut self.profiler {
            profiler.shutdown();
        }

        if let Some(analyzer) = &mut self.performance_analyzer {
            analyzer.shutdown();
        }
    }
}

impl Drop for DebugManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}
