//! Query tracer for rustdb
//!
//! Provides detailed tracing of SQL query execution with timing data for each stage

use crate::debug::DebugConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use uuid::Uuid;

/// Query execution stage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryStage {
    /// Query received
    Received,
    /// Lexical analysis
    Lexing,
    /// Parsing
    Parsing,
    /// Semantic analysis
    SemanticAnalysis,
    /// Query planning
    Planning,
    /// Query optimization
    Optimization,
    /// Query execution
    Execution,
    /// Result return
    ResultReturn,
    /// Completed
    Completed,
    /// Error
    Error,
}

impl std::fmt::Display for QueryStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryStage::Received => write!(f, "RECEIVED"),
            QueryStage::Lexing => write!(f, "LEXING"),
            QueryStage::Parsing => write!(f, "PARSING"),
            QueryStage::SemanticAnalysis => write!(f, "SEMANTIC_ANALYSIS"),
            QueryStage::Planning => write!(f, "PLANNING"),
            QueryStage::Optimization => write!(f, "OPTIMIZATION"),
            QueryStage::Execution => write!(f, "EXECUTION"),
            QueryStage::ResultReturn => write!(f, "RESULT_RETURN"),
            QueryStage::Completed => write!(f, "COMPLETED"),
            QueryStage::Error => write!(f, "ERROR"),
        }
    }
}

/// Query trace event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTraceEvent {
    /// Timestamp (microseconds)
    pub timestamp: u64,
    /// Execution stage
    pub stage: QueryStage,
    /// Message
    pub message: String,
    /// Additional data
    pub data: Option<serde_json::Value>,
    /// Stage duration (microseconds)
    pub duration_us: Option<u64>,
    /// Memory usage (bytes)
    pub memory_usage: Option<u64>,
    /// Rows processed
    pub rows_processed: Option<u64>,
}

/// Query trace
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTrace {
    /// Query unique ID
    pub query_id: String,
    /// SQL query text
    pub sql_query: String,
    /// Start timestamp
    pub start_time: u64,
    /// End timestamp
    pub end_time: Option<u64>,
    /// Total execution time (microseconds)
    pub total_duration_us: Option<u64>,
    /// Trace events
    pub events: Vec<QueryTraceEvent>,
    /// Current stage
    pub current_stage: QueryStage,
    /// Execution status
    pub status: QueryStatus,
    /// Rows returned
    pub rows_returned: Option<u64>,
    /// Result size (bytes)
    pub result_size: Option<u64>,
    /// Error message (if any)
    pub error: Option<String>,
    /// Query metadata
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Query execution status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryStatus {
    /// Running
    Running,
    /// Completed successfully
    Completed,
    /// Failed with error
    Failed,
    /// Cancelled
    Cancelled,
}

impl std::fmt::Display for QueryStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryStatus::Running => write!(f, "RUNNING"),
            QueryStatus::Completed => write!(f, "COMPLETED"),
            QueryStatus::Failed => write!(f, "FAILED"),
            QueryStatus::Cancelled => write!(f, "CANCELLED"),
        }
    }
}

/// Query tracing statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryTracingStats {
    /// Total traced queries
    pub total_queries: u64,
    /// Successfully completed queries
    pub completed_queries: u64,
    /// Failed queries
    pub failed_queries: u64,
    /// Cancelled queries
    pub cancelled_queries: u64,
    /// Average execution time (microseconds)
    pub avg_execution_time_us: u64,
    /// Minimum execution time
    pub min_execution_time_us: u64,
    /// Maximum execution time
    pub max_execution_time_us: u64,
    /// Total rows processed
    pub total_rows_processed: u64,
    /// Average rows per query
    pub avg_rows_per_query: u64,
    /// Last query timestamp
    pub last_query_time: u64,
}

/// Query tracer
pub struct QueryTracer {
    config: DebugConfig,
    active_traces: Arc<RwLock<HashMap<String, QueryTrace>>>,
    completed_traces: Arc<RwLock<Vec<QueryTrace>>>,
    stats: Arc<RwLock<QueryTracingStats>>,
    background_handle: Option<JoinHandle<()>>,
}

impl QueryTracer {
    /// Creates a new query tracer
    pub fn new(config: &DebugConfig) -> Self {
        let mut tracer = Self {
            config: config.clone(),
            active_traces: Arc::new(RwLock::new(HashMap::new())),
            completed_traces: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(RwLock::new(QueryTracingStats::default())),
            background_handle: None,
        };

        // Start background cleanup task
        tracer.start_background_cleanup();

        tracer
    }

    /// Starts background cleanup job
    fn start_background_cleanup(&mut self) {
        let active_traces = self.active_traces.clone();
        let completed_traces = self.completed_traces.clone();
        let _config = self.config.clone();

        self.background_handle = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));

            loop {
                interval.tick().await;

                // Move finished traces
                {
                    let mut active = active_traces.write().unwrap();
                    let mut completed = completed_traces.write().unwrap();

                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();

                    let mut to_move = Vec::new();

                    for (query_id, trace) in active.iter() {
                        if let Some(end_time) = trace.end_time {
                            // Move traces older than 1 hour
                            if now - (end_time / 1_000_000) > 3600 {
                                to_move.push(query_id.clone());
                            }
                        }
                    }

                    for query_id in to_move {
                        if let Some(trace) = active.remove(&query_id) {
                            completed.push(trace);
                        }
                    }

                    // Limit number of completed traces
                    let len = completed.len();
                    if len > 1000 {
                        completed.drain(0..len - 1000);
                    }
                }
            }
        }));
    }

    /// Starts tracing a new query
    pub fn start_trace(&self, sql_query: &str) -> String {
        let query_id = Uuid::new_v4().to_string();
        let start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        let trace = QueryTrace {
            query_id: query_id.clone(),
            sql_query: sql_query.to_string(),
            start_time,
            end_time: None,
            total_duration_us: None,
            events: Vec::new(),
            current_stage: QueryStage::Received,
            status: QueryStatus::Running,
            rows_returned: None,
            result_size: None,
            error: None,
            metadata: HashMap::new(),
        };

        // Add initial event
        let initial_event = QueryTraceEvent {
            timestamp: start_time,
            stage: QueryStage::Received,
            message: "Query received".to_string(),
            data: Some(serde_json::json!({
                "sql": sql_query,
                "query_id": query_id
            })),
            duration_us: None,
            memory_usage: None,
            rows_processed: None,
        };

        let mut trace = trace;
        trace.events.push(initial_event);

        // Store trace
        {
            let mut active_traces = self.active_traces.write().unwrap();
            active_traces.insert(query_id.clone(), trace);
        }

        // Update statistics
        {
            let mut stats = self.stats.write().unwrap();
            stats.total_queries += 1;
            stats.last_query_time = start_time;
        }

        query_id
    }

    /// Adds an event to a trace
    pub fn add_event(
        &self,
        query_id: &str,
        stage: QueryStage,
        message: &str,
        data: Option<serde_json::Value>,
        duration: Option<Duration>,
        memory_usage: Option<u64>,
        rows_processed: Option<u64>,
    ) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        let event = QueryTraceEvent {
            timestamp,
            stage: stage.clone(),
            message: message.to_string(),
            data,
            duration_us: duration.map(|d| d.as_micros() as u64),
            memory_usage,
            rows_processed,
        };

        {
            let mut active_traces = self.active_traces.write().unwrap();
            if let Some(trace) = active_traces.get_mut(query_id) {
                trace.events.push(event);
                trace.current_stage = stage;
            }
        }
    }

    /// Finishes tracing a query
    pub fn finish_trace(
        &self,
        query_id: &str,
        status: QueryStatus,
        rows_returned: Option<u64>,
        result_size: Option<u64>,
        error: Option<String>,
    ) {
        let end_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        let trace = {
            let mut active_traces = self.active_traces.write().unwrap();
            active_traces.remove(query_id)
        };

        if let Some(mut trace) = trace {
            trace.end_time = Some(end_time);
            trace.total_duration_us = Some(end_time - trace.start_time);
            trace.status = status.clone();
            trace.rows_returned = rows_returned;
            trace.result_size = result_size;
            trace.error = error;

            // Append final event
            let final_event = QueryTraceEvent {
                timestamp: end_time,
                stage: match status {
                    QueryStatus::Completed => QueryStage::Completed,
                    QueryStatus::Failed => QueryStage::Error,
                    QueryStatus::Cancelled => QueryStage::Error,
                    QueryStatus::Running => QueryStage::Completed, // Should not happen
                },
                message: format!("Query {}", status),
                data: Some(serde_json::json!({
                    "total_duration_us": trace.total_duration_us,
                    "rows_returned": rows_returned,
                    "result_size": result_size
                })),
                duration_us: trace.total_duration_us,
                memory_usage: None,
                rows_processed: rows_returned,
            };

            trace.events.push(final_event);

            // Move to completed list
            {
                let mut completed_traces = self.completed_traces.write().unwrap();
                completed_traces.push(trace);
            }

            // Update statistics
            {
                let mut stats = self.stats.write().unwrap();
                match status {
                    QueryStatus::Completed => stats.completed_queries += 1,
                    QueryStatus::Failed => stats.failed_queries += 1,
                    QueryStatus::Cancelled => stats.cancelled_queries += 1,
                    QueryStatus::Running => {} // Should not happen
                }
            }
        }
    }

    /// Returns active trace by ID
    pub fn get_active_trace(&self, query_id: &str) -> Option<QueryTrace> {
        let active_traces = self.active_traces.read().unwrap();
        active_traces.get(query_id).cloned()
    }

    /// Returns all active traces
    pub fn get_active_traces(&self) -> Vec<QueryTrace> {
        let active_traces = self.active_traces.read().unwrap();
        active_traces.values().cloned().collect()
    }

    /// Returns recently completed traces
    pub fn get_completed_traces(&self, limit: usize) -> Vec<QueryTrace> {
        let completed_traces = self.completed_traces.read().unwrap();
        let start = completed_traces.len().saturating_sub(limit);
        completed_traces[start..].to_vec()
    }

    /// Returns tracing statistics
    pub fn get_stats(&self) -> QueryTracingStats {
        self.stats.read().unwrap().clone()
    }

    /// Generates query performance report
    pub fn generate_performance_report(&self) -> String {
        let stats = self.get_stats();
        let completed_traces = self.get_completed_traces(100);

        let mut report = String::new();

        report.push_str("=== Query performance report ===\n\n");

        // General statistics
        report.push_str("General statistics:\n");
        report.push_str(&format!("  Total queries: {}\n", stats.total_queries));
        report.push_str(&format!(
            "  Completed successfully: {}\n",
            stats.completed_queries
        ));
        report.push_str(&format!("  Failed: {}\n", stats.failed_queries));
        report.push_str(&format!("  Cancelled: {}\n", stats.cancelled_queries));
        report.push_str("\n");

        // Execution time
        if !completed_traces.is_empty() {
            let mut total_time = 0u64;
            let mut min_time = u64::MAX;
            let mut max_time = 0u64;
            let mut total_rows = 0u64;

            for trace in &completed_traces {
                if let Some(duration) = trace.total_duration_us {
                    total_time += duration;
                    min_time = min_time.min(duration);
                    max_time = max_time.max(duration);
                }
                if let Some(rows) = trace.rows_returned {
                    total_rows += rows;
                }
            }

            let avg_time = total_time / completed_traces.len() as u64;
            let avg_rows = total_rows / completed_traces.len() as u64;

            report.push_str("Execution time:\n");
            report.push_str(&format!("  Average: {:.2} ms\n", avg_time as f64 / 1000.0));
            report.push_str(&format!("  Minimum: {:.2} ms\n", min_time as f64 / 1000.0));
            report.push_str(&format!("  Maximum: {:.2} ms\n", max_time as f64 / 1000.0));
            report.push_str("\n");

            report.push_str("Data processing:\n");
            report.push_str(&format!("  Average rows: {}\n", avg_rows));
            report.push_str(&format!("  Total rows: {}\n", total_rows));
            report.push_str("\n");

            // Slowest queries
            let mut slow_queries: Vec<_> = completed_traces
                .iter()
                .filter(|t| t.total_duration_us.is_some())
                .collect();
            slow_queries.sort_by(|a, b| b.total_duration_us.cmp(&a.total_duration_us));

            report.push_str("Top 5 slow queries:\n");
            for (i, trace) in slow_queries.iter().take(5).enumerate() {
                if let Some(duration) = trace.total_duration_us {
                    report.push_str(&format!(
                        "  {}. {} ms - {}\n",
                        i + 1,
                        duration as f64 / 1000.0,
                        if trace.sql_query.len() > 50 {
                            format!("{}...", &trace.sql_query[..47])
                        } else {
                            trace.sql_query.clone()
                        }
                    ));
                }
            }
        }

        report
    }

    /// Generates tracer status report
    pub fn generate_status_report(&self) -> String {
        let stats = self.get_stats();
        let active_count = self.active_traces.read().unwrap().len();
        let completed_count = self.completed_traces.read().unwrap().len();

        let mut report = String::new();

        report.push_str(&format!("Active traces: {}\n", active_count));
        report.push_str(&format!("Completed traces: {}\n", completed_count));
        report.push_str(&format!("Total queries: {}\n", stats.total_queries));
        report.push_str(&format!(
            "Succeeded: {} ({:.1}%)\n",
            stats.completed_queries,
            if stats.total_queries > 0 {
                stats.completed_queries as f64 / stats.total_queries as f64 * 100.0
            } else {
                0.0
            }
        ));
        report.push_str(&format!(
            "Failed: {} ({:.1}%)\n",
            stats.failed_queries,
            if stats.total_queries > 0 {
                stats.failed_queries as f64 / stats.total_queries as f64 * 100.0
            } else {
                0.0
            }
        ));

        report
    }

    /// Stops tracer
    pub fn shutdown(&mut self) {
        if let Some(handle) = self.background_handle.take() {
            handle.abort();
        }
    }
}

impl Drop for QueryTracer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_query_tracer() {
        let config = DebugConfig {
            enable_query_tracing: true,
            ..Default::default()
        };

        let tracer = QueryTracer::new(&config);

        // Start a trace
        let query_id = tracer.start_trace("SELECT * FROM users WHERE age > 18");

        // Add events
        tracer.add_event(
            &query_id,
            QueryStage::Lexing,
            "Lexical analysis completed",
            Some(serde_json::json!({"tokens": 15})),
            Some(Duration::from_millis(1)),
            Some(1024),
            None,
        );

        tracer.add_event(
            &query_id,
            QueryStage::Parsing,
            "Parsing completed",
            Some(serde_json::json!({"ast_nodes": 8})),
            Some(Duration::from_millis(2)),
            Some(2048),
            None,
        );

        tracer.add_event(
            &query_id,
            QueryStage::Execution,
            "Query execution completed",
            Some(serde_json::json!({"rows_processed": 100})),
            Some(Duration::from_millis(50)),
            Some(4096),
            Some(100),
        );

        // Finish trace
        tracer.finish_trace(
            &query_id,
            QueryStatus::Completed,
            Some(100),
            Some(8192),
            None,
        );

        // Verify statistics
        let stats = tracer.get_stats();
        assert_eq!(stats.total_queries, 1);
        assert_eq!(stats.completed_queries, 1);
        assert_eq!(stats.failed_queries, 0);

        // Verify completed traces
        let completed = tracer.get_completed_traces(10);
        assert_eq!(completed.len(), 1);

        let trace = &completed[0];
        assert_eq!(trace.query_id, query_id);
        assert_eq!(trace.sql_query, "SELECT * FROM users WHERE age > 18");
        assert!(matches!(trace.status, QueryStatus::Completed));
        assert_eq!(trace.rows_returned, Some(100));
        assert_eq!(trace.result_size, Some(8192));
        assert!(trace.events.len() >= 4); // Initial + 3 added + final

        // Verify report contents
        let report = tracer.generate_performance_report();
        assert!(report.contains("Query performance report"));
        assert!(report.contains("Total queries: 1"));
        assert!(report.contains("Completed successfully: 1"));
    }

    #[tokio::test]
    async fn test_query_tracer_error() {
        let config = DebugConfig {
            enable_query_tracing: true,
            ..Default::default()
        };

        let tracer = QueryTracer::new(&config);

        let query_id = tracer.start_trace("INVALID SQL QUERY");

        tracer.add_event(
            &query_id,
            QueryStage::Parsing,
            "Parsing failed",
            Some(serde_json::json!({"error": "Syntax error"})),
            Some(Duration::from_millis(1)),
            Some(512),
            None,
        );

        tracer.finish_trace(
            &query_id,
            QueryStatus::Failed,
            None,
            None,
            Some("Syntax error at line 1".to_string()),
        );

        let stats = tracer.get_stats();
        assert_eq!(stats.total_queries, 1);
        assert_eq!(stats.completed_queries, 0);
        assert_eq!(stats.failed_queries, 1);

        let completed = tracer.get_completed_traces(10);
        assert_eq!(completed.len(), 1);

        let trace = &completed[0];
        assert!(matches!(trace.status, QueryStatus::Failed));
        assert_eq!(trace.error, Some("Syntax error at line 1".to_string()));
    }
}
