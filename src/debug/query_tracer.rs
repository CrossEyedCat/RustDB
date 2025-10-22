//! Трассировщик запросов для rustdb
//!
//! Предоставляет детальную трассировку выполнения SQL запросов
//! с отслеживанием времени выполнения каждого этапа

use crate::debug::DebugConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use uuid::Uuid;

/// Этап выполнения запроса
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryStage {
    /// Получение запроса
    Received,
    /// Лексический анализ
    Lexing,
    /// Синтаксический анализ
    Parsing,
    /// Семантический анализ
    SemanticAnalysis,
    /// Планирование запроса
    Planning,
    /// Оптимизация запроса
    Optimization,
    /// Выполнение запроса
    Execution,
    /// Возврат результата
    ResultReturn,
    /// Завершение
    Completed,
    /// Ошибка
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

/// Событие в трассировке запроса
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTraceEvent {
    /// Временная метка (микросекунды)
    pub timestamp: u64,
    /// Этап выполнения
    pub stage: QueryStage,
    /// Сообщение
    pub message: String,
    /// Дополнительные данные
    pub data: Option<serde_json::Value>,
    /// Время выполнения этапа (микросекунды)
    pub duration_us: Option<u64>,
    /// Использование памяти (байты)
    pub memory_usage: Option<u64>,
    /// Количество обработанных строк
    pub rows_processed: Option<u64>,
}

/// Трассировка запроса
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTrace {
    /// Уникальный ID запроса
    pub query_id: String,
    /// SQL запрос
    pub sql_query: String,
    /// Время начала выполнения
    pub start_time: u64,
    /// Время завершения выполнения
    pub end_time: Option<u64>,
    /// Общее время выполнения (микросекунды)
    pub total_duration_us: Option<u64>,
    /// События трассировки
    pub events: Vec<QueryTraceEvent>,
    /// Текущий этап
    pub current_stage: QueryStage,
    /// Статус выполнения
    pub status: QueryStatus,
    /// Количество возвращенных строк
    pub rows_returned: Option<u64>,
    /// Размер результата (байты)
    pub result_size: Option<u64>,
    /// Ошибка (если есть)
    pub error: Option<String>,
    /// Метаданные запроса
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Статус выполнения запроса
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryStatus {
    /// Выполняется
    Running,
    /// Завершен успешно
    Completed,
    /// Завершен с ошибкой
    Failed,
    /// Отменен
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

/// Статистика трассировки запросов
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryTracingStats {
    /// Общее количество трассированных запросов
    pub total_queries: u64,
    /// Успешно завершенные запросы
    pub completed_queries: u64,
    /// Запросы с ошибками
    pub failed_queries: u64,
    /// Отмененные запросы
    pub cancelled_queries: u64,
    /// Среднее время выполнения (микросекунды)
    pub avg_execution_time_us: u64,
    /// Минимальное время выполнения
    pub min_execution_time_us: u64,
    /// Максимальное время выполнения
    pub max_execution_time_us: u64,
    /// Общее количество обработанных строк
    pub total_rows_processed: u64,
    /// Среднее количество строк на запрос
    pub avg_rows_per_query: u64,
    /// Время последнего запроса
    pub last_query_time: u64,
}

/// Трассировщик запросов
pub struct QueryTracer {
    config: DebugConfig,
    active_traces: Arc<RwLock<HashMap<String, QueryTrace>>>,
    completed_traces: Arc<RwLock<Vec<QueryTrace>>>,
    stats: Arc<RwLock<QueryTracingStats>>,
    background_handle: Option<JoinHandle<()>>,
}

impl QueryTracer {
    /// Создает новый трассировщик запросов
    pub fn new(config: &DebugConfig) -> Self {
        let mut tracer = Self {
            config: config.clone(),
            active_traces: Arc::new(RwLock::new(HashMap::new())),
            completed_traces: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(RwLock::new(QueryTracingStats::default())),
            background_handle: None,
        };

        // Запускаем фоновую задачу для очистки старых трассировок
        tracer.start_background_cleanup();

        tracer
    }

    /// Запускает фоновую задачу очистки
    fn start_background_cleanup(&mut self) {
        let active_traces = self.active_traces.clone();
        let completed_traces = self.completed_traces.clone();
        let _config = self.config.clone();

        self.background_handle = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));

            loop {
                interval.tick().await;

                // Перемещаем завершенные трассировки
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
                            // Перемещаем трассировки старше 1 часа
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

                    // Ограничиваем количество завершенных трассировок
                    let len = completed.len();
                    if len > 1000 {
                        completed.drain(0..len - 1000);
                    }
                }
            }
        }));
    }

    /// Начинает трассировку нового запроса
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

        // Добавляем начальное событие
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

        // Сохраняем трассировку
        {
            let mut active_traces = self.active_traces.write().unwrap();
            active_traces.insert(query_id.clone(), trace);
        }

        // Обновляем статистику
        {
            let mut stats = self.stats.write().unwrap();
            stats.total_queries += 1;
            stats.last_query_time = start_time;
        }

        query_id
    }

    /// Добавляет событие в трассировку
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

    /// Завершает трассировку запроса
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

            // Добавляем финальное событие
            let final_event = QueryTraceEvent {
                timestamp: end_time,
                stage: match status {
                    QueryStatus::Completed => QueryStage::Completed,
                    QueryStatus::Failed => QueryStage::Error,
                    QueryStatus::Cancelled => QueryStage::Error,
                    QueryStatus::Running => QueryStage::Completed, // Не должно происходить
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

            // Перемещаем в завершенные
            {
                let mut completed_traces = self.completed_traces.write().unwrap();
                completed_traces.push(trace);
            }

            // Обновляем статистику
            {
                let mut stats = self.stats.write().unwrap();
                match status {
                    QueryStatus::Completed => stats.completed_queries += 1,
                    QueryStatus::Failed => stats.failed_queries += 1,
                    QueryStatus::Cancelled => stats.cancelled_queries += 1,
                    QueryStatus::Running => {} // Не должно происходить
                }
            }
        }
    }

    /// Получает активную трассировку
    pub fn get_active_trace(&self, query_id: &str) -> Option<QueryTrace> {
        let active_traces = self.active_traces.read().unwrap();
        active_traces.get(query_id).cloned()
    }

    /// Получает все активные трассировки
    pub fn get_active_traces(&self) -> Vec<QueryTrace> {
        let active_traces = self.active_traces.read().unwrap();
        active_traces.values().cloned().collect()
    }

    /// Получает завершенные трассировки
    pub fn get_completed_traces(&self, limit: usize) -> Vec<QueryTrace> {
        let completed_traces = self.completed_traces.read().unwrap();
        let start = completed_traces.len().saturating_sub(limit);
        completed_traces[start..].to_vec()
    }

    /// Получает статистику трассировки
    pub fn get_stats(&self) -> QueryTracingStats {
        self.stats.read().unwrap().clone()
    }

    /// Создает отчет о производительности запросов
    pub fn generate_performance_report(&self) -> String {
        let stats = self.get_stats();
        let completed_traces = self.get_completed_traces(100);

        let mut report = String::new();

        report.push_str("=== Отчет о производительности запросов ===\n\n");

        // Общая статистика
        report.push_str("Общая статистика:\n");
        report.push_str(&format!("  Всего запросов: {}\n", stats.total_queries));
        report.push_str(&format!(
            "  Успешно завершенных: {}\n",
            stats.completed_queries
        ));
        report.push_str(&format!("  С ошибками: {}\n", stats.failed_queries));
        report.push_str(&format!("  Отмененных: {}\n", stats.cancelled_queries));
        report.push_str("\n");

        // Время выполнения
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

            report.push_str("Время выполнения:\n");
            report.push_str(&format!("  Среднее: {:.2} мс\n", avg_time as f64 / 1000.0));
            report.push_str(&format!(
                "  Минимальное: {:.2} мс\n",
                min_time as f64 / 1000.0
            ));
            report.push_str(&format!(
                "  Максимальное: {:.2} мс\n",
                max_time as f64 / 1000.0
            ));
            report.push_str("\n");

            report.push_str("Обработка данных:\n");
            report.push_str(&format!("  Среднее количество строк: {}\n", avg_rows));
            report.push_str(&format!("  Общее количество строк: {}\n", total_rows));
            report.push_str("\n");

            // Топ медленных запросов
            let mut slow_queries: Vec<_> = completed_traces
                .iter()
                .filter(|t| t.total_duration_us.is_some())
                .collect();
            slow_queries.sort_by(|a, b| b.total_duration_us.cmp(&a.total_duration_us));

            report.push_str("Топ 5 медленных запросов:\n");
            for (i, trace) in slow_queries.iter().take(5).enumerate() {
                if let Some(duration) = trace.total_duration_us {
                    report.push_str(&format!(
                        "  {}. {} мс - {}\n",
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

    /// Создает отчет о состоянии трассировщика
    pub fn generate_status_report(&self) -> String {
        let stats = self.get_stats();
        let active_count = self.active_traces.read().unwrap().len();
        let completed_count = self.completed_traces.read().unwrap().len();

        let mut report = String::new();

        report.push_str(&format!("Активных трассировок: {}\n", active_count));
        report.push_str(&format!("Завершенных трассировок: {}\n", completed_count));
        report.push_str(&format!("Всего запросов: {}\n", stats.total_queries));
        report.push_str(&format!(
            "Успешных: {} ({:.1}%)\n",
            stats.completed_queries,
            if stats.total_queries > 0 {
                stats.completed_queries as f64 / stats.total_queries as f64 * 100.0
            } else {
                0.0
            }
        ));
        report.push_str(&format!(
            "С ошибками: {} ({:.1}%)\n",
            stats.failed_queries,
            if stats.total_queries > 0 {
                stats.failed_queries as f64 / stats.total_queries as f64 * 100.0
            } else {
                0.0
            }
        ));

        report
    }

    /// Останавливает трассировщик
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

        // Начинаем трассировку
        let query_id = tracer.start_trace("SELECT * FROM users WHERE age > 18");

        // Добавляем события
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

        // Завершаем трассировку
        tracer.finish_trace(
            &query_id,
            QueryStatus::Completed,
            Some(100),
            Some(8192),
            None,
        );

        // Проверяем статистику
        let stats = tracer.get_stats();
        assert_eq!(stats.total_queries, 1);
        assert_eq!(stats.completed_queries, 1);
        assert_eq!(stats.failed_queries, 0);

        // Проверяем завершенные трассировки
        let completed = tracer.get_completed_traces(10);
        assert_eq!(completed.len(), 1);

        let trace = &completed[0];
        assert_eq!(trace.query_id, query_id);
        assert_eq!(trace.sql_query, "SELECT * FROM users WHERE age > 18");
        assert!(matches!(trace.status, QueryStatus::Completed));
        assert_eq!(trace.rows_returned, Some(100));
        assert_eq!(trace.result_size, Some(8192));
        assert!(trace.events.len() >= 4); // Начальное + 3 добавленных + финальное

        // Проверяем отчет
        let report = tracer.generate_performance_report();
        assert!(report.contains("Отчет о производительности запросов"));
        assert!(report.contains("Всего запросов: 1"));
        assert!(report.contains("Успешно завершенных: 1"));
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
