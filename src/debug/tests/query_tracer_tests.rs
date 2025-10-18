//! Тесты для трассировщика запросов

use crate::debug::query_tracer::*;
use crate::debug::DebugConfig;
use std::time::Duration;

#[tokio::test]
async fn test_query_tracer_creation() {
    let config = DebugConfig {
        enable_query_tracing: true,
        ..Default::default()
    };

    let tracer = QueryTracer::new(&config);
    
    // Проверяем, что трассировщик создался
    let stats = tracer.get_stats();
    assert_eq!(stats.total_queries, 0);
}

#[tokio::test]
async fn test_query_trace_lifecycle() {
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
    assert_eq!(trace.status, QueryStatus::Completed);
    assert_eq!(trace.rows_returned, Some(100));
    assert_eq!(trace.result_size, Some(8192));
    assert!(trace.events.len() >= 4); // Начальное + 3 добавленных + финальное
}

#[tokio::test]
async fn test_query_trace_error() {
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
    assert_eq!(trace.status, QueryStatus::Failed);
    assert_eq!(trace.error, Some("Syntax error at line 1".to_string()));
}

#[tokio::test]
async fn test_multiple_query_traces() {
    let config = DebugConfig {
        enable_query_tracing: true,
        ..Default::default()
    };

    let tracer = QueryTracer::new(&config);

    // Трассируем несколько запросов
    let query_ids = vec![
        tracer.start_trace("SELECT * FROM users"),
        tracer.start_trace("INSERT INTO users VALUES (1, 'John')"),
        tracer.start_trace("UPDATE users SET name = 'Jane' WHERE id = 1"),
    ];

    // Завершаем все запросы
    for (i, query_id) in query_ids.iter().enumerate() {
        tracer.finish_trace(
            query_id,
            QueryStatus::Completed,
            Some(10 + i as u64),
            Some(1024 * (i + 1) as u64),
            None,
        );
    }

    let stats = tracer.get_stats();
    assert_eq!(stats.total_queries, 3);
    assert_eq!(stats.completed_queries, 3);
    assert_eq!(stats.failed_queries, 0);

    let completed = tracer.get_completed_traces(10);
    assert_eq!(completed.len(), 3);
}

#[tokio::test]
async fn test_active_trace_retrieval() {
    let config = DebugConfig {
        enable_query_tracing: true,
        ..Default::default()
    };

    let tracer = QueryTracer::new(&config);

    let query_id = tracer.start_trace("SELECT * FROM large_table");

    // Добавляем событие
    tracer.add_event(
        &query_id,
        QueryStage::Execution,
        "Still executing...",
        None,
        None,
        Some(8192),
        Some(1000),
    );

    // Получаем активную трассировку
    let active_trace = tracer.get_active_trace(&query_id);
    assert!(active_trace.is_some());
    
    let trace = active_trace.unwrap();
    assert_eq!(trace.query_id, query_id);
    assert_eq!(trace.status, QueryStatus::Running);
    assert_eq!(trace.current_stage, QueryStage::Execution);
    assert!(trace.events.len() >= 2); // Начальное + добавленное

    // Завершаем трассировку
    tracer.finish_trace(
        &query_id,
        QueryStatus::Completed,
        Some(1000),
        Some(16384),
        None,
    );

    // Теперь трассировка должна быть в завершенных
    let active_trace = tracer.get_active_trace(&query_id);
    assert!(active_trace.is_none());

    let completed = tracer.get_completed_traces(10);
    assert_eq!(completed.len(), 1);
}

#[tokio::test]
async fn test_performance_report() {
    let config = DebugConfig {
        enable_query_tracing: true,
        ..Default::default()
    };

    let tracer = QueryTracer::new(&config);

    // Создаем несколько трассировок с разным временем выполнения
    let queries = vec![
        ("SELECT * FROM users", Duration::from_millis(10)),
        ("SELECT * FROM orders", Duration::from_millis(50)),
        ("SELECT * FROM products", Duration::from_millis(100)),
    ];

    for (sql, duration) in queries {
        let query_id = tracer.start_trace(sql);
        
        // Симулируем выполнение
        tokio::time::sleep(duration).await;
        
        tracer.finish_trace(
            &query_id,
            QueryStatus::Completed,
            Some(100),
            Some(1024),
            None,
        );
    }

    let report = tracer.generate_performance_report();
    assert!(report.contains("Отчет о производительности запросов"));
    assert!(report.contains("Всего запросов: 3"));
    assert!(report.contains("Успешно завершенных: 3"));
    assert!(report.contains("Топ 5 медленных запросов"));
}

#[tokio::test]
async fn test_status_report() {
    let config = DebugConfig {
        enable_query_tracing: true,
        ..Default::default()
    };

    let tracer = QueryTracer::new(&config);

    // Создаем несколько трассировок
    let query_id1 = tracer.start_trace("SELECT * FROM users");
    tracer.finish_trace(&query_id1, QueryStatus::Completed, Some(10), Some(1024), None);

    let query_id2 = tracer.start_trace("INVALID QUERY");
    tracer.finish_trace(&query_id2, QueryStatus::Failed, None, None, Some("Error".to_string()));

    let report = tracer.generate_status_report();
    assert!(report.contains("Активных трассировок: 0"));
    assert!(report.contains("Завершенных трассировок: 2"));
    assert!(report.contains("Всего запросов: 2"));
    assert!(report.contains("Успешных: 1 (50.0%)"));
    assert!(report.contains("С ошибками: 1 (50.0%)"));
}
