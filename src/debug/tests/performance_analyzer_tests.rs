//! Тесты для анализатора производительности

use crate::debug::performance_analyzer::*;
use crate::debug::DebugConfig;
use std::time::Duration;

#[tokio::test]
async fn test_performance_analyzer_creation() {
    let config = DebugConfig {
        metrics_collection_interval: 1,
        ..Default::default()
    };

    let analyzer = PerformanceAnalyzer::new(&config);

    // Проверяем, что анализатор создался
    let stats = analyzer.get_stats();
    assert_eq!(stats.total_analyses, 0);
}

#[tokio::test]
async fn test_performance_analysis() {
    let config = DebugConfig {
        metrics_collection_interval: 1,
        ..Default::default()
    };

    let analyzer = PerformanceAnalyzer::new(&config);

    // Ждем немного, чтобы накопились анализы
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Проверяем статистику
    let stats = analyzer.get_stats();
    assert!(stats.total_analyses > 0);

    // Проверяем последний анализ
    let latest_analysis = analyzer.get_latest_analysis();
    assert!(latest_analysis.is_some());

    if let Some(analysis) = latest_analysis {
        assert!(analysis.overall_score >= 0.0 && analysis.overall_score <= 100.0);
        assert!(!analysis.metrics.is_empty());
    }
}

#[tokio::test]
async fn test_bottleneck_detection() {
    let config = DebugConfig {
        metrics_collection_interval: 1,
        ..Default::default()
    };

    let analyzer = PerformanceAnalyzer::new(&config);

    // Ждем накопления анализов
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let latest_analysis = analyzer.get_latest_analysis();
    assert!(latest_analysis.is_some());

    if let Some(analysis) = latest_analysis {
        // Проверяем, что анализ содержит метрики
        assert!(!analysis.metrics.is_empty());
        
        // Проверяем, что каждая метрика имеет правильную структуру
        for metric in &analysis.metrics {
            assert!(!metric.name.is_empty());
            assert!(metric.value >= 0.0);
            assert!(!metric.unit.is_empty());
            assert!(metric.timestamp > 0);
            assert!(!metric.component.is_empty());
            assert!(metric.thresholds.warning < metric.thresholds.critical);
        }
    }
}

#[tokio::test]
async fn test_performance_report() {
    let config = DebugConfig {
        metrics_collection_interval: 1,
        ..Default::default()
    };

    let analyzer = PerformanceAnalyzer::new(&config);

    tokio::time::sleep(Duration::from_millis(1500)).await;

    let report = analyzer.generate_performance_report();
    assert!(report.contains("Отчет анализа производительности"));
    assert!(report.contains("Общая статистика"));
    assert!(report.contains("Рекомендации по улучшению"));
}

#[tokio::test]
async fn test_status_report() {
    let config = DebugConfig {
        metrics_collection_interval: 1,
        ..Default::default()
    };

    let analyzer = PerformanceAnalyzer::new(&config);

    tokio::time::sleep(Duration::from_millis(500)).await;

    let report = analyzer.generate_status_report();
    assert!(report.contains("Анализов в памяти"));
    assert!(report.contains("Метрик в истории"));
    assert!(report.contains("Всего анализов"));
    assert!(report.contains("Интервал сбора метрик"));
}

#[test]
fn test_metrics_collection() {
    let metrics = PerformanceAnalyzer::collect_performance_metrics();
    
    assert!(!metrics.is_empty());
    
    // Проверяем, что все ожидаемые метрики присутствуют
    let metric_names: std::collections::HashSet<String> = metrics.iter()
        .map(|m| m.name.clone())
        .collect();
    
    assert!(metric_names.contains("cpu_usage"));
    assert!(metric_names.contains("memory_usage"));
    assert!(metric_names.contains("disk_io"));
    assert!(metric_names.contains("network_io"));
    assert!(metric_names.contains("cache_hit_ratio"));
    assert!(metric_names.contains("lock_contention"));
    
    // Проверяем структуру каждой метрики
    for metric in &metrics {
        assert!(!metric.name.is_empty());
        assert!(metric.value >= 0.0);
        assert!(!metric.unit.is_empty());
        assert!(metric.timestamp > 0);
        assert!(!metric.component.is_empty());
        assert!(metric.thresholds.warning < metric.thresholds.critical);
    }
}

#[test]
fn test_bottleneck_detection_critical() {
    let metrics = vec![
        PerformanceMetric {
            name: "cpu_usage".to_string(),
            value: 95.0, // Критическое значение
            unit: "%".to_string(),
            timestamp: 1000,
            component: "System".to_string(),
            thresholds: Thresholds { warning: 70.0, critical: 90.0 },
        },
        PerformanceMetric {
            name: "memory_usage".to_string(),
            value: 75.0, // Предупреждающее значение
            unit: "%".to_string(),
            timestamp: 1000,
            component: "System".to_string(),
            thresholds: Thresholds { warning: 80.0, critical: 95.0 },
        },
    ];

    let analysis = PerformanceAnalyzer::perform_analysis(&metrics);

    assert!(analysis.overall_score < 100.0);
    assert!(!analysis.bottlenecks.is_empty());
    
    let critical_bottlenecks: Vec<_> = analysis.bottlenecks.iter()
        .filter(|b| matches!(b.severity, SeverityLevel::Critical))
        .collect();
    
    assert!(!critical_bottlenecks.is_empty());
    assert!(critical_bottlenecks.iter().any(|b| matches!(b.bottleneck_type, BottleneckType::Cpu)));
}

#[test]
fn test_bottleneck_detection_warning() {
    let metrics = vec![
        PerformanceMetric {
            name: "cpu_usage".to_string(),
            value: 75.0, // Предупреждающее значение
            unit: "%".to_string(),
            timestamp: 1000,
            component: "System".to_string(),
            thresholds: Thresholds { warning: 70.0, critical: 90.0 },
        },
        PerformanceMetric {
            name: "memory_usage".to_string(),
            value: 50.0, // Нормальное значение
            unit: "%".to_string(),
            timestamp: 1000,
            component: "System".to_string(),
            thresholds: Thresholds { warning: 80.0, critical: 95.0 },
        },
    ];

    let analysis = PerformanceAnalyzer::perform_analysis(&metrics);

    assert!(analysis.overall_score < 100.0);
    assert!(!analysis.bottlenecks.is_empty());
    
    let warning_bottlenecks: Vec<_> = analysis.bottlenecks.iter()
        .filter(|b| matches!(b.severity, SeverityLevel::Warning))
        .collect();
    
    assert!(!warning_bottlenecks.is_empty());
    assert!(warning_bottlenecks.iter().any(|b| matches!(b.bottleneck_type, BottleneckType::Cpu)));
}

#[test]
fn test_bottleneck_detection_normal() {
    let metrics = vec![
        PerformanceMetric {
            name: "cpu_usage".to_string(),
            value: 50.0, // Нормальное значение
            unit: "%".to_string(),
            timestamp: 1000,
            component: "System".to_string(),
            thresholds: Thresholds { warning: 70.0, critical: 90.0 },
        },
        PerformanceMetric {
            name: "memory_usage".to_string(),
            value: 40.0, // Нормальное значение
            unit: "%".to_string(),
            timestamp: 1000,
            component: "System".to_string(),
            thresholds: Thresholds { warning: 80.0, critical: 95.0 },
        },
    ];

    let analysis = PerformanceAnalyzer::perform_analysis(&metrics);

    assert_eq!(analysis.overall_score, 100.0);
    assert!(analysis.bottlenecks.is_empty());
}

#[test]
fn test_recommendations() {
    let cpu_recommendations = PerformanceAnalyzer::get_recommendations("cpu_usage", SeverityLevel::Critical);
    assert!(!cpu_recommendations.is_empty());
    assert!(cpu_recommendations.iter().any(|r| r.contains("оптимизируйте")));

    let memory_recommendations = PerformanceAnalyzer::get_recommendations("memory_usage", SeverityLevel::Warning);
    assert!(!memory_recommendations.is_empty());
    assert!(memory_recommendations.iter().any(|r| r.contains("память")));

    let cache_recommendations = PerformanceAnalyzer::get_recommendations("cache_hit_ratio", SeverityLevel::Warning);
    assert!(!cache_recommendations.is_empty());
    assert!(cache_recommendations.iter().any(|r| r.contains("кэш")));

    let lock_recommendations = PerformanceAnalyzer::get_recommendations("lock_contention", SeverityLevel::Critical);
    assert!(!lock_recommendations.is_empty());
    assert!(lock_recommendations.iter().any(|r| r.contains("блокировок")));
}

#[test]
fn test_bottleneck_type_detection() {
    assert_eq!(
        PerformanceAnalyzer::get_bottleneck_type("cpu_usage"),
        BottleneckType::Cpu
    );
    assert_eq!(
        PerformanceAnalyzer::get_bottleneck_type("memory_usage"),
        BottleneckType::Memory
    );
    assert_eq!(
        PerformanceAnalyzer::get_bottleneck_type("disk_io"),
        BottleneckType::Io
    );
    assert_eq!(
        PerformanceAnalyzer::get_bottleneck_type("network_io"),
        BottleneckType::Network
    );
    assert_eq!(
        PerformanceAnalyzer::get_bottleneck_type("lock_contention"),
        BottleneckType::Locking
    );
    assert_eq!(
        PerformanceAnalyzer::get_bottleneck_type("cache_hit_ratio"),
        BottleneckType::Cache
    );
}

#[test]
fn test_trend_analysis() {
    let metrics = vec![
        PerformanceMetric {
            name: "cpu_usage".to_string(),
            value: 50.0,
            unit: "%".to_string(),
            timestamp: 1000,
            component: "System".to_string(),
            thresholds: Thresholds { warning: 70.0, critical: 90.0 },
        },
    ];

    let analysis = PerformanceAnalyzer::perform_analysis(&metrics);
    
    assert!(!analysis.trends.is_empty());
    assert!(analysis.trends.contains_key("cpu_usage"));
    
    let trend = &analysis.trends["cpu_usage"];
    assert!(!trend.is_empty());
}
