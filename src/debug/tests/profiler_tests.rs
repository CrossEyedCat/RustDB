//! Тесты для профилировщика

use crate::debug::profiler::*;
use crate::debug::DebugConfig;
use std::time::Duration;

#[tokio::test]
async fn test_profiler_creation() {
    let config = DebugConfig {
        enable_cpu_profiling: true,
        enable_memory_profiling: true,
        ..Default::default()
    };

    let profiler = Profiler::new(&config);

    // Проверяем, что профилировщик создался
    let stats = profiler.get_stats();
    assert_eq!(stats.total_snapshots, 0);
}

#[tokio::test]
async fn test_profiler_data_collection() {
    let config = DebugConfig {
        enable_cpu_profiling: true,
        enable_memory_profiling: true,
        ..Default::default()
    };

    let profiler = Profiler::new(&config);

    // Ждем немного, чтобы накопились снимки
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Проверяем статистику
    let stats = profiler.get_stats();
    assert!(stats.total_snapshots > 0);
    assert!(stats.profiling_duration_seconds > 0);

    // Проверяем снимки
    let snapshots = profiler.get_snapshots(10);
    assert!(!snapshots.is_empty());

    // Проверяем, что снимки содержат ожидаемые данные
    for snapshot in &snapshots {
        assert!(snapshot.timestamp > 0);
        
        if let Some(cpu) = &snapshot.cpu {
            assert!(cpu.cpu_usage >= 0.0 && cpu.cpu_usage <= 100.0);
            assert!(cpu.thread_count > 0);
        }
        
        if let Some(memory) = &snapshot.memory {
            assert!(memory.memory_usage_percent >= 0.0 && memory.memory_usage_percent <= 100.0);
            assert!(memory.total_memory > 0);
            assert!(memory.used_memory <= memory.total_memory);
        }
    }
}

#[tokio::test]
async fn test_profiler_start_stop() {
    let config = DebugConfig {
        enable_cpu_profiling: true,
        enable_memory_profiling: false,
        ..Default::default()
    };

    let mut profiler = Profiler::new(&config);

    // Ждем немного для сбора данных
    tokio::time::sleep(Duration::from_millis(200)).await;

    let stats_before = profiler.get_stats();
    assert!(stats_before.total_snapshots > 0);

    // Останавливаем профилирование
    profiler.stop_profiling();
    
    let is_profiling = *profiler.is_profiling.read().unwrap();
    assert!(!is_profiling);

    // Ждем еще немного
    tokio::time::sleep(Duration::from_millis(200)).await;

    let stats_after = profiler.get_stats();
    // Количество снимков не должно увеличиться после остановки
    assert_eq!(stats_before.total_snapshots, stats_after.total_snapshots);
}

#[tokio::test]
async fn test_cpu_only_profiling() {
    let config = DebugConfig {
        enable_cpu_profiling: true,
        enable_memory_profiling: false,
        ..Default::default()
    };

    let profiler = Profiler::new(&config);

    tokio::time::sleep(Duration::from_millis(300)).await;

    let snapshots = profiler.get_snapshots(5);
    assert!(!snapshots.is_empty());

    for snapshot in &snapshots {
        assert!(snapshot.cpu.is_some());
        assert!(snapshot.memory.is_none());
    }
}

#[tokio::test]
async fn test_memory_only_profiling() {
    let config = DebugConfig {
        enable_cpu_profiling: false,
        enable_memory_profiling: true,
        ..Default::default()
    };

    let profiler = Profiler::new(&config);

    tokio::time::sleep(Duration::from_millis(300)).await;

    let snapshots = profiler.get_snapshots(5);
    assert!(!snapshots.is_empty());

    for snapshot in &snapshots {
        assert!(snapshot.cpu.is_none());
        assert!(snapshot.memory.is_some());
    }
}

#[tokio::test]
async fn test_performance_report() {
    let config = DebugConfig {
        enable_cpu_profiling: true,
        enable_memory_profiling: true,
        ..Default::default()
    };

    let profiler = Profiler::new(&config);

    tokio::time::sleep(Duration::from_millis(500)).await;

    let report = profiler.generate_performance_report();
    assert!(report.contains("Отчет о производительности системы"));
    assert!(report.contains("Общая информация"));
    assert!(report.contains("CPU статистика"));
    assert!(report.contains("Memory статистика"));
    assert!(report.contains("Рекомендации"));
}

#[tokio::test]
async fn test_status_report() {
    let config = DebugConfig {
        enable_cpu_profiling: true,
        enable_memory_profiling: true,
        ..Default::default()
    };

    let profiler = Profiler::new(&config);

    tokio::time::sleep(Duration::from_millis(200)).await;

    let report = profiler.generate_status_report();
    assert!(report.contains("Профилирование активно: true"));
    assert!(report.contains("CPU профилирование: true"));
    assert!(report.contains("Memory профилирование: true"));
    assert!(report.contains("Длительность профилирования"));
}

#[test]
fn test_performance_snapshot_creation() {
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
        assert!(cpu.user_time_us > 0);
        assert!(cpu.system_time_us > 0);
        assert!(cpu.idle_time_us > 0);
    }
    
    if let Some(memory) = snapshot.memory {
        assert!(memory.memory_usage_percent >= 0.0 && memory.memory_usage_percent <= 100.0);
        assert!(memory.total_memory > 0);
        assert!(memory.used_memory <= memory.total_memory);
        assert!(memory.available_memory <= memory.total_memory);
        assert!(memory.process_memory > 0);
        assert!(memory.process_virtual_memory > 0);
    }
}

#[test]
fn test_trend_analysis() {
    // Создаем тестовые снимки с растущим трендом
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
            additional_metrics: std::collections::HashMap::new(),
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
            additional_metrics: std::collections::HashMap::new(),
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
            additional_metrics: std::collections::HashMap::new(),
        },
    ];

    let trend = Profiler::analyze_trend(&snapshots, |s| s.cpu.as_ref().map(|c| c.cpu_usage));
    assert!(trend.is_some());
    assert!(trend.unwrap().contains("Растущий"));
}

#[test]
fn test_trend_analysis_stable() {
    // Создаем тестовые снимки со стабильным трендом
    let snapshots = vec![
        PerformanceSnapshot {
            timestamp: 1000,
            cpu: Some(CpuMetric {
                timestamp: 1000,
                cpu_usage: 50.0,
                thread_count: 4,
                user_time_us: 1000,
                system_time_us: 100,
                idle_time_us: 900,
            }),
            memory: None,
            additional_metrics: std::collections::HashMap::new(),
        },
        PerformanceSnapshot {
            timestamp: 2000,
            cpu: Some(CpuMetric {
                timestamp: 2000,
                cpu_usage: 51.0,
                thread_count: 4,
                user_time_us: 2000,
                system_time_us: 200,
                idle_time_us: 1800,
            }),
            memory: None,
            additional_metrics: std::collections::HashMap::new(),
        },
        PerformanceSnapshot {
            timestamp: 3000,
            cpu: Some(CpuMetric {
                timestamp: 3000,
                cpu_usage: 49.0,
                thread_count: 4,
                user_time_us: 3000,
                system_time_us: 300,
                idle_time_us: 2700,
            }),
            memory: None,
            additional_metrics: std::collections::HashMap::new(),
        },
    ];

    let trend = Profiler::analyze_trend(&snapshots, |s| s.cpu.as_ref().map(|c| c.cpu_usage));
    assert!(trend.is_some());
    assert!(trend.unwrap().contains("Стабильный"));
}
