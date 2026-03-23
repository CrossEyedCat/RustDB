//! Profiler tests

use crate::debug::profiler::*;
use crate::debug::DebugConfig;
use std::time::Duration;

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn test_profiler_creation() {
    let config = DebugConfig {
        enable_cpu_profiling: true,
        enable_memory_profiling: true,
        ..Default::default()
    };

    let profiler = Profiler::new(&config);

    // Checking that the profiler has been created
    let stats = profiler.get_stats();
    assert_eq!(stats.total_snapshots, 0);
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn test_profiler_data_collection() {
    let config = DebugConfig {
        enable_cpu_profiling: true,
        enable_memory_profiling: true,
        ..Default::default()
    };

    let profiler = Profiler::new(&config);

    // We'll wait a bit for the pictures to accumulate.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Checking the statistics
    let stats = profiler.get_stats();
    assert!(stats.total_snapshots > 0);
    assert!(stats.profiling_duration_seconds > 0);

    // Checking the pictures
    let snapshots = profiler.get_snapshots(10);
    assert!(!snapshots.is_empty());

    // Checking that the images contain the expected data
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

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn test_profiler_start_stop() {
    let config = DebugConfig {
        enable_cpu_profiling: true,
        enable_memory_profiling: false,
        ..Default::default()
    };

    let mut profiler = Profiler::new(&config);

    // We wait a little to collect data
    tokio::time::sleep(Duration::from_millis(200)).await;

    let stats_before = profiler.get_stats();
    assert!(stats_before.total_snapshots > 0);

    // Stopping profiling
    profiler.stop_profiling();
    
    let is_profiling = *profiler.is_profiling.read().unwrap();
    assert!(!is_profiling);

    // We'll wait a little longer
    tokio::time::sleep(Duration::from_millis(200)).await;

    let stats_after = profiler.get_stats();
    // The number of shots should not increase after stopping
    assert_eq!(stats_before.total_snapshots, stats_after.total_snapshots);
}

#[cfg_attr(miri, ignore)]
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

#[cfg_attr(miri, ignore)]
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

#[cfg_attr(miri, ignore)]
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
    assert!(report.contains("System Performance Report"));
    assert!(report.contains("General information"));
    assert!(report.contains("CPU statistics"));
    assert!(report.contains("Memory statistics"));
    assert!(report.contains("Recommendations"));
}

#[cfg_attr(miri, ignore)]
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
    assert!(report.contains("Profiling active: true"));
    assert!(report.contains("CPU profiling: true"));
    assert!(report.contains("Memory profiling: true"));
    assert!(report.contains("Profiling duration"));
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
    // We create test images with a growing trend
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
    assert!(trend.unwrap().contains("Growing"));
}

#[test]
fn test_trend_analysis_stable() {
    // We create test images with a stable trend
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
    assert!(trend.unwrap().contains("Stable"));
}
