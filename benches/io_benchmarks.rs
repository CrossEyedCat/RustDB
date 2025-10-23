//! Бенчмарки для измерения производительности I/O операций rustdb

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rustdb::common::types::PAGE_SIZE;
use rustdb::storage::{
    database_file::{DatabaseFileType, ExtensionStrategy},
    io_optimization::{BufferedIoManager, IoBufferConfig},
    optimized_file_manager::OptimizedFileManager,
};
use tempfile::TempDir;

/// Бенчмарк базовых операций I/O
fn bench_basic_io_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("basic_io");
    group.throughput(Throughput::Bytes(PAGE_SIZE as u64));

    // Тестируем различные размеры кэша
    for cache_size in [1000, 5000, 10000].iter() {
        let config = IoBufferConfig {
            page_cache_size: *cache_size,
            ..Default::default()
        };

        let manager = BufferedIoManager::new(config);
        let data = vec![42u8; PAGE_SIZE];

        group.bench_with_input(
            BenchmarkId::new("write_page", cache_size),
            cache_size,
            |b, _| {
                b.iter(|| {
                    // Простое тестирование без async
                    let result = manager.write_page_async(1, 1, data.clone());
                    std::mem::drop(criterion::black_box(result));
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("read_page", cache_size),
            cache_size,
            |b, _| {
                b.iter(|| {
                    let result = manager.read_page_async(1, 1);
                    std::mem::drop(criterion::black_box(result));
                });
            },
        );
    }

    group.finish();
}

/// Бенчмарк операций с буферизованным I/O
fn bench_buffered_io_operations(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let _temp_path = temp_dir.path().join("buffered_test.db");

    let mut group = c.benchmark_group("buffered_io");
    group.throughput(Throughput::Bytes(PAGE_SIZE as u64));

    // Тестируем различные конфигурации буфера
    let configs = vec![
        (
            "small",
            IoBufferConfig {
                page_cache_size: 1000,
                ..Default::default()
            },
        ),
        (
            "medium",
            IoBufferConfig {
                page_cache_size: 5000,
                ..Default::default()
            },
        ),
        (
            "large",
            IoBufferConfig {
                page_cache_size: 10000,
                ..Default::default()
            },
        ),
    ];

    for (name, config) in configs {
        let manager = BufferedIoManager::new(config);
        let data = vec![0xAB; PAGE_SIZE];

        group.bench_with_input(BenchmarkId::new("write_operation", name), &name, |b, _| {
            b.iter(|| {
                let result = manager.write_page_async(1, 1, data.clone());
                std::mem::drop(criterion::black_box(result));
            });
        });

        group.bench_with_input(BenchmarkId::new("read_operation", name), &name, |b, _| {
            b.iter(|| {
                let result = manager.read_page_async(1, 1);
                std::mem::drop(criterion::black_box(result));
            });
        });
    }

    group.finish();
}

/// Бенчмарк операций с оптимизированным файловым менеджером
fn bench_optimized_file_manager_operations(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().join("optimized_test.db");

    let mut group = c.benchmark_group("optimized_file_manager");
    group.throughput(Throughput::Bytes(PAGE_SIZE as u64));

    // Создаем оптимизированный файловый менеджер
    let manager = OptimizedFileManager::new(temp_path.clone()).unwrap();
    let data = vec![0xCD; PAGE_SIZE];

    group.bench_function("create_file", |b| {
        b.iter(|| {
            let result = manager.create_database_file(
                "test.db",
                DatabaseFileType::Data,
                1,
                ExtensionStrategy::Linear,
            );
            std::mem::drop(criterion::black_box(result));
        });
    });

    group.bench_function("write_page", |b| {
        b.iter(|| {
            let result = manager.write_page(1, 1, &data);
            std::mem::drop(criterion::black_box(result));
        });
    });

    group.bench_function("read_page", |b| {
        b.iter(|| {
            let result = manager.read_page(1, 1);
            std::mem::drop(criterion::black_box(result));
        });
    });

    group.finish();
}

/// Бенчмарк операций с различными паттернами доступа
fn bench_access_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("access_patterns");
    group.throughput(Throughput::Bytes(PAGE_SIZE as u64));

    let manager = BufferedIoManager::new(IoBufferConfig::default());
    let data = vec![0xEF; PAGE_SIZE];

    // Последовательный доступ
    group.bench_function("sequential_access", |b| {
        b.iter(|| {
            for i in 0..10 {
                let result = manager.write_page_async(1, i, data.clone());
                std::mem::drop(criterion::black_box(result));
            }
        });
    });

    // Случайный доступ (упрощенная версия)
    group.bench_function("random_access", |b| {
        b.iter(|| {
            let pages = [1, 5, 3, 8, 2, 9, 4, 7, 6, 0];
            for &page_id in &pages {
                let result = manager.write_page_async(1, page_id, data.clone());
                std::mem::drop(criterion::black_box(result));
            }
        });
    });

    group.finish();
}

/// Бенчмарк операций с различными соотношениями чтения/записи
fn bench_read_write_ratios(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_write_ratios");
    group.throughput(Throughput::Bytes(PAGE_SIZE as u64));

    let manager = BufferedIoManager::new(IoBufferConfig::default());
    let data = vec![0x12; PAGE_SIZE];

    // 90% чтения, 10% записи
    group.bench_function("90_read_10_write", |b| {
        b.iter(|| {
            for i in 0..10 {
                if i < 9 {
                    let result = manager.read_page_async(1, i);
                    std::mem::drop(criterion::black_box(result));
                } else {
                    let result = manager.write_page_async(1, i, data.clone());
                    std::mem::drop(criterion::black_box(result));
                }
            }
        });
    });

    // 50% чтения, 50% записи
    group.bench_function("50_read_50_write", |b| {
        b.iter(|| {
            for i in 0..10 {
                if i % 2 == 0 {
                    let result = manager.read_page_async(1, i);
                    std::mem::drop(criterion::black_box(result));
                } else {
                    let result = manager.write_page_async(1, i, data.clone());
                    std::mem::drop(criterion::black_box(result));
                }
            }
        });
    });

    // 10% чтения, 90% записи
    group.bench_function("10_read_90_write", |b| {
        b.iter(|| {
            for i in 0..10 {
                if i == 0 {
                    let result = manager.read_page_async(1, i);
                    std::mem::drop(criterion::black_box(result));
                } else {
                    let result = manager.write_page_async(1, i, data.clone());
                    std::mem::drop(criterion::black_box(result));
                }
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_basic_io_operations,
    bench_buffered_io_operations,
    bench_optimized_file_manager_operations,
    bench_access_patterns,
    bench_read_write_ratios
);

criterion_main!(benches);
