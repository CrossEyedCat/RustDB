//! Бенчмарки для измерения производительности I/O операций RustBD

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use rustbd::storage::{
    io_optimization::{BufferedIoManager, IoBufferConfig},
    optimized_file_manager::OptimizedFileManager,
    database_file::{DatabaseFileType, ExtensionStrategy, BLOCK_SIZE},
};
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// Бенчмарк базовых операций I/O
fn bench_basic_io_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("basic_io");
    group.throughput(Throughput::Bytes(BLOCK_SIZE as u64));

    // Тестируем различные размеры кэша
    for cache_size in [1000, 5000, 10000].iter() {
        let mut config = IoBufferConfig::default();
        config.page_cache_size = *cache_size;
        
        let manager = BufferedIoManager::new(config);
        let data = vec![42u8; BLOCK_SIZE];

        group.bench_with_input(
            BenchmarkId::new("write_page", cache_size),
            cache_size,
            |b, _| {
                let mut page_id = 0;
                b.to_async(&rt).iter(|| async {
                    let _ = manager.write_page_async(1, page_id, data.clone()).await;
                    page_id += 1;
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("read_page_cached", cache_size),
            cache_size,
            |b, _| {
                // Предварительно записываем данные для кэширования
                rt.block_on(async {
                    for i in 0..100 {
                        let _ = manager.write_page_async(1, i, data.clone()).await;
                    }
                });

                let mut page_id = 0;
                b.to_async(&rt).iter(|| async {
                    let _ = manager.read_page_async(1, page_id % 100).await;
                    page_id += 1;
                });
            },
        );
    }

    group.finish();
}

/// Бенчмарк оптимизированного менеджера файлов
fn bench_optimized_file_manager(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("optimized_manager");
    group.throughput(Throughput::Bytes(BLOCK_SIZE as u64));

    // Тестируем различные стратегии расширения
    for strategy in [
        ExtensionStrategy::Fixed,
        ExtensionStrategy::Linear,
        ExtensionStrategy::Exponential,
        ExtensionStrategy::Adaptive,
    ].iter() {
        let temp_dir = TempDir::new().unwrap();
        let manager = rt.block_on(async {
            OptimizedFileManager::new(temp_dir.path()).unwrap()
        });

        let file_id = rt.block_on(async {
            manager.create_database_file(
                "bench.db",
                DatabaseFileType::Data,
                123,
                *strategy,
            ).await.unwrap()
        });

        let data = vec![100u8; BLOCK_SIZE];

        group.bench_with_input(
            BenchmarkId::new("write_sequential", format!("{:?}", strategy)),
            strategy,
            |b, _| {
                let mut page_id = 0;
                b.to_async(&rt).iter(|| async {
                    // Выделяем страницу если нужно
                    if page_id % 1000 == 0 {
                        let _ = manager.allocate_pages(file_id, 1000).await;
                    }
                    
                    let _ = manager.write_page(file_id, page_id, &data).await;
                    page_id += 1;
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("read_sequential", format!("{:?}", strategy)),
            strategy,
            |b, _| {
                // Предварительно записываем данные
                rt.block_on(async {
                    let _ = manager.allocate_pages(file_id, 1000).await;
                    for i in 0..1000 {
                        let _ = manager.write_page(file_id, i, &data).await;
                    }
                });

                let mut page_id = 0;
                b.to_async(&rt).iter(|| async {
                    let _ = manager.read_page(file_id, page_id % 1000).await;
                    page_id += 1;
                });
            },
        );
    }

    group.finish();
}

/// Бенчмарк кэша страниц
fn bench_page_cache(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("page_cache");

    // Тестируем различные паттерны доступа
    let access_patterns = [
        ("sequential", (0..1000).collect::<Vec<_>>()),
        ("random", {
            use rand::prelude::*;
            let mut rng = thread_rng();
            let mut pattern: Vec<usize> = (0..1000).collect();
            pattern.shuffle(&mut rng);
            pattern
        }),
        ("hot_cold", {
            let mut pattern = Vec::new();
            // 80% обращений к первым 20% страниц (hot data)
            for _ in 0..800 {
                pattern.push(fastrand::usize(0..200));
            }
            // 20% обращений к остальным 80% страниц (cold data)
            for _ in 0..200 {
                pattern.push(fastrand::usize(200..1000));
            }
            pattern
        }),
    ];

    for (pattern_name, pattern) in access_patterns.iter() {
        let config = IoBufferConfig::default();
        let manager = BufferedIoManager::new(config);
        let data = vec![123u8; BLOCK_SIZE];

        // Предварительно заполняем кэш
        rt.block_on(async {
            for i in 0..1000 {
                let _ = manager.write_page_async(1, i, data.clone()).await;
            }
        });

        group.bench_with_input(
            BenchmarkId::new("cache_access", pattern_name),
            pattern,
            |b, pattern| {
                let mut index = 0;
                b.to_async(&rt).iter(|| async {
                    let page_id = pattern[index % pattern.len()] as u64;
                    let _ = manager.read_page_async(1, page_id).await;
                    index += 1;
                });
            },
        );
    }

    group.finish();
}

/// Бенчмарк буферизации записи
fn bench_write_buffering(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("write_buffering");
    group.throughput(Throughput::Bytes(BLOCK_SIZE as u64));

    // Тестируем различные размеры буфера
    for buffer_size in [100, 500, 1000, 2000].iter() {
        let mut config = IoBufferConfig::default();
        config.max_write_buffer_size = *buffer_size;
        
        let manager = BufferedIoManager::new(config);
        let data = vec![200u8; BLOCK_SIZE];

        group.bench_with_input(
            BenchmarkId::new("buffered_writes", buffer_size),
            buffer_size,
            |b, _| {
                let mut page_id = 0;
                b.to_async(&rt).iter(|| async {
                    let _ = manager.write_page_async(1, page_id, data.clone()).await;
                    page_id += 1;
                });
            },
        );
    }

    group.finish();
}

/// Бенчмарк смешанных операций (чтение + запись)
fn bench_mixed_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("mixed_operations");
    group.throughput(Throughput::Bytes(BLOCK_SIZE as u64));

    // Тестируем различные соотношения чтения/записи
    let read_write_ratios = [
        ("read_heavy", 0.8), // 80% чтения, 20% записи
        ("balanced", 0.5),   // 50% чтения, 50% записи
        ("write_heavy", 0.2), // 20% чтения, 80% записи
    ];

    for (ratio_name, read_ratio) in read_write_ratios.iter() {
        let temp_dir = TempDir::new().unwrap();
        let manager = rt.block_on(async {
            OptimizedFileManager::new(temp_dir.path()).unwrap()
        });

        let file_id = rt.block_on(async {
            manager.create_database_file(
                "mixed.db",
                DatabaseFileType::Data,
                456,
                ExtensionStrategy::Adaptive,
            ).await.unwrap()
        });

        let data = vec![150u8; BLOCK_SIZE];

        // Предварительно создаем некоторые данные
        rt.block_on(async {
            let _ = manager.allocate_pages(file_id, 1000).await;
            for i in 0..1000 {
                let _ = manager.write_page(file_id, i, &data).await;
            }
        });

        group.bench_with_input(
            BenchmarkId::new("mixed_ops", ratio_name),
            ratio_name,
            |b, _| {
                let mut operation_count = 0;
                b.to_async(&rt).iter(|| async {
                    let should_read = fastrand::f64() < *read_ratio;
                    let page_id = fastrand::u64(0..1000);

                    if should_read {
                        let _ = manager.read_page(file_id, page_id).await;
                    } else {
                        let _ = manager.write_page(file_id, page_id, &data).await;
                    }
                    
                    operation_count += 1;
                });
            },
        );
    }

    group.finish();
}

/// Бенчмарк производительности под нагрузкой
fn bench_concurrent_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("concurrent_operations");
    group.sample_size(10); // Меньше образцов для долгих тестов

    // Тестируем различное количество одновременных операций
    for concurrency in [1, 4, 8, 16].iter() {
        let temp_dir = TempDir::new().unwrap();
        let manager = rt.block_on(async {
            OptimizedFileManager::new(temp_dir.path()).unwrap()
        });

        let file_id = rt.block_on(async {
            manager.create_database_file(
                "concurrent.db",
                DatabaseFileType::Data,
                789,
                ExtensionStrategy::Fixed,
            ).await.unwrap()
        });

        let data = vec![75u8; BLOCK_SIZE];

        group.bench_with_input(
            BenchmarkId::new("concurrent_writes", concurrency),
            concurrency,
            |b, &concurrency| {
                b.to_async(&rt).iter(|| async {
                    let mut handles = Vec::new();
                    
                    for i in 0..concurrency {
                        let manager_ref = &manager;
                        let data_ref = &data;
                        let handle = tokio::spawn(async move {
                            for j in 0..100 {
                                let page_id = (i * 100 + j) as u64;
                                if page_id % 1000 == 0 {
                                    let _ = manager_ref.allocate_pages(file_id, 1000).await;
                                }
                                let _ = manager_ref.write_page(file_id, page_id, data_ref).await;
                            }
                        });
                        handles.push(handle);
                    }
                    
                    for handle in handles {
                        let _ = handle.await;
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_basic_io_operations,
    bench_optimized_file_manager,
    bench_page_cache,
    bench_write_buffering,
    bench_mixed_operations,
    bench_concurrent_operations
);

criterion_main!(benches);
