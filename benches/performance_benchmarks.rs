//! Бенчмарки производительности RustBD

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use rustbd::storage::{
    file_manager::FileManager,
    advanced_file_manager::AdvancedFileManager,
    database_file::{DatabaseFileType, ExtensionStrategy},
    block::{Block, BLOCK_SIZE},
    page::PAGE_SIZE,
};
use rustbd::common::types::{BlockId, PageId};
use tempfile::TempDir;

fn bench_block_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("block_operations");
    
    // Бенчмарк создания блока
    group.bench_function("create_block", |b| {
        b.iter(|| {
            let block = Block::new(black_box(BlockId(1)));
            black_box(block);
        });
    });
    
    // Бенчмарк записи данных в блок
    let test_data = vec![0xAB; 1024];
    group.bench_function("write_block_data", |b| {
        b.iter(|| {
            let mut block = Block::new(BlockId(1));
            block.write_data(black_box(0), black_box(&test_data)).unwrap();
            black_box(block);
        });
    });
    
    // Бенчмарк чтения данных из блока
    let mut block = Block::new(BlockId(1));
    let test_data = vec![0xCD; 1024];
    block.write_data(0, &test_data).unwrap();
    
    group.bench_function("read_block_data", |b| {
        b.iter(|| {
            let mut buffer = vec![0u8; 1024];
            block.read_data(black_box(0), black_box(&mut buffer)).unwrap();
            black_box(buffer);
        });
    });
    
    // Бенчмарк сериализации блока
    group.bench_function("serialize_block", |b| {
        b.iter(|| {
            let serialized = block.serialize();
            black_box(serialized);
        });
    });
    
    // Бенчмарк десериализации блока
    let serialized = block.serialize();
    group.bench_function("deserialize_block", |b| {
        b.iter(|| {
            let deserialized = Block::deserialize(black_box(&serialized), black_box(BlockId(1))).unwrap();
            black_box(deserialized);
        });
    });
    
    group.finish();
}

fn bench_file_manager_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("file_manager_operations");
    
    // Подготовка
    let temp_dir = TempDir::new().unwrap();
    let mut file_manager = FileManager::new(temp_dir.path().to_path_buf());
    let file_id = file_manager.create_file("benchmark.dat").unwrap();
    
    // Бенчмарк записи блока
    let mut test_block = Block::new(BlockId(1));
    let test_data = vec![0xEF; BLOCK_SIZE];
    test_block.write_data(0, &test_data).unwrap();
    
    group.bench_function("write_block", |b| {
        let mut block_id = 1;
        b.iter(|| {
            block_id += 1;
            let mut block = Block::new(BlockId(block_id));
            block.write_data(0, &test_data).unwrap();
            file_manager.write_block(black_box(file_id), black_box(BlockId(block_id)), black_box(&block)).unwrap();
        });
    });
    
    // Записываем несколько блоков для тестов чтения
    for i in 1..=100 {
        let mut block = Block::new(BlockId(i));
        let data = vec![i as u8; 1024];
        block.write_data(0, &data).unwrap();
        file_manager.write_block(file_id, BlockId(i), &block).unwrap();
    }
    
    // Бенчмарк чтения блока
    group.bench_function("read_block", |b| {
        let mut block_id = 1;
        b.iter(|| {
            block_id = (block_id % 100) + 1; // Циклически читаем блоки 1-100
            let block = file_manager.read_block(black_box(file_id), black_box(BlockId(block_id))).unwrap();
            black_box(block);
        });
    });
    
    group.finish();
}

fn bench_advanced_file_manager_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("advanced_file_manager_operations");
    
    // Подготовка
    let temp_dir = TempDir::new().unwrap();
    let mut manager = AdvancedFileManager::new(temp_dir.path().to_path_buf());
    let file_id = manager.create_database_file(
        "advanced_benchmark.dat",
        DatabaseFileType::Data,
        PAGE_SIZE,
        ExtensionStrategy::Linear(PAGE_SIZE * 10),
    ).unwrap();
    
    // Бенчмарк выделения страниц
    group.bench_function("allocate_pages", |b| {
        b.iter(|| {
            let pages = manager.allocate_pages(black_box(file_id), black_box(10)).unwrap();
            black_box(pages);
        });
    });
    
    // Подготавливаем страницы для тестов записи/чтения
    let pages = manager.allocate_pages(file_id, 100).unwrap();
    let test_data = vec![0x42; PAGE_SIZE];
    
    // Бенчмарк записи страницы
    group.bench_function("write_page", |b| {
        let mut page_index = 0;
        b.iter(|| {
            page_index = (page_index + 1) % pages.len();
            manager.write_page(
                black_box(file_id), 
                black_box(pages[page_index]), 
                black_box(&test_data)
            ).unwrap();
        });
    });
    
    // Записываем данные на все страницы для тестов чтения
    for &page_id in &pages {
        manager.write_page(file_id, page_id, &test_data).unwrap();
    }
    
    // Бенчмарк чтения страницы
    group.bench_function("read_page", |b| {
        let mut page_index = 0;
        b.iter(|| {
            page_index = (page_index + 1) % pages.len();
            let data = manager.read_page(black_box(file_id), black_box(pages[page_index])).unwrap();
            black_box(data);
        });
    });
    
    group.finish();
}

fn bench_extension_strategies(c: &mut Criterion) {
    let mut group = c.benchmark_group("extension_strategies");
    
    let strategies = vec![
        ("fixed", ExtensionStrategy::Fixed(PAGE_SIZE)),
        ("linear", ExtensionStrategy::Linear(PAGE_SIZE * 2)),
        ("exponential", ExtensionStrategy::Exponential { 
            initial: PAGE_SIZE, 
            factor: 2.0, 
            max: PAGE_SIZE * 1000 
        }),
        ("adaptive", ExtensionStrategy::Adaptive { 
            min: PAGE_SIZE, 
            max: PAGE_SIZE * 100, 
            threshold: 0.8 
        }),
    ];
    
    for (name, strategy) in strategies {
        group.bench_with_input(
            BenchmarkId::new("allocate_many_pages", name),
            &strategy,
            |b, strategy| {
                b.iter_with_setup(
                    || {
                        let temp_dir = TempDir::new().unwrap();
                        let mut manager = AdvancedFileManager::new(temp_dir.path().to_path_buf());
                        let file_id = manager.create_database_file(
                            "strategy_bench.dat",
                            DatabaseFileType::Data,
                            PAGE_SIZE,
                            strategy.clone(),
                        ).unwrap();
                        (manager, file_id)
                    },
                    |(mut manager, file_id)| {
                        // Выделяем много страниц за раз
                        let pages = manager.allocate_pages(black_box(file_id), black_box(50)).unwrap();
                        black_box(pages);
                    }
                );
            }
        );
    }
    
    group.finish();
}

fn bench_concurrent_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_operations");
    
    // Бенчмарк последовательных операций (базовая линия)
    group.bench_function("sequential_operations", |b| {
        b.iter_with_setup(
            || {
                let temp_dir = TempDir::new().unwrap();
                let mut manager = AdvancedFileManager::new(temp_dir.path().to_path_buf());
                let file_id = manager.create_database_file(
                    "sequential.dat",
                    DatabaseFileType::Data,
                    PAGE_SIZE,
                    ExtensionStrategy::Linear(PAGE_SIZE * 10),
                ).unwrap();
                (manager, file_id)
            },
            |(mut manager, file_id)| {
                let pages = manager.allocate_pages(file_id, 20).unwrap();
                let test_data = vec![0x33; PAGE_SIZE];
                
                // Последовательная запись
                for &page_id in &pages {
                    manager.write_page(file_id, page_id, &test_data).unwrap();
                }
                
                // Последовательное чтение
                for &page_id in &pages {
                    let data = manager.read_page(file_id, page_id).unwrap();
                    black_box(data);
                }
            }
        );
    });
    
    group.finish();
}

fn bench_memory_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_operations");
    
    // Бенчмарк копирования больших блоков данных
    let large_data = vec![0x77; PAGE_SIZE];
    group.bench_function("copy_page_data", |b| {
        b.iter(|| {
            let copied = black_box(&large_data).clone();
            black_box(copied);
        });
    });
    
    // Бенчмарк заполнения страницы данными
    group.bench_function("fill_page_data", |b| {
        b.iter(|| {
            let mut data = vec![0u8; PAGE_SIZE];
            data.fill(black_box(0x88));
            black_box(data);
        });
    });
    
    // Бенчмарк сравнения страниц
    let data1 = vec![0x99; PAGE_SIZE];
    let data2 = vec![0x99; PAGE_SIZE];
    group.bench_function("compare_page_data", |b| {
        b.iter(|| {
            let result = black_box(&data1) == black_box(&data2);
            black_box(result);
        });
    });
    
    group.finish();
}

fn bench_serialization_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("serialization_performance");
    
    // Подготавливаем данные разных размеров
    let small_data = vec![0xAA; 1024];      // 1KB
    let medium_data = vec![0xBB; 64 * 1024]; // 64KB
    let large_data = vec![0xCC; 1024 * 1024]; // 1MB
    
    group.bench_function("serialize_small", |b| {
        b.iter(|| {
            let serialized = bincode::serialize(black_box(&small_data)).unwrap();
            black_box(serialized);
        });
    });
    
    group.bench_function("serialize_medium", |b| {
        b.iter(|| {
            let serialized = bincode::serialize(black_box(&medium_data)).unwrap();
            black_box(serialized);
        });
    });
    
    group.bench_function("serialize_large", |b| {
        b.iter(|| {
            let serialized = bincode::serialize(black_box(&large_data)).unwrap();
            black_box(serialized);
        });
    });
    
    // Бенчмарки десериализации
    let serialized_small = bincode::serialize(&small_data).unwrap();
    let serialized_medium = bincode::serialize(&medium_data).unwrap();
    let serialized_large = bincode::serialize(&large_data).unwrap();
    
    group.bench_function("deserialize_small", |b| {
        b.iter(|| {
            let data: Vec<u8> = bincode::deserialize(black_box(&serialized_small)).unwrap();
            black_box(data);
        });
    });
    
    group.bench_function("deserialize_medium", |b| {
        b.iter(|| {
            let data: Vec<u8> = bincode::deserialize(black_box(&serialized_medium)).unwrap();
            black_box(data);
        });
    });
    
    group.bench_function("deserialize_large", |b| {
        b.iter(|| {
            let data: Vec<u8> = bincode::deserialize(black_box(&serialized_large)).unwrap();
            black_box(data);
        });
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_block_operations,
    bench_file_manager_operations,
    bench_advanced_file_manager_operations,
    bench_extension_strategies,
    bench_concurrent_operations,
    bench_memory_operations,
    bench_serialization_performance
);

criterion_main!(benches);
