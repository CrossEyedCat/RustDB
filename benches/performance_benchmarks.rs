//! Бенчмарки производительности RustDB

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use rustdb::common::types::PAGE_SIZE;
use rustdb::storage::{
    advanced_file_manager::AdvancedFileManager,
    block::{Block, BlockType},
    database_file::{DatabaseFileType, ExtensionStrategy},
    file_manager::{FileManager, BLOCK_SIZE},
};
// use rustdb::storage::block::BlockId; // unused
use tempfile::TempDir;

fn bench_block_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("block_operations");

    // Бенчмарк создания блока
    group.bench_function("create_block", |b| {
        b.iter(|| {
            let block = Block::new(black_box(1), BlockType::Data, PAGE_SIZE as u32);
            black_box(block);
        });
    });

    // Бенчмарк сериализации блока
    let block = Block::new(1, BlockType::Data, PAGE_SIZE as u32);
    group.bench_function("serialize_block", |b| {
        b.iter(|| {
            let serialized = block.to_bytes();
            let _ = black_box(serialized);
        });
    });

    // Бенчмарк десериализации блока
    let block_data = block.to_bytes().unwrap();
    group.bench_function("deserialize_block", |b| {
        b.iter(|| {
            let deserialized = Block::from_bytes(black_box(&block_data)).unwrap();
            black_box(deserialized);
        });
    });

    group.finish();
}

fn bench_file_manager_operations(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().join("test.db");

    let mut group = c.benchmark_group("file_manager_operations");

    // Бенчмарк создания файлового менеджера
    group.bench_function("create_file_manager", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().unwrap();
            let temp_path = temp_dir.path().join("test.db");
            let file_manager = FileManager::new(temp_path.clone());
            let _ = black_box(file_manager);
        });
    });

    // Бенчмарк создания файла
    group.bench_function("create_file", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().unwrap();
            let temp_path = temp_dir.path().join("test.db");
            let mut file_manager = FileManager::new(temp_path.clone()).unwrap();
            let file_id = file_manager.create_file("benchmark.dat").unwrap();
            black_box(file_id);
        });
    });

    // Подготовка данных для бенчмарков записи и чтения
    let mut file_manager = FileManager::new(temp_path.clone()).unwrap();
    let file_id = file_manager.create_file("test_file.dat").unwrap();
    let test_block_data = vec![0xAB; BLOCK_SIZE];

    group.bench_function("write_block", |b| {
        b.iter(|| {
            file_manager
                .write_block(black_box(file_id), black_box(1), black_box(&test_block_data))
                .unwrap();
        });
    });

    // Бенчмарк чтения блока
    file_manager.write_block(file_id, 1, &test_block_data).unwrap();

    group.bench_function("read_block", |b| {
        b.iter(|| {
            let block = file_manager
                .read_block(black_box(file_id), black_box(1))
                .unwrap();
            let _ = black_box(block);
        });
    });

    group.finish();
}

fn bench_advanced_file_manager_operations(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().join("advanced_test.db");

    let mut group = c.benchmark_group("advanced_file_manager_operations");

    // Бенчмарк создания продвинутого файлового менеджера
    group.bench_function("create_advanced_file_manager", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().unwrap();
            let temp_path = temp_dir.path().join("advanced_test.db");
            let manager = AdvancedFileManager::new(temp_path.clone());
            let _ = black_box(manager);
        });
    });

    // Бенчмарк создания файла базы данных
    group.bench_function("create_database_file", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().unwrap();
            let temp_path = temp_dir.path().join("advanced_test.db");
            let mut manager = AdvancedFileManager::new(temp_path.clone()).unwrap();
            let file_id = manager
                .create_database_file(
                    "benchmark.db",
                    DatabaseFileType::Data,
                    1,
                    ExtensionStrategy::Linear,
                )
                .unwrap();
            black_box(file_id);
        });
    });

    // Подготовка данных для остальных бенчмарков
    let mut manager = AdvancedFileManager::new(temp_path.clone()).unwrap();
    let file_id = manager
        .create_database_file(
            "test_file.db",
            DatabaseFileType::Data,
            1,
            ExtensionStrategy::Linear,
        )
        .unwrap();

    group.bench_function("allocate_pages", |b| {
        b.iter(|| {
            let pages = manager
                .allocate_pages(black_box(file_id), black_box(10))
                .unwrap();
            let _ = black_box(pages);
        });
    });

    // Бенчмарк записи страниц
    let pages = manager.allocate_pages(file_id, 100).unwrap();
    let test_data = vec![0xCD; PAGE_SIZE];

    group.bench_function("write_page", |b| {
        b.iter(|| {
            manager
                .write_page(black_box(file_id), black_box(pages), black_box(&test_data))
                .unwrap();
        });
    });

    // Бенчмарк чтения страниц
    manager.write_page(file_id, pages, &test_data).unwrap();

    group.bench_function("read_page", |b| {
        b.iter(|| {
            let data = manager
                .read_page(black_box(file_id), black_box(pages))
                .unwrap();
            let _ = black_box(data);
        });
    });

    group.finish();
}

fn bench_extension_strategies(c: &mut Criterion) {
    let strategies = vec![
        ("fixed", ExtensionStrategy::Fixed),
        ("linear", ExtensionStrategy::Linear),
        ("exponential", ExtensionStrategy::Exponential),
        ("adaptive", ExtensionStrategy::Adaptive),
    ];

    let mut group = c.benchmark_group("extension_strategies");

    for (name, strategy) in strategies {
        group.bench_with_input(
            BenchmarkId::new("create_file", name),
            &strategy,
            |b, strategy| {
                b.iter(|| {
                    let temp_dir = TempDir::new().unwrap();
                    let temp_path = temp_dir.path().join("strategy_test.db");
                    let mut manager = AdvancedFileManager::new(temp_path.clone()).unwrap();
                    let file_id = manager
                        .create_database_file(
                            "strategy_test.db",
                            DatabaseFileType::Data,
                            1,
                            *strategy,
                        )
                        .unwrap();
                    let _ = black_box(file_id);
                });
            },
        );
    }

    group.finish();
}

fn bench_memory_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_operations");

    // Бенчмарк выделения памяти
    group.bench_function("allocate_memory", |b| {
        b.iter(|| {
            let data = vec![0u8; PAGE_SIZE];
            black_box(data);
        });
    });

    // Бенчмарк копирования памяти
    let source = vec![0xAA; PAGE_SIZE];
    group.bench_function("copy_memory", |b| {
        b.iter(|| {
            let mut dest = vec![0u8; PAGE_SIZE];
            dest.copy_from_slice(&source);
            black_box(dest);
        });
    });

    // Бенчмарк заполнения памяти
    group.bench_function("fill_memory", |b| {
        b.iter(|| {
            let mut data = vec![0u8; PAGE_SIZE];
            data.fill(0xBB);
            black_box(data);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_block_operations,
    bench_file_manager_operations,
    // bench_advanced_file_manager_operations, // Disabled: causes disk space issues
    bench_extension_strategies,
    bench_memory_operations
);

criterion_main!(benches);
