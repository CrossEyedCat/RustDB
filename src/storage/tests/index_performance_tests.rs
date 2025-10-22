//! Тесты производительности для индексов rustdb
//!
//! Эти тесты измеряют производительность различных операций с индексами
//! и помогают выявить узкие места в реализации.

use crate::storage::index::{BPlusTree, Index, SimpleHashIndex};
use std::time::{Duration, Instant};

/// Структура для хранения результатов бенчмарка
#[derive(Debug)]
struct BenchmarkResult {
    operation: String,
    index_type: String,
    elements: usize,
    duration: Duration,
    ops_per_second: f64,
}

impl BenchmarkResult {
    fn new(operation: &str, index_type: &str, elements: usize, duration: Duration) -> Self {
        let ops_per_second = if duration.as_secs_f64() > 0.0 {
            elements as f64 / duration.as_secs_f64()
        } else {
            0.0
        };

        Self {
            operation: operation.to_string(),
            index_type: index_type.to_string(),
            elements,
            duration,
            ops_per_second,
        }
    }

    fn print(&self) {
        println!(
            "  {} - {}: {} элементов за {:?} ({:.0} ops/sec)",
            self.index_type, self.operation, self.elements, self.duration, self.ops_per_second
        );
    }
}

#[test]
fn test_insertion_performance() {
    println!("🚀 Тест производительности вставки:");

    let test_sizes = [100, 1_000, 10_000];
    let mut results = Vec::new();

    for &size in &test_sizes {
        // Тестируем B+ дерево
        let mut btree: BPlusTree<i32, String> = BPlusTree::new_default();
        let start = Instant::now();

        for i in 1..=size {
            btree.insert(i as i32, format!("value_{}", i)).unwrap();
        }

        let btree_duration = start.elapsed();
        results.push(BenchmarkResult::new(
            "вставка",
            "B+ дерево",
            size,
            btree_duration,
        ));

        // Тестируем хеш-индекс
        let mut hash_index: SimpleHashIndex<i32, String> = SimpleHashIndex::with_capacity(size);
        let start = Instant::now();

        for i in 1..=size {
            hash_index.insert(i as i32, format!("value_{}", i)).unwrap();
        }

        let hash_duration = start.elapsed();
        results.push(BenchmarkResult::new(
            "вставка",
            "Хеш-индекс",
            size,
            hash_duration,
        ));
    }

    for result in &results {
        result.print();
    }

    // Проверяем, что операции выполняются за разумное время
    for result in &results {
        assert!(
            result.ops_per_second > 1000.0,
            "Слишком медленная вставка: {:.0} ops/sec",
            result.ops_per_second
        );
    }
}

#[test]
fn test_search_performance() {
    println!("🔍 Тест производительности поиска:");

    const SIZE: usize = 10_000;
    let mut results = Vec::new();

    // Подготавливаем B+ дерево
    let mut btree: BPlusTree<i32, String> = BPlusTree::new_default();
    for i in 1..=SIZE {
        btree.insert(i as i32, format!("value_{}", i)).unwrap();
    }

    // Подготавливаем хеш-индекс
    let mut hash_index: SimpleHashIndex<i32, String> = SimpleHashIndex::with_capacity(SIZE);
    for i in 1..=SIZE {
        hash_index.insert(i as i32, format!("value_{}", i)).unwrap();
    }

    // Тестируем последовательный поиск
    let start = Instant::now();
    for i in 1..=SIZE {
        let _ = btree.search(&(i as i32)).unwrap();
    }
    let btree_sequential = start.elapsed();
    results.push(BenchmarkResult::new(
        "последовательный поиск",
        "B+ дерево",
        SIZE,
        btree_sequential,
    ));

    let start = Instant::now();
    for i in 1..=SIZE {
        let _ = hash_index.search(&(i as i32)).unwrap();
    }
    let hash_sequential = start.elapsed();
    results.push(BenchmarkResult::new(
        "последовательный поиск",
        "Хеш-индекс",
        SIZE,
        hash_sequential,
    ));

    // Тестируем случайный поиск
    let random_keys: Vec<i32> = (1..=SIZE).map(|i| ((i * 7919) % SIZE + 1) as i32).collect();

    let start = Instant::now();
    for &key in &random_keys {
        let _ = btree.search(&key).unwrap();
    }
    let btree_random = start.elapsed();
    results.push(BenchmarkResult::new(
        "случайный поиск",
        "B+ дерево",
        SIZE,
        btree_random,
    ));

    let start = Instant::now();
    for &key in &random_keys {
        let _ = hash_index.search(&key).unwrap();
    }
    let hash_random = start.elapsed();
    results.push(BenchmarkResult::new(
        "случайный поиск",
        "Хеш-индекс",
        SIZE,
        hash_random,
    ));

    for result in &results {
        result.print();
    }

    // Проверяем производительность
    for result in &results {
        assert!(
            result.ops_per_second > 10_000.0,
            "Слишком медленный поиск: {:.0} ops/sec",
            result.ops_per_second
        );
    }
}

#[test]
fn test_range_query_performance() {
    println!("📊 Тест производительности диапазонных запросов:");

    const SIZE: usize = 10_000;
    let mut btree: BPlusTree<i32, String> = BPlusTree::new_default();

    // Подготавливаем данные
    for i in 1..=SIZE {
        btree.insert(i as i32, format!("value_{}", i)).unwrap();
    }

    let range_sizes = [10, 100, 1_000, 5_000];

    for &range_size in &range_sizes {
        let start_key = (SIZE / 2 - range_size / 2) as i32;
        let end_key = (SIZE / 2 + range_size / 2) as i32;

        let start = Instant::now();
        let results = btree.range_search(&start_key, &end_key).unwrap();
        let duration = start.elapsed();

        println!(
            "  Диапазон размером {}: {} результатов за {:?}",
            range_size,
            results.len(),
            duration
        );

        // Проверяем корректность результатов
        assert!(results.len() <= range_size + 1); // +1 из-за включительных границ
        assert!(duration.as_millis() < 100); // Должно быть быстро
    }
}

#[test]
fn test_deletion_performance() {
    println!("🗑️ Тест производительности удаления:");

    const SIZE: usize = 10_000;
    let mut results = Vec::new();

    // Подготавливаем хеш-индекс
    let mut hash_index: SimpleHashIndex<i32, String> = SimpleHashIndex::with_capacity(SIZE);
    for i in 1..=SIZE {
        hash_index.insert(i as i32, format!("value_{}", i)).unwrap();
    }

    // Тестируем удаление каждого второго элемента
    let keys_to_delete: Vec<i32> = (1..=SIZE).step_by(2).map(|i| i as i32).collect();
    let delete_count = keys_to_delete.len();

    let start = Instant::now();
    for &key in &keys_to_delete {
        hash_index.delete(&key).unwrap();
    }
    let deletion_duration = start.elapsed();

    results.push(BenchmarkResult::new(
        "удаление",
        "Хеш-индекс",
        delete_count,
        deletion_duration,
    ));

    // Проверяем, что элементы действительно удалены
    for &key in &keys_to_delete {
        assert_eq!(hash_index.search(&key).unwrap(), None);
    }

    // Проверяем, что оставшиеся элементы на месте
    for i in (2..=SIZE).step_by(2) {
        assert!(hash_index.search(&(i as i32)).unwrap().is_some());
    }

    for result in &results {
        result.print();
    }

    assert_eq!(hash_index.size(), SIZE - delete_count);
}

#[test]
fn test_mixed_operations_performance() {
    println!("🔄 Тест производительности смешанных операций:");

    const OPERATIONS: usize = 10_000;
    let mut hash_index: SimpleHashIndex<i32, String> = SimpleHashIndex::new();

    let start = Instant::now();

    for i in 0..OPERATIONS {
        match i % 4 {
            0 => {
                // Вставка
                hash_index.insert(i as i32, format!("value_{}", i)).unwrap();
            }
            1 => {
                // Поиск существующего элемента
                if i > 0 {
                    let _ = hash_index.search(&((i - 1) as i32)).unwrap();
                }
            }
            2 => {
                // Поиск несуществующего элемента
                let _ = hash_index.search(&(-1)).unwrap();
            }
            3 => {
                // Удаление
                if i > 3 {
                    hash_index.delete(&((i - 3) as i32)).unwrap();
                }
            }
            _ => unreachable!(),
        }
    }

    let total_duration = start.elapsed();
    let ops_per_second = OPERATIONS as f64 / total_duration.as_secs_f64();

    println!(
        "  Смешанные операции: {} операций за {:?} ({:.0} ops/sec)",
        OPERATIONS, total_duration, ops_per_second
    );

    assert!(
        ops_per_second > 50_000.0,
        "Слишком медленные смешанные операции: {:.0} ops/sec",
        ops_per_second
    );
}

#[test]
fn test_memory_usage_scaling() {
    println!("💾 Тест масштабирования использования памяти:");

    use std::mem;

    let sizes = [100, 1_000, 10_000];

    for &size in &sizes {
        // Тестируем B+ дерево
        let mut btree: BPlusTree<i32, String> = BPlusTree::new_default();
        for i in 1..=size {
            btree.insert(i as i32, format!("value_{:06}", i)).unwrap();
        }

        let btree_size = mem::size_of_val(&btree);
        println!(
            "  B+ дерево с {} элементами: ~{} байт базовой структуры",
            size, btree_size
        );

        // Тестируем хеш-индекс
        let mut hash_index: SimpleHashIndex<i32, String> = SimpleHashIndex::new();
        for i in 1..=size {
            hash_index
                .insert(i as i32, format!("value_{:06}", i))
                .unwrap();
        }

        let hash_size = mem::size_of_val(&hash_index);
        println!(
            "  Хеш-индекс с {} элементами: ~{} байт базовой структуры",
            size, hash_size
        );

        // Базовые структуры не должны сильно расти
        assert!(btree_size < 1024);
        assert!(hash_size < 1024);
    }
}

#[test]
fn test_cache_efficiency() {
    println!("⚡ Тест эффективности кеша:");

    const SIZE: usize = 100_000;
    let mut btree: BPlusTree<i32, i32> = BPlusTree::new_default();

    // Заполняем индекс
    for i in 1..=SIZE {
        btree.insert(i as i32, i as i32).unwrap();
    }

    // Тестируем локальность доступа (последовательный доступ)
    let start = Instant::now();
    for i in 1..=SIZE {
        let _ = btree.search(&(i as i32)).unwrap();
    }
    let sequential_duration = start.elapsed();

    // Тестируем случайный доступ
    let random_keys: Vec<i32> = (1..=SIZE)
        .map(|i| ((i * 31337) % SIZE + 1) as i32)
        .collect();
    let start = Instant::now();
    for &key in &random_keys {
        let _ = btree.search(&key).unwrap();
    }
    let random_duration = start.elapsed();

    println!("  Последовательный доступ: {:?}", sequential_duration);
    println!("  Случайный доступ: {:?}", random_duration);

    // Последовательный доступ должен быть быстрее (лучше использует кеш)
    let sequential_ops_per_sec = SIZE as f64 / sequential_duration.as_secs_f64();
    let random_ops_per_sec = SIZE as f64 / random_duration.as_secs_f64();

    println!("  Последовательный: {:.0} ops/sec", sequential_ops_per_sec);
    println!("  Случайный: {:.0} ops/sec", random_ops_per_sec);

    // Оба типа доступа должны быть достаточно быстрыми
    assert!(sequential_ops_per_sec > 100_000.0);
    assert!(random_ops_per_sec > 50_000.0);
}
