//! Интеграционные тесты для индексов rustdb
//! 
//! Эти тесты проверяют интеграцию индексов с другими компонентами системы
//! и их работу в реальных сценариях использования.

use crate::storage::index::{BPlusTree, SimpleHashIndex, Index};
use crate::common::Result;

#[test]
fn test_btree_with_different_types() {
    // Тестируем B+ дерево с разными типами данных
    
    // Строковые ключи
    let mut string_btree: BPlusTree<String, i32> = BPlusTree::new_default();
    let words = ["apple", "banana", "cherry", "date", "elderberry"];
    
    for (i, word) in words.iter().enumerate() {
        string_btree.insert(word.to_string(), i as i32).unwrap();
    }
    
    assert_eq!(string_btree.size(), 5);
    assert_eq!(string_btree.search(&"banana".to_string()).unwrap(), Some(1));
    assert_eq!(string_btree.search(&"fig".to_string()).unwrap(), None);
    
    // Диапазонный поиск строк
    let range = string_btree.range_search(&"banana".to_string(), &"date".to_string()).unwrap();
    assert_eq!(range.len(), 3); // banana, cherry, date
    
    // Числовые ключи с плавающей точкой
    let mut float_btree: BPlusTree<i32, f64> = BPlusTree::new_default();
    for i in 1..=10 {
        float_btree.insert(i, (i as f64) * 1.5).unwrap();
    }
    
    assert_eq!(float_btree.size(), 10);
    assert_eq!(float_btree.search(&5).unwrap(), Some(7.5));
}

#[test]
fn test_hash_index_with_complex_types() {
    // Тестируем хеш-индекс со сложными типами данных
    
    #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
    struct UserId(u64);
    
    #[derive(Debug, Clone, PartialEq)]
    struct UserInfo {
        name: String,
        age: u32,
        email: String,
    }
    
    let mut user_index: SimpleHashIndex<UserId, UserInfo> = SimpleHashIndex::new();
    
    let users = [
        (UserId(1), UserInfo { name: "Alice".to_string(), age: 25, email: "alice@example.com".to_string() }),
        (UserId(2), UserInfo { name: "Bob".to_string(), age: 30, email: "bob@example.com".to_string() }),
        (UserId(3), UserInfo { name: "Charlie".to_string(), age: 35, email: "charlie@example.com".to_string() }),
    ];
    
    for (id, info) in users.clone() {
        user_index.insert(id, info).unwrap();
    }
    
    assert_eq!(user_index.size(), 3);
    
    // Поиск пользователя
    let alice = user_index.search(&UserId(1)).unwrap().unwrap();
    assert_eq!(alice.name, "Alice");
    assert_eq!(alice.age, 25);
    
    // Обновление информации
    let updated_alice = UserInfo { 
        name: "Alice Smith".to_string(), 
        age: 26, 
        email: "alice.smith@example.com".to_string() 
    };
    user_index.insert(UserId(1), updated_alice.clone()).unwrap();
    
    let retrieved = user_index.search(&UserId(1)).unwrap().unwrap();
    assert_eq!(retrieved.name, "Alice Smith");
    assert_eq!(retrieved.age, 26);
}

#[test]
fn test_index_performance_comparison() {
    // Сравниваем производительность разных типов индексов
    use std::time::Instant;
    
    const TEST_SIZE: i32 = 1000;
    
    // Подготавливаем данные
    let test_data: Vec<(i32, String)> = (1..=TEST_SIZE)
        .map(|i| (i, format!("value_{}", i)))
        .collect();
    
    // Тестируем B+ дерево
    let mut btree: BPlusTree<i32, String> = BPlusTree::new_default();
    let btree_start = Instant::now();
    
    for (key, value) in &test_data {
        btree.insert(*key, value.clone()).unwrap();
    }
    
    let btree_insert_time = btree_start.elapsed();
    
    // Поиск в B+ дереве
    let btree_search_start = Instant::now();
    for i in 1..=TEST_SIZE {
        let _ = btree.search(&i).unwrap();
    }
    let btree_search_time = btree_search_start.elapsed();
    
    // Тестируем хеш-индекс
    let mut hash_index: SimpleHashIndex<i32, String> = SimpleHashIndex::with_capacity(TEST_SIZE as usize);
    let hash_start = Instant::now();
    
    for (key, value) in &test_data {
        hash_index.insert(*key, value.clone()).unwrap();
    }
    
    let hash_insert_time = hash_start.elapsed();
    
    // Поиск в хеш-индексе
    let hash_search_start = Instant::now();
    for i in 1..=TEST_SIZE {
        let _ = hash_index.search(&i).unwrap();
    }
    let hash_search_time = hash_search_start.elapsed();
    
    // Проверяем, что оба индекса работают корректно
    assert_eq!(btree.size(), TEST_SIZE as usize);
    assert_eq!(hash_index.size(), TEST_SIZE as usize);
    
    // Выводим результаты для анализа
    println!("Производительность для {} элементов:", TEST_SIZE);
    println!("B+ дерево - вставка: {:?}, поиск: {:?}", btree_insert_time, btree_search_time);
    println!("Хеш-индекс - вставка: {:?}, поиск: {:?}", hash_insert_time, hash_search_time);
    
    // Оба индекса должны работать достаточно быстро
    assert!(btree_insert_time.as_millis() < 100);
    assert!(hash_insert_time.as_millis() < 100);
}

#[test]
fn test_btree_range_queries() {
    // Детальное тестирование диапазонных запросов B+ дерева
    let mut btree: BPlusTree<i32, String> = BPlusTree::new_default();
    
    // Вставляем данные не по порядку
    let data = [10, 5, 15, 3, 7, 12, 18, 1, 6, 8, 11, 14, 16, 20];
    for &key in &data {
        btree.insert(key, format!("value_{}", key)).unwrap();
    }
    
    // Тестируем различные диапазоны
    
    // Полный диапазон
    let full_range = btree.range_search(&1, &20).unwrap();
    assert_eq!(full_range.len(), data.len());
    
    // Проверяем, что результаты отсортированы
    for i in 1..full_range.len() {
        assert!(full_range[i-1].0 <= full_range[i].0);
    }
    
    // Частичные диапазоны
    let mid_range = btree.range_search(&5, &15).unwrap();
    assert!(mid_range.len() >= 7); // Должно быть минимум 7 элементов
    
    // Пустой диапазон
    let empty_range = btree.range_search(&25, &30).unwrap();
    assert!(empty_range.is_empty());
    
    // Диапазон с одним элементом
    let single_range = btree.range_search(&10, &10).unwrap();
    assert_eq!(single_range.len(), 1);
    assert_eq!(single_range[0].0, 10);
    
    // Обратный диапазон (start > end)
    let reverse_range = btree.range_search(&15, &5).unwrap();
    assert!(reverse_range.is_empty());
}

#[test]
fn test_hash_index_collision_handling() {
    // Тестируем обработку коллизий в хеш-индексе
    let mut hash_index: SimpleHashIndex<String, i32> = SimpleHashIndex::with_capacity(4); // Маленькая емкость для коллизий
    
    // Вставляем много элементов, чтобы вызвать коллизии
    let test_keys: Vec<String> = (1..=20)
        .map(|i| format!("key_{:03}", i))
        .collect();
    
    for (i, key) in test_keys.iter().enumerate() {
        hash_index.insert(key.clone(), i as i32).unwrap();
    }
    
    assert_eq!(hash_index.size(), 20);
    
    // Проверяем, что все элементы можно найти
    for (i, key) in test_keys.iter().enumerate() {
        let value = hash_index.search(key).unwrap().unwrap();
        assert_eq!(value, i as i32);
    }
    
    // Удаляем половину элементов
    for i in (0..10).step_by(2) {
        let removed = hash_index.delete(&test_keys[i]).unwrap();
        assert!(removed);
    }
    
    assert_eq!(hash_index.size(), 15);
    
    // Проверяем, что удаленные элементы не найдены
    for i in (0..10).step_by(2) {
        assert_eq!(hash_index.search(&test_keys[i]).unwrap(), None);
    }
    
    // Проверяем, что оставшиеся элементы все еще доступны
    for i in (1..20).step_by(2) {
        let value = hash_index.search(&test_keys[i]).unwrap().unwrap();
        assert_eq!(value, i as i32);
    }
}

#[test]
fn test_index_statistics_accuracy() {
    // Тестируем точность статистики индексов
    let mut btree: BPlusTree<i32, String> = BPlusTree::new_default();
    let mut hash_index: SimpleHashIndex<i32, String> = SimpleHashIndex::new();
    
    // Начальная статистика
    assert_eq!(btree.get_statistics().total_elements, 0);
    assert_eq!(btree.get_statistics().insert_operations, 0);
    assert_eq!(hash_index.get_statistics().total_elements, 0);
    assert_eq!(hash_index.get_statistics().insert_operations, 0);
    
    // Вставляем элементы
    for i in 1..=10 {
        btree.insert(i, format!("value_{}", i)).unwrap();
        hash_index.insert(i, format!("value_{}", i)).unwrap();
    }
    
    // Проверяем статистику после вставки
    let btree_stats = btree.get_statistics();
    assert_eq!(btree_stats.total_elements, 10);
    assert_eq!(btree_stats.insert_operations, 10);
    assert!(btree_stats.fill_factor > 0.0);
    
    let hash_stats = hash_index.get_statistics();
    assert_eq!(hash_stats.total_elements, 10);
    assert_eq!(hash_stats.insert_operations, 10);
    assert!(hash_stats.fill_factor > 0.0);
    
    // Удаляем элементы из хеш-индекса
    for i in 1..=5 {
        hash_index.delete(&i).unwrap();
    }
    
    let hash_stats_after_delete = hash_index.get_statistics();
    assert_eq!(hash_stats_after_delete.total_elements, 5);
    assert_eq!(hash_stats_after_delete.delete_operations, 5);
}

#[test]
fn test_index_edge_cases() {
    // Тестируем граничные случаи
    
    // Пустые индексы
    let btree: BPlusTree<i32, String> = BPlusTree::new_default();
    let hash_index: SimpleHashIndex<i32, String> = SimpleHashIndex::new();
    
    assert!(btree.is_empty());
    assert!(hash_index.is_empty());
    assert_eq!(btree.search(&1).unwrap(), None);
    assert_eq!(hash_index.search(&1).unwrap(), None);
    
    // Диапазонный поиск в пустом дереве
    let empty_range = btree.range_search(&1, &10).unwrap();
    assert!(empty_range.is_empty());
    
    // Диапазонный поиск в хеш-индексе (всегда пустой)
    let hash_range = hash_index.range_search(&1, &10).unwrap();
    assert!(hash_range.is_empty());
    
    // Удаление из пустого индекса
    let mut empty_hash: SimpleHashIndex<i32, String> = SimpleHashIndex::new();
    assert!(!empty_hash.delete(&1).unwrap());
    
    // Вставка одного элемента
    let mut single_btree: BPlusTree<i32, String> = BPlusTree::new_default();
    single_btree.insert(42, "answer".to_string()).unwrap();
    
    assert_eq!(single_btree.size(), 1);
    assert_eq!(single_btree.search(&42).unwrap(), Some("answer".to_string()));
    
    let single_range = single_btree.range_search(&42, &42).unwrap();
    assert_eq!(single_range.len(), 1);
    assert_eq!(single_range[0], (42, "answer".to_string()));
}

#[test]
fn test_index_memory_efficiency() {
    // Тестируем эффективность использования памяти
    use std::mem;
    
    let btree: BPlusTree<i32, String> = BPlusTree::new_default();
    let hash_index: SimpleHashIndex<i32, String> = SimpleHashIndex::new();
    
    // Проверяем, что пустые индексы не занимают много памяти
    let btree_size = mem::size_of_val(&btree);
    let hash_size = mem::size_of_val(&hash_index);
    
    println!("Размер пустого B+ дерева: {} байт", btree_size);
    println!("Размер пустого хеш-индекса: {} байт", hash_size);
    
    // Оба индекса должны быть достаточно компактными
    assert!(btree_size < 1024); // Менее 1KB
    assert!(hash_size < 1024);  // Менее 1KB
}

#[cfg(test)]
mod concurrent_tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::thread;
    
    #[test]
    fn test_index_thread_safety_simulation() {
        // Симулируем многопоточное использование с помощью Mutex
        // (В реальной системе нужна более сложная синхронизация)
        
        let btree = Arc::new(Mutex::new(BPlusTree::<i32, String>::new_default()));
        let hash_index = Arc::new(Mutex::new(SimpleHashIndex::<i32, String>::new()));
        
        let mut handles = vec![];
        
        // Запускаем несколько потоков для записи
        for thread_id in 0..4 {
            let btree_clone = Arc::clone(&btree);
            let hash_clone = Arc::clone(&hash_index);
            
            let handle = thread::spawn(move || {
                let start = thread_id * 100;
                let end = start + 100;
                
                for i in start..end {
                    {
                        let mut btree_lock = btree_clone.lock().unwrap();
                        btree_lock.insert(i, format!("btree_value_{}", i)).unwrap();
                    }
                    
                    {
                        let mut hash_lock = hash_clone.lock().unwrap();
                        hash_lock.insert(i, format!("hash_value_{}", i)).unwrap();
                    }
                }
            });
            
            handles.push(handle);
        }
        
        // Ждем завершения всех потоков
        for handle in handles {
            handle.join().unwrap();
        }
        
        // Проверяем результаты
        {
            let btree_lock = btree.lock().unwrap();
            let hash_lock = hash_index.lock().unwrap();
            
            assert_eq!(btree_lock.size(), 400);
            assert_eq!(hash_lock.size(), 400);
            
            // Проверяем несколько случайных элементов
            for i in [0, 100, 200, 300, 399] {
                assert!(btree_lock.search(&i).unwrap().is_some());
                assert!(hash_lock.search(&i).unwrap().is_some());
            }
        }
    }
}
