//! Интеграционные тесты для индексов с другими компонентами rustdb
//!
//! Эти тесты проверяют, как индексы работают в связке с менеджером страниц,
//! файловой системой и другими компонентами базы данных.

use crate::common::Result;
use crate::storage::advanced_file_manager::AdvancedFileManager;
use crate::storage::index::{BPlusTree, Index, SimpleHashIndex};
use crate::storage::page_manager::PageManager;
use std::collections::HashMap;
use tempfile::TempDir;

/// Структура, имитирующая запись в таблице базы данных
#[derive(Debug, Clone, PartialEq)]
struct DatabaseRecord {
    id: u32,
    name: String,
    age: u32,
    email: String,
}

/// Простая имитация таблицы с индексами
struct IndexedTable {
    // Основные данные хранятся по ID записи
    records: HashMap<u32, DatabaseRecord>,
    // Индекс по ID (первичный ключ)
    id_index: SimpleHashIndex<u32, u32>, // key -> record_id
    // Индекс по имени
    name_index: BPlusTree<String, Vec<u32>>, // name -> list of record_ids
    // Индекс по возрасту (для диапазонных запросов)
    age_index: BPlusTree<u32, Vec<u32>>, // age -> list of record_ids
    next_id: u32,
}

impl IndexedTable {
    fn new() -> Self {
        Self {
            records: HashMap::new(),
            id_index: SimpleHashIndex::new(),
            name_index: BPlusTree::new_default(),
            age_index: BPlusTree::new_default(),
            next_id: 1,
        }
    }

    fn insert(&mut self, name: String, age: u32, email: String) -> Result<u32> {
        let record_id = self.next_id;
        self.next_id += 1;

        let record = DatabaseRecord {
            id: record_id,
            name: name.clone(),
            age,
            email,
        };

        // Вставляем запись
        self.records.insert(record_id, record);

        // Обновляем индексы
        self.id_index.insert(record_id, record_id)?;

        // Индекс по имени (может быть несколько записей с одним именем)
        match self.name_index.search(&name)? {
            Some(mut ids) => {
                ids.push(record_id);
                self.name_index.insert(name, ids)?;
            }
            None => {
                self.name_index.insert(name, vec![record_id])?;
            }
        }

        // Индекс по возрасту
        match self.age_index.search(&age)? {
            Some(mut ids) => {
                ids.push(record_id);
                self.age_index.insert(age, ids)?;
            }
            None => {
                self.age_index.insert(age, vec![record_id])?;
            }
        }

        Ok(record_id)
    }

    fn find_by_id(&self, id: u32) -> Result<Option<DatabaseRecord>> {
        if let Some(&record_id) = self.id_index.search(&id)?.as_ref() {
            Ok(self.records.get(&record_id).cloned())
        } else {
            Ok(None)
        }
    }

    fn find_by_name(&self, name: &str) -> Result<Vec<DatabaseRecord>> {
        if let Some(ids) = self.name_index.search(&name.to_string())? {
            Ok(ids
                .iter()
                .filter_map(|&id| self.records.get(&id))
                .cloned()
                .collect())
        } else {
            Ok(Vec::new())
        }
    }

    fn find_by_age_range(&self, min_age: u32, max_age: u32) -> Result<Vec<DatabaseRecord>> {
        let age_results = self.age_index.range_search(&min_age, &max_age)?;
        let mut records = Vec::new();

        for (_, ids) in age_results {
            for id in ids {
                if let Some(record) = self.records.get(&id) {
                    records.push(record.clone());
                }
            }
        }

        Ok(records)
    }

    fn delete(&mut self, id: u32) -> Result<bool> {
        if let Some(record) = self.records.remove(&id) {
            // Удаляем из индекса по ID
            self.id_index.delete(&id)?;

            // Обновляем индекс по имени
            if let Some(mut ids) = self.name_index.search(&record.name)? {
                ids.retain(|&x| x != id);
                if ids.is_empty() {
                    // Если это был последний элемент с таким именем, удаляем запись
                    // В реальной реализации нужно было бы удалить ключ из индекса
                } else {
                    self.name_index.insert(record.name.clone(), ids)?;
                }
            }

            // Обновляем индекс по возрасту
            if let Some(mut ids) = self.age_index.search(&record.age)? {
                ids.retain(|&x| x != id);
                if !ids.is_empty() {
                    self.age_index.insert(record.age, ids)?;
                }
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn size(&self) -> usize {
        self.records.len()
    }
}

#[test]
fn test_indexed_table_basic_operations() {
    let mut table = IndexedTable::new();

    // Вставляем тестовые данные
    let alice_id = table
        .insert("Alice".to_string(), 25, "alice@example.com".to_string())
        .unwrap();
    let bob_id = table
        .insert("Bob".to_string(), 30, "bob@example.com".to_string())
        .unwrap();
    let _charlie_id = table
        .insert("Charlie".to_string(), 25, "charlie@example.com".to_string())
        .unwrap();

    assert_eq!(table.size(), 3);

    // Поиск по ID
    let alice = table.find_by_id(alice_id).unwrap().unwrap();
    assert_eq!(alice.name, "Alice");
    assert_eq!(alice.age, 25);

    // Поиск по имени
    let alices = table.find_by_name("Alice").unwrap();
    assert_eq!(alices.len(), 1);
    assert_eq!(alices[0].id, alice_id);

    // Поиск по диапазону возраста
    let young_people = table.find_by_age_range(20, 26).unwrap();
    assert_eq!(young_people.len(), 2); // Alice и Charlie

    // Удаление записи
    assert!(table.delete(bob_id).unwrap());
    assert_eq!(table.size(), 2);
    assert!(table.find_by_id(bob_id).unwrap().is_none());
}

#[test]
fn test_indexed_table_with_duplicates() {
    let mut table = IndexedTable::new();

    // Вставляем несколько записей с одинаковыми именами и возрастами
    let _john1 = table
        .insert("John".to_string(), 30, "john1@example.com".to_string())
        .unwrap();
    let _john2 = table
        .insert("John".to_string(), 30, "john2@example.com".to_string())
        .unwrap();
    let _john3 = table
        .insert("John".to_string(), 25, "john3@example.com".to_string())
        .unwrap();

    // Поиск по имени должен вернуть всех Johns
    let johns = table.find_by_name("John").unwrap();
    assert_eq!(johns.len(), 3);

    // Поиск по возрасту 30 должен вернуть двух Johns
    let thirty_year_olds = table.find_by_age_range(30, 30).unwrap();
    assert_eq!(thirty_year_olds.len(), 2);

    // Поиск по диапазону возраста должен вернуть всех Johns
    let all_johns_by_age = table.find_by_age_range(25, 30).unwrap();
    assert_eq!(all_johns_by_age.len(), 3);
}

#[test]
fn test_index_consistency_after_operations() {
    let mut table = IndexedTable::new();

    // Вставляем много записей
    let mut inserted_ids = Vec::new();
    for i in 1..=100 {
        let id = table
            .insert(
                format!("User{}", i),
                20 + (i % 50) as u32, // Возраст от 20 до 69
                format!("user{}@example.com", i),
            )
            .unwrap();
        inserted_ids.push(id);
    }

    assert_eq!(table.size(), 100);

    // Проверяем, что все записи можно найти по ID
    for &id in &inserted_ids {
        assert!(table.find_by_id(id).unwrap().is_some());
    }

    // Удаляем каждую вторую запись
    for i in (0..inserted_ids.len()).step_by(2) {
        assert!(table.delete(inserted_ids[i]).unwrap());
    }

    assert_eq!(table.size(), 50);

    // Проверяем, что удаленные записи не найдены
    for i in (0..inserted_ids.len()).step_by(2) {
        assert!(table.find_by_id(inserted_ids[i]).unwrap().is_none());
    }

    // Проверяем, что оставшиеся записи все еще доступны
    for i in (1..inserted_ids.len()).step_by(2) {
        assert!(table.find_by_id(inserted_ids[i]).unwrap().is_some());
    }
}

#[test]
fn test_index_with_page_manager_simulation() {
    // Симулируем интеграцию с менеджером страниц
    let _temp_dir = TempDir::new().unwrap();

    // Создаем индекс, который будет хранить указатели на страницы
    let mut page_index: BPlusTree<String, u32> = BPlusTree::new_default(); // key -> page_id

    // Симулируем вставку данных на разные страницы
    let test_data = [
        ("apple", 1),
        ("banana", 1),
        ("cherry", 2),
        ("date", 2),
        ("elderberry", 3),
    ];

    for (key, page_id) in test_data {
        page_index.insert(key.to_string(), page_id).unwrap();
    }

    // Проверяем поиск
    assert_eq!(page_index.search(&"apple".to_string()).unwrap(), Some(1));
    assert_eq!(page_index.search(&"cherry".to_string()).unwrap(), Some(2));

    // Диапазонный поиск для определения, какие страницы нужно загрузить
    let range_results = page_index
        .range_search(&"banana".to_string(), &"date".to_string())
        .unwrap();
    let affected_pages: std::collections::HashSet<u32> = range_results
        .into_iter()
        .map(|(_, page_id)| page_id)
        .collect();

    // Должны быть затронуты страницы 1 и 2
    assert!(affected_pages.contains(&1));
    assert!(affected_pages.contains(&2));
    assert_eq!(affected_pages.len(), 2);
}

#[test]
fn test_index_statistics_integration() {
    let mut btree: BPlusTree<i32, String> = BPlusTree::new_default();
    let mut hash_index: SimpleHashIndex<i32, String> = SimpleHashIndex::new();

    // Выполняем смешанные операции и отслеживаем статистику
    for i in 1..=1000 {
        btree.insert(i, format!("btree_value_{}", i)).unwrap();
        hash_index.insert(i, format!("hash_value_{}", i)).unwrap();
    }

    // Выполняем поиски
    for i in 1..=500 {
        let _ = btree.search(&i).unwrap();
        let _ = hash_index.search(&i).unwrap();
    }

    // Удаляем из хеш-индекса
    for i in 1..=100 {
        hash_index.delete(&i).unwrap();
    }

    // Диапазонные запросы в B+ дереве
    for start in (1..=900).step_by(100) {
        let _ = btree.range_search(&start, &(start + 50)).unwrap();
    }

    // Проверяем статистику
    let btree_stats = btree.get_statistics();
    let hash_stats = hash_index.get_statistics();

    assert_eq!(btree_stats.insert_operations, 1000);
    assert_eq!(hash_stats.insert_operations, 1000);
    assert_eq!(hash_stats.delete_operations, 100);

    assert_eq!(btree_stats.total_elements, 1000);
    assert_eq!(hash_stats.total_elements, 900); // 1000 - 100 удаленных

    assert!(btree_stats.fill_factor > 0.0);
    assert!(hash_stats.fill_factor > 0.0);
}

#[test]
fn test_concurrent_access_simulation() {
    use std::sync::{Arc, Mutex};
    use std::thread;

    // Симулируем многопоточный доступ к индексированной таблице
    let table = Arc::new(Mutex::new(IndexedTable::new()));
    let mut handles = vec![];

    // Запускаем потоки для записи
    for thread_id in 0..4 {
        let table_clone = Arc::clone(&table);
        let handle = thread::spawn(move || {
            for i in 0..25 {
                let user_id = thread_id * 25 + i;
                let mut table_lock = table_clone.lock().unwrap();
                table_lock
                    .insert(
                        format!("User{}", user_id),
                        20 + (user_id % 50) as u32,
                        format!("user{}@example.com", user_id),
                    )
                    .unwrap();
            }
        });
        handles.push(handle);
    }

    // Ждем завершения записи
    for handle in handles {
        handle.join().unwrap();
    }

    // Проверяем результаты
    {
        let table_lock = table.lock().unwrap();
        assert_eq!(table_lock.size(), 100);

        // Проверяем, что можем найти записи по разным критериям
        assert!(table_lock.find_by_name("User0").unwrap().len() > 0);
        assert!(table_lock.find_by_age_range(25, 35).unwrap().len() > 0);
    }

    // Запускаем потоки для чтения
    let mut read_handles = vec![];
    for _ in 0..4 {
        let table_clone = Arc::clone(&table);
        let handle = thread::spawn(move || {
            let table_lock = table_clone.lock().unwrap();

            // Выполняем различные типы запросов
            for i in 1..=10 {
                let _ = table_lock.find_by_id(i);
                let _ = table_lock.find_by_name(&format!("User{}", i));
                let _ = table_lock.find_by_age_range(20, 30);
            }
        });
        read_handles.push(handle);
    }

    for handle in read_handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_large_dataset_handling() {
    let mut table = IndexedTable::new();

    const LARGE_SIZE: usize = 10_000;

    // Вставляем большой набор данных
    for i in 1..=LARGE_SIZE {
        table
            .insert(
                format!("User{:05}", i),
                20 + (i % 60) as u32, // Возраст от 20 до 79
                format!("user{:05}@example.com", i),
            )
            .unwrap();
    }

    assert_eq!(table.size(), LARGE_SIZE);

    // Тестируем производительность различных типов запросов
    use std::time::Instant;

    // Поиск по ID
    let start = Instant::now();
    for i in 1..=1000 {
        let _ = table.find_by_id(i as u32).unwrap();
    }
    let id_search_time = start.elapsed();

    // Поиск по имени
    let start = Instant::now();
    for i in 1..=100 {
        let _ = table.find_by_name(&format!("User{:05}", i)).unwrap();
    }
    let name_search_time = start.elapsed();

    // Диапазонные запросы по возрасту
    let start = Instant::now();
    for age in 20..=30 {
        let _ = table.find_by_age_range(age, age + 5).unwrap();
    }
    let age_range_time = start.elapsed();

    println!("Производительность на {} записях:", LARGE_SIZE);
    println!("  Поиск по ID (1000 операций): {:?}", id_search_time);
    println!("  Поиск по имени (100 операций): {:?}", name_search_time);
    println!("  Диапазонные запросы (11 операций): {:?}", age_range_time);

    // Все операции должны выполняться за разумное время
    assert!(id_search_time.as_millis() < 100);
    assert!(name_search_time.as_millis() < 100);
    assert!(age_range_time.as_millis() < 100);
}
