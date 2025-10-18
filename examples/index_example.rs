//! Пример использования индексов rustdb
//! 
//! Этот пример демонстрирует использование B+ дерева и хеш-индексов
//! для быстрого поиска данных.

use rustdb::storage::index::{BPlusTree, SimpleHashIndex, Index};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🌳 Пример использования индексов rustdb");
    
    // Демонстрация B+ дерева
    println!("\n📊 B+ дерево:");
    btree_example()?;
    
    // Демонстрация хеш-индекса
    println!("\n🔗 Хеш-индекс:");
    hash_index_example()?;
    
    // Сравнение производительности
    println!("\n⚡ Сравнение производительности:");
    performance_comparison()?;
    
    Ok(())
}

fn btree_example() -> Result<(), Box<dyn std::error::Error>> {
    let mut btree: BPlusTree<i32, String> = BPlusTree::new_default();
    
    // Вставляем данные
    println!("Вставляем данные в B+ дерево...");
    for i in [5, 2, 8, 1, 9, 3, 7, 4, 6] {
        btree.insert(i, format!("Значение {}", i))?;
    }
    
    println!("Размер дерева: {} элементов", btree.size());
    
    // Поиск отдельных элементов
    println!("Поиск элементов:");
    for key in [1, 5, 9, 10] {
        match btree.search(&key)? {
            Some(value) => println!("  Ключ {}: {}", key, value),
            None => println!("  Ключ {} не найден", key),
        }
    }
    
    // Диапазонный поиск
    println!("Диапазонный поиск (3-7):");
    let range_results = btree.range_search(&3, &7)?;
    for (key, value) in range_results {
        println!("  {}: {}", key, value);
    }
    
    // Статистика
    let stats = btree.get_statistics();
    println!("Статистика B+ дерева:");
    println!("  Операций вставки: {}", stats.insert_operations);
    println!("  Глубина: {}", stats.depth);
    println!("  Коэффициент заполнения: {:.2}", stats.fill_factor);
    
    Ok(())
}

fn hash_index_example() -> Result<(), Box<dyn std::error::Error>> {
    let mut hash_index: SimpleHashIndex<String, i32> = SimpleHashIndex::new();
    
    // Вставляем данные
    println!("Вставляем данные в хеш-индекс...");
    let users = [
        ("alice", 25),
        ("bob", 30),
        ("charlie", 35),
        ("diana", 28),
        ("eve", 32),
    ];
    
    for (name, age) in users {
        hash_index.insert(name.to_string(), age)?;
    }
    
    println!("Размер индекса: {} элементов", hash_index.size());
    
    // Поиск элементов
    println!("Поиск пользователей:");
    for name in ["alice", "bob", "frank"] {
        match hash_index.search(&name.to_string())? {
            Some(age) => println!("  {}: {} лет", name, age),
            None => println!("  {} не найден", name),
        }
    }
    
    // Удаление элемента
    println!("Удаляем пользователя 'charlie'...");
    if hash_index.delete(&"charlie".to_string())? {
        println!("  Пользователь удален");
    }
    
    println!("Размер после удаления: {} элементов", hash_index.size());
    
    // Обновление значения
    println!("Обновляем возраст Alice...");
    hash_index.insert("alice".to_string(), 26)?;
    
    if let Some(age) = hash_index.search(&"alice".to_string())? {
        println!("  Новый возраст Alice: {} лет", age);
    }
    
    // Статистика
    let stats = hash_index.get_statistics();
    println!("Статистика хеш-индекса:");
    println!("  Операций вставки: {}", stats.insert_operations);
    println!("  Операций удаления: {}", stats.delete_operations);
    println!("  Коэффициент заполнения: {:.2}", stats.fill_factor);
    
    Ok(())
}

fn performance_comparison() -> Result<(), Box<dyn std::error::Error>> {
    use std::time::Instant;
    
    const N: i32 = 10000;
    
    // Тестируем B+ дерево
    let mut btree: BPlusTree<i32, String> = BPlusTree::new_default();
    let start = Instant::now();
    
    for i in 1..=N {
        btree.insert(i, format!("value_{}", i))?;
    }
    
    let btree_insert_time = start.elapsed();
    
    let start = Instant::now();
    for i in 1..=N {
        let _ = btree.search(&i)?;
    }
    let btree_search_time = start.elapsed();
    
    // Тестируем хеш-индекс
    let mut hash_index: SimpleHashIndex<i32, String> = SimpleHashIndex::with_capacity(N as usize);
    let start = Instant::now();
    
    for i in 1..=N {
        hash_index.insert(i, format!("value_{}", i))?;
    }
    
    let hash_insert_time = start.elapsed();
    
    let start = Instant::now();
    for i in 1..=N {
        let _ = hash_index.search(&i)?;
    }
    let hash_search_time = start.elapsed();
    
    println!("Результаты для {} элементов:", N);
    println!("B+ дерево:");
    println!("  Время вставки: {:?}", btree_insert_time);
    println!("  Время поиска: {:?}", btree_search_time);
    println!("  Глубина: {}", btree.get_statistics().depth);
    
    println!("Хеш-индекс:");
    println!("  Время вставки: {:?}", hash_insert_time);
    println!("  Время поиска: {:?}", hash_search_time);
    println!("  Коэффициент заполнения: {:.2}", hash_index.get_statistics().fill_factor);
    
    println!("\nВыводы:");
    if hash_search_time < btree_search_time {
        println!("  ✅ Хеш-индекс быстрее для поиска отдельных элементов");
    } else {
        println!("  ✅ B+ дерево конкурентоспособно для поиска");
    }
    println!("  📊 B+ дерево поддерживает диапазонные запросы");
    println!("  🔗 Хеш-индекс оптимален для точного поиска");
    
    Ok(())
}
