//! Хеш-индекс для rustdb
//! 
//! Реализация хеш-индекса для быстрого поиска по ключу.
//! Поддерживает динамическое расширение и различные методы разрешения коллизий.

use crate::common::{Result, Error};
use crate::storage::index::{Index, IndexStatistics};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::fmt::Debug;

/// Стратегия разрешения коллизий
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CollisionResolution {
    /// Метод цепочек (chaining)
    Chaining,
    /// Открытая адресация (open addressing)
    OpenAddressing,
}

/// Элемент хеш-таблицы для метода цепочек
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChainEntry<K, V> 
where
    K: Hash + Eq + Clone + Serialize + for<'de> Deserialize<'de>,
    V: Clone + Serialize + for<'de> Deserialize<'de>,
{
    key: K,
    value: V,
    next: Option<Box<ChainEntry<K, V>>>,
}

/// Элемент хеш-таблицы для открытой адресации
#[derive(Debug, Clone, Serialize, Deserialize)]
enum HashEntry<K, V>
where
    K: Hash + Eq + Clone + Serialize + for<'de> Deserialize<'de>,
    V: Clone + Serialize + for<'de> Deserialize<'de>,
{
    /// Пустая ячейка
    Empty,
    /// Удаленная ячейка (tombstone)
    Deleted,
    /// Занятая ячейка
    Occupied { key: K, value: V },
}

/// Хеш-индекс
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashIndex<K, V>
where
    K: Hash + Eq + Clone + Serialize + for<'de> Deserialize<'de>,
    V: Clone + Serialize + for<'de> Deserialize<'de>,
{
    /// Таблица для метода цепочек
    chains: Option<Vec<Option<ChainEntry<K, V>>>>,
    /// Таблица для открытой адресации
    open_table: Option<Vec<HashEntry<K, V>>>,
    /// Стратегия разрешения коллизий
    collision_resolution: CollisionResolution,
    /// Размер таблицы
    capacity: usize,
    /// Количество элементов
    size: usize,
    /// Количество удаленных элементов (для открытой адресации)
    deleted_count: usize,
    /// Пороговое значение коэффициента загрузки
    load_factor_threshold: f64,
    /// Статистика операций
    statistics: IndexStatistics,
}

impl<K, V> HashIndex<K, V>
where
    K: Hash + Eq + Clone + Serialize + for<'de> Deserialize<'de>,
    V: Clone + Serialize + for<'de> Deserialize<'de>,
{
    /// Создает новый хеш-индекс с заданными параметрами
    pub fn new(
        initial_capacity: usize,
        collision_resolution: CollisionResolution,
        load_factor_threshold: f64,
    ) -> Self {
        let capacity = initial_capacity.max(16); // Минимальный размер
        
        let (chains, open_table) = match collision_resolution {
            CollisionResolution::Chaining => {
                (Some(vec![None; capacity]), None)
            },
            CollisionResolution::OpenAddressing => {
                (None, Some(vec![HashEntry::Empty; capacity]))
            },
        };
        
        Self {
            chains,
            open_table,
            collision_resolution,
            capacity,
            size: 0,
            deleted_count: 0,
            load_factor_threshold,
            statistics: IndexStatistics::default(),
        }
    }
    
    /// Создает новый хеш-индекс с параметрами по умолчанию
    pub fn new_default() -> Self {
        Self::new(1024, CollisionResolution::Chaining, 0.75)
    }
    
    /// Возвращает статистику индекса
    pub fn get_statistics(&self) -> &IndexStatistics {
        &self.statistics
    }
    
    /// Вычисляет хеш ключа
    fn hash_key(&self, key: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.capacity
    }
    
    /// Вычисляет хеш для открытой адресации (двойное хеширование)
    fn hash_key_double(&self, key: &K, attempt: usize) -> usize {
        let mut hasher1 = DefaultHasher::new();
        key.hash(&mut hasher1);
        let hash1 = hasher1.finish() as usize;
        
        let mut hasher2 = DefaultHasher::new();
        key.hash(&mut hasher2);
        let hash2 = (hasher2.finish() as usize) | 1; // Делаем нечетным
        
        (hash1 + attempt * hash2) % self.capacity
    }
    
    /// Проверяет, нужно ли расширение таблицы
    fn should_resize(&self) -> bool {
        let load_factor = match self.collision_resolution {
            CollisionResolution::Chaining => {
                self.size as f64 / self.capacity as f64
            },
            CollisionResolution::OpenAddressing => {
                (self.size + self.deleted_count) as f64 / self.capacity as f64
            },
        };
        
        load_factor > self.load_factor_threshold
    }
    
    /// Расширяет хеш-таблицу
    fn resize(&mut self) -> Result<()> {
        let old_capacity = self.capacity;
        let new_capacity = old_capacity * 2;
        
        // Сохраняем старые данные
        let old_chains = self.chains.take();
        let old_open_table = self.open_table.take();
        
        // Создаем новую таблицу
        self.capacity = new_capacity;
        self.size = 0;
        self.deleted_count = 0;
        
        match self.collision_resolution {
            CollisionResolution::Chaining => {
                self.chains = Some(vec![None; new_capacity]);
            },
            CollisionResolution::OpenAddressing => {
                self.open_table = Some(vec![HashEntry::Empty; new_capacity]);
            },
        }
        
        // Перехешируем все элементы
        match (old_chains, old_open_table) {
            (Some(chains), None) => {
                for chain_head in chains {
                    let mut current = chain_head;
                    while let Some(entry) = current {
                        self.insert_without_resize(entry.key, entry.value)?;
                        current = entry.next.map(|boxed| *boxed);
                    }
                }
            },
            (None, Some(table)) => {
                for entry in table {
                    if let HashEntry::Occupied { key, value } = entry {
                        self.insert_without_resize(key, value)?;
                    }
                }
            },
            _ => unreachable!(),
        }
        
        Ok(())
    }
    
    /// Вставляет элемент без проверки на расширение
    fn insert_without_resize(&mut self, key: K, value: V) -> Result<()> {
        match self.collision_resolution {
            CollisionResolution::Chaining => {
                self.insert_chaining(key, value)
            },
            CollisionResolution::OpenAddressing => {
                self.insert_open_addressing(key, value)
            },
        }
    }
    
    /// Вставляет элемент с использованием метода цепочек
    fn insert_chaining(&mut self, key: K, value: V) -> Result<()> {
        let chains = self.chains.as_mut().unwrap();
        let index = self.hash_key(&key);
        
        // Проверяем, существует ли уже такой ключ
        let mut current = &mut chains[index];
        loop {
            match current {
                None => {
                    // Вставляем новый элемент
                    *current = Some(ChainEntry {
                        key,
                        value,
                        next: None,
                    });
                    self.size += 1;
                    self.statistics.total_elements += 1;
                    break;
                },
                Some(ref mut entry) => {
                    if entry.key == key {
                        // Обновляем существующий элемент
                        entry.value = value;
                        break;
                    }
                    
                    if entry.next.is_none() {
                        // Добавляем в конец цепочки
                        entry.next = Some(Box::new(ChainEntry {
                            key,
                            value,
                            next: None,
                        }));
                        self.size += 1;
                        self.statistics.total_elements += 1;
                        break;
                    }
                    
                    // Переходим к следующему элементу
                    current = &mut entry.next.as_mut().unwrap().next;
                }
            }
        }
        
        Ok(())
    }
    
    /// Вставляет элемент с использованием открытой адресации
    fn insert_open_addressing(&mut self, key: K, value: V) -> Result<()> {
        let table = self.open_table.as_mut().unwrap();
        
        for attempt in 0..self.capacity {
            let index = self.hash_key_double(&key, attempt);
            
            match &table[index] {
                HashEntry::Empty | HashEntry::Deleted => {
                    if matches!(table[index], HashEntry::Deleted) {
                        self.deleted_count -= 1;
                    }
                    table[index] = HashEntry::Occupied { key, value };
                    self.size += 1;
                    self.statistics.total_elements += 1;
                    return Ok(());
                },
                HashEntry::Occupied { key: existing_key, .. } => {
                    if *existing_key == key {
                        // Обновляем существующий элемент
                        table[index] = HashEntry::Occupied { key, value };
                        return Ok(());
                    }
                    // Продолжаем поиск
                }
            }
        }
        
        Err(Error::database("Hash table is full"))
    }
    
    /// Ищет элемент с использованием метода цепочек
    fn search_chaining(&self, key: &K) -> Option<V> {
        let chains = self.chains.as_ref().unwrap();
        let index = self.hash_key(key);
        
        let mut current = &chains[index];
        while let Some(ref entry) = current {
            if entry.key == *key {
                return Some(entry.value.clone());
            }
            current = &entry.next.as_ref().map(|boxed| boxed.as_ref());
        }
        
        None
    }
    
    /// Ищет элемент с использованием открытой адресации
    fn search_open_addressing(&self, key: &K) -> Option<V> {
        let table = self.open_table.as_ref().unwrap();
        
        for attempt in 0..self.capacity {
            let index = self.hash_key_double(key, attempt);
            
            match &table[index] {
                HashEntry::Empty => return None,
                HashEntry::Deleted => continue,
                HashEntry::Occupied { key: existing_key, value } => {
                    if *existing_key == *key {
                        return Some(value.clone());
                    }
                }
            }
        }
        
        None
    }
    
    /// Удаляет элемент с использованием метода цепочек
    fn delete_chaining(&mut self, key: &K) -> Result<bool> {
        let chains = self.chains.as_mut().unwrap();
        let index = self.hash_key(key);
        
        let chain_head = &mut chains[index];
        
        // Проверяем первый элемент
        if let Some(ref entry) = chain_head {
            if entry.key == *key {
                *chain_head = entry.next.as_ref().map(|boxed| *boxed.clone());
                self.size -= 1;
                self.statistics.total_elements -= 1;
                return Ok(true);
            }
        }
        
        // Ищем в остальной части цепочки
        let mut current = chain_head;
        while let Some(ref mut entry) = current {
            if let Some(ref mut next_entry) = entry.next {
                if next_entry.key == *key {
                    entry.next = next_entry.next.take();
                    self.size -= 1;
                    self.statistics.total_elements -= 1;
                    return Ok(true);
                }
            }
            current = &mut entry.next.as_mut().map(|boxed| boxed.as_mut());
        }
        
        Ok(false)
    }
    
    /// Удаляет элемент с использованием открытой адресации
    fn delete_open_addressing(&mut self, key: &K) -> Result<bool> {
        let table = self.open_table.as_mut().unwrap();
        
        for attempt in 0..self.capacity {
            let index = self.hash_key_double(key, attempt);
            
            match &table[index] {
                HashEntry::Empty => return Ok(false),
                HashEntry::Deleted => continue,
                HashEntry::Occupied { key: existing_key, .. } => {
                    if *existing_key == *key {
                        table[index] = HashEntry::Deleted;
                        self.size -= 1;
                        self.deleted_count += 1;
                        self.statistics.total_elements -= 1;
                        return Ok(true);
                    }
                }
            }
        }
        
        Ok(false)
    }
    
    /// Обновляет статистику индекса
    fn update_statistics(&mut self) {
        self.statistics.fill_factor = self.size as f64 / self.capacity as f64;
        self.statistics.depth = 1; // Хеш-таблица имеет глубину 1
    }
}

impl<K, V> Index for HashIndex<K, V>
where
    K: Hash + Eq + Clone + Serialize + for<'de> Deserialize<'de> + Debug,
    V: Clone + Serialize + for<'de> Deserialize<'de> + Debug,
{
    type Key = K;
    type Value = V;
    
    fn insert(&mut self, key: Self::Key, value: Self::Value) -> Result<()> {
        self.statistics.insert_operations += 1;
        
        if self.should_resize() {
            self.resize()?;
        }
        
        self.insert_without_resize(key, value)?;
        self.update_statistics();
        Ok(())
    }
    
    fn search(&self, key: &Self::Key) -> Result<Option<Self::Value>> {
        // self.statistics.search_operations += 1; // TODO: Сделать статистику мутабельной
        
        let result = match self.collision_resolution {
            CollisionResolution::Chaining => self.search_chaining(key),
            CollisionResolution::OpenAddressing => self.search_open_addressing(key),
        };
        
        Ok(result)
    }
    
    fn delete(&mut self, key: &Self::Key) -> Result<bool> {
        self.statistics.delete_operations += 1;
        
        let result = match self.collision_resolution {
            CollisionResolution::Chaining => self.delete_chaining(key)?,
            CollisionResolution::OpenAddressing => self.delete_open_addressing(key)?,
        };
        
        self.update_statistics();
        Ok(result)
    }
    
    fn range_search(&self, _start: &Self::Key, _end: &Self::Key) -> Result<Vec<(Self::Key, Self::Value)>> {
        // self.statistics.range_search_operations += 1; // TODO: Сделать статистику мутабельной
        
        // Хеш-индексы не поддерживают эффективные диапазонные запросы
        // Возвращаем пустой результат
        Ok(Vec::new())
    }
    
    fn size(&self) -> usize {
        self.size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_hash_index_chaining() {
        let mut index = HashIndex::new(16, CollisionResolution::Chaining, 0.75);
        
        // Вставляем элементы
        index.insert("key1".to_string(), "value1".to_string()).unwrap();
        index.insert("key2".to_string(), "value2".to_string()).unwrap();
        index.insert("key3".to_string(), "value3".to_string()).unwrap();
        
        assert_eq!(index.size(), 3);
        
        // Проверяем поиск
        assert_eq!(index.search(&"key1".to_string()).unwrap(), Some("value1".to_string()));
        assert_eq!(index.search(&"key2".to_string()).unwrap(), Some("value2".to_string()));
        assert_eq!(index.search(&"key3".to_string()).unwrap(), Some("value3".to_string()));
        assert_eq!(index.search(&"key4".to_string()).unwrap(), None);
    }
    
    #[test]
    fn test_hash_index_open_addressing() {
        let mut index = HashIndex::new(16, CollisionResolution::OpenAddressing, 0.75);
        
        // Вставляем элементы
        index.insert("key1".to_string(), "value1".to_string()).unwrap();
        index.insert("key2".to_string(), "value2".to_string()).unwrap();
        index.insert("key3".to_string(), "value3".to_string()).unwrap();
        
        assert_eq!(index.size(), 3);
        
        // Проверяем поиск
        assert_eq!(index.search(&"key1".to_string()).unwrap(), Some("value1".to_string()));
        assert_eq!(index.search(&"key2".to_string()).unwrap(), Some("value2".to_string()));
        assert_eq!(index.search(&"key3".to_string()).unwrap(), Some("value3".to_string()));
        assert_eq!(index.search(&"key4".to_string()).unwrap(), None);
    }
    
    #[test]
    fn test_hash_index_deletion() {
        let mut index = HashIndex::new_default();
        
        // Вставляем элементы
        for i in 1..=10 {
            index.insert(i, format!("value_{}", i)).unwrap();
        }
        
        assert_eq!(index.size(), 10);
        
        // Удаляем некоторые элементы
        assert!(index.delete(&5).unwrap());
        assert!(index.delete(&7).unwrap());
        assert!(!index.delete(&15).unwrap()); // Не существует
        
        assert_eq!(index.size(), 8);
        
        // Проверяем, что удаленные элементы не найдены
        assert_eq!(index.search(&5).unwrap(), None);
        assert_eq!(index.search(&7).unwrap(), None);
        
        // Проверяем, что остальные элементы на месте
        assert_eq!(index.search(&1).unwrap(), Some("value_1".to_string()));
        assert_eq!(index.search(&10).unwrap(), Some("value_10".to_string()));
    }
    
    #[test]
    fn test_hash_index_resize() {
        let mut index = HashIndex::new(4, CollisionResolution::Chaining, 0.5); // Низкий порог для тестирования
        
        // Вставляем много элементов для вызова расширения
        for i in 1..=20 {
            index.insert(i, format!("value_{}", i)).unwrap();
        }
        
        assert_eq!(index.size(), 20);
        assert!(index.capacity > 4); // Таблица должна была расшириться
        
        // Проверяем, что все элементы доступны после расширения
        for i in 1..=20 {
            assert_eq!(index.search(&i).unwrap(), Some(format!("value_{}", i)));
        }
    }
    
    #[test]
    fn test_hash_index_update() {
        let mut index = HashIndex::new_default();
        
        // Вставляем элемент
        index.insert("key".to_string(), "original_value".to_string()).unwrap();
        assert_eq!(index.size(), 1);
        
        // Обновляем тот же ключ
        index.insert("key".to_string(), "updated_value".to_string()).unwrap();
        assert_eq!(index.size(), 1); // Размер не должен измениться
        
        // Проверяем, что значение обновлено
        assert_eq!(index.search(&"key".to_string()).unwrap(), Some("updated_value".to_string()));
    }
}
