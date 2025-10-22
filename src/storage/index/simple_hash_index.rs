//! Простой хеш-индекс для rustdb
//!
//! Упрощенная реализация хеш-индекса без сериализации
//! для быстрого поиска по ключу.

use crate::common::{Error, Result};
use crate::storage::index::{Index, IndexStatistics};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

/// Простой хеш-индекс на основе HashMap
#[derive(Debug, Clone)]
pub struct SimpleHashIndex<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    /// Внутренняя хеш-таблица
    data: HashMap<K, V>,
    /// Статистика операций
    statistics: IndexStatistics,
}

impl<K, V> SimpleHashIndex<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    /// Создает новый простой хеш-индекс
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            statistics: IndexStatistics::default(),
        }
    }

    /// Создает новый хеш-индекс с заданной начальной емкостью
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: HashMap::with_capacity(capacity),
            statistics: IndexStatistics::default(),
        }
    }

    /// Возвращает статистику индекса
    pub fn get_statistics(&self) -> &IndexStatistics {
        &self.statistics
    }

    /// Обновляет статистику индекса
    fn update_statistics(&mut self) {
        self.statistics.total_elements = self.data.len() as u64;
        self.statistics.fill_factor = if self.data.capacity() == 0 {
            0.0
        } else {
            self.data.len() as f64 / self.data.capacity() as f64
        };
        self.statistics.depth = 1; // Хеш-таблица имеет глубину 1
    }
}

impl<K, V> Index for SimpleHashIndex<K, V>
where
    K: Hash + Eq + Clone + Ord + Debug,
    V: Clone + Debug,
{
    type Key = K;
    type Value = V;

    fn insert(&mut self, key: Self::Key, value: Self::Value) -> Result<()> {
        self.statistics.insert_operations += 1;
        self.data.insert(key, value);
        self.update_statistics();
        Ok(())
    }

    fn search(&self, key: &Self::Key) -> Result<Option<Self::Value>> {
        // self.statistics.search_operations += 1; // TODO: Сделать статистику мутабельной
        Ok(self.data.get(key).cloned())
    }

    fn delete(&mut self, key: &Self::Key) -> Result<bool> {
        self.statistics.delete_operations += 1;
        let result = self.data.remove(key).is_some();
        self.update_statistics();
        Ok(result)
    }

    fn range_search(
        &self,
        _start: &Self::Key,
        _end: &Self::Key,
    ) -> Result<Vec<(Self::Key, Self::Value)>> {
        // self.statistics.range_search_operations += 1; // TODO: Сделать статистику мутабельной

        // Хеш-индексы не поддерживают эффективные диапазонные запросы
        // Возвращаем пустой результат
        Ok(Vec::new())
    }

    fn size(&self) -> usize {
        self.data.len()
    }
}

impl<K, V> Default for SimpleHashIndex<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_hash_index_creation() {
        let index: SimpleHashIndex<i32, String> = SimpleHashIndex::new();
        assert!(index.is_empty());
        assert_eq!(index.size(), 0);
    }

    #[test]
    fn test_simple_hash_index_insert_and_search() {
        let mut index = SimpleHashIndex::new();

        // Вставляем элементы
        index
            .insert("key1".to_string(), "value1".to_string())
            .unwrap();
        index
            .insert("key2".to_string(), "value2".to_string())
            .unwrap();
        index
            .insert("key3".to_string(), "value3".to_string())
            .unwrap();

        assert_eq!(index.size(), 3);

        // Проверяем поиск
        assert_eq!(
            index.search(&"key1".to_string()).unwrap(),
            Some("value1".to_string())
        );
        assert_eq!(
            index.search(&"key2".to_string()).unwrap(),
            Some("value2".to_string())
        );
        assert_eq!(
            index.search(&"key3".to_string()).unwrap(),
            Some("value3".to_string())
        );
        assert_eq!(index.search(&"key4".to_string()).unwrap(), None);
    }

    #[test]
    fn test_simple_hash_index_deletion() {
        let mut index = SimpleHashIndex::new();

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
    fn test_simple_hash_index_update() {
        let mut index = SimpleHashIndex::new();

        // Вставляем элемент
        index
            .insert("key".to_string(), "original_value".to_string())
            .unwrap();
        assert_eq!(index.size(), 1);

        // Обновляем тот же ключ
        index
            .insert("key".to_string(), "updated_value".to_string())
            .unwrap();
        assert_eq!(index.size(), 1); // Размер не должен измениться

        // Проверяем, что значение обновлено
        assert_eq!(
            index.search(&"key".to_string()).unwrap(),
            Some("updated_value".to_string())
        );
    }

    #[test]
    fn test_simple_hash_index_range_search() {
        let mut index = SimpleHashIndex::new();

        // Вставляем элементы
        for i in 1..=10 {
            index.insert(i, format!("value_{}", i)).unwrap();
        }

        // Тестируем диапазонный поиск (должен возвращать пустой результат)
        let results = index.range_search(&3, &7).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_simple_hash_index_statistics() {
        let mut index = SimpleHashIndex::new();

        // Вставляем элементы
        for i in 1..=5 {
            index.insert(i, format!("value_{}", i)).unwrap();
        }

        let stats = index.get_statistics();
        assert_eq!(stats.total_elements, 5);
        assert_eq!(stats.insert_operations, 5);
        assert!(stats.fill_factor > 0.0);
        assert_eq!(stats.depth, 1);

        // Удаляем элемент
        index.delete(&3).unwrap();

        let stats = index.get_statistics();
        assert_eq!(stats.total_elements, 4);
        assert_eq!(stats.delete_operations, 1);
    }
}
