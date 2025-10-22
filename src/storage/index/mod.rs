//! Модуль индексов для rustdb
//!
//! Этот модуль предоставляет реализации различных типов индексов,
//! включая B+ деревья и хеш-индексы.

pub mod btree;
pub mod simple_hash_index;
// pub mod hash_index; // Временно отключено

pub use btree::BPlusTree;
pub use simple_hash_index::SimpleHashIndex;
// pub use hash_index::{HashIndex, CollisionResolution};

use crate::common::{
    types::{PageId, RecordId},
    Result,
};
use serde::{Deserialize, Serialize};

/// Трейт для всех типов индексов
pub trait Index {
    type Key: Ord + Clone;
    type Value: Clone;

    /// Вставляет ключ-значение в индекс
    fn insert(&mut self, key: Self::Key, value: Self::Value) -> Result<()>;

    /// Ищет значение по ключу
    fn search(&self, key: &Self::Key) -> Result<Option<Self::Value>>;

    /// Удаляет ключ из индекса
    fn delete(&mut self, key: &Self::Key) -> Result<bool>;

    /// Возвращает все ключи в диапазоне [start, end]
    fn range_search(
        &self,
        start: &Self::Key,
        end: &Self::Key,
    ) -> Result<Vec<(Self::Key, Self::Value)>>;

    /// Возвращает количество элементов в индексе
    fn size(&self) -> usize;

    /// Проверяет, пуст ли индекс
    fn is_empty(&self) -> bool {
        self.size() == 0
    }
}

/// Статистика индекса
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStatistics {
    /// Общее количество элементов
    pub total_elements: u64,
    /// Количество операций вставки
    pub insert_operations: u64,
    /// Количество операций поиска
    pub search_operations: u64,
    /// Количество операций удаления
    pub delete_operations: u64,
    /// Количество операций диапазонного поиска
    pub range_search_operations: u64,
    /// Глубина индекса (для деревьев)
    pub depth: u32,
    /// Коэффициент заполнения
    pub fill_factor: f64,
}

impl Default for IndexStatistics {
    fn default() -> Self {
        Self {
            total_elements: 0,
            insert_operations: 0,
            search_operations: 0,
            delete_operations: 0,
            range_search_operations: 0,
            depth: 0,
            fill_factor: 0.0,
        }
    }
}

/// Тип индекса
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexType {
    /// B+ дерево
    BPlusTree,
    /// Хеш-индекс
    Hash,
}

/// Конфигурация индекса
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    /// Тип индекса
    pub index_type: IndexType,
    /// Максимальное количество ключей в узле (для B+ дерева)
    pub max_keys_per_node: usize,
    /// Начальный размер хеш-таблицы
    pub initial_hash_size: usize,
    /// Коэффициент загрузки для расширения хеш-таблицы
    pub load_factor_threshold: f64,
    /// Включить ли кеширование
    pub enable_caching: bool,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            index_type: IndexType::BPlusTree,
            max_keys_per_node: 255, // Стандартный размер для B+ дерева
            initial_hash_size: 1024,
            load_factor_threshold: 0.75,
            enable_caching: true,
        }
    }
}
