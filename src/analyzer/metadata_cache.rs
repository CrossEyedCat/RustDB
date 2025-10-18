//! Модуль для кэширования метаданных семантического анализатора

// use crate::common::{Error, Result}; // Not used in this simplified version
use crate::parser::ast::DataType;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use serde::{Serialize, Deserialize};

/// Запись в кэше метаданных
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntry {
    /// Данные записи
    pub data: CacheData,
    /// Время создания записи
    pub created_at: u64, // Unix timestamp в миллисекундах
    /// Время последнего доступа
    pub last_accessed: u64,
    /// Количество обращений к записи
    pub access_count: u64,
    /// Время жизни записи (TTL) в миллисекундах
    pub ttl_ms: Option<u64>,
}

impl CacheEntry {
    pub fn new(data: CacheData) -> Self {
        let now = current_timestamp_ms();
        Self {
            data,
            created_at: now,
            last_accessed: now,
            access_count: 0,
            ttl_ms: None,
        }
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl_ms = Some(ttl.as_millis() as u64);
        self
    }

    pub fn is_expired(&self) -> bool {
        if let Some(ttl_ms) = self.ttl_ms {
            let now = current_timestamp_ms();
            (now - self.created_at) > ttl_ms
        } else {
            false
        }
    }

    pub fn access(&mut self) {
        self.last_accessed = current_timestamp_ms();
        self.access_count += 1;
    }

    pub fn age_ms(&self) -> u64 {
        current_timestamp_ms() - self.created_at
    }
}

/// Данные, хранящиеся в кэше
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CacheData {
    /// Информация о таблице
    TableInfo {
        name: String,
        columns: Vec<ColumnInfo>,
        indexes: Vec<String>,
        exists: bool,
    },
    /// Информация о колонке
    ColumnInfo {
        table_name: String,
        column_name: String,
        data_type: DataType,
        is_nullable: bool,
        is_primary_key: bool,
        exists: bool,
    },
    /// Информация об индексе
    IndexInfo {
        name: String,
        table_name: String,
        columns: Vec<String>,
        is_unique: bool,
        exists: bool,
    },
    /// Результат проверки типов
    TypeCheckResult {
        expression: String,
        result_type: DataType,
        is_valid: bool,
    },
    /// Результат проверки прав доступа
    AccessCheckResult {
        object_name: String,
        username: String,
        permission: String,
        allowed: bool,
    },
}

/// Информация о колонке
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: DataType,
    pub is_nullable: bool,
    pub is_primary_key: bool,
    pub default_value: Option<String>,
}

/// Статистика кэша
#[derive(Debug, Clone)]
pub struct CacheStatistics {
    /// Общее количество записей
    pub total_entries: usize,
    /// Количество попаданий в кэш
    pub hits: u64,
    /// Количество промахов кэша
    pub misses: u64,
    /// Количество истёкших записей
    pub expired_entries: usize,
    /// Размер кэша в байтах (примерный)
    pub estimated_size_bytes: usize,
    /// Время последней очистки
    pub last_cleanup: Option<u64>,
}

impl CacheStatistics {
    pub fn new() -> Self {
        Self {
            total_entries: 0,
            hits: 0,
            misses: 0,
            expired_entries: 0,
            estimated_size_bytes: 0,
            last_cleanup: None,
        }
    }

    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total > 0 {
            self.hits as f64 / total as f64
        } else {
            0.0
        }
    }

    pub fn miss_rate(&self) -> f64 {
        1.0 - self.hit_rate()
    }
}

/// Стратегия вытеснения записей из кэша
#[derive(Debug, Clone)]
pub enum EvictionStrategy {
    /// Least Recently Used - вытесняем наименее недавно используемые
    LRU,
    /// Least Frequently Used - вытесняем наименее часто используемые
    LFU,
    /// First In First Out - вытесняем самые старые
    FIFO,
    /// Time To Live - вытесняем истёкшие
    TTL,
}

/// Настройки кэша
#[derive(Debug, Clone)]
pub struct CacheSettings {
    /// Максимальное количество записей в кэше
    pub max_entries: usize,
    /// Стратегия вытеснения
    pub eviction_strategy: EvictionStrategy,
    /// TTL по умолчанию для записей
    pub default_ttl: Option<Duration>,
    /// Интервал автоматической очистки
    pub cleanup_interval: Duration,
    /// Включена ли сериализация кэша
    pub enable_persistence: bool,
}

impl Default for CacheSettings {
    fn default() -> Self {
        Self {
            max_entries: 1000,
            eviction_strategy: EvictionStrategy::LRU,
            default_ttl: Some(Duration::from_secs(3600)), // 1 час
            cleanup_interval: Duration::from_secs(300), // 5 минут
            enable_persistence: false,
        }
    }
}

/// Кэш метаданных
pub struct MetadataCache {
    /// Хранилище записей кэша
    entries: HashMap<String, CacheEntry>,
    /// Настройки кэша
    settings: CacheSettings,
    /// Статистика кэша
    statistics: CacheStatistics,
    /// Включен ли кэш
    enabled: bool,
    /// Время последней очистки
    last_cleanup: Instant,
}

impl MetadataCache {
    /// Создает новый кэш метаданных
    pub fn new(enabled: bool) -> Self {
        Self {
            entries: HashMap::new(),
            settings: CacheSettings::default(),
            statistics: CacheStatistics::new(),
            enabled,
            last_cleanup: Instant::now(),
        }
    }

    /// Создает кэш с настройками
    pub fn with_settings(enabled: bool, settings: CacheSettings) -> Self {
        Self {
            entries: HashMap::new(),
            settings,
            statistics: CacheStatistics::new(),
            enabled,
            last_cleanup: Instant::now(),
        }
    }

    /// Получает запись из кэша
    pub fn get(&mut self, key: &str) -> Option<CacheData> {
        if !self.enabled {
            return None;
        }

        // Сначала проверяем, не истекла ли запись
        let is_expired = if let Some(entry) = self.entries.get(key) {
            entry.is_expired()
        } else {
            false
        };

        if is_expired {
            self.entries.remove(key);
            self.statistics.misses += 1;
            return None;
        }

        if let Some(entry) = self.entries.get_mut(key) {
            // Обновляем статистику доступа
            entry.access();
            self.statistics.hits += 1;
            Some(entry.data.clone())
        } else {
            self.statistics.misses += 1;
            None
        }
    }

    /// Добавляет запись в кэш
    pub fn put(&mut self, key: String, data: CacheData) {
        if !self.enabled {
            return;
        }

        let mut entry = CacheEntry::new(data);
        
        // Применяем TTL по умолчанию
        if let Some(default_ttl) = self.settings.default_ttl {
            entry = entry.with_ttl(default_ttl);
        }

        // Проверяем, нужно ли освободить место
        if self.entries.len() >= self.settings.max_entries {
            self.evict_entries();
        }

        self.entries.insert(key, entry);
        self.update_statistics();
    }

    /// Добавляет запись с кастомным TTL
    pub fn put_with_ttl(&mut self, key: String, data: CacheData, ttl: Duration) {
        if !self.enabled {
            return;
        }

        let entry = CacheEntry::new(data).with_ttl(ttl);

        // Проверяем, нужно ли освободить место
        if self.entries.len() >= self.settings.max_entries {
            self.evict_entries();
        }

        self.entries.insert(key, entry);
        self.update_statistics();
    }

    /// Удаляет запись из кэша
    pub fn remove(&mut self, key: &str) -> bool {
        if self.entries.remove(key).is_some() {
            self.update_statistics();
            true
        } else {
            false
        }
    }

    /// Проверяет существование записи в кэше
    pub fn contains(&self, key: &str) -> bool {
        if !self.enabled {
            return false;
        }

        if let Some(entry) = self.entries.get(key) {
            !entry.is_expired()
        } else {
            false
        }
    }

    /// Очищает весь кэш
    pub fn clear(&mut self) {
        self.entries.clear();
        self.statistics = CacheStatistics::new();
    }

    /// Выполняет очистку истёкших записей
    pub fn cleanup(&mut self) {
        let now = Instant::now();
        
        // Проверяем, нужна ли очистка
        if now.duration_since(self.last_cleanup) < self.settings.cleanup_interval {
            return;
        }

        let mut expired_keys = Vec::new();
        
        for (key, entry) in &self.entries {
            if entry.is_expired() {
                expired_keys.push(key.clone());
            }
        }

        for key in expired_keys {
            self.entries.remove(&key);
        }

        self.last_cleanup = now;
        self.statistics.last_cleanup = Some(current_timestamp_ms());
        self.update_statistics();
    }

    /// Получает статистику кэша
    pub fn statistics(&self) -> (usize, usize) {
        (self.statistics.hits as usize, self.statistics.misses as usize)
    }

    /// Получает подробную статистику кэша
    pub fn detailed_statistics(&mut self) -> CacheStatistics {
        self.update_statistics();
        self.statistics.clone()
    }

    /// Включает или отключает кэш
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
        if !enabled {
            self.clear();
        }
    }

    /// Проверяет, включен ли кэш
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Обновляет настройки кэша
    pub fn update_settings(&mut self, settings: CacheSettings) {
        self.settings = settings;
    }

    /// Получает текущие настройки кэша
    pub fn settings(&self) -> &CacheSettings {
        &self.settings
    }

    // Вспомогательные методы

    fn evict_entries(&mut self) {
        let evict_count = self.entries.len() / 4; // Удаляем 25% записей
        
        match self.settings.eviction_strategy {
            EvictionStrategy::LRU => self.evict_lru(evict_count),
            EvictionStrategy::LFU => self.evict_lfu(evict_count),
            EvictionStrategy::FIFO => self.evict_fifo(evict_count),
            EvictionStrategy::TTL => self.evict_expired(),
        }
    }

    fn evict_lru(&mut self, count: usize) {
        let mut entries: Vec<_> = self.entries.iter().map(|(k, v)| (k.clone(), v.last_accessed)).collect();
        entries.sort_by_key(|(_, last_accessed)| *last_accessed);
        
        for (key, _) in entries.into_iter().take(count) {
            self.entries.remove(&key);
        }
    }

    fn evict_lfu(&mut self, count: usize) {
        let mut entries: Vec<_> = self.entries.iter().map(|(k, v)| (k.clone(), v.access_count)).collect();
        entries.sort_by_key(|(_, access_count)| *access_count);
        
        for (key, _) in entries.into_iter().take(count) {
            self.entries.remove(&key);
        }
    }

    fn evict_fifo(&mut self, count: usize) {
        let mut entries: Vec<_> = self.entries.iter().map(|(k, v)| (k.clone(), v.created_at)).collect();
        entries.sort_by_key(|(_, created_at)| *created_at);
        
        for (key, _) in entries.into_iter().take(count) {
            self.entries.remove(&key);
        }
    }

    fn evict_expired(&mut self) {
        let expired_keys: Vec<_> = self.entries
            .iter()
            .filter(|(_, entry)| entry.is_expired())
            .map(|(key, _)| key.clone())
            .collect();
        
        for key in expired_keys {
            self.entries.remove(&key);
        }
    }

    fn update_statistics(&mut self) {
        self.statistics.total_entries = self.entries.len();
        self.statistics.expired_entries = self.entries
            .values()
            .filter(|entry| entry.is_expired())
            .count();
        
        // Примерная оценка размера (очень грубая)
        self.statistics.estimated_size_bytes = self.entries.len() * 256; // ~256 байт на запись
    }

    // Удобные методы для работы с конкретными типами данных

    /// Кэширует информацию о таблице
    pub fn cache_table_info(&mut self, table_name: &str, columns: Vec<ColumnInfo>, indexes: Vec<String>, exists: bool) {
        let key = format!("table:{}", table_name);
        let data = CacheData::TableInfo {
            name: table_name.to_string(),
            columns,
            indexes,
            exists,
        };
        self.put(key, data);
    }

    /// Получает информацию о таблице из кэша
    pub fn get_table_info(&mut self, table_name: &str) -> Option<(Vec<ColumnInfo>, Vec<String>, bool)> {
        let key = format!("table:{}", table_name);
        if let Some(CacheData::TableInfo { columns, indexes, exists, .. }) = self.get(&key) {
            Some((columns, indexes, exists))
        } else {
            None
        }
    }

    /// Кэширует информацию о колонке
    pub fn cache_column_info(&mut self, table_name: &str, column_name: &str, data_type: DataType, is_nullable: bool, is_primary_key: bool, exists: bool) {
        let key = format!("column:{}:{}", table_name, column_name);
        let data = CacheData::ColumnInfo {
            table_name: table_name.to_string(),
            column_name: column_name.to_string(),
            data_type,
            is_nullable,
            is_primary_key,
            exists,
        };
        self.put(key, data);
    }

    /// Получает информацию о колонке из кэша
    pub fn get_column_info(&mut self, table_name: &str, column_name: &str) -> Option<(DataType, bool, bool, bool)> {
        let key = format!("column:{}:{}", table_name, column_name);
        if let Some(CacheData::ColumnInfo { data_type, is_nullable, is_primary_key, exists, .. }) = self.get(&key) {
            Some((data_type, is_nullable, is_primary_key, exists))
        } else {
            None
        }
    }

    /// Кэширует результат проверки типов
    pub fn cache_type_check(&mut self, expression: &str, result_type: DataType, is_valid: bool) {
        let key = format!("type_check:{}", expression);
        let data = CacheData::TypeCheckResult {
            expression: expression.to_string(),
            result_type,
            is_valid,
        };
        self.put(key, data);
    }

    /// Получает результат проверки типов из кэша
    pub fn get_type_check(&mut self, expression: &str) -> Option<(DataType, bool)> {
        let key = format!("type_check:{}", expression);
        if let Some(CacheData::TypeCheckResult { result_type, is_valid, .. }) = self.get(&key) {
            Some((result_type, is_valid))
        } else {
            None
        }
    }
}

impl Default for MetadataCache {
    fn default() -> Self {
        Self::new(true)
    }
}

// Вспомогательная функция для получения текущего времени
fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_entry_creation() {
        let data = CacheData::TableInfo {
            name: "test_table".to_string(),
            columns: Vec::new(),
            indexes: Vec::new(),
            exists: true,
        };
        
        let entry = CacheEntry::new(data);
        assert_eq!(entry.access_count, 0);
        assert!(!entry.is_expired());
    }

    #[test]
    fn test_cache_entry_with_ttl() {
        let data = CacheData::TableInfo {
            name: "test_table".to_string(),
            columns: Vec::new(),
            indexes: Vec::new(),
            exists: true,
        };
        
        let entry = CacheEntry::new(data).with_ttl(Duration::from_millis(1));
        
        // Ждем истечения TTL
        std::thread::sleep(Duration::from_millis(2));
        assert!(entry.is_expired());
    }

    #[test]
    fn test_metadata_cache_basic_operations() {
        let mut cache = MetadataCache::new(true);
        
        let data = CacheData::TableInfo {
            name: "users".to_string(),
            columns: Vec::new(),
            indexes: Vec::new(),
            exists: true,
        };
        
        // Добавляем запись
        cache.put("test_key".to_string(), data);
        assert!(cache.contains("test_key"));
        
        // Получаем запись
        let retrieved = cache.get("test_key");
        assert!(retrieved.is_some());
        
        // Удаляем запись
        assert!(cache.remove("test_key"));
        assert!(!cache.contains("test_key"));
    }

    #[test]
    fn test_cache_statistics() {
        let mut cache = MetadataCache::new(true);
        
        let data = CacheData::TableInfo {
            name: "users".to_string(),
            columns: Vec::new(),
            indexes: Vec::new(),
            exists: true,
        };
        
        // Добавляем запись
        cache.put("test_key".to_string(), data);
        
        // Попадание в кэш
        cache.get("test_key");
        
        // Промах кэша
        cache.get("nonexistent_key");
        
        let (hits, misses) = cache.statistics();
        assert_eq!(hits, 1);
        assert_eq!(misses, 1);
    }

    #[test]
    fn test_disabled_cache() {
        let mut cache = MetadataCache::new(false);
        
        let data = CacheData::TableInfo {
            name: "users".to_string(),
            columns: Vec::new(),
            indexes: Vec::new(),
            exists: true,
        };
        
        // Попытка добавить в отключенный кэш
        cache.put("test_key".to_string(), data);
        
        // Запись не должна быть добавлена
        assert!(!cache.contains("test_key"));
        assert!(cache.get("test_key").is_none());
    }

    #[test]
    fn test_cache_cleanup() {
        let settings = CacheSettings {
            max_entries: 100,
            eviction_strategy: EvictionStrategy::TTL,
            default_ttl: Some(Duration::from_millis(1)),
            cleanup_interval: Duration::from_millis(1),
            enable_persistence: false,
        };
        
        let mut cache = MetadataCache::with_settings(true, settings);
        
        let data = CacheData::TableInfo {
            name: "users".to_string(),
            columns: Vec::new(),
            indexes: Vec::new(),
            exists: true,
        };
        
        // Добавляем запись
        cache.put("test_key".to_string(), data);
        
        // Ждем истечения TTL
        std::thread::sleep(Duration::from_millis(2));
        
        // Выполняем очистку
        cache.cleanup();
        
        // Запись должна быть удалена
        assert!(!cache.contains("test_key"));
    }

    #[test]
    fn test_convenience_methods() {
        let mut cache = MetadataCache::new(true);
        
        // Кэшируем информацию о таблице
        cache.cache_table_info("users", Vec::new(), Vec::new(), true);
        
        // Получаем информацию о таблице
        let table_info = cache.get_table_info("users");
        assert!(table_info.is_some());
        
        let (columns, indexes, exists) = table_info.unwrap();
        assert!(exists);
        assert!(columns.is_empty());
        assert!(indexes.is_empty());
    }
}
