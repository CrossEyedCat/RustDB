//! Тесты для кэша метаданных

use crate::analyzer::MetadataCache;
use crate::analyzer::metadata_cache::{CacheData, CacheSettings, EvictionStrategy, ColumnInfo};
use crate::parser::ast::DataType;
use std::time::Duration;
// use crate::common::Result; // Not used in these tests

#[test]
fn test_cache_creation() {
    let cache = MetadataCache::new(true);
    assert!(cache.is_enabled());
    
    let disabled_cache = MetadataCache::new(false);
    assert!(!disabled_cache.is_enabled());
}

#[test]
fn test_cache_basic_operations() {
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
fn test_cache_ttl() {
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

#[test]
fn test_column_info_caching() {
    let mut cache = MetadataCache::new(true);
    
    // Кэшируем информацию о колонке
    cache.cache_column_info("users", "name", DataType::Text, false, false, true);
    
    // Получаем информацию о колонке
    let column_info = cache.get_column_info("users", "name");
    assert!(column_info.is_some());
    
    let (data_type, is_nullable, is_primary_key, exists) = column_info.unwrap();
    assert_eq!(data_type, DataType::Text);
    assert!(!is_nullable);
    assert!(!is_primary_key);
    assert!(exists);
}

#[test]
fn test_type_check_caching() {
    let mut cache = MetadataCache::new(true);
    
    // Кэшируем результат проверки типов
    cache.cache_type_check("1 + 2", DataType::Integer, true);
    
    // Получаем результат проверки типов
    let type_check = cache.get_type_check("1 + 2");
    assert!(type_check.is_some());
    
    let (result_type, is_valid) = type_check.unwrap();
    assert_eq!(result_type, DataType::Integer);
    assert!(is_valid);
}

#[test]
fn test_cache_clear() {
    let mut cache = MetadataCache::new(true);
    
    // Добавляем несколько записей
    let data = CacheData::TableInfo {
        name: "users".to_string(),
        columns: Vec::new(),
        indexes: Vec::new(),
        exists: true,
    };
    
    cache.put("key1".to_string(), data.clone());
    cache.put("key2".to_string(), data);
    
    // Проверяем, что записи есть
    assert!(cache.contains("key1"));
    assert!(cache.contains("key2"));
    
    // Очищаем кэш
    cache.clear();
    
    // Проверяем, что кэш пуст
    assert!(!cache.contains("key1"));
    assert!(!cache.contains("key2"));
}

#[test]
fn test_cache_enable_disable() {
    let mut cache = MetadataCache::new(true);
    
    let data = CacheData::TableInfo {
        name: "users".to_string(),
        columns: Vec::new(),
        indexes: Vec::new(),
        exists: true,
    };
    
    // Добавляем запись в включенный кэш
    cache.put("test_key".to_string(), data.clone());
    assert!(cache.contains("test_key"));
    
    // Отключаем кэш
    cache.set_enabled(false);
    assert!(!cache.is_enabled());
    
    // Попытка добавить в отключенный кэш
    cache.put("test_key2".to_string(), data);
    assert!(!cache.contains("test_key2"));
    
    // Включаем кэш обратно
    cache.set_enabled(true);
    assert!(cache.is_enabled());
}
