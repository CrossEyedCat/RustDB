//! Module for caching semantic analyzer metadata

// use crate::common::{Error, Result}; // Not used in this simplified version
use crate::parser::ast::DataType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Metadata cache entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntry {
    /// Entry data
    pub data: CacheData,
    /// Entry creation time
    pub created_at: u64, // Unix timestamp in milliseconds
    /// Last access time
    pub last_accessed: u64,
    /// Number of accesses to entry
    pub access_count: u64,
    /// Entry time to live (TTL) in milliseconds
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

/// Data stored in cache
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CacheData {
    /// Table information
    TableInfo {
        name: String,
        columns: Vec<ColumnInfo>,
        indexes: Vec<String>,
        exists: bool,
    },
    /// Column information
    ColumnInfo {
        table_name: String,
        column_name: String,
        data_type: DataType,
        is_nullable: bool,
        is_primary_key: bool,
        exists: bool,
    },
    /// Index information
    IndexInfo {
        name: String,
        table_name: String,
        columns: Vec<String>,
        is_unique: bool,
        exists: bool,
    },
    /// Type check result
    TypeCheckResult {
        expression: String,
        result_type: DataType,
        is_valid: bool,
    },
    /// Access check result
    AccessCheckResult {
        object_name: String,
        username: String,
        permission: String,
        allowed: bool,
    },
}

/// Column information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: DataType,
    pub is_nullable: bool,
    pub is_primary_key: bool,
    pub default_value: Option<String>,
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStatistics {
    /// Total number of entries
    pub total_entries: usize,
    /// Number of cache hits
    pub hits: u64,
    /// Number of cache misses
    pub misses: u64,
    /// Number of expired entries
    pub expired_entries: usize,
    /// Cache size in bytes (approximate)
    pub estimated_size_bytes: usize,
    /// Time of last cleanup
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

/// Cache eviction strategy
#[derive(Debug, Clone)]
pub enum EvictionStrategy {
    /// Least Recently Used - evict least recently used
    LRU,
    /// Least Frequently Used - evict least frequently used
    LFU,
    /// First In First Out - evict oldest
    FIFO,
    /// Time To Live - evict expired
    TTL,
}

/// Cache settings
#[derive(Debug, Clone)]
pub struct CacheSettings {
    /// Maximum number of entries in cache
    pub max_entries: usize,
    /// Eviction strategy
    pub eviction_strategy: EvictionStrategy,
    /// Default TTL for entries
    pub default_ttl: Option<Duration>,
    /// Automatic cleanup interval
    pub cleanup_interval: Duration,
    /// Whether cache serialization is enabled
    pub enable_persistence: bool,
}

impl Default for CacheSettings {
    fn default() -> Self {
        Self {
            max_entries: 1000,
            eviction_strategy: EvictionStrategy::LRU,
            default_ttl: Some(Duration::from_secs(3600)), // 1 hour
            cleanup_interval: Duration::from_secs(300),   // 5 minutes
            enable_persistence: false,
        }
    }
}

/// Metadata cache
pub struct MetadataCache {
    /// Cache entries storage
    entries: HashMap<String, CacheEntry>,
    /// Cache settings
    settings: CacheSettings,
    /// Cache statistics
    statistics: CacheStatistics,
    /// Whether cache is enabled
    enabled: bool,
    /// Time of last cleanup
    last_cleanup: Instant,
}

impl MetadataCache {
    /// Create new metadata cache
    pub fn new(enabled: bool) -> Self {
        Self {
            entries: HashMap::new(),
            settings: CacheSettings::default(),
            statistics: CacheStatistics::new(),
            enabled,
            last_cleanup: Instant::now(),
        }
    }

    /// Create cache with settings
    pub fn with_settings(enabled: bool, settings: CacheSettings) -> Self {
        Self {
            entries: HashMap::new(),
            settings,
            statistics: CacheStatistics::new(),
            enabled,
            last_cleanup: Instant::now(),
        }
    }

    /// Get entry from cache
    pub fn get(&mut self, key: &str) -> Option<CacheData> {
        if !self.enabled {
            return None;
        }

        // First check if entry has expired
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
            // Update access statistics
            entry.access();
            self.statistics.hits += 1;
            Some(entry.data.clone())
        } else {
            self.statistics.misses += 1;
            None
        }
    }

    /// Add entry to cache
    pub fn put(&mut self, key: String, data: CacheData) {
        if !self.enabled {
            return;
        }

        let mut entry = CacheEntry::new(data);

        // Apply default TTL
        if let Some(default_ttl) = self.settings.default_ttl {
            entry = entry.with_ttl(default_ttl);
        }

        // Check if we need to free space
        if self.entries.len() >= self.settings.max_entries {
            self.evict_entries();
        }

        self.entries.insert(key, entry);
        self.update_statistics();
    }

    /// Add entry with custom TTL
    pub fn put_with_ttl(&mut self, key: String, data: CacheData, ttl: Duration) {
        if !self.enabled {
            return;
        }

        let entry = CacheEntry::new(data).with_ttl(ttl);

        // Check if we need to free space
        if self.entries.len() >= self.settings.max_entries {
            self.evict_entries();
        }

        self.entries.insert(key, entry);
        self.update_statistics();
    }

    /// Remove entry from cache
    pub fn remove(&mut self, key: &str) -> bool {
        if self.entries.remove(key).is_some() {
            self.update_statistics();
            true
        } else {
            false
        }
    }

    /// Check if entry exists in cache
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

    /// Clear entire cache
    pub fn clear(&mut self) {
        self.entries.clear();
        self.statistics = CacheStatistics::new();
    }

    /// Cleanup expired entries
    pub fn cleanup(&mut self) {
        let now = Instant::now();

        // Check if cleanup is needed
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

    /// Get cache statistics
    pub fn statistics(&self) -> (usize, usize) {
        (
            self.statistics.hits as usize,
            self.statistics.misses as usize,
        )
    }

    /// Get detailed cache statistics
    pub fn detailed_statistics(&mut self) -> CacheStatistics {
        self.update_statistics();
        self.statistics.clone()
    }

    /// Enable or disable cache
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
        if !enabled {
            self.clear();
        }
    }

    /// Check if cache is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Update cache settings
    pub fn update_settings(&mut self, settings: CacheSettings) {
        self.settings = settings;
    }

    /// Get current cache settings
    pub fn settings(&self) -> &CacheSettings {
        &self.settings
    }

    // Helper methods

    fn evict_entries(&mut self) {
        let evict_count = self.entries.len() / 4; // Remove 25% of entries

        match self.settings.eviction_strategy {
            EvictionStrategy::LRU => self.evict_lru(evict_count),
            EvictionStrategy::LFU => self.evict_lfu(evict_count),
            EvictionStrategy::FIFO => self.evict_fifo(evict_count),
            EvictionStrategy::TTL => self.evict_expired(),
        }
    }

    fn evict_lru(&mut self, count: usize) {
        let mut entries: Vec<_> = self
            .entries
            .iter()
            .map(|(k, v)| (k.clone(), v.last_accessed))
            .collect();
        entries.sort_by_key(|(_, last_accessed)| *last_accessed);

        for (key, _) in entries.into_iter().take(count) {
            self.entries.remove(&key);
        }
    }

    fn evict_lfu(&mut self, count: usize) {
        let mut entries: Vec<_> = self
            .entries
            .iter()
            .map(|(k, v)| (k.clone(), v.access_count))
            .collect();
        entries.sort_by_key(|(_, access_count)| *access_count);

        for (key, _) in entries.into_iter().take(count) {
            self.entries.remove(&key);
        }
    }

    fn evict_fifo(&mut self, count: usize) {
        let mut entries: Vec<_> = self
            .entries
            .iter()
            .map(|(k, v)| (k.clone(), v.created_at))
            .collect();
        entries.sort_by_key(|(_, created_at)| *created_at);

        for (key, _) in entries.into_iter().take(count) {
            self.entries.remove(&key);
        }
    }

    fn evict_expired(&mut self) {
        let expired_keys: Vec<_> = self
            .entries
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
        self.statistics.expired_entries = self
            .entries
            .values()
            .filter(|entry| entry.is_expired())
            .count();

        // Approximate size estimate (very rough)
        self.statistics.estimated_size_bytes = self.entries.len() * 256; // ~256 bytes per entry
    }

    // Convenience methods for working with specific data types

    /// Cache table information
    pub fn cache_table_info(
        &mut self,
        table_name: &str,
        columns: Vec<ColumnInfo>,
        indexes: Vec<String>,
        exists: bool,
    ) {
        let key = format!("table:{}", table_name);
        let data = CacheData::TableInfo {
            name: table_name.to_string(),
            columns,
            indexes,
            exists,
        };
        self.put(key, data);
    }

    /// Get table information from cache
    pub fn get_table_info(
        &mut self,
        table_name: &str,
    ) -> Option<(Vec<ColumnInfo>, Vec<String>, bool)> {
        let key = format!("table:{}", table_name);
        if let Some(CacheData::TableInfo {
            columns,
            indexes,
            exists,
            ..
        }) = self.get(&key)
        {
            Some((columns, indexes, exists))
        } else {
            None
        }
    }

    /// Cache column information
    pub fn cache_column_info(
        &mut self,
        table_name: &str,
        column_name: &str,
        data_type: DataType,
        is_nullable: bool,
        is_primary_key: bool,
        exists: bool,
    ) {
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

    /// Get column information from cache
    pub fn get_column_info(
        &mut self,
        table_name: &str,
        column_name: &str,
    ) -> Option<(DataType, bool, bool, bool)> {
        let key = format!("column:{}:{}", table_name, column_name);
        if let Some(CacheData::ColumnInfo {
            data_type,
            is_nullable,
            is_primary_key,
            exists,
            ..
        }) = self.get(&key)
        {
            Some((data_type, is_nullable, is_primary_key, exists))
        } else {
            None
        }
    }

    /// Cache type check result
    pub fn cache_type_check(&mut self, expression: &str, result_type: DataType, is_valid: bool) {
        let key = format!("type_check:{}", expression);
        let data = CacheData::TypeCheckResult {
            expression: expression.to_string(),
            result_type,
            is_valid,
        };
        self.put(key, data);
    }

    /// Get type check result from cache
    pub fn get_type_check(&mut self, expression: &str) -> Option<(DataType, bool)> {
        let key = format!("type_check:{}", expression);
        if let Some(CacheData::TypeCheckResult {
            result_type,
            is_valid,
            ..
        }) = self.get(&key)
        {
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

// Helper function to get current time (milliseconds).
// Under Miri with isolation, `SystemTime`/`REALTIME` is unavailable; monotonic elapsed time
// preserves TTL and cleanup behavior because only deltas matter here.
fn current_timestamp_ms() -> u64 {
    #[cfg(miri)]
    {
        static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
        START.get_or_init(Instant::now).elapsed().as_millis() as u64
    }
    #[cfg(not(miri))]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
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

        // Wait for TTL expiration
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

        // Add entry
        cache.put("test_key".to_string(), data);
        assert!(cache.contains("test_key"));

        // Get entry
        let retrieved = cache.get("test_key");
        assert!(retrieved.is_some());

        // Remove entry
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

        // Add entry
        cache.put("test_key".to_string(), data);

        // Cache hit
        cache.get("test_key");

        // Cache miss
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

        // Try to add to disabled cache
        cache.put("test_key".to_string(), data);

        // Entry should not be added
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

        // Add entry
        cache.put("test_key".to_string(), data);

        // Wait for TTL expiration
        std::thread::sleep(Duration::from_millis(2));

        // Perform cleanup
        cache.cleanup();

        // Entry should be removed
        assert!(!cache.contains("test_key"));
    }

    #[test]
    fn test_convenience_methods() {
        let mut cache = MetadataCache::new(true);

        // Cache table information
        cache.cache_table_info("users", Vec::new(), Vec::new(), true);

        // Get table information
        let table_info = cache.get_table_info("users");
        assert!(table_info.is_some());

        let (columns, indexes, exists) = table_info.unwrap();
        assert!(exists);
        assert!(columns.is_empty());
        assert!(indexes.is_empty());
    }
}
