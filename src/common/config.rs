//! Конфигурация для rustdb
//!
//! Предоставляет структуры конфигурации для различных компонентов системы

use crate::common::i18n::Language;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Основная конфигурация базы данных
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Имя базы данных
    pub name: String,
    /// Директория для хранения данных
    pub data_directory: String,
    /// Максимальное количество подключений
    pub max_connections: usize,
    /// Таймаут подключения (в секундах)
    pub connection_timeout: u64,
    /// Таймаут запроса (в секундах)
    pub query_timeout: u64,
    /// Язык интерфейса
    pub language: Language,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            name: "rustdb".to_string(),
            data_directory: "./data".to_string(),
            max_connections: 100,
            connection_timeout: 30,
            query_timeout: 60,
            language: Language::English,
        }
    }
}

/// Конфигурация хранения
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Размер страницы в байтах
    pub page_size: usize,
    /// Размер пула буферов
    pub buffer_pool_size: usize,
    /// Интервал создания контрольных точек
    pub checkpoint_interval: Duration,
    /// Включить WAL
    pub wal_enabled: bool,
    /// Включить сжатие
    pub compression_enabled: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            page_size: 8192,
            buffer_pool_size: 1000,
            checkpoint_interval: Duration::from_secs(300),
            wal_enabled: true,
            compression_enabled: false,
        }
    }
}

/// Конфигурация логирования
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Уровень логирования
    pub level: String,
    /// Файл лога
    pub file: PathBuf,
    /// Максимальный размер файла лога
    pub max_file_size: String,
    /// Максимальное количество файлов логов
    pub max_files: usize,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            file: PathBuf::from("./logs/rustdb.log"),
            max_file_size: "100MB".to_string(),
            max_files: 10,
        }
    }
}

/// Конфигурация сети
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// Хост для прослушивания
    pub host: String,
    /// Порт для прослушивания
    pub port: u16,
    /// Максимальное количество подключений
    pub max_connections: usize,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 8080,
            max_connections: 1000,
        }
    }
}

/// Конфигурация производительности
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Таймаут блокировки
    pub lock_timeout: Duration,
    /// Интервал обнаружения взаимоблокировок
    pub deadlock_detection_interval: Duration,
    /// Максимальный размер кэша планов запросов
    pub max_query_plan_cache_size: usize,
    /// Включить оптимизацию запросов
    pub enable_query_optimization: bool,
    /// Включить параллельное выполнение
    pub enable_parallel_execution: bool,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            lock_timeout: Duration::from_secs(10),
            deadlock_detection_interval: Duration::from_millis(1000),
            max_query_plan_cache_size: 1000,
            enable_query_optimization: true,
            enable_parallel_execution: true,
        }
    }
}

impl DatabaseConfig {
    /// Загружает конфигурацию из TOML файла
    pub fn from_file(path: &PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: DatabaseConfig = toml::from_str(&content)?;
        Ok(config)
    }

    /// Сохраняет конфигурацию в TOML файл
    pub fn to_file(&self, path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
        let content = toml::to_string_pretty(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    /// Загружает конфигурацию из переменных окружения
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let mut config = DatabaseConfig::default();

        if let Ok(name) = std::env::var("RUSTDB_NAME") {
            config.name = name;
        }

        if let Ok(data_dir) = std::env::var("RUSTDB_DATA_DIR") {
            config.data_directory = data_dir;
        }

        if let Ok(max_conn) = std::env::var("RUSTDB_MAX_CONNECTIONS") {
            config.max_connections = max_conn.parse()?;
        }

        if let Ok(lang) = std::env::var("RUSTDB_LANGUAGE") {
            config.language = lang.parse()?;
        }

        // if let Ok(host) = std::env::var("RUSTDB_HOST") {
        //     config.network.host = host;
        // }

        // if let Ok(port) = std::env::var("RUSTDB_PORT") {
        //     config.network.port = port.parse()?;
        // }

        // if let Ok(log_level) = std::env::var("RUSTDB_LOG_LEVEL") {
        //     config.logging.level = log_level;
        // }

        Ok(config)
    }

    /// Объединяет конфигурацию с другой
    pub fn merge(mut self, other: Self) -> Self {
        if other.name != "rustdb" {
            self.name = other.name;
        }
        if other.data_directory != "./data" {
            self.data_directory = other.data_directory;
        }
        if other.max_connections != 100 {
            self.max_connections = other.max_connections;
        }
        if other.connection_timeout != 30 {
            self.connection_timeout = other.connection_timeout;
        }
        if other.query_timeout != 60 {
            self.query_timeout = other.query_timeout;
        }
        if other.language != Language::English {
            self.language = other.language;
        }

        // Merge nested configs
        // self.storage = self.storage.merge(other.storage);
        // self.logging = self.logging.merge(other.logging);
        // self.network = self.network.merge(other.network);
        // self.performance = self.performance.merge(other.performance);

        self
    }

    /// Валидирует конфигурацию
    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() {
            return Err("Database name cannot be empty".to_string());
        }

        if self.max_connections == 0 {
            return Err("Max connections must be greater than 0".to_string());
        }

        // if self.storage.page_size == 0 {
        //     return Err("Page size must be greater than 0".to_string());
        // }

        // if self.storage.buffer_pool_size == 0 {
        //     return Err("Buffer pool size must be greater than 0".to_string());
        // }

        // if self.network.port == 0 {
        //     return Err("Port must be greater than 0".to_string());
        // }

        // if self.network.max_connections == 0 {
        //     return Err("Network max connections must be greater than 0".to_string());
        // }

        Ok(())
    }
}

impl StorageConfig {
    fn merge(mut self, other: Self) -> Self {
        if other.page_size != 8192 {
            self.page_size = other.page_size;
        }
        if other.buffer_pool_size != 1000 {
            self.buffer_pool_size = other.buffer_pool_size;
        }
        if other.checkpoint_interval != Duration::from_secs(300) {
            self.checkpoint_interval = other.checkpoint_interval;
        }
        if other.wal_enabled != true {
            self.wal_enabled = other.wal_enabled;
        }
        if other.compression_enabled != false {
            self.compression_enabled = other.compression_enabled;
        }
        self
    }
}

impl LoggingConfig {
    fn merge(mut self, other: Self) -> Self {
        if other.level != "info" {
            self.level = other.level;
        }
        if other.file != PathBuf::from("./logs/rustdb.log") {
            self.file = other.file;
        }
        if other.max_file_size != "100MB" {
            self.max_file_size = other.max_file_size;
        }
        if other.max_files != 10 {
            self.max_files = other.max_files;
        }
        self
    }
}

impl NetworkConfig {
    fn merge(mut self, other: Self) -> Self {
        if other.host != "0.0.0.0" {
            self.host = other.host;
        }
        if other.port != 8080 {
            self.port = other.port;
        }
        if other.max_connections != 1000 {
            self.max_connections = other.max_connections;
        }
        self
    }
}

impl PerformanceConfig {
    fn merge(mut self, other: Self) -> Self {
        if other.lock_timeout != Duration::from_secs(10) {
            self.lock_timeout = other.lock_timeout;
        }
        if other.deadlock_detection_interval != Duration::from_millis(1000) {
            self.deadlock_detection_interval = other.deadlock_detection_interval;
        }
        if other.max_query_plan_cache_size != 1000 {
            self.max_query_plan_cache_size = other.max_query_plan_cache_size;
        }
        if other.enable_query_optimization != true {
            self.enable_query_optimization = other.enable_query_optimization;
        }
        if other.enable_parallel_execution != true {
            self.enable_parallel_execution = other.enable_parallel_execution;
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = DatabaseConfig::default();
        assert_eq!(config.name, "rustdb");
        assert_eq!(config.max_connections, 100);
        assert_eq!(config.language, Language::English);
        assert_eq!(config.connection_timeout, 30);
    }

    #[test]
    fn test_config_validation() {
        let mut config = DatabaseConfig::default();
        assert!(config.validate().is_ok());

        config.name = String::new();
        assert!(config.validate().is_err());

        config = DatabaseConfig::default();
        config.max_connections = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_merge() {
        let mut config1 = DatabaseConfig::default();
        let mut config2 = DatabaseConfig::default();

        config2.name = "testdb".to_string();
        config2.connection_timeout = 60;
        config2.language = Language::Russian;

        let merged = config1.merge(config2);
        assert_eq!(merged.name, "testdb");
        assert_eq!(merged.connection_timeout, 60);
        assert_eq!(merged.language, Language::Russian);
    }
}
