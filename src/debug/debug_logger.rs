//! Детальный логгер отладки для rustdb
//!
//! Предоставляет расширенное логирование операций с различными уровнями детализации

use crate::debug::DebugConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;

/// Уровень детализации логирования
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LogLevel {
    /// Только критические ошибки
    Critical = 0,
    /// Ошибки
    Error = 1,
    /// Предупреждения
    Warning = 2,
    /// Информационные сообщения
    Info = 3,
    /// Отладочная информация
    Debug = 4,
    /// Максимальная детализация
    Trace = 5,
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogLevel::Critical => write!(f, "CRITICAL"),
            LogLevel::Error => write!(f, "ERROR"),
            LogLevel::Warning => write!(f, "WARNING"),
            LogLevel::Info => write!(f, "INFO"),
            LogLevel::Debug => write!(f, "DEBUG"),
            LogLevel::Trace => write!(f, "TRACE"),
        }
    }
}

/// Категория лог-записи
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogCategory {
    /// Операции с транзакциями
    Transaction,
    /// Операции с данными
    Data,
    /// Операции с индексами
    Index,
    /// Операции с буферами
    Buffer,
    /// Операции с файлами
    File,
    /// Операции с сетью
    Network,
    /// Парсинг и планирование
    Query,
    /// Системные операции
    System,
    /// Операции восстановления
    Recovery,
    /// Операции логирования
    Logging,
}

impl std::fmt::Display for LogCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogCategory::Transaction => write!(f, "TX"),
            LogCategory::Data => write!(f, "DATA"),
            LogCategory::Index => write!(f, "INDEX"),
            LogCategory::Buffer => write!(f, "BUFFER"),
            LogCategory::File => write!(f, "FILE"),
            LogCategory::Network => write!(f, "NET"),
            LogCategory::Query => write!(f, "QUERY"),
            LogCategory::System => write!(f, "SYSTEM"),
            LogCategory::Recovery => write!(f, "RECOVERY"),
            LogCategory::Logging => write!(f, "LOG"),
        }
    }
}

/// Структура лог-записи
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebugLogEntry {
    /// Временная метка (микросекунды с эпохи Unix)
    pub timestamp: u64,
    /// Уровень логирования
    pub level: LogLevel,
    /// Категория
    pub category: LogCategory,
    /// Компонент системы
    pub component: String,
    /// Сообщение
    pub message: String,
    /// Дополнительные данные (JSON)
    pub data: Option<serde_json::Value>,
    /// ID потока
    pub thread_id: u64,
    /// ID транзакции (если применимо)
    pub transaction_id: Option<u64>,
    /// ID запроса (если применимо)
    pub query_id: Option<String>,
    /// Время выполнения операции (микросекунды)
    pub duration_us: Option<u64>,
    /// Размер данных (байты)
    pub data_size: Option<u64>,
}

impl DebugLogEntry {
    /// Создает новую лог-запись
    pub fn new(level: LogLevel, category: LogCategory, component: &str, message: &str) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        let thread_id = 0; // Временное решение для совместимости

        Self {
            timestamp,
            level,
            category,
            component: component.to_string(),
            message: message.to_string(),
            data: None,
            thread_id,
            transaction_id: None,
            query_id: None,
            duration_us: None,
            data_size: None,
        }
    }

    /// Добавляет дополнительные данные
    pub fn with_data(mut self, data: serde_json::Value) -> Self {
        self.data = Some(data);
        self
    }

    /// Добавляет ID транзакции
    pub fn with_transaction_id(mut self, tx_id: u64) -> Self {
        self.transaction_id = Some(tx_id);
        self
    }

    /// Добавляет ID запроса
    pub fn with_query_id(mut self, query_id: &str) -> Self {
        self.query_id = Some(query_id.to_string());
        self
    }

    /// Добавляет время выполнения
    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.duration_us = Some(duration.as_micros() as u64);
        self
    }

    /// Добавляет размер данных
    pub fn with_data_size(mut self, size: u64) -> Self {
        self.data_size = Some(size);
        self
    }

    /// Форматирует запись для вывода
    pub fn format(&self) -> String {
        let mut formatted = String::new();

        // Временная метка
        let datetime = SystemTime::UNIX_EPOCH + Duration::from_micros(self.timestamp);
        let datetime_str = format!("{:?}", datetime);
        formatted.push_str(&format!("[{}] ", datetime_str));

        // Уровень и категория
        formatted.push_str(&format!("{}:{} ", self.level, self.category));

        // Компонент
        formatted.push_str(&format!("[{}] ", self.component));

        // ID транзакции и запроса
        if let Some(tx_id) = self.transaction_id {
            formatted.push_str(&format!("TX:{} ", tx_id));
        }
        if let Some(query_id) = &self.query_id {
            formatted.push_str(&format!("Q:{} ", query_id));
        }

        // Время выполнения
        if let Some(duration) = self.duration_us {
            formatted.push_str(&format!("({}μs) ", duration));
        }

        // Размер данных
        if let Some(size) = self.data_size {
            formatted.push_str(&format!("[{}B] ", size));
        }

        // Сообщение
        formatted.push_str(&self.message);

        // Дополнительные данные
        if let Some(data) = &self.data {
            formatted.push_str(&format!(" | Data: {}", data));
        }

        formatted
    }
}

/// Статистика логирования
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LoggingStats {
    /// Общее количество записей
    pub total_entries: u64,
    /// Записи по уровням
    pub entries_by_level: HashMap<String, u64>,
    /// Записи по категориям
    pub entries_by_category: HashMap<String, u64>,
    /// Записи по компонентам
    pub entries_by_component: HashMap<String, u64>,
    /// Время последней записи
    pub last_entry_time: u64,
    /// Размер лог-файла (байты)
    pub log_file_size: u64,
    /// Количество ошибок записи
    pub write_errors: u64,
}

/// Детальный логгер отладки
pub struct DebugLogger {
    config: DebugConfig,
    log_file: Arc<Mutex<Option<BufWriter<File>>>>,
    stats: Arc<RwLock<LoggingStats>>,
    background_handle: Option<JoinHandle<()>>,
    log_buffer: Arc<Mutex<Vec<DebugLogEntry>>>,
    buffer_size: usize,
}

impl DebugLogger {
    /// Создает новый логгер отладки
    pub fn new(config: &DebugConfig) -> Self {
        let mut logger = Self {
            config: config.clone(),
            log_file: Arc::new(Mutex::new(None)),
            stats: Arc::new(RwLock::new(LoggingStats::default())),
            background_handle: None,
            log_buffer: Arc::new(Mutex::new(Vec::new())),
            buffer_size: 1000,
        };

        // Инициализируем лог-файл
        logger.initialize_log_file();

        // Запускаем фоновую задачу
        logger.start_background_task();

        logger
    }

    /// Инициализирует лог-файл
    fn initialize_log_file(&mut self) {
        let log_path = Path::new("debug.log");

        match OpenOptions::new().create(true).append(true).open(log_path) {
            Ok(file) => {
                let writer = BufWriter::new(file);
                *self.log_file.lock().unwrap() = Some(writer);
            }
            Err(e) => {
                eprintln!("Ошибка создания лог-файла: {}", e);
            }
        }
    }

    /// Запускает фоновую задачу для записи логов
    fn start_background_task(&mut self) {
        let log_file = self.log_file.clone();
        let stats = self.stats.clone();
        let log_buffer = self.log_buffer.clone();
        let buffer_size = self.buffer_size;

        self.background_handle = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));

            loop {
                interval.tick().await;

                // Получаем записи из буфера
                let entries = {
                    let mut buffer = log_buffer.lock().unwrap();
                    if buffer.len() >= buffer_size || !buffer.is_empty() {
                        let entries = buffer.drain(..).collect::<Vec<_>>();
                        entries
                    } else {
                        continue;
                    }
                };

                // Записываем в файл
                if let Some(writer) = log_file.lock().unwrap().as_mut() {
                    for entry in entries {
                        if let Err(e) = writeln!(writer, "{}", entry.format()) {
                            eprintln!("Ошибка записи в лог: {}", e);
                            let mut stats = stats.write().unwrap();
                            stats.write_errors += 1;
                        }
                    }

                    if let Err(e) = writer.flush() {
                        eprintln!("Ошибка сброса буфера лога: {}", e);
                    }
                }
            }
        }));
    }

    /// Логирует запись
    pub fn log(&self, entry: DebugLogEntry) {
        // Проверяем уровень детализации
        if entry.level as u8 > self.config.detail_level {
            return;
        }

        // Добавляем в буфер
        {
            let mut buffer = self.log_buffer.lock().unwrap();
            buffer.push(entry.clone());
        }

        // Обновляем статистику
        {
            let mut stats = self.stats.write().unwrap();
            stats.total_entries += 1;
            stats.last_entry_time = entry.timestamp;

            *stats
                .entries_by_level
                .entry(entry.level.to_string())
                .or_default() += 1;
            *stats
                .entries_by_category
                .entry(entry.category.to_string())
                .or_default() += 1;
            *stats
                .entries_by_component
                .entry(entry.component.clone())
                .or_default() += 1;
        }

        // Выводим в консоль для критических ошибок
        if entry.level == LogLevel::Critical || entry.level == LogLevel::Error {
            eprintln!("{}", entry.format());
        }
    }

    /// Создает запись с указанными параметрами
    pub fn create_entry(
        &self,
        level: LogLevel,
        category: LogCategory,
        component: &str,
        message: &str,
    ) -> DebugLogEntry {
        DebugLogEntry::new(level, category, component, message)
    }

    /// Логирует операцию с транзакцией
    pub fn log_transaction_operation(
        &self,
        level: LogLevel,
        operation: &str,
        transaction_id: u64,
        duration: Option<Duration>,
        data_size: Option<u64>,
    ) {
        let mut entry = self.create_entry(
            level,
            LogCategory::Transaction,
            "TransactionManager",
            &format!("Transaction {}: {}", transaction_id, operation),
        );

        entry = entry.with_transaction_id(transaction_id);

        if let Some(dur) = duration {
            entry = entry.with_duration(dur);
        }

        if let Some(size) = data_size {
            entry = entry.with_data_size(size);
        }

        self.log(entry);
    }

    /// Логирует операцию с данными
    pub fn log_data_operation(
        &self,
        level: LogLevel,
        operation: &str,
        table: &str,
        duration: Option<Duration>,
        data_size: Option<u64>,
    ) {
        let mut entry = self.create_entry(
            level,
            LogCategory::Data,
            "DataManager",
            &format!("Table '{}': {}", table, operation),
        );

        if let Some(dur) = duration {
            entry = entry.with_duration(dur);
        }

        if let Some(size) = data_size {
            entry = entry.with_data_size(size);
        }

        self.log(entry);
    }

    /// Логирует операцию с запросом
    pub fn log_query_operation(
        &self,
        level: LogLevel,
        operation: &str,
        query_id: &str,
        duration: Option<Duration>,
        data_size: Option<u64>,
    ) {
        let mut entry = self.create_entry(
            level,
            LogCategory::Query,
            "QueryEngine",
            &format!("Query {}: {}", query_id, operation),
        );

        entry = entry.with_query_id(query_id);

        if let Some(dur) = duration {
            entry = entry.with_duration(dur);
        }

        if let Some(size) = data_size {
            entry = entry.with_data_size(size);
        }

        self.log(entry);
    }

    /// Логирует системную операцию
    pub fn log_system_operation(
        &self,
        level: LogLevel,
        component: &str,
        operation: &str,
        duration: Option<Duration>,
    ) {
        let mut entry = self.create_entry(level, LogCategory::System, component, operation);

        if let Some(dur) = duration {
            entry = entry.with_duration(dur);
        }

        self.log(entry);
    }

    /// Получает статистику логирования
    pub fn get_stats(&self) -> LoggingStats {
        self.stats.read().unwrap().clone()
    }

    /// Создает отчет о состоянии логгера
    pub fn generate_status_report(&self) -> String {
        let stats = self.get_stats();
        let mut report = String::new();

        report.push_str(&format!(
            "Общее количество записей: {}\n",
            stats.total_entries
        ));
        report.push_str(&format!("Размер лог-файла: {} байт\n", stats.log_file_size));
        report.push_str(&format!("Ошибки записи: {}\n", stats.write_errors));

        if !stats.entries_by_level.is_empty() {
            report.push_str("Записи по уровням:\n");
            for (level, count) in &stats.entries_by_level {
                report.push_str(&format!("  {}: {}\n", level, count));
            }
        }

        if !stats.entries_by_category.is_empty() {
            report.push_str("Записи по категориям:\n");
            for (category, count) in &stats.entries_by_category {
                report.push_str(&format!("  {}: {}\n", category, count));
            }
        }

        report
    }

    /// Останавливает логгер
    pub fn shutdown(&mut self) {
        if let Some(handle) = self.background_handle.take() {
            handle.abort();
        }

        // Сбрасываем оставшиеся записи
        if let Some(writer) = self.log_file.lock().unwrap().as_mut() {
            let _ = writer.flush();
        }
    }
}

impl Drop for DebugLogger {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_log_entry_creation() {
        let entry = DebugLogEntry::new(
            LogLevel::Info,
            LogCategory::Transaction,
            "TestComponent",
            "Test message",
        );

        assert_eq!(entry.level, LogLevel::Info);
        assert!(matches!(entry.category, LogCategory::Transaction));
        assert_eq!(entry.component, "TestComponent");
        assert_eq!(entry.message, "Test message");
        assert!(entry.timestamp > 0);
    }

    #[test]
    fn test_log_entry_with_data() {
        let data = serde_json::json!({"key": "value"});
        let entry = DebugLogEntry::new(
            LogLevel::Debug,
            LogCategory::Data,
            "TestComponent",
            "Test message",
        )
        .with_data(data.clone())
        .with_transaction_id(123)
        .with_duration(Duration::from_millis(100))
        .with_data_size(1024);

        assert_eq!(entry.data, Some(data));
        assert_eq!(entry.transaction_id, Some(123));
        assert_eq!(entry.duration_us, Some(100_000));
        assert_eq!(entry.data_size, Some(1024));
    }

    #[test]
    fn test_log_entry_formatting() {
        let entry = DebugLogEntry::new(
            LogLevel::Error,
            LogCategory::System,
            "TestComponent",
            "Test error message",
        )
        .with_transaction_id(456)
        .with_duration(Duration::from_micros(500))
        .with_data_size(2048);

        let formatted = entry.format();
        assert!(formatted.contains("ERROR:SYSTEM"));
        assert!(formatted.contains("TestComponent"));
        assert!(formatted.contains("TX:456"));
        assert!(formatted.contains("(500μs)"));
        assert!(formatted.contains("[2048B]"));
        assert!(formatted.contains("Test error message"));
    }

    #[tokio::test]
    async fn test_debug_logger() {
        let config = DebugConfig {
            enable_debug_logging: true,
            detail_level: 5,
            ..Default::default()
        };

        let logger = DebugLogger::new(&config);

        // Тестируем различные типы логирования
        logger.log_transaction_operation(
            LogLevel::Info,
            "BEGIN",
            123,
            Some(Duration::from_millis(10)),
            Some(256),
        );

        logger.log_data_operation(
            LogLevel::Debug,
            "INSERT",
            "users",
            Some(Duration::from_millis(5)),
            Some(512),
        );

        logger.log_query_operation(
            LogLevel::Info,
            "EXECUTE",
            "query_001",
            Some(Duration::from_millis(50)),
            Some(1024),
        );

        logger.log_system_operation(
            LogLevel::Warning,
            "BufferManager",
            "Buffer overflow detected",
            Some(Duration::from_millis(1)),
        );

        // Проверяем статистику
        let stats = logger.get_stats();
        assert!(stats.total_entries >= 4);
        assert!(stats.entries_by_level.contains_key("INFO"));
        assert!(stats.entries_by_level.contains_key("DEBUG"));
        assert!(stats.entries_by_level.contains_key("WARNING"));
        assert!(stats.entries_by_category.contains_key("TX"));
        assert!(stats.entries_by_category.contains_key("DATA"));
        assert!(stats.entries_by_category.contains_key("QUERY"));
        assert!(stats.entries_by_category.contains_key("SYSTEM"));

        // Тестируем отчет
        let report = logger.generate_status_report();
        assert!(report.contains("Общее количество записей"));
        assert!(report.contains("Записи по уровням"));
        assert!(report.contains("Записи по категориям"));
    }
}
