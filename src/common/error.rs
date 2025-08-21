//! Обработка ошибок для RustBD

use thiserror::Error;

/// Основной тип ошибки для RustBD
#[derive(Error, Debug)]
pub enum Error {
    /// Ошибка I/O операций
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Ошибка сериализации/десериализации
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Ошибка базы данных
    #[error("Database error: {message}")]
    Database { message: String },

    /// Ошибка парсинга SQL
    #[error("SQL parsing error: {message}")]
    SqlParsing { message: String },

    /// Ошибка планирования запроса
    #[error("Query planning error: {message}")]
    QueryPlanning { message: String },

    /// Ошибка выполнения запроса
    #[error("Query execution error: {message}")]
    QueryExecution { message: String },

    /// Ошибка транзакции
    #[error("Transaction error: {message}")]
    Transaction { message: String },

    /// Ошибка блокировки
    #[error("Lock error: {message}")]
    Lock { message: String },

    /// Ошибка валидации
    #[error("Validation error: {message}")]
    Validation { message: String },

    /// Ошибка конфигурации
    #[error("Configuration error: {message}")]
    Configuration { message: String },

    /// Неподдерживаемая операция
    #[error("Unsupported operation: {operation}")]
    Unsupported { operation: String },

    /// Внутренняя ошибка
    #[error("Internal error: {message}")]
    Internal { message: String },
}

/// Тип результата для RustBD
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Создает ошибку базы данных
    pub fn database(message: impl Into<String>) -> Self {
        Self::Database {
            message: message.into(),
        }
    }

    /// Создает ошибку SQL парсинга
    pub fn sql_parsing(message: impl Into<String>) -> Self {
        Self::SqlParsing {
            message: message.into(),
        }
    }

    /// Создает ошибку планирования запроса
    pub fn query_planning(message: impl Into<String>) -> Self {
        Self::QueryPlanning {
            message: message.into(),
        }
    }

    /// Создает ошибку выполнения запроса
    pub fn query_execution(message: impl Into<String>) -> Self {
        Self::QueryExecution {
            message: message.into(),
        }
    }

    /// Создает ошибку транзакции
    pub fn transaction(message: impl Into<String>) -> Self {
        Self::Transaction {
            message: message.into(),
        }
    }

    /// Создает ошибку блокировки
    pub fn lock(message: impl Into<String>) -> Self {
        Self::Lock {
            message: message.into(),
        }
    }

    /// Создает ошибку валидации
    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation {
            message: message.into(),
        }
    }

    /// Создает ошибку конфигурации
    pub fn configuration(message: impl Into<String>) -> Self {
        Self::Configuration {
            message: message.into(),
        }
    }

    /// Создает ошибку неподдерживаемой операции
    pub fn unsupported(operation: impl Into<String>) -> Self {
        Self::Unsupported {
            operation: operation.into(),
        }
    }

    /// Создает внутреннюю ошибку
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }
}
