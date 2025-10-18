//! Обработка ошибок для rustdb

use thiserror::Error;
use bincode;
use crate::common::i18n::{MessageKey, t, t_with_params};

/// Основной тип ошибки для rustdb
#[derive(Error, Debug)]
pub enum Error {
    /// Ошибка I/O операций
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Ошибка сериализации/десериализации JSON
    #[error("JSON serialization error: {0}")]
    JsonSerialization(#[from] serde_json::Error),

    /// Ошибка сериализации/десериализации bincode
    #[error("Bincode serialization error: {0}")]
    BincodeSerialization(#[from] Box<bincode::ErrorKind>),

    /// Ошибка базы данных
    #[error("Database error: {message}")]
    Database { message: String },

    /// Ошибка парсинга SQL
    #[error("SQL parsing error: {message}")]
    SqlParsing { message: String },

    /// Ошибка семантического анализа
    #[error("Semantic analysis error: {message}")]
    SemanticAnalysis { message: String },

    /// Ошибка планирования запроса
    #[error("Query planning error: {message}")]
    QueryPlanning { message: String },

    /// Ошибка выполнения запроса
    #[error("Query execution error: {message}")]
    QueryExecution { message: String },

    /// Ошибка транзакции
    #[error("Transaction error: {0}")]
    TransactionError(String),

    /// Ошибка блокировки
    #[error("Lock error: {0}")]
    LockError(String),

    /// Обнаружен дедлок
    #[error("Deadlock detected: {0}")]
    DeadlockDetected(String),

    /// Ошибка транзакции (устаревший формат)
    #[error("Transaction error: {message}")]
    Transaction { message: String },

    /// Ошибка блокировки (устаревший формат)
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

    /// Ошибка таймаута
    #[error("Timeout error: {message}")]
    Timeout { message: String },

    /// Ошибка конфликта
    #[error("Conflict error: {message}")]
    Conflict { message: String },
}

/// Тип результата для rustdb
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

    /// Создает ошибку парсера (алиас для sql_parsing)
    pub fn parser(message: impl Into<String>) -> Self {
        Self::sql_parsing(message)
    }

    /// Создает ошибку таймаута
    pub fn timeout(message: impl Into<String>) -> Self {
        Self::Timeout {
            message: message.into(),
        }
    }

    /// Создает ошибку конфликта
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::Conflict {
            message: message.into(),
        }
    }

    /// Создает ошибку семантического анализа
    pub fn semantic_analysis(message: impl Into<String>) -> Self {
        Self::SemanticAnalysis {
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
    
    /// Создает локализованную ошибку базы данных
    pub fn localized_database(key: MessageKey) -> Self {
        Self::Database {
            message: t(key),
        }
    }
    
    /// Создает локализованную ошибку базы данных с параметрами
    pub fn localized_database_with_params(key: MessageKey, params: &[&str]) -> Self {
        Self::Database {
            message: t_with_params(key, params),
        }
    }
    
    /// Создает локализованную ошибку парсинга SQL
    pub fn localized_sql_parsing(key: MessageKey) -> Self {
        Self::SqlParsing {
            message: t(key),
        }
    }
    
    /// Создает локализованную ошибку парсинга SQL с параметрами
    pub fn localized_sql_parsing_with_params(key: MessageKey, params: &[&str]) -> Self {
        Self::SqlParsing {
            message: t_with_params(key, params),
        }
    }
    
    /// Создает локализованную ошибку транзакции
    pub fn localized_transaction(key: MessageKey) -> Self {
        Self::TransactionError(t(key))
    }
    
    /// Создает локализованную ошибку транзакции с параметрами
    pub fn localized_transaction_with_params(key: MessageKey, params: &[&str]) -> Self {
        Self::TransactionError(t_with_params(key, params))
    }
    
    /// Создает локализованную ошибку блокировки
    pub fn localized_lock(key: MessageKey) -> Self {
        Self::LockError(t(key))
    }
    
    /// Создает локализованную ошибку блокировки с параметрами
    pub fn localized_lock_with_params(key: MessageKey, params: &[&str]) -> Self {
        Self::LockError(t_with_params(key, params))
    }
    
    /// Создает локализованную ошибку валидации
    pub fn localized_validation(key: MessageKey) -> Self {
        Self::Validation {
            message: t(key),
        }
    }
    
    /// Создает локализованную ошибку валидации с параметрами
    pub fn localized_validation_with_params(key: MessageKey, params: &[&str]) -> Self {
        Self::Validation {
            message: t_with_params(key, params),
        }
    }
    
    /// Создает локализованную ошибку конфигурации
    pub fn localized_configuration(key: MessageKey) -> Self {
        Self::Configuration {
            message: t(key),
        }
    }
    
    /// Создает локализованную ошибку конфигурации с параметрами
    pub fn localized_configuration_with_params(key: MessageKey, params: &[&str]) -> Self {
        Self::Configuration {
            message: t_with_params(key, params),
        }
    }
    
    /// Создает локализованную внутреннюю ошибку
    pub fn localized_internal(key: MessageKey) -> Self {
        Self::Internal {
            message: t(key),
        }
    }
    
    /// Создает локализованную внутреннюю ошибку с параметрами
    pub fn localized_internal_with_params(key: MessageKey, params: &[&str]) -> Self {
        Self::Internal {
            message: t_with_params(key, params),
        }
    }
}
