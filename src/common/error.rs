//! Error handling for rustdb

use crate::common::i18n::{t, t_with_params, MessageKey};
use thiserror::Error;

/// Bincode encode/decode failure (`bincode-next` uses distinct error types).
#[derive(Error, Debug)]
pub enum BincodeError {
    #[error("{0}")]
    Encode(#[from] bincode_next::error::EncodeError),
    #[error("{0}")]
    Decode(#[from] bincode_next::error::DecodeError),
}

/// Main error type for rustdb
#[derive(Error, Debug)]
pub enum Error {
    /// I/O operation error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization/deserialization error
    #[error("JSON serialization error: {0}")]
    JsonSerialization(#[from] serde_json::Error),

    /// Bincode serialization/deserialization error
    #[error(transparent)]
    BincodeSerialization(#[from] BincodeError),

    /// Database error
    #[error("Database error: {message}")]
    Database { message: String },

    /// SQL parsing error
    #[error("SQL parsing error: {message}")]
    SqlParsing { message: String },

    /// Semantic analysis error
    #[error("Semantic analysis error: {message}")]
    SemanticAnalysis { message: String },

    /// Query planning error
    #[error("Query planning error: {message}")]
    QueryPlanning { message: String },

    /// Query execution error
    #[error("Query execution error: {message}")]
    QueryExecution { message: String },

    /// Transaction error
    #[error("Transaction error: {0}")]
    TransactionError(String),

    /// Lock error
    #[error("Lock error: {0}")]
    LockError(String),

    /// Deadlock detected
    #[error("Deadlock detected: {0}")]
    DeadlockDetected(String),

    /// Transaction error (deprecated format)
    #[error("Transaction error: {message}")]
    Transaction { message: String },

    /// Lock error (deprecated format)
    #[error("Lock error: {message}")]
    Lock { message: String },

    /// Validation error
    #[error("Validation error: {message}")]
    Validation { message: String },

    /// Configuration error
    #[error("Configuration error: {message}")]
    Configuration { message: String },

    /// Unsupported operation
    #[error("Unsupported operation: {operation}")]
    Unsupported { operation: String },

    /// Internal error
    #[error("Internal error: {message}")]
    Internal { message: String },

    /// Timeout error
    #[error("Timeout error: {message}")]
    Timeout { message: String },

    /// Conflict error
    #[error("Conflict error: {message}")]
    Conflict { message: String },
}

/// Result type for rustdb
pub type Result<T> = std::result::Result<T, Error>;

impl From<bincode_next::error::EncodeError> for Error {
    fn from(e: bincode_next::error::EncodeError) -> Self {
        Self::BincodeSerialization(e.into())
    }
}

impl From<bincode_next::error::DecodeError> for Error {
    fn from(e: bincode_next::error::DecodeError) -> Self {
        Self::BincodeSerialization(e.into())
    }
}

impl Error {
    /// Creates a database error
    pub fn database(message: impl Into<String>) -> Self {
        Self::Database {
            message: message.into(),
        }
    }

    /// Creates a SQL parsing error
    pub fn sql_parsing(message: impl Into<String>) -> Self {
        Self::SqlParsing {
            message: message.into(),
        }
    }

    /// Creates a parser error (alias for sql_parsing)
    pub fn parser(message: impl Into<String>) -> Self {
        Self::sql_parsing(message)
    }

    /// Creates a timeout error
    pub fn timeout(message: impl Into<String>) -> Self {
        Self::Timeout {
            message: message.into(),
        }
    }

    /// Creates a conflict error
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::Conflict {
            message: message.into(),
        }
    }

    /// Creates a semantic analysis error
    pub fn semantic_analysis(message: impl Into<String>) -> Self {
        Self::SemanticAnalysis {
            message: message.into(),
        }
    }

    /// Creates a query planning error
    pub fn query_planning(message: impl Into<String>) -> Self {
        Self::QueryPlanning {
            message: message.into(),
        }
    }

    /// Creates a query execution error
    pub fn query_execution(message: impl Into<String>) -> Self {
        Self::QueryExecution {
            message: message.into(),
        }
    }

    /// Creates a transaction error
    pub fn transaction(message: impl Into<String>) -> Self {
        Self::Transaction {
            message: message.into(),
        }
    }

    /// Creates a lock error
    pub fn lock(message: impl Into<String>) -> Self {
        Self::Lock {
            message: message.into(),
        }
    }

    /// Creates a validation error
    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation {
            message: message.into(),
        }
    }

    /// Creates a configuration error
    pub fn configuration(message: impl Into<String>) -> Self {
        Self::Configuration {
            message: message.into(),
        }
    }

    /// Creates an unsupported operation error
    pub fn unsupported(operation: impl Into<String>) -> Self {
        Self::Unsupported {
            operation: operation.into(),
        }
    }

    /// Creates an internal error
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    /// Creates a localized database error
    pub fn localized_database(key: MessageKey) -> Self {
        Self::Database { message: t(key) }
    }

    /// Creates a localized database error with parameters
    pub fn localized_database_with_params(key: MessageKey, params: &[&str]) -> Self {
        Self::Database {
            message: t_with_params(key, params),
        }
    }

    /// Creates a localized SQL parsing error
    pub fn localized_sql_parsing(key: MessageKey) -> Self {
        Self::SqlParsing { message: t(key) }
    }

    /// Creates a localized SQL parsing error with parameters
    pub fn localized_sql_parsing_with_params(key: MessageKey, params: &[&str]) -> Self {
        Self::SqlParsing {
            message: t_with_params(key, params),
        }
    }

    /// Creates a localized transaction error
    pub fn localized_transaction(key: MessageKey) -> Self {
        Self::TransactionError(t(key))
    }

    /// Creates a localized transaction error with parameters
    pub fn localized_transaction_with_params(key: MessageKey, params: &[&str]) -> Self {
        Self::TransactionError(t_with_params(key, params))
    }

    /// Creates a localized lock error
    pub fn localized_lock(key: MessageKey) -> Self {
        Self::LockError(t(key))
    }

    /// Creates a localized lock error with parameters
    pub fn localized_lock_with_params(key: MessageKey, params: &[&str]) -> Self {
        Self::LockError(t_with_params(key, params))
    }

    /// Creates a localized validation error
    pub fn localized_validation(key: MessageKey) -> Self {
        Self::Validation { message: t(key) }
    }

    /// Creates a localized validation error with parameters
    pub fn localized_validation_with_params(key: MessageKey, params: &[&str]) -> Self {
        Self::Validation {
            message: t_with_params(key, params),
        }
    }

    /// Creates a localized configuration error
    pub fn localized_configuration(key: MessageKey) -> Self {
        Self::Configuration { message: t(key) }
    }

    /// Creates a localized configuration error with parameters
    pub fn localized_configuration_with_params(key: MessageKey, params: &[&str]) -> Self {
        Self::Configuration {
            message: t_with_params(key, params),
        }
    }

    /// Creates a localized internal error
    pub fn localized_internal(key: MessageKey) -> Self {
        Self::Internal { message: t(key) }
    }

    /// Creates a localized internal error with parameters
    pub fn localized_internal_with_params(key: MessageKey, params: &[&str]) -> Self {
        Self::Internal {
            message: t_with_params(key, params),
        }
    }
}
