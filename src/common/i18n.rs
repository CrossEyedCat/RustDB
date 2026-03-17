//! Internationalization module for rustdb
//!
//! Provides support for multiple languages for the user interface

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Supported languages
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Language {
    /// English language
    #[serde(rename = "en")]
    English,
}

impl Default for Language {
    fn default() -> Self {
        Language::English
    }
}

impl std::fmt::Display for Language {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Language::English => write!(f, "en"),
        }
    }
}

impl std::str::FromStr for Language {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "en" | "english" => Ok(Language::English),
            _ => Err(format!("Unsupported language: {}", s)),
        }
    }
}

/// Keys for localized messages
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MessageKey {
    // General messages
    Welcome,
    Goodbye,
    Loading,
    Error,
    Success,
    Warning,
    Info,

    // Error messages
    DatabaseError,
    ConnectionError,
    TransactionError,
    LockTimeout,
    DeadlockDetected,
    InvalidQuery,
    TableNotFound,
    ColumnNotFound,
    IndexNotFound,
    ConstraintViolation,
    PermissionDenied,
    InvalidCredentials,
    TooManyConnections,
    InternalError,

    // Transaction messages
    TransactionStarted,
    TransactionCommitted,
    TransactionRolledBack,
    TransactionAborted,
    SavepointCreated,
    SavepointRolledBack,

    // Lock messages
    LockAcquired,
    LockReleased,
    LockWaiting,
    LockUpgraded,
    LockDowngraded,

    // Recovery messages
    RecoveryStarted,
    RecoveryCompleted,
    CheckpointCreated,
    LogFlushed,

    // Performance messages
    QueryOptimized,
    IndexCreated,
    IndexDropped,
    StatisticsUpdated,

    // Debug messages
    DebugEnabled,
    ProfilingStarted,
    ProfilingStopped,
    TraceStarted,
    TraceCompleted,

    // Language names
    English,
}

/// Localized messages
pub type LocalizedMessages = HashMap<MessageKey, String>;

/// Internationalization manager
#[derive(Debug)]
pub struct I18nManager {
    current_language: Arc<RwLock<Language>>,
    messages: HashMap<Language, LocalizedMessages>,
}

impl I18nManager {
    /// Creates a new internationalization manager
    pub fn new() -> Self {
        let mut manager = Self {
            current_language: Arc::new(RwLock::new(Language::English)),
            messages: HashMap::new(),
        };

        // Load messages for all languages
        manager.load_messages();
        manager
    }

    /// Loads messages for all supported languages
    fn load_messages(&mut self) {
        self.messages
            .insert(Language::English, Self::english_messages());
    }

    /// Returns messages in English
    fn english_messages() -> LocalizedMessages {
        let mut messages = HashMap::new();

        // General messages
        messages.insert(MessageKey::Welcome, "Welcome to RustDB".to_string());
        messages.insert(MessageKey::Goodbye, "Goodbye!".to_string());
        messages.insert(MessageKey::Loading, "Loading...".to_string());
        messages.insert(MessageKey::Error, "Error".to_string());
        messages.insert(MessageKey::Success, "Success".to_string());
        messages.insert(MessageKey::Warning, "Warning".to_string());
        messages.insert(MessageKey::Info, "Information".to_string());

        // Error messages
        messages.insert(MessageKey::DatabaseError, "Database error".to_string());
        messages.insert(MessageKey::ConnectionError, "Connection error".to_string());
        messages.insert(
            MessageKey::TransactionError,
            "Transaction error".to_string(),
        );
        messages.insert(MessageKey::LockTimeout, "Lock timeout".to_string());
        messages.insert(
            MessageKey::DeadlockDetected,
            "Deadlock detected".to_string(),
        );
        messages.insert(MessageKey::InvalidQuery, "Invalid query".to_string());
        messages.insert(MessageKey::TableNotFound, "Table not found".to_string());
        messages.insert(MessageKey::ColumnNotFound, "Column not found".to_string());
        messages.insert(MessageKey::IndexNotFound, "Index not found".to_string());
        messages.insert(
            MessageKey::ConstraintViolation,
            "Constraint violation".to_string(),
        );
        messages.insert(
            MessageKey::PermissionDenied,
            "Permission denied".to_string(),
        );
        messages.insert(
            MessageKey::InvalidCredentials,
            "Invalid credentials".to_string(),
        );
        messages.insert(
            MessageKey::TooManyConnections,
            "Too many connections".to_string(),
        );
        messages.insert(MessageKey::InternalError, "Internal error".to_string());

        // Transaction messages
        messages.insert(
            MessageKey::TransactionStarted,
            "Transaction started".to_string(),
        );
        messages.insert(
            MessageKey::TransactionCommitted,
            "Transaction committed".to_string(),
        );
        messages.insert(
            MessageKey::TransactionRolledBack,
            "Transaction rolled back".to_string(),
        );
        messages.insert(
            MessageKey::TransactionAborted,
            "Transaction aborted".to_string(),
        );
        messages.insert(
            MessageKey::SavepointCreated,
            "Savepoint created".to_string(),
        );
        messages.insert(
            MessageKey::SavepointRolledBack,
            "Savepoint rolled back".to_string(),
        );

        // Lock messages
        messages.insert(MessageKey::LockAcquired, "Lock acquired".to_string());
        messages.insert(MessageKey::LockReleased, "Lock released".to_string());
        messages.insert(MessageKey::LockWaiting, "Waiting for lock".to_string());
        messages.insert(MessageKey::LockUpgraded, "Lock upgraded".to_string());
        messages.insert(MessageKey::LockDowngraded, "Lock downgraded".to_string());

        // Recovery messages
        messages.insert(MessageKey::RecoveryStarted, "Recovery started".to_string());
        messages.insert(
            MessageKey::RecoveryCompleted,
            "Recovery completed".to_string(),
        );
        messages.insert(
            MessageKey::CheckpointCreated,
            "Checkpoint created".to_string(),
        );
        messages.insert(MessageKey::LogFlushed, "Log flushed".to_string());

        // Performance messages
        messages.insert(MessageKey::QueryOptimized, "Query optimized".to_string());
        messages.insert(MessageKey::IndexCreated, "Index created".to_string());
        messages.insert(MessageKey::IndexDropped, "Index dropped".to_string());
        messages.insert(
            MessageKey::StatisticsUpdated,
            "Statistics updated".to_string(),
        );

        // Debug messages
        messages.insert(MessageKey::DebugEnabled, "Debug enabled".to_string());
        messages.insert(
            MessageKey::ProfilingStarted,
            "Profiling started".to_string(),
        );
        messages.insert(
            MessageKey::ProfilingStopped,
            "Profiling stopped".to_string(),
        );
        messages.insert(MessageKey::TraceStarted, "Trace started".to_string());
        messages.insert(MessageKey::TraceCompleted, "Trace completed".to_string());

        // Language names
        messages.insert(MessageKey::English, "English".to_string());

        messages
    }

    /// Sets the current language
    pub fn set_language(&self, language: Language) -> Result<(), String> {
        let mut current = self.current_language.write().map_err(|e| e.to_string())?;
        *current = language;
        Ok(())
    }

    /// Returns the current language
    pub fn get_language(&self) -> Result<Language, String> {
        let current = self.current_language.read().map_err(|e| e.to_string())?;
        Ok(*current)
    }

    /// Returns a localized message
    pub fn get_message(&self, key: MessageKey) -> String {
        let language = self.get_language().unwrap_or(Language::English);
        self.messages
            .get(&language)
            .and_then(|msgs| msgs.get(&key))
            .cloned()
            .unwrap_or_else(|| {
                // Fallback to English if message not found
                self.messages
                    .get(&Language::English)
                    .and_then(|msgs| msgs.get(&key))
                    .cloned()
                    .unwrap_or_else(|| format!("Missing message: {:?}", key))
            })
    }

    /// Returns a localized message with parameters
    pub fn get_message_with_params(&self, key: MessageKey, params: &[&str]) -> String {
        let mut message = self.get_message(key);

        // Simple parameter replacement {0}, {1}, etc.
        for (i, param) in params.iter().enumerate() {
            message = message.replace(&format!("{{{}}}", i), param);
        }

        message
    }

    /// Returns a list of supported languages
    pub fn supported_languages() -> Vec<Language> {
        vec![Language::English]
    }

    /// Returns the language name in the current language
    pub fn get_language_name(&self, language: Language) -> String {
        match language {
            Language::English => self.get_message(MessageKey::English),
        }
    }
}

impl Default for I18nManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Global instance of the internationalization manager
lazy_static::lazy_static! {
    pub static ref I18N: Arc<I18nManager> = Arc::new(I18nManager::new());
}

/// Convenience function to get a localized message
pub fn t(key: MessageKey) -> String {
    I18N.get_message(key)
}

/// Convenience function to get a localized message with parameters
pub fn t_with_params(key: MessageKey, params: &[&str]) -> String {
    I18N.get_message_with_params(key, params)
}

/// Convenience function to set the language
pub fn set_language(language: Language) -> Result<(), String> {
    I18N.set_language(language)
}

/// Convenience function to get the current language
pub fn get_language() -> Result<Language, String> {
    I18N.get_language()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_language_parsing() {
        assert_eq!("en".parse::<Language>().unwrap(), Language::English);
        assert_eq!("english".parse::<Language>().unwrap(), Language::English);
    }

    #[test]
    fn test_language_display() {
        assert_eq!(Language::English.to_string(), "en");
    }

    #[test]
    fn test_i18n_manager() {
        let manager = I18nManager::new();

        // Test English messages
        manager.set_language(Language::English).unwrap();
        assert_eq!(
            manager.get_message(MessageKey::Welcome),
            "Welcome to RustDB"
        );
        assert_eq!(manager.get_message(MessageKey::Error), "Error");
    }

    #[test]
    fn test_global_functions() {
        // Test global functions
        set_language(Language::English).unwrap();
        assert_eq!(t(MessageKey::Welcome), "Welcome to RustDB");
    }

    #[test]
    fn test_supported_languages() {
        let languages = I18nManager::supported_languages();
        assert_eq!(languages.len(), 1);
        assert!(languages.contains(&Language::English));
    }
}
