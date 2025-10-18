//! Модуль интернационализации для rustdb
//!
//! Предоставляет поддержку множественных языков для пользовательского интерфейса

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use serde::{Deserialize, Serialize};

/// Поддерживаемые языки
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Language {
    /// Английский язык
    #[serde(rename = "en")]
    English,
    /// Русский язык
    #[serde(rename = "ru")]
    Russian,
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
            Language::Russian => write!(f, "ru"),
        }
    }
}

impl std::str::FromStr for Language {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "en" | "english" => Ok(Language::English),
            "ru" | "russian" | "русский" => Ok(Language::Russian),
            _ => Err(format!("Unsupported language: {}", s)),
        }
    }
}

/// Ключи для локализованных сообщений
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MessageKey {
    // Общие сообщения
    Welcome,
    Goodbye,
    Loading,
    Error,
    Success,
    Warning,
    Info,
    
    // Сообщения об ошибках
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
    
    // Сообщения транзакций
    TransactionStarted,
    TransactionCommitted,
    TransactionRolledBack,
    TransactionAborted,
    SavepointCreated,
    SavepointRolledBack,
    
    // Сообщения блокировок
    LockAcquired,
    LockReleased,
    LockWaiting,
    LockUpgraded,
    LockDowngraded,
    
    // Сообщения восстановления
    RecoveryStarted,
    RecoveryCompleted,
    CheckpointCreated,
    LogFlushed,
    
    // Сообщения производительности
    QueryOptimized,
    IndexCreated,
    IndexDropped,
    StatisticsUpdated,
    
    // Сообщения отладки
    DebugEnabled,
    ProfilingStarted,
    ProfilingStopped,
    TraceStarted,
    TraceCompleted,
    
    // Названия языков
    English,
    Russian,
}

/// Локализованные сообщения
pub type LocalizedMessages = HashMap<MessageKey, String>;

/// Менеджер интернационализации
#[derive(Debug)]
pub struct I18nManager {
    current_language: Arc<RwLock<Language>>,
    messages: HashMap<Language, LocalizedMessages>,
}

impl I18nManager {
    /// Создает новый менеджер интернационализации
    pub fn new() -> Self {
        let mut manager = Self {
            current_language: Arc::new(RwLock::new(Language::English)),
            messages: HashMap::new(),
        };
        
        // Загружаем сообщения для всех языков
        manager.load_messages();
        manager
    }
    
    /// Загружает сообщения для всех поддерживаемых языков
    fn load_messages(&mut self) {
        self.messages.insert(Language::English, Self::english_messages());
        self.messages.insert(Language::Russian, Self::russian_messages());
    }
    
    /// Возвращает сообщения на английском языке
    fn english_messages() -> LocalizedMessages {
        let mut messages = HashMap::new();
        
        // Общие сообщения
        messages.insert(MessageKey::Welcome, "Welcome to RustDB".to_string());
        messages.insert(MessageKey::Goodbye, "Goodbye!".to_string());
        messages.insert(MessageKey::Loading, "Loading...".to_string());
        messages.insert(MessageKey::Error, "Error".to_string());
        messages.insert(MessageKey::Success, "Success".to_string());
        messages.insert(MessageKey::Warning, "Warning".to_string());
        messages.insert(MessageKey::Info, "Information".to_string());
        
        // Сообщения об ошибках
        messages.insert(MessageKey::DatabaseError, "Database error".to_string());
        messages.insert(MessageKey::ConnectionError, "Connection error".to_string());
        messages.insert(MessageKey::TransactionError, "Transaction error".to_string());
        messages.insert(MessageKey::LockTimeout, "Lock timeout".to_string());
        messages.insert(MessageKey::DeadlockDetected, "Deadlock detected".to_string());
        messages.insert(MessageKey::InvalidQuery, "Invalid query".to_string());
        messages.insert(MessageKey::TableNotFound, "Table not found".to_string());
        messages.insert(MessageKey::ColumnNotFound, "Column not found".to_string());
        messages.insert(MessageKey::IndexNotFound, "Index not found".to_string());
        messages.insert(MessageKey::ConstraintViolation, "Constraint violation".to_string());
        messages.insert(MessageKey::PermissionDenied, "Permission denied".to_string());
        messages.insert(MessageKey::InvalidCredentials, "Invalid credentials".to_string());
        messages.insert(MessageKey::TooManyConnections, "Too many connections".to_string());
        messages.insert(MessageKey::InternalError, "Internal error".to_string());
        
        // Сообщения транзакций
        messages.insert(MessageKey::TransactionStarted, "Transaction started".to_string());
        messages.insert(MessageKey::TransactionCommitted, "Transaction committed".to_string());
        messages.insert(MessageKey::TransactionRolledBack, "Transaction rolled back".to_string());
        messages.insert(MessageKey::TransactionAborted, "Transaction aborted".to_string());
        messages.insert(MessageKey::SavepointCreated, "Savepoint created".to_string());
        messages.insert(MessageKey::SavepointRolledBack, "Savepoint rolled back".to_string());
        
        // Сообщения блокировок
        messages.insert(MessageKey::LockAcquired, "Lock acquired".to_string());
        messages.insert(MessageKey::LockReleased, "Lock released".to_string());
        messages.insert(MessageKey::LockWaiting, "Waiting for lock".to_string());
        messages.insert(MessageKey::LockUpgraded, "Lock upgraded".to_string());
        messages.insert(MessageKey::LockDowngraded, "Lock downgraded".to_string());
        
        // Сообщения восстановления
        messages.insert(MessageKey::RecoveryStarted, "Recovery started".to_string());
        messages.insert(MessageKey::RecoveryCompleted, "Recovery completed".to_string());
        messages.insert(MessageKey::CheckpointCreated, "Checkpoint created".to_string());
        messages.insert(MessageKey::LogFlushed, "Log flushed".to_string());
        
        // Сообщения производительности
        messages.insert(MessageKey::QueryOptimized, "Query optimized".to_string());
        messages.insert(MessageKey::IndexCreated, "Index created".to_string());
        messages.insert(MessageKey::IndexDropped, "Index dropped".to_string());
        messages.insert(MessageKey::StatisticsUpdated, "Statistics updated".to_string());
        
        // Сообщения отладки
        messages.insert(MessageKey::DebugEnabled, "Debug enabled".to_string());
        messages.insert(MessageKey::ProfilingStarted, "Profiling started".to_string());
        messages.insert(MessageKey::ProfilingStopped, "Profiling stopped".to_string());
        messages.insert(MessageKey::TraceStarted, "Trace started".to_string());
        messages.insert(MessageKey::TraceCompleted, "Trace completed".to_string());
        
        // Названия языков
        messages.insert(MessageKey::English, "English".to_string());
        messages.insert(MessageKey::Russian, "Russian".to_string());
        
        messages
    }
    
    /// Возвращает сообщения на русском языке
    fn russian_messages() -> LocalizedMessages {
        let mut messages = HashMap::new();
        
        // Общие сообщения
        messages.insert(MessageKey::Welcome, "Добро пожаловать в RustDB".to_string());
        messages.insert(MessageKey::Goodbye, "До свидания!".to_string());
        messages.insert(MessageKey::Loading, "Загрузка...".to_string());
        messages.insert(MessageKey::Error, "Ошибка".to_string());
        messages.insert(MessageKey::Success, "Успешно".to_string());
        messages.insert(MessageKey::Warning, "Предупреждение".to_string());
        messages.insert(MessageKey::Info, "Информация".to_string());
        
        // Сообщения об ошибках
        messages.insert(MessageKey::DatabaseError, "Ошибка базы данных".to_string());
        messages.insert(MessageKey::ConnectionError, "Ошибка подключения".to_string());
        messages.insert(MessageKey::TransactionError, "Ошибка транзакции".to_string());
        messages.insert(MessageKey::LockTimeout, "Таймаут блокировки".to_string());
        messages.insert(MessageKey::DeadlockDetected, "Обнаружена взаимоблокировка".to_string());
        messages.insert(MessageKey::InvalidQuery, "Неверный запрос".to_string());
        messages.insert(MessageKey::TableNotFound, "Таблица не найдена".to_string());
        messages.insert(MessageKey::ColumnNotFound, "Колонка не найдена".to_string());
        messages.insert(MessageKey::IndexNotFound, "Индекс не найден".to_string());
        messages.insert(MessageKey::ConstraintViolation, "Нарушение ограничения".to_string());
        messages.insert(MessageKey::PermissionDenied, "Доступ запрещен".to_string());
        messages.insert(MessageKey::InvalidCredentials, "Неверные учетные данные".to_string());
        messages.insert(MessageKey::TooManyConnections, "Слишком много подключений".to_string());
        messages.insert(MessageKey::InternalError, "Внутренняя ошибка".to_string());
        
        // Сообщения транзакций
        messages.insert(MessageKey::TransactionStarted, "Транзакция начата".to_string());
        messages.insert(MessageKey::TransactionCommitted, "Транзакция подтверждена".to_string());
        messages.insert(MessageKey::TransactionRolledBack, "Транзакция откачена".to_string());
        messages.insert(MessageKey::TransactionAborted, "Транзакция прервана".to_string());
        messages.insert(MessageKey::SavepointCreated, "Точка сохранения создана".to_string());
        messages.insert(MessageKey::SavepointRolledBack, "Откат к точке сохранения".to_string());
        
        // Сообщения блокировок
        messages.insert(MessageKey::LockAcquired, "Блокировка получена".to_string());
        messages.insert(MessageKey::LockReleased, "Блокировка освобождена".to_string());
        messages.insert(MessageKey::LockWaiting, "Ожидание блокировки".to_string());
        messages.insert(MessageKey::LockUpgraded, "Блокировка повышена".to_string());
        messages.insert(MessageKey::LockDowngraded, "Блокировка понижена".to_string());
        
        // Сообщения восстановления
        messages.insert(MessageKey::RecoveryStarted, "Восстановление начато".to_string());
        messages.insert(MessageKey::RecoveryCompleted, "Восстановление завершено".to_string());
        messages.insert(MessageKey::CheckpointCreated, "Контрольная точка создана".to_string());
        messages.insert(MessageKey::LogFlushed, "Лог сброшен".to_string());
        
        // Сообщения производительности
        messages.insert(MessageKey::QueryOptimized, "Запрос оптимизирован".to_string());
        messages.insert(MessageKey::IndexCreated, "Индекс создан".to_string());
        messages.insert(MessageKey::IndexDropped, "Индекс удален".to_string());
        messages.insert(MessageKey::StatisticsUpdated, "Статистика обновлена".to_string());
        
        // Сообщения отладки
        messages.insert(MessageKey::DebugEnabled, "Отладка включена".to_string());
        messages.insert(MessageKey::ProfilingStarted, "Профилирование начато".to_string());
        messages.insert(MessageKey::ProfilingStopped, "Профилирование остановлено".to_string());
        messages.insert(MessageKey::TraceStarted, "Трассировка начата".to_string());
        messages.insert(MessageKey::TraceCompleted, "Трассировка завершена".to_string());
        
        // Названия языков
        messages.insert(MessageKey::English, "Английский".to_string());
        messages.insert(MessageKey::Russian, "Русский".to_string());
        
        messages
    }
    
    /// Устанавливает текущий язык
    pub fn set_language(&self, language: Language) -> Result<(), String> {
        let mut current = self.current_language.write().map_err(|e| e.to_string())?;
        *current = language;
        Ok(())
    }
    
    /// Возвращает текущий язык
    pub fn get_language(&self) -> Result<Language, String> {
        let current = self.current_language.read().map_err(|e| e.to_string())?;
        Ok(*current)
    }
    
    /// Возвращает локализованное сообщение
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
    
    /// Возвращает локализованное сообщение с параметрами
    pub fn get_message_with_params(&self, key: MessageKey, params: &[&str]) -> String {
        let mut message = self.get_message(key);
        
        // Простая замена параметров {0}, {1}, etc.
        for (i, param) in params.iter().enumerate() {
            message = message.replace(&format!("{{{}}}", i), param);
        }
        
        message
    }
    
    /// Возвращает список поддерживаемых языков
    pub fn supported_languages() -> Vec<Language> {
        vec![Language::English, Language::Russian]
    }
    
    /// Возвращает название языка на текущем языке
    pub fn get_language_name(&self, language: Language) -> String {
        match language {
            Language::English => self.get_message(MessageKey::English),
            Language::Russian => self.get_message(MessageKey::Russian),
        }
    }
}

impl Default for I18nManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Глобальный экземпляр менеджера интернационализации
lazy_static::lazy_static! {
    pub static ref I18N: Arc<I18nManager> = Arc::new(I18nManager::new());
}

/// Удобная функция для получения локализованного сообщения
pub fn t(key: MessageKey) -> String {
    I18N.get_message(key)
}

/// Удобная функция для получения локализованного сообщения с параметрами
pub fn t_with_params(key: MessageKey, params: &[&str]) -> String {
    I18N.get_message_with_params(key, params)
}

/// Удобная функция для установки языка
pub fn set_language(language: Language) -> Result<(), String> {
    I18N.set_language(language)
}

/// Удобная функция для получения текущего языка
pub fn get_language() -> Result<Language, String> {
    I18N.get_language()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_language_parsing() {
        assert_eq!("en".parse::<Language>().unwrap(), Language::English);
        assert_eq!("ru".parse::<Language>().unwrap(), Language::Russian);
        assert_eq!("english".parse::<Language>().unwrap(), Language::English);
        assert_eq!("russian".parse::<Language>().unwrap(), Language::Russian);
        assert_eq!("русский".parse::<Language>().unwrap(), Language::Russian);
    }

    #[test]
    fn test_language_display() {
        assert_eq!(Language::English.to_string(), "en");
        assert_eq!(Language::Russian.to_string(), "ru");
    }

    #[test]
    fn test_i18n_manager() {
        let manager = I18nManager::new();
        
        // Test English messages
        manager.set_language(Language::English).unwrap();
        assert_eq!(manager.get_message(MessageKey::Welcome), "Welcome to RustDB");
        assert_eq!(manager.get_message(MessageKey::Error), "Error");
        
        // Test Russian messages
        manager.set_language(Language::Russian).unwrap();
        assert_eq!(manager.get_message(MessageKey::Welcome), "Добро пожаловать в RustDB");
        assert_eq!(manager.get_message(MessageKey::Error), "Ошибка");
    }

    #[test]
    fn test_global_functions() {
        // Test global functions
        set_language(Language::English).unwrap();
        assert_eq!(t(MessageKey::Welcome), "Welcome to RustDB");
        
        set_language(Language::Russian).unwrap();
        assert_eq!(t(MessageKey::Welcome), "Добро пожаловать в RustDB");
    }

    #[test]
    fn test_supported_languages() {
        let languages = I18nManager::supported_languages();
        assert_eq!(languages.len(), 2);
        assert!(languages.contains(&Language::English));
        assert!(languages.contains(&Language::Russian));
    }
}
