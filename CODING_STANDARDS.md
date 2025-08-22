# Стандарты кодирования RustBD

## 🎯 Общие принципы

### Читаемость и понятность
- Код должен быть самодокументируемым
- Имена переменных, функций и структур должны быть описательными
- Избегайте магических чисел и строк
- Комментируйте сложную логику

### Производительность
- Используйте `&str` вместо `String` где возможно
- Предпочитайте итераторы циклам
- Избегайте ненужных аллокаций
- Используйте `Cow<T>` для условного владения

### Безопасность
- Всегда обрабатывайте ошибки
- Используйте `Result<T, E>` для операций, которые могут завершиться неудачей
- Избегайте `unwrap()` и `expect()` в продакшн коде
- Используйте `Option<T>` для значений, которые могут отсутствовать

## 📝 Стиль кода

### Именование

#### Переменные и функции
```rust
// ✅ Хорошо
let user_count: usize = 0;
let is_valid_user: bool = true;
fn calculate_user_score(user: &User) -> f64 { ... }

// ❌ Плохо
let cnt: usize = 0;
let valid: bool = true;
fn calc_score(u: &User) -> f64 { ... }
```

#### Константы
```rust
// ✅ Хорошо
const MAX_CONNECTIONS: usize = 100;
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

// ❌ Плохо
const max_connections: usize = 100;
const default_timeout: Duration = Duration::from_secs(30);
```

#### Структуры и перечисления
```rust
// ✅ Хорошо
#[derive(Debug, Clone)]
pub struct DatabaseConnection {
    pub host: String,
    pub port: u16,
    pub credentials: ConnectionCredentials,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionState {
    Connected,
    Disconnected,
    Connecting,
}

// ❌ Плохо
#[derive(Debug, Clone)]
pub struct db_conn {
    pub host: String,
    pub port: u16,
    pub creds: conn_creds,
}
```

### Форматирование

#### Отступы и пробелы
- Используйте 4 пробела для отступов
- Оставляйте одну пустую строку между функциями
- Оставляйте две пустые строки между модулями
- Выравнивайте параметры функций для читаемости

```rust
// ✅ Хорошо
pub fn create_connection(
    host: &str,
    port: u16,
    credentials: &ConnectionCredentials,
    timeout: Duration,
) -> Result<DatabaseConnection, ConnectionError> {
    // ...
}

// ❌ Плохо
pub fn create_connection(host: &str, port: u16, credentials: &ConnectionCredentials, timeout: Duration) -> Result<DatabaseConnection, ConnectionError> {
    // ...
}
```

#### Импорты
- Группируйте импорты по категориям
- Используйте абсолютные пути для внешних зависимостей
- Используйте относительные пути для внутренних модулей

```rust
// ✅ Хорошо
// Стандартная библиотека
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

// Внешние зависимости
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

// Внутренние модули
use crate::common::error::DatabaseError;
use crate::storage::page::Page;
use super::schema::TableSchema;
```

### Документация

#### Комментарии к функциям
```rust
/// Создает новое соединение с базой данных.
///
/// # Arguments
///
/// * `host` - Хост сервера базы данных
/// * `port` - Порт сервера
/// * `credentials` - Учетные данные для подключения
/// * `timeout` - Таймаут подключения
///
/// # Returns
///
/// Возвращает `Ok(DatabaseConnection)` при успешном подключении
/// или `Err(ConnectionError)` при ошибке.
///
/// # Examples
///
/// ```
/// use rustbd::network::connection::create_connection;
/// use std::time::Duration;
///
/// let connection = create_connection(
///     "localhost",
///     5432,
///     &credentials,
///     Duration::from_secs(30)
/// )?;
/// ```
///
/// # Errors
///
/// Функция может вернуть следующие ошибки:
/// - `ConnectionError::InvalidHost` - неверный формат хоста
/// - `ConnectionError::ConnectionTimeout` - превышен таймаут
/// - `ConnectionError::AuthenticationFailed` - ошибка аутентификации
pub fn create_connection(
    host: &str,
    port: u16,
    credentials: &ConnectionCredentials,
    timeout: Duration,
) -> Result<DatabaseConnection, ConnectionError> {
    // ...
}
```

#### Комментарии к структурам
```rust
/// Представляет соединение с базой данных.
///
/// Структура содержит всю необходимую информацию для работы
/// с базой данных, включая сокет, состояние и метаданные.
#[derive(Debug, Clone)]
pub struct DatabaseConnection {
    /// Хост сервера базы данных
    pub host: String,
    /// Порт сервера
    pub port: u16,
    /// Учетные данные для подключения
    pub credentials: ConnectionCredentials,
    /// Текущее состояние соединения
    pub state: ConnectionState,
}
```

### Обработка ошибок

#### Определение ошибок
```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("Ошибка подключения: {message}")]
    ConnectionError { message: String },
    
    #[error("Ошибка SQL: {sql}, детали: {details}")]
    SqlError { sql: String, details: String },
    
    #[error("Ошибка транзакции: {0}")]
    TransactionError(#[from] TransactionError),
    
    #[error("Ошибка ввода-вывода: {0}")]
    IoError(#[from] std::io::Error),
}

impl From<ConnectionError> for DatabaseError {
    fn from(err: ConnectionError) -> Self {
        DatabaseError::ConnectionError {
            message: err.to_string(),
        }
    }
}
```

#### Использование ошибок
```rust
// ✅ Хорошо
pub fn execute_query(&self, query: &str) -> Result<QueryResult, DatabaseError> {
    let parsed_query = self.parser.parse(query)
        .map_err(|e| DatabaseError::SqlError {
            sql: query.to_string(),
            details: e.to_string(),
        })?;
    
    let result = self.executor.execute(parsed_query)?;
    Ok(result)
}

// ❌ Плохо
pub fn execute_query(&self, query: &str) -> Result<QueryResult, DatabaseError> {
    let parsed_query = self.parser.parse(query).unwrap();
    let result = self.executor.execute(parsed_query).unwrap();
    Ok(result)
}
```

### Тестирование

#### Структура тестов
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::create_test_database;

    #[test]
    fn test_create_connection_success() {
        // Arrange
        let host = "localhost";
        let port = 5432;
        let credentials = ConnectionCredentials::new("user", "password");
        let timeout = Duration::from_secs(30);

        // Act
        let result = create_connection(host, port, &credentials, timeout);

        // Assert
        assert!(result.is_ok());
        let connection = result.unwrap();
        assert_eq!(connection.host, host);
        assert_eq!(connection.port, port);
    }

    #[test]
    fn test_create_connection_invalid_host() {
        // Arrange
        let host = "invalid-host-name-very-long-and-invalid";
        let port = 5432;
        let credentials = ConnectionCredentials::new("user", "password");
        let timeout = Duration::from_secs(30);

        // Act
        let result = create_connection(host, port, &credentials, timeout);

        // Assert
        assert!(result.is_err());
        match result.unwrap_err() {
            DatabaseError::ConnectionError { message } => {
                assert!(message.contains("invalid host"));
            }
            _ => panic!("Expected ConnectionError"),
        }
    }

    #[tokio::test]
    async fn test_async_connection() {
        // Arrange
        let db = create_test_database().await;

        // Act & Assert
        let connection = db.connect().await.unwrap();
        assert!(connection.is_connected());
    }
}
```

### Производительность

#### Избегание ненужных аллокаций
```rust
// ✅ Хорошо
pub fn process_data(data: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(data.len());
    for &byte in data {
        if byte > 128 {
            result.push(byte);
        }
    }
    result
}

// ❌ Плохо
pub fn process_data(data: &[u8]) -> Vec<u8> {
    data.iter()
        .filter(|&&byte| byte > 128)
        .cloned()
        .collect()
}
```

#### Использование итераторов
```rust
// ✅ Хорошо
pub fn find_users_by_age(users: &[User], min_age: u32, max_age: u32) -> Vec<&User> {
    users.iter()
        .filter(|user| user.age >= min_age && user.age <= max_age)
        .collect()
}

// ❌ Плохо
pub fn find_users_by_age(users: &[User], min_age: u32, max_age: u32) -> Vec<&User> {
    let mut result = Vec::new();
    for user in users {
        if user.age >= min_age && user.age <= max_age {
            result.push(user);
        }
    }
    result
}
```

### Безопасность

#### Проверка входных данных
```rust
// ✅ Хорошо
pub fn create_table(name: &str, schema: &TableSchema) -> Result<Table, DatabaseError> {
    // Валидация имени таблицы
    if name.is_empty() {
        return Err(DatabaseError::InvalidTableName {
            name: name.to_string(),
            reason: "Table name cannot be empty".to_string(),
        });
    }
    
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(DatabaseError::InvalidTableName {
            name: name.to_string(),
            reason: "Table name contains invalid characters".to_string(),
        });
    }
    
    // Создание таблицы
    Ok(Table::new(name.to_string(), schema.clone()))
}

// ❌ Плохо
pub fn create_table(name: &str, schema: &TableSchema) -> Result<Table, DatabaseError> {
    Ok(Table::new(name.to_string(), schema.clone()))
}
```

## 🔧 Инструменты

### Автоматическое форматирование
```bash
# Форматирование кода
cargo fmt

# Проверка стиля
cargo clippy

# Запуск тестов
cargo test

# Проверка документации
cargo doc --no-deps
```

### Конфигурация rustfmt
```toml
# rustfmt.toml
edition = "2021"
max_width = 100
tab_spaces = 4
newline_style = "Unix"
use_small_heuristics = "Default"
```

### Конфигурация clippy
```toml
# .clippy.toml
# Разрешить некоторые предупреждения
# allow = ["clippy::all"]

# Настройки для конкретных правил
warn = [
    "clippy::pedantic",
    "clippy::nursery",
    "clippy::cargo",
]

# Игнорировать некоторые правила
allow = [
    "clippy::module_name_repetitions",
    "clippy::missing_errors_doc",
    "clippy::missing_panics_doc",
]
```

## 📚 Дополнительные ресурсы

- [Rust Book](https://doc.rust-lang.org/book/)
- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- [Rust Performance Book](https://nnethercote.github.io/perf-book/)
- [Rust Error Handling](https://blog.burntsushi.net/rust-error-handling/)

## 🤝 Вклад в проект

При внесении изменений в код:

1. Убедитесь, что код соответствует этим стандартам
2. Запустите `cargo fmt` и `cargo clippy`
3. Добавьте тесты для новой функциональности
4. Обновите документацию при необходимости
5. Создайте pull request с описанием изменений

Следование этим стандартам поможет поддерживать высокое качество кода и облегчит совместную работу над проектом.
