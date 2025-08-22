# Руководство по rustdoc для RustBD

## 📚 Обзор

rustdoc - это встроенный инструмент Rust для генерации документации из комментариев в коде. Это руководство описывает, как правильно документировать код RustBD для создания качественной API документации.

## 🔧 Генерация документации

### Базовые команды

```bash
# Генерация документации для текущего проекта
cargo doc

# Генерация документации без зависимостей
cargo doc --no-deps

# Генерация документации с открытием в браузере
cargo doc --open

# Генерация документации для конкретной цели
cargo doc --target x86_64-unknown-linux-gnu

# Генерация документации с приватными элементами
cargo doc --document-private-items
```

### Конфигурация в Cargo.toml

```toml
[package]
name = "rustbd"
version = "0.1.0"
description = "High-performance database system written in Rust"
documentation = "https://docs.rs/rustbd"
repository = "https://github.com/your-org/rustbd"

[package.metadata.docs.rs]
# Настройки для docs.rs
rustdoc-args = ["--cfg", "docsrs"]
features = ["docs"]
```

## 📝 Документирование кода

### Документирование модулей

```rust
//! # RustBD - Высокопроизводительная система баз данных
//!
//! RustBD - это современная система управления базами данных, написанная на Rust.
//! Система обеспечивает высокую производительность, безопасность и надежность.
//!
//! ## Основные возможности
//!
//! - ACID транзакции
//! - Поддержка SQL
//! - Высокая производительность
//! - Безопасность памяти
//!
//! ## Быстрый старт
//!
//! ```rust
//! use rustbd::Database;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let db = Database::connect("localhost:5432").await?;
//!     println!("Подключение установлено!");
//!     Ok(())
//! }
//! ```
//!
//! ## Архитектура
//!
//! Система состоит из следующих основных компонентов:
//!
//! - **Storage**: Управление хранением данных
//! - **Executor**: Выполнение запросов
//! - **Parser**: Парсинг SQL
//! - **Network**: Сетевой слой
//!
//! ## Лицензия
//!
//! MIT License - см. файл [LICENSE](LICENSE) для деталей.

pub mod storage;
pub mod executor;
pub mod parser;
pub mod network;
```

### Документирование структур

```rust
/// Представляет соединение с базой данных.
///
/// Структура `DatabaseConnection` содержит всю необходимую информацию
/// для работы с базой данных, включая сокет, состояние и метаданные.
///
/// # Примеры
///
/// ## Создание соединения
///
/// ```rust
/// use rustbd::DatabaseConnection;
/// use std::time::Duration;
///
/// let connection = DatabaseConnection::new("localhost", 5432)
///     .timeout(Duration::from_secs(30))
///     .build()?;
/// ```
///
/// ## Выполнение запроса
///
/// ```rust
/// let result = connection.execute("SELECT * FROM users").await?;
/// for row in result {
///     println!("Пользователь: {:?}", row);
/// }
/// ```
///
/// # Безопасность
///
/// Соединение автоматически закрывается при выходе из области видимости.
/// Для явного закрытия используйте метод [`close`].
///
/// # Производительность
///
/// Соединения могут быть переиспользованы через пул соединений.
/// См. [`ConnectionPool`] для деталей.
///
/// [`close`]: #method.close
/// [`ConnectionPool`]: struct.ConnectionPool.html
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
    /// Таймаут подключения
    pub timeout: Duration,
}
```

### Документирование методов

```rust
impl DatabaseConnection {
    /// Создает новое соединение с базой данных.
    ///
    /// # Arguments
    ///
    /// * `host` - Хост сервера базы данных (например, "localhost")
    /// * `port` - Порт сервера (например, 5432)
    ///
    /// # Returns
    ///
    /// Возвращает `Ok(DatabaseConnection)` при успешном создании
    /// или `Err(ConnectionError)` при ошибке.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use rustbd::DatabaseConnection;
    ///
    /// let connection = DatabaseConnection::new("localhost", 5432)?;
    /// ```
    ///
    /// # Errors
    ///
    /// Функция может вернуть следующие ошибки:
    ///
    /// - `ConnectionError::InvalidHost` - неверный формат хоста
    /// - `ConnectionError::InvalidPort` - неверный номер порта
    ///
    /// # Panics
    ///
    /// Функция может вызвать панику в следующих случаях:
    ///
    /// - Хост содержит недопустимые символы
    /// - Порт равен 0
    ///
    /// # Асинхронность
    ///
    /// Эта функция не является асинхронной, но создает соединение,
    /// которое может использоваться в асинхронном контексте.
    pub fn new(host: &str, port: u16) -> Result<Self, ConnectionError> {
        // Реализация...
    }

    /// Устанавливает таймаут для операций с базой данных.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Таймаут в секундах
    ///
    /// # Returns
    ///
    /// Возвращает `&mut Self` для цепочки вызовов.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use rustbd::DatabaseConnection;
    /// use std::time::Duration;
    ///
    /// let connection = DatabaseConnection::new("localhost", 5432)?
    ///     .timeout(Duration::from_secs(30))
    ///     .build()?;
    /// ```
    ///
    /// # Примечания
    ///
    /// - Таймаут применяется ко всем операциям чтения/записи
    /// - Значение по умолчанию: 30 секунд
    /// - Минимальное значение: 1 секунда
    pub fn timeout(&mut self, timeout: Duration) -> &mut Self {
        self.timeout = timeout;
        self
    }

    /// Выполняет SQL запрос.
    ///
    /// # Arguments
    ///
    /// * `sql` - SQL запрос для выполнения
    /// * `params` - Параметры для подготовленного запроса
    ///
    /// # Returns
    ///
    /// Возвращает `Ok(QueryResult)` при успешном выполнении
    /// или `Err(DatabaseError)` при ошибке.
    ///
    /// # Examples
    ///
    /// ## Простой запрос
    ///
    /// ```rust
    /// let result = connection.execute("SELECT * FROM users").await?;
    /// ```
    ///
    /// ## Запрос с параметрами
    ///
    /// ```rust
    /// let result = connection.execute(
    ///     "SELECT * FROM users WHERE age > ?",
    ///     &[&18]
    /// ).await?;
    /// ```
    ///
    /// ## Запрос с несколькими параметрами
    ///
    /// ```rust
    /// let result = connection.execute(
    ///     "INSERT INTO users (name, age) VALUES (?, ?)",
    ///     &[&"John", &25]
    /// ).await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Функция может вернуть следующие ошибки:
    ///
    /// - `DatabaseError::SqlError` - ошибка в SQL запросе
    /// - `DatabaseError::ConnectionError` - ошибка соединения
    /// - `DatabaseError::TimeoutError` - превышен таймаут
    ///
    /// # Безопасность
    ///
    /// Все параметры автоматически экранируются для предотвращения SQL-инъекций.
    /// Используйте подготовленные запросы для лучшей производительности.
    ///
    /// # Производительность
    ///
    /// - Запросы кэшируются для повторного использования
    /// - Параметры передаются по ссылке для минимизации копирования
    /// - Асинхронное выполнение не блокирует поток
    pub async fn execute(
        &self,
        sql: &str,
        params: &[&dyn ToValue],
    ) -> Result<QueryResult, DatabaseError> {
        // Реализация...
    }
}
```

### Документирование перечислений

```rust
/// Представляет состояние соединения с базой данных.
///
/// Состояние соединения отслеживается для обеспечения корректной
/// работы и диагностики проблем.
///
/// # Примеры
///
/// ```rust
/// use rustbd::ConnectionState;
///
/// match connection.state() {
///     ConnectionState::Connected => println!("Соединение активно"),
///     ConnectionState::Connecting => println!("Подключение..."),
///     ConnectionState::Disconnected => println!("Соединение разорвано"),
///     ConnectionState::Error(ref error) => println!("Ошибка: {}", error),
/// }
/// ```
///
/// # Переходы состояний
///
/// ```mermaid
/// graph LR
///     A[Disconnected] --> B[Connecting]
///     B --> C[Connected]
///     B --> D[Error]
///     C --> A
///     D --> A
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionState {
    /// Соединение активно и готово к использованию
    Connected,
    
    /// Выполняется подключение к серверу
    Connecting,
    
    /// Соединение разорвано или не установлено
    Disconnected,
    
    /// Произошла ошибка при работе с соединением
    Error(ConnectionError),
}
```

### Документирование трейтов

```rust
/// Трейт для типов, которые могут быть преобразованы в значения базы данных.
///
/// Этот трейт позволяет различным типам Rust автоматически
/// преобразовываться в типы, поддерживаемые базой данных.
///
/// # Примеры
///
/// ## Реализация для стандартных типов
///
/// ```rust
/// impl ToValue for String {
///     fn to_value(&self) -> Value {
///         Value::String(self.clone())
///     }
/// }
///
/// impl ToValue for i32 {
///     fn to_value(&self) -> Value {
///         Value::Integer(*self)
///     }
/// }
/// ```
///
/// ## Использование в запросах
///
/// ```rust
/// let name = "John".to_string();
/// let age = 25;
///
/// let result = connection.execute(
///     "INSERT INTO users (name, age) VALUES (?, ?)",
///     &[&name, &age]
/// ).await?;
/// ```
///
/// # Требования
///
/// - Тип должен быть `Send + Sync` для использования в асинхронном контексте
/// - Реализация должна быть идемпотентной
/// - Значение должно быть сериализуемым
///
/// # Производительность
///
/// - Преобразование происходит лениво при выполнении запроса
/// - Результат может кэшироваться для повторного использования
/// - Избегайте создания больших объектов в `to_value`
pub trait ToValue: Send + Sync {
    /// Преобразует значение в тип базы данных.
    ///
    /// # Returns
    ///
    /// Возвращает `Value`, представляющее данное значение в базе данных.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let value = 42.to_value();
    /// assert!(matches!(value, Value::Integer(42)));
    /// ```
    fn to_value(&self) -> Value;
}
```

## 🔗 Ссылки и перекрестные ссылки

### Внутренние ссылки

```rust
/// Создает новое соединение с базой данных.
///
/// Для получения дополнительной информации см.:
/// - [`DatabaseConnection`] - основная структура соединения
/// - [`ConnectionPool`] - пул соединений для высокой нагрузки
/// - [`ConnectionError`] - типы ошибок соединения
///
/// [`DatabaseConnection`]: struct.DatabaseConnection.html
/// [`ConnectionPool`]: struct.ConnectionPool.html
/// [`ConnectionError`]: enum.ConnectionError.html
pub fn create_connection(host: &str, port: u16) -> Result<DatabaseConnection, ConnectionError> {
    // ...
}
```

### Ссылки на модули

```rust
/// Основной модуль для работы с базой данных.
///
/// См. также:
/// - [`storage`] - управление хранением данных
/// - [`executor`] - выполнение запросов
/// - [`parser`] - парсинг SQL
///
/// [`storage`]: storage/index.html
/// [`executor`]: executor/index.html
/// [`parser`]: parser/index.html
pub mod database {
    // ...
}
```

### Ссылки на внешние ресурсы

```rust
/// Выполняет SQL запрос.
///
/// # Спецификация SQL
///
/// Поддерживается стандарт SQL:2016. См. [SQL стандарт](https://www.iso.org/standard/63555.html)
/// для детального описания синтаксиса.
///
/// # Безопасность
///
/// Все параметры автоматически экранируются. См. [OWASP SQL Injection](https://owasp.org/www-community/attacks/SQL_Injection)
/// для понимания рисков.
pub async fn execute(&self, sql: &str, params: &[&dyn ToValue]) -> Result<QueryResult, DatabaseError> {
    // ...
}
```

## 📊 Примеры кода

### Тестируемые примеры

```rust
/// Создает новую таблицу пользователей.
///
/// # Examples
///
/// ```rust
/// use rustbd::{Database, TableSchema, ColumnType, ColumnConstraint};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let db = Database::connect("localhost:5432").await?;
///     
///     let schema = TableSchema::new("users")
///         .add_column("id", ColumnType::Integer, vec![ColumnConstraint::PrimaryKey])
///         .add_column("username", ColumnType::Varchar(50), vec![ColumnConstraint::NotNull])
///         .build()?;
///     
///     db.create_table(&schema).await?;
///     println!("Таблица создана!");
///     Ok(())
/// }
/// ```
///
/// # Errors
///
/// Функция может вернуть ошибку, если:
/// - Таблица уже существует
/// - Схема содержит недопустимые элементы
/// - Нет прав на создание таблицы
pub async fn create_users_table(db: &Database) -> Result<(), DatabaseError> {
    // ...
}
```

### Примеры с обработкой ошибок

```rust
/// Подключается к базе данных с повторными попытками.
///
/// # Examples
///
/// ```rust,no_run
/// use rustbd::Database;
/// use std::time::Duration;
///
/// let db = connect_with_retry("localhost:5432", 3).await?;
/// ```
///
/// ```rust,ignore
/// // Этот пример игнорируется при тестировании
/// let db = Database::connect("invalid-host").await?;
/// ```
///
/// # Panics
///
/// Функция может вызвать панику, если:
/// - Количество попыток равно 0
/// - Таймаут между попытками равен 0
pub async fn connect_with_retry(
    connection_string: &str,
    max_retries: u32,
) -> Result<Database, DatabaseError> {
    // ...
}
```

## 🚨 Документирование ошибок

### Структура ошибок

```rust
/// Ошибки, которые могут возникнуть при работе с базой данных.
///
/// # Примеры
///
/// ## Обработка ошибок подключения
///
/// ```rust
/// use rustbd::DatabaseError;
///
/// match db.execute("SELECT 1").await {
///     Ok(result) => println!("Успех: {:?}", result),
///     Err(DatabaseError::ConnectionError { message }) => {
///         eprintln!("Ошибка подключения: {}", message);
///     }
///     Err(DatabaseError::SqlError { sql, details }) => {
///         eprintln!("SQL ошибка в '{}': {}", sql, details);
///     }
///     Err(other) => eprintln!("Неожиданная ошибка: {:?}", other),
/// }
/// ```
///
/// ## Создание пользовательских ошибок
///
/// ```rust
/// use rustbd::DatabaseError;
///
/// let error = DatabaseError::CustomError {
///     code: "E001",
///     message: "Пользовательская ошибка".to_string(),
/// };
/// ```
#[derive(Error, Debug)]
pub enum DatabaseError {
    /// Ошибка подключения к базе данных
    #[error("Ошибка подключения: {message}")]
    ConnectionError { message: String },
    
    /// Ошибка в SQL запросе
    #[error("SQL ошибка в '{}': {}", sql, details)]
    SqlError { sql: String, details: String },
    
    /// Пользовательская ошибка
    #[error("Ошибка {code}: {message}")]
    CustomError { code: String, message: String },
}
```

## 🔧 Конфигурация rustdoc

### Настройки в .cargo/config.toml

```toml
[target.x86_64-unknown-linux-gnu]
rustflags = ["-C", "target-feature=+crt-static"]

[doc]
# Настройки для rustdoc
rustdoc-args = [
    "--html-in-header", "docs/header.html",
    "--html-before-content", "docs/before.html",
    "--html-after-content", "docs/after.html",
    "--markdown-css", "docs/style.css",
    "--markdown-before-content", "docs/before.md",
    "--markdown-after-content", "docs/after.md",
]
```

### Кастомные CSS и HTML

```html
<!-- docs/header.html -->
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>RustBD Documentation</title>
    <link rel="stylesheet" href="style.css">
</head>
<body>
```

```css
/* docs/style.css */
body {
    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    line-height: 1.6;
    color: #333;
}

.rustdoc {
    max-width: 1200px;
    margin: 0 auto;
    padding: 20px;
}

.docblock {
    background: #f9f9f9;
    border-left: 4px solid #007acc;
    padding: 15px;
    margin: 15px 0;
}
```

## 📚 Публикация документации

### На docs.rs

```toml
# Cargo.toml
[package.metadata.docs.rs]
# Включить все функции
all-features = true
# Настройки rustdoc
rustdoc-args = ["--cfg", "docsrs"]
# Целевые платформы
targets = ["x86_64-unknown-linux-gnu"]
```

### На GitHub Pages

```yaml
# .github/workflows/docs.yml
name: Deploy Documentation

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    
    - name: Setup Rust
      uses: actions-rs/toolchain@v1
      with:
        toolchain: stable
        
    - name: Generate Documentation
      run: cargo doc --no-deps
      
    - name: Deploy to GitHub Pages
      uses: peaceiris/actions-gh-pages@v3
      with:
        github_token: ${{ secrets.GITHUB_TOKEN }}
        publish_dir: ./target/doc
```

## 🧪 Тестирование документации

### Запуск тестов документации

```bash
# Тестирование примеров в документации
cargo test --doc

# Тестирование конкретного модуля
cargo test --doc --package rustbd --lib

# Тестирование с выводом
cargo test --doc -- --nocapture
```

### Проверка качества документации

```bash
# Проверка покрытия документацией
cargo doc --document-private-items --open

# Проверка ссылок
cargo doc --no-deps --open

# Проверка стиля
cargo clippy -- -D clippy::missing_docs_in_private_items
```

## 📖 Лучшие практики

### Общие принципы

1. **Полнота**: Документируйте все публичные элементы
2. **Ясность**: Используйте простой и понятный язык
3. **Примеры**: Предоставляйте практические примеры
4. **Обработка ошибок**: Описывайте все возможные ошибки
5. **Производительность**: Указывайте особенности производительности

### Структура документации

1. **Краткое описание** - что делает элемент
2. **Подробное описание** - как это работает
3. **Примеры** - практическое использование
4. **Ошибки** - что может пойти не так
5. **Ссылки** - связанные элементы

### Стиль написания

1. **Активный залог**: "Функция создает..." вместо "Создается функцией..."
2. **Настоящее время**: "Функция возвращает..." вместо "Функция вернет..."
3. **Конкретность**: "Возвращает количество строк" вместо "Возвращает результат"
4. **Последовательность**: Используйте единообразную терминологию

## 🔗 Дополнительные ресурсы

- [Rust Book - Documentation](https://doc.rust-lang.org/book/ch14-02-publishing-to-crates-io.html#documentation-comments)
- [rustdoc Book](https://doc.rust-lang.org/rustdoc/)
- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/documentation.html)
- [docs.rs](https://docs.rs/)

Следуя этим рекомендациям, вы создадите качественную документацию, которая поможет разработчикам эффективно использовать RustBD.
