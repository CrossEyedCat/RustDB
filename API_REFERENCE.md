# Справочник API RustBD

## 📚 Обзор

RustBD предоставляет современный, высокопроизводительный API для работы с реляционными базами данных. API построен на принципах безопасности, производительности и удобства использования.

## 🏗️ Основные компоненты

### Database
Основной интерфейс для работы с базой данных.

```rust
pub struct Database {
    // Приватные поля
}

impl Database {
    /// Создает новое подключение к базе данных
    pub async fn connect(connection_string: &str) -> Result<Self, DatabaseError>
    
    /// Выполняет SQL запрос
    pub async fn execute(&self, sql: &str, params: &[&dyn ToValue]) -> Result<ExecuteResult, DatabaseError>
    
    /// Выполняет SELECT запрос
    pub async fn query(&self, sql: &str, params: &[&dyn ToValue]) -> Result<Vec<Row>, DatabaseError>
    
    /// Начинает новую транзакцию
    pub async fn begin_transaction(&self) -> Result<Transaction, DatabaseError>
    
    /// Создает новую таблицу
    pub async fn create_table(&self, schema: &TableSchema) -> Result<(), DatabaseError>
    
    /// Создает новый индекс
    pub async fn create_index(&self, table: &str, name: &str, columns: &[&str], index_type: IndexType) -> Result<(), DatabaseError>
}
```

### TableSchema
Определяет структуру таблицы.

```rust
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
    pub constraints: Vec<TableConstraint>,
}

impl TableSchema {
    /// Создает новую схему таблицы
    pub fn new(name: &str) -> Self
    
    /// Добавляет колонку в схему
    pub fn add_column(mut self, name: &str, column_type: ColumnType, constraints: Vec<ColumnConstraint>) -> Self
    
    /// Добавляет ограничение на уровне таблицы
    pub fn add_constraint(mut self, constraint: TableConstraint) -> Self
    
    /// Строит финальную схему
    pub fn build(self) -> Result<Self, SchemaError>
}
```

### ColumnType
Поддерживаемые типы данных.

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    /// 8-битное целое число
    TinyInt,
    /// 16-битное целое число
    SmallInt,
    /// 32-битное целое число
    Integer,
    /// 64-битное целое число
    BigInt,
    /// Число с плавающей точкой (32 бита)
    Float,
    /// Число с плавающей точкой (64 бита)
    Double,
    /// Строка фиксированной длины
    Char(usize),
    /// Строка переменной длины
    Varchar(usize),
    /// Текст неограниченной длины
    Text,
    /// Булево значение
    Boolean,
    /// Дата
    Date,
    /// Временная метка
    Timestamp,
    /// JSON данные
    Json,
    /// Массив
    Array(Box<ColumnType>),
    /// Бинарные данные
    Blob,
}
```

### ColumnConstraint
Ограничения на уровне колонок.

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraint {
    /// Значение не может быть NULL
    NotNull,
    /// Значение должно быть уникальным
    Unique,
    /// Первичный ключ
    PrimaryKey,
    /// Автоинкремент
    AutoIncrement,
    /// Значение по умолчанию
    Default(Value),
    /// Проверка значения
    Check(String),
    /// Внешний ключ
    ForeignKey {
        table: String,
        column: String,
        on_delete: ForeignKeyAction,
        on_update: ForeignKeyAction,
    },
}
```

### Value
Значения, которые могут храниться в базе данных.

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// NULL значение
    Null,
    /// Булево значение
    Boolean(bool),
    /// 8-битное целое число
    TinyInt(i8),
    /// 16-битное целое число
    SmallInt(i16),
    /// 32-битное целое число
    Integer(i32),
    /// 64-битное целое число
    BigInt(i64),
    /// Число с плавающей точкой (32 бита)
    Float(f32),
    /// Число с плавающей точкой (64 бита)
    Double(f64),
    /// Строка
    String(String),
    /// Дата
    Date(NaiveDate),
    /// Временная метка
    Timestamp(DateTime<Utc>),
    /// JSON данные
    Json(serde_json::Value),
    /// Массив
    Array(Vec<Value>),
    /// Бинарные данные
    Blob(Vec<u8>),
}
```

### Row
Строка данных из таблицы.

```rust
pub struct Row {
    data: HashMap<String, Value>,
}

impl Row {
    /// Создает новую пустую строку
    pub fn new() -> Self
    
    /// Устанавливает значение для колонки
    pub fn set(mut self, column: &str, value: Value) -> Self
    
    /// Строит финальную строку
    pub fn build(self) -> Result<Self, RowError>
    
    /// Получает значение колонки
    pub fn get(&self, column: &str) -> Result<&Value, RowError>
    
    /// Получает значение колонки с приведением типа
    pub fn get_as<T>(&self, column: &str) -> Result<T, RowError>
    where
        T: TryFrom<Value>,
        T::Error: Into<RowError>,
}
```

## 🔄 Транзакции

### Transaction
Интерфейс для работы с транзакциями.

```rust
pub struct Transaction {
    // Приватные поля
}

impl Transaction {
    /// Выполняет SQL запрос в рамках транзакции
    pub async fn execute(&self, sql: &str, params: &[&dyn ToValue]) -> Result<ExecuteResult, DatabaseError>
    
    /// Выполняет SELECT запрос в рамках транзакции
    pub async fn query(&self, sql: &str, params: &[&dyn ToValue]) -> Result<Vec<Row>, DatabaseError>
    
    /// Подтверждает транзакцию
    pub async fn commit(self) -> Result<(), DatabaseError>
    
    /// Откатывает транзакцию
    pub async fn rollback(self) -> Result<(), DatabaseError>
}
```

### Уровни изоляции

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum IsolationLevel {
    /// Чтение незафиксированных данных
    ReadUncommitted,
    /// Чтение зафиксированных данных
    ReadCommitted,
    /// Повторяемое чтение
    RepeatableRead,
    /// Сериализуемость
    Serializable,
}
```

## 🗂️ Индексы

### IndexType
Типы поддерживаемых индексов.

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum IndexType {
    /// B+ дерево (по умолчанию)
    BTree,
    /// Хеш-индекс
    Hash,
    /// Полнотекстовый индекс
    FullText,
    /// Пространственный индекс
    Spatial,
}
```

### IndexStatistics
Статистика использования индекса.

```rust
pub struct IndexStatistics {
    /// Количество использований индекса
    pub usage_count: u64,
    /// Селективность индекса (0.0 - 1.0)
    pub selectivity: f64,
    /// Размер индекса в байтах
    pub size_bytes: u64,
    /// Количество страниц в индексе
    pub page_count: u32,
}
```

## 🔐 Управление пользователями

### User
Информация о пользователе.

```rust
pub struct User {
    pub username: String,
    pub password_hash: String,
    pub role: UserRole,
    pub created_at: DateTime<Utc>,
    pub last_login: Option<DateTime<Utc>>,
    pub is_active: bool,
}

impl User {
    /// Создает нового пользователя
    pub fn new(username: &str) -> Self
    
    /// Устанавливает пароль
    pub fn with_password(mut self, password: &str) -> Self
    
    /// Устанавливает роль
    pub fn with_role(mut self, role: UserRole) -> Self
    
    /// Строит финального пользователя
    pub fn build(self) -> Result<Self, UserError>
}
```

### UserRole
Роли пользователей.

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum UserRole {
    /// Администратор
    Administrator,
    /// Обычный пользователь
    Regular,
    /// Пользователь только для чтения
    ReadOnly,
    /// Пользователь для разработки
    Developer,
}
```

### Permission
Права доступа.

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum Permission {
    /// Чтение данных
    Select,
    /// Вставка данных
    Insert,
    /// Обновление данных
    Update,
    /// Удаление данных
    Delete,
    /// Создание таблиц
    Create,
    /// Удаление таблиц
    Drop,
    /// Создание индексов
    CreateIndex,
    /// Удаление индексов
    DropIndex,
    /// Выполнение транзакций
    Execute,
}
```

### PermissionLevel
Уровни прав доступа.

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum PermissionLevel {
    /// Права на уровне базы данных
    Database,
    /// Права на уровне схемы
    Schema,
    /// Права на уровне таблицы
    Table,
    /// Права на уровне колонки
    Column,
}
```

## 📊 Мониторинг и метрики

### PerformanceMetrics
Метрики производительности.

```rust
pub struct PerformanceMetrics {
    /// Количество активных соединений
    pub active_connections: u32,
    /// Запросов в секунду
    pub queries_per_second: f64,
    /// Среднее время выполнения запроса
    pub avg_query_time: Duration,
    /// Hit ratio буфера (0.0 - 1.0)
    pub buffer_hit_ratio: f64,
    /// Количество транзакций в секунду
    pub transactions_per_second: f64,
    /// Размер буфера в байтах
    pub buffer_size_bytes: u64,
    /// Количество страниц в буфере
    pub buffer_page_count: u32,
}
```

### TableStatistics
Статистика таблицы.

```rust
pub struct TableStatistics {
    /// Количество строк в таблице
    pub row_count: u64,
    /// Размер таблицы в байтах
    pub size_bytes: u64,
    /// Количество страниц в таблице
    pub page_count: u32,
    /// Время последнего обновления статистики
    pub last_analyzed: DateTime<Utc>,
    /// Средний размер строки в байтах
    pub avg_row_size: u32,
}
```

## 🧪 Тестирование

### TestDatabase
Утилита для тестирования.

```rust
pub struct TestDatabase {
    // Приватные поля
}

impl TestDatabase {
    /// Создает новую тестовую базу данных
    pub async fn new() -> Self
    
    /// Создает тестовую базу данных с определенной схемой
    pub async fn with_schema(schema: &str) -> Self
    
    /// Очищает все данные
    pub async fn clear(&self) -> Result<(), DatabaseError>
    
    /// Уничтожает тестовую базу данных
    pub async fn destroy(self) -> Result<(), DatabaseError>
}
```

## 🔧 Конфигурация

### ConnectionConfig
Конфигурация подключения.

```rust
pub struct ConnectionConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
    pub max_connections: u32,
    pub connection_timeout: Duration,
    pub idle_timeout: Duration,
    pub ssl_mode: SslMode,
}

impl ConnectionConfig {
    /// Создает новую конфигурацию
    pub fn new() -> Self
    
    /// Устанавливает хост
    pub fn host(mut self, host: &str) -> Self
    
    /// Устанавливает порт
    pub fn port(mut self, port: u16) -> Self
    
    /// Устанавливает имя базы данных
    pub fn database(mut self, database: &str) -> Self
    
    /// Устанавливает имя пользователя
    pub fn username(mut self, username: &str) -> Self
    
    /// Устанавливает пароль
    pub fn password(mut self, password: &str) -> Self
    
    /// Устанавливает максимальное количество соединений
    pub fn max_connections(mut self, max_connections: u32) -> Self
    
    /// Устанавливает таймаут подключения
    pub fn connection_timeout(mut self, timeout: Duration) -> Self
    
    /// Устанавливает таймаут простоя
    pub fn idle_timeout(mut self, timeout: Duration) -> Self
    
    /// Устанавливает режим SSL
    pub fn ssl_mode(mut self, ssl_mode: SslMode) -> Self
    
    /// Строит финальную конфигурацию
    pub fn build(self) -> Result<Self, ConfigError>
}
```

### SslMode
Режимы SSL подключения.

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum SslMode {
    /// SSL отключен
    Disable,
    /// SSL разрешен
    Allow,
    /// SSL предпочтителен
    Prefer,
    /// SSL обязателен
    Require,
    /// SSL обязателен с проверкой сертификата
    VerifyCa,
    /// SSL обязателен с полной проверкой
    VerifyFull,
}
```

### LogConfig
Конфигурация логирования.

```rust
pub struct LogConfig {
    pub level: LogLevel,
    pub file: Option<String>,
    pub max_size: u64,
    pub max_files: u32,
    pub format: LogFormat,
}

impl LogConfig {
    /// Создает новую конфигурацию логирования
    pub fn new() -> Self
    
    /// Устанавливает уровень логирования
    pub fn level(mut self, level: LogLevel) -> Self
    
    /// Устанавливает файл для логирования
    pub fn file(mut self, file: &str) -> Self
    
    /// Устанавливает максимальный размер файла
    pub fn max_size(mut self, size: u64) -> Self
    
    /// Устанавливает максимальное количество файлов
    pub fn max_files(mut self, count: u32) -> Self
    
    /// Устанавливает формат логирования
    pub fn format(mut self, format: LogFormat) -> Self
    
    /// Строит финальную конфигурацию
    pub fn build(self) -> Result<Self, ConfigError>
}
```

### LogLevel
Уровни логирования.

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum LogLevel {
    /// Трассировка
    Trace,
    /// Отладка
    Debug,
    /// Информация
    Info,
    /// Предупреждение
    Warn,
    /// Ошибка
    Error,
}
```

### LogFormat
Форматы логирования.

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum LogFormat {
    /// Простой текстовый формат
    Simple,
    /// JSON формат
    Json,
    /// Структурированный формат
    Structured,
}
```

## 🚨 Обработка ошибок

### DatabaseError
Основной тип ошибок базы данных.

```rust
#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("Ошибка подключения: {message}")]
    ConnectionError { message: String },
    
    #[error("Ошибка SQL: {sql}, детали: {details}")]
    SqlError { sql: String, details: String },
    
    #[error("Ошибка транзакции: {0}")]
    TransactionError(#[from] TransactionError),
    
    #[error("Ошибка схемы: {0}")]
    SchemaError(#[from] SchemaError),
    
    #[error("Ошибка ввода-вывода: {0}")]
    IoError(#[from] std::io::Error),
    
    #[error("Ошибка аутентификации: {message}")]
    AuthenticationError { message: String },
    
    #[error("Ошибка авторизации: {message}")]
    AuthorizationError { message: String },
    
    #[error("Ошибка блокировки: {message}")]
    LockError { message: String },
    
    #[error("Ошибка индекса: {message}")]
    IndexError { message: String },
    
    #[error("Ошибка пользователя: {0}")]
    UserError(#[from] UserError),
    
    #[error("Ошибка конфигурации: {0}")]
    ConfigError(#[from] ConfigError),
    
    #[error("Ошибка строки: {0}")]
    RowError(#[from] RowError),
}
```

### Специфические ошибки

```rust
#[derive(Error, Debug)]
pub enum TransactionError {
    #[error("Транзакция уже завершена")]
    AlreadyCommitted,
    #[error("Транзакция уже откачена")]
    AlreadyRolledBack,
    #[error("Deadlock обнаружен")]
    DeadlockDetected,
    #[error("Таймаут транзакции")]
    Timeout,
}

#[derive(Error, Debug)]
pub enum SchemaError {
    #[error("Таблица уже существует: {table}")]
    TableAlreadyExists { table: String },
    #[error("Таблица не найдена: {table}")]
    TableNotFound { table: String },
    #[error("Колонка уже существует: {table}.{column}")]
    ColumnAlreadyExists { table: String, column: String },
    #[error("Колонка не найдена: {table}.{column}")]
    ColumnNotFound { table: String, column: String },
    #[error("Неверный тип данных: {expected}, получен {actual}")]
    InvalidDataType { expected: String, actual: String },
}

#[derive(Error, Debug)]
pub enum UserError {
    #[error("Пользователь уже существует: {username}")]
    UserAlreadyExists { username: String },
    #[error("Пользователь не найден: {username}")]
    UserNotFound { username: String },
    #[error("Неверный пароль для пользователя: {username}")]
    InvalidPassword { username: String },
    #[error("Пользователь неактивен: {username}")]
    UserInactive { username: String },
}
```

## 📝 Примеры использования

### Создание и настройка базы данных

```rust
use rustbd::{Database, ConnectionConfig, LogConfig, LogLevel};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Настройка логирования
    let log_config = LogConfig::new()
        .level(LogLevel::Info)
        .file("rustbd.log")
        .max_size(100 * 1024 * 1024)
        .max_files(5)
        .build()?;
    
    rustbd::init_logging(log_config)?;
    
    // Конфигурация подключения
    let config = ConnectionConfig::new()
        .host("localhost")
        .port(5432)
        .database("mydb")
        .username("myuser")
        .password("mypassword")
        .max_connections(20)
        .connection_timeout(Duration::from_secs(30))
        .idle_timeout(Duration::from_secs(300))
        .build()?;
    
    // Подключение к базе данных
    let db = Database::connect_with_config(&config).await?;
    
    println!("Подключение к базе данных установлено!");
    
    Ok(())
}
```

### Работа с транзакциями

```rust
use rustbd::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::connect("localhost:5432").await?;
    
    // Начало транзакции
    let transaction = db.begin_transaction().await?;
    
    // Выполнение операций
    transaction.execute("INSERT INTO users (username, email) VALUES (?, ?)", &["user1", "user1@example.com"]).await?;
    transaction.execute("INSERT INTO users (username, email) VALUES (?, ?)", &["user2", "user2@example.com"]).await?;
    
    // Проверка результатов
    let users = transaction.query("SELECT * FROM users WHERE username IN (?, ?)", &["user1", "user2"]).await?;
    
    if users.len() == 2 {
        transaction.commit().await?;
        println!("Транзакция выполнена успешно!");
    } else {
        transaction.rollback().await?;
        println!("Транзакция отменена!");
    }
    
    Ok(())
}
```

### Управление пользователями и правами

```rust
use rustbd::{Database, User, UserRole, Permission, PermissionLevel};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::connect("localhost:5432").await?;
    
    // Создание пользователя
    let user = User::new("new_user")
        .with_password("secure_password")
        .with_role(UserRole::Regular)
        .build()?;
    
    db.create_user(&user).await?;
    
    // Назначение прав
    db.grant_permission("new_user", "users", Permission::Select, PermissionLevel::Table).await?;
    db.grant_permission("new_user", "users", Permission::Insert, PermissionLevel::Table).await?;
    
    println!("Пользователь создан и права назначены!");
    
    Ok(())
}
```

## 🔗 Дополнительные ресурсы

- [Примеры использования](EXAMPLES.md)
- [Стандарты кодирования](CODING_STANDARDS.md)
- [Архитектура системы](ARCHITECTURE.md)
- [Руководство по разработке](DEVELOPMENT.md)
- [Руководство по вкладу](CONTRIBUTING.md)

Для получения дополнительной информации или помощи обратитесь к документации проекта или создайте issue в репозитории.
