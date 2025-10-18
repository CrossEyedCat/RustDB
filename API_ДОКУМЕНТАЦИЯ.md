# Справочная документация API RustDB

## Содержание
1. [Основные типы](#основные-типы)
2. [Подключение к базе данных](#подключение-к-базе-данных)
3. [Управление транзакциями](#управление-транзакциями)
4. [Выполнение запросов](#выполнение-запросов)
5. [Слой хранения](#слой-хранения)
6. [Логирование и восстановление](#логирование-и-восстановление)
7. [Управление конкурентностью](#управление-конкурентностью)
8. [Отладка и профилирование](#отладка-и-профилирование)
9. [Обработка ошибок](#обработка-ошибок)
10. [Конфигурация](#конфигурация)

## Основные типы

### Database
Основной тип подключения к базе данных.

```rust
pub struct Database {
    // Приватные поля
}

impl Database {
    /// Подключиться к экземпляру базы данных
    pub async fn connect(connection_string: &str) -> Result<Self, Error>;
    
    /// Подключиться с пользовательской конфигурацией
    pub async fn connect_with_config(
        connection_string: &str, 
        config: DatabaseConfig
    ) -> Result<Self, Error>;
    
    /// Выполнить SQL-запрос
    pub async fn execute(&self, sql: &str) -> Result<QueryResult, Error>;
    
    /// Выполнить подготовленный запрос
    pub async fn execute_prepared(
        &self, 
        stmt: &PreparedStatement, 
        params: &[Value]
    ) -> Result<QueryResult, Error>;
    
    /// Начать новую транзакцию
    pub async fn begin_transaction(
        &self, 
        isolation_level: IsolationLevel
    ) -> Result<Transaction, Error>;
    
    /// Получить статистику базы данных
    pub async fn get_stats(&self) -> Result<DatabaseStats, Error>;
}
```

### Transaction
Представляет транзакцию базы данных.

```rust
pub struct Transaction {
    // Приватные поля
}

impl Transaction {
    /// Выполнить SQL-запрос в рамках транзакции
    pub async fn execute(&self, sql: &str) -> Result<QueryResult, Error>;
    
    /// Подтвердить транзакцию
    pub async fn commit(self) -> Result<(), Error>;
    
    /// Откатить транзакцию
    pub async fn rollback(self) -> Result<(), Error>;
    
    /// Получить ID транзакции
    pub fn id(&self) -> TransactionId;
    
    /// Получить уровень изоляции
    pub fn isolation_level(&self) -> IsolationLevel;
}
```

### QueryResult
Результат выполнения запроса.

```rust
pub struct QueryResult {
    pub rows: Vec<Row>,
    pub columns: Vec<ColumnInfo>,
    pub affected_rows: usize,
    pub execution_time: Duration,
}

pub struct Row {
    pub values: Vec<Value>,
}

pub struct ColumnInfo {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}
```

## Подключение к базе данных

### Методы подключения

```rust
// Базовое подключение
let db = Database::connect("localhost:8080").await?;

// Подключение с конфигурацией
let config = DatabaseConfig::default()
    .with_timeout(Duration::from_secs(30))
    .with_max_connections(100);
let db = Database::connect_with_config("localhost:8080", config).await?;

// Подключение с аутентификацией
let db = Database::connect("user:password@localhost:8080/database").await?;
```

### Пул подключений

```rust
pub struct ConnectionPool {
    // Приватные поля
}

impl ConnectionPool {
    /// Создать новый пул подключений
    pub fn new(config: PoolConfig) -> Self;
    
    /// Получить подключение из пула
    pub async fn get_connection(&self) -> Result<PooledConnection, Error>;
    
    /// Получить статистику пула
    pub fn stats(&self) -> PoolStats;
}

pub struct PooledConnection {
    connection: Database,
    pool: Arc<ConnectionPool>,
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        // Возврат подключения в пул
    }
}
```

## Управление транзакциями

### Уровни изоляции

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}
```

### Операции с транзакциями

```rust
// Начать транзакцию с определенным уровнем изоляции
let tx = db.begin_transaction(IsolationLevel::ReadCommitted).await?;

// Выполнить операции в рамках транзакции
tx.execute("INSERT INTO users (name) VALUES ('Иван')").await?;
tx.execute("UPDATE accounts SET balance = balance - 100").await?;

// Подтвердить или откатить
match some_condition {
    true => tx.commit().await?,
    false => tx.rollback().await?,
}
```

### Точки сохранения

```rust
impl Transaction {
    /// Создать точку сохранения
    pub async fn create_savepoint(&self, name: &str) -> Result<(), Error>;
    
    /// Откатиться к точке сохранения
    pub async fn rollback_to_savepoint(&self, name: &str) -> Result<(), Error>;
    
    /// Освободить точку сохранения
    pub async fn release_savepoint(&self, name: &str) -> Result<(), Error>;
}

// Пример использования
let tx = db.begin_transaction(IsolationLevel::ReadCommitted).await?;
tx.execute("INSERT INTO users (name) VALUES ('Иван')").await?;
tx.create_savepoint("after_insert").await?;

tx.execute("UPDATE accounts SET balance = balance - 100").await?;
if some_error_condition {
    tx.rollback_to_savepoint("after_insert").await?;
}
tx.commit().await?;
```

## Выполнение запросов

### Базовое выполнение запросов

```rust
// Выполнить SELECT запрос
let result = db.execute("SELECT * FROM users WHERE age > 18").await?;
for row in result.rows {
    let name: String = row.get("name")?;
    let age: i32 = row.get("age")?;
    println!("Имя: {}, Возраст: {}", name, age);
}

// Выполнить INSERT запрос
let result = db.execute("INSERT INTO users (name, age) VALUES ('Иван', 25)").await?;
println!("Вставлено {} строк", result.affected_rows);

// Выполнить UPDATE запрос
let result = db.execute("UPDATE users SET age = 26 WHERE name = 'Иван'").await?;
println!("Обновлено {} строк", result.affected_rows);
```

### Подготовленные запросы

```rust
pub struct PreparedStatement {
    // Приватные поля
}

impl Database {
    /// Подготовить SQL-запрос
    pub async fn prepare(&self, sql: &str) -> Result<PreparedStatement, Error>;
}

// Пример использования
let stmt = db.prepare("INSERT INTO users (name, age) VALUES (?, ?)").await?;
let result = db.execute_prepared(&stmt, &[Value::String("Иван".to_string()), Value::Integer(25)]).await?;
```

### Пакетные операции

```rust
impl Database {
    /// Выполнить несколько запросов в пакете
    pub async fn execute_batch(&self, statements: &[String]) -> Result<Vec<QueryResult>, Error>;
    
    /// Выполнить пакет с параметрами
    pub async fn execute_batch_prepared(
        &self, 
        stmt: &PreparedStatement, 
        param_sets: &[Vec<Value>]
    ) -> Result<Vec<QueryResult>, Error>;
}

// Пример использования
let statements = vec![
    "INSERT INTO users (name) VALUES ('Иван')".to_string(),
    "INSERT INTO users (name) VALUES ('Петр')".to_string(),
    "INSERT INTO users (name) VALUES ('Анна')".to_string(),
];
let results = db.execute_batch(&statements).await?;
```

## Слой хранения

### Менеджер страниц

```rust
pub struct PageManager {
    // Приватные поля
}

impl PageManager {
    /// Создать новый менеджер страниц
    pub fn new(config: PageManagerConfig) -> Self;
    
    /// Прочитать страницу
    pub async fn read_page(&self, page_id: PageId) -> Result<Page, Error>;
    
    /// Записать страницу
    pub async fn write_page(&self, page: &Page) -> Result<(), Error>;
    
    /// Выделить новую страницу
    pub async fn allocate_page(&self) -> Result<PageId, Error>;
    
    /// Освободить страницу
    pub async fn deallocate_page(&self, page_id: PageId) -> Result<(), Error>;
}
```

### Менеджер буферов

```rust
pub struct BufferManager {
    // Приватные поля
}

impl BufferManager {
    /// Создать новый менеджер буферов
    pub fn new(config: BufferConfig) -> Self;
    
    /// Закрепить страницу в пуле буферов
    pub async fn pin_page(&self, page_id: PageId) -> Result<PageRef, Error>;
    
    /// Открепить страницу
    pub async fn unpin_page(&self, page_id: PageId) -> Result<(), Error>;
    
    /// Сбросить грязные страницы на диск
    pub async fn flush_pages(&self) -> Result<(), Error>;
    
    /// Получить статистику пула буферов
    pub fn get_stats(&self) -> BufferStats;
}
```

### Управление индексами

```rust
pub struct IndexManager {
    // Приватные поля
}

impl IndexManager {
    /// Создать новый индекс
    pub async fn create_index(
        &self, 
        table_name: &str, 
        columns: &[String], 
        index_type: IndexType
    ) -> Result<IndexId, Error>;
    
    /// Удалить индекс
    pub async fn drop_index(&self, index_id: IndexId) -> Result<(), Error>;
    
    /// Вставить пару ключ-значение в индекс
    pub async fn insert(&self, index_id: IndexId, key: &Value, value: &Value) -> Result<(), Error>;
    
    /// Удалить ключ из индекса
    pub async fn delete(&self, index_id: IndexId, key: &Value) -> Result<(), Error>;
    
    /// Найти ключ в индексе
    pub async fn search(&self, index_id: IndexId, key: &Value) -> Result<Option<Value>, Error>;
    
    /// Сканирование диапазона в индексе
    pub async fn range_scan(
        &self, 
        index_id: IndexId, 
        start: &Value, 
        end: &Value
    ) -> Result<IndexIterator, Error>;
}
```

## Логирование и восстановление

### Write-Ahead Log (WAL)

```rust
pub struct WriteAheadLog {
    // Приватные поля
}

impl WriteAheadLog {
    /// Создать новый WAL
    pub fn new(config: WalConfig) -> Self;
    
    /// Записать запись в лог
    pub async fn write_record(&self, record: LogRecord) -> Result<LogSequenceNumber, Error>;
    
    /// Сбросить лог на диск
    pub async fn flush(&self) -> Result<(), Error>;
    
    /// Прочитать записи лога с определенного LSN
    pub async fn read_from(&self, lsn: LogSequenceNumber) -> Result<LogIterator, Error>;
    
    /// Получить текущий LSN
    pub fn current_lsn(&self) -> LogSequenceNumber;
}
```

### Менеджер восстановления

```rust
pub struct RecoveryManager {
    // Приватные поля
}

impl RecoveryManager {
    /// Создать новый менеджер восстановления
    pub fn new(config: RecoveryConfig) -> Self;
    
    /// Выполнить восстановление после сбоя
    pub async fn recover(&self) -> Result<RecoveryResult, Error>;
    
    /// Создать контрольную точку
    pub async fn checkpoint(&self) -> Result<(), Error>;
    
    /// Получить статистику восстановления
    pub fn get_stats(&self) -> RecoveryStats;
}
```

### Типы записей лога

```rust
#[derive(Debug, Clone)]
pub enum LogRecordType {
    BeginTransaction { transaction_id: TransactionId },
    CommitTransaction { transaction_id: TransactionId },
    AbortTransaction { transaction_id: TransactionId },
    Insert { table_id: TableId, row_id: RowId, data: Vec<u8> },
    Update { table_id: TableId, row_id: RowId, old_data: Vec<u8>, new_data: Vec<u8> },
    Delete { table_id: TableId, row_id: RowId, data: Vec<u8> },
    Checkpoint { lsn: LogSequenceNumber },
}

#[derive(Debug, Clone)]
pub struct LogRecord {
    pub lsn: LogSequenceNumber,
    pub transaction_id: TransactionId,
    pub record_type: LogRecordType,
    pub timestamp: SystemTime,
}
```

## Управление конкурентностью

### Менеджер блокировок

```rust
pub struct LockManager {
    // Приватные поля
}

impl LockManager {
    /// Создать новый менеджер блокировок
    pub fn new(config: LockConfig) -> Self;
    
    /// Получить блокировку
    pub async fn acquire_lock(
        &self, 
        transaction_id: TransactionId, 
        resource: ResourceId, 
        lock_mode: LockMode
    ) -> Result<(), Error>;
    
    /// Освободить блокировку
    pub async fn release_lock(
        &self, 
        transaction_id: TransactionId, 
        resource: ResourceId
    ) -> Result<(), Error>;
    
    /// Освободить все блокировки для транзакции
    pub async fn release_all_locks(&self, transaction_id: TransactionId) -> Result<(), Error>;
    
    /// Проверить на взаимоблокировки
    pub async fn detect_deadlock(&self) -> Result<Option<Vec<TransactionId>>, Error>;
}
```

### Режимы блокировок

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    Shared,
    Exclusive,
    IntentionShared,
    IntentionExclusive,
    SharedIntentionExclusive,
}
```

### MVCC (Многоверсионное управление конкурентностью)

```rust
pub struct MVCCManager {
    // Приватные поля
}

impl MVCCManager {
    /// Создать новый менеджер MVCC
    pub fn new(config: MVCCConfig) -> Self;
    
    /// Создать новую версию строки
    pub async fn create_version(
        &self, 
        table_id: TableId, 
        row_id: RowId, 
        data: Vec<u8>
    ) -> Result<VersionId, Error>;
    
    /// Получить видимую версию строки для транзакции
    pub async fn get_visible_version(
        &self, 
        table_id: TableId, 
        row_id: RowId, 
        transaction_id: TransactionId
    ) -> Result<Option<Version>, Error>;
    
    /// Очистить старые версии
    pub async fn vacuum(&self, min_transaction_id: TransactionId) -> Result<(), Error>;
}
```

## Отладка и профилирование

### Отладочный логгер

```rust
pub struct DebugLogger {
    // Приватные поля
}

impl DebugLogger {
    /// Создать новый отладочный логгер
    pub fn new(config: DebugLoggerConfig) -> Self;
    
    /// Записать отладочное сообщение
    pub fn log(&self, level: LogLevel, category: LogCategory, message: &str);
    
    /// Записать с дополнительными данными
    pub fn log_with_data(
        &self, 
        level: LogLevel, 
        category: LogCategory, 
        message: &str, 
        data: &[u8]
    );
    
    /// Получить записи лога
    pub fn get_entries(&self, filter: LogFilter) -> Vec<LogEntry>;
}

#[derive(Debug, Clone, Copy)]
pub enum LogCategory {
    Transaction,
    Query,
    Storage,
    Network,
    Concurrency,
    Recovery,
}
```

### Трассировщик запросов

```rust
pub struct QueryTracer {
    // Приватные поля
}

impl QueryTracer {
    /// Создать новый трассировщик запросов
    pub fn new(config: QueryTracerConfig) -> Self;
    
    /// Начать трассировку запроса
    pub fn start_query(&self, query_id: QueryId, sql: &str) -> QueryTrace;
    
    /// Добавить событие в трассировку запроса
    pub fn add_event(&self, trace: &mut QueryTrace, event: QueryEvent);
    
    /// Завершить трассировку запроса
    pub fn complete_query(&self, trace: &mut QueryTrace, status: QueryStatus);
    
    /// Получить статистику запросов
    pub fn get_stats(&self) -> QueryStats;
}
```

### Профилировщик

```rust
pub struct Profiler {
    // Приватные поля
}

impl Profiler {
    /// Создать новый профилировщик
    pub fn new(config: ProfilerConfig) -> Self;
    
    /// Начать профилирование
    pub fn start(&self);
    
    /// Остановить профилирование
    pub fn stop(&self);
    
    /// Сделать снимок производительности
    pub fn take_snapshot(&self) -> PerformanceSnapshot;
    
    /// Получить статистику производительности
    pub fn get_stats(&self) -> ProfilerStats;
    
    /// Анализировать тренды производительности
    pub fn analyze_trends(&self, duration: Duration) -> TrendAnalysis;
}
```

### Анализатор производительности

```rust
pub struct PerformanceAnalyzer {
    // Приватные поля
}

impl PerformanceAnalyzer {
    /// Создать новый анализатор производительности
    pub fn new(config: PerformanceAnalyzerConfig) -> Self;
    
    /// Анализировать метрики производительности
    pub fn analyze_metrics(&self, metrics: &[PerformanceMetric]) -> AnalysisResult;
    
    /// Обнаружить узкие места
    pub fn detect_bottlenecks(&self, metrics: &[PerformanceMetric]) -> Vec<Bottleneck>;
    
    /// Сгенерировать рекомендации
    pub fn generate_recommendations(&self, bottlenecks: &[Bottleneck]) -> Vec<String>;
}
```

## Обработка ошибок

### Типы ошибок

```rust
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Ошибка подключения: {0}")]
    Connection(String),
    
    #[error("Ошибка парсинга SQL: {0}")]
    SqlParsing(String),
    
    #[error("Ошибка выполнения: {0}")]
    Execution(String),
    
    #[error("Ошибка транзакции: {0}")]
    Transaction(String),
    
    #[error("Таймаут блокировки: {0}")]
    LockTimeout(String),
    
    #[error("Обнаружена взаимоблокировка: {0}")]
    Deadlock(String),
    
    #[error("Ошибка хранения: {0}")]
    Storage(String),
    
    #[error("Ошибка восстановления: {0}")]
    Recovery(String),
    
    #[error("Ошибка конфигурации: {0}")]
    Configuration(String),
    
    #[error("Внутренняя ошибка: {0}")]
    Internal(String),
}
```

### Тип результата

```rust
pub type Result<T> = std::result::Result<T, Error>;
```

### Примеры обработки ошибок

```rust
// Базовая обработка ошибок
match db.execute("SELECT * FROM users").await {
    Ok(result) => {
        println!("Запрос выполнен успешно: {} строк", result.rows.len());
    }
    Err(Error::SqlParsing(msg)) => {
        eprintln!("Ошибка парсинга SQL: {}", msg);
    }
    Err(Error::Execution(msg)) => {
        eprintln!("Ошибка выполнения: {}", msg);
    }
    Err(e) => {
        eprintln!("Неожиданная ошибка: {}", e);
    }
}

// Использование оператора ?
let result = db.execute("SELECT * FROM users").await?;
println!("Найдено {} пользователей", result.rows.len());
```

## Конфигурация

### Конфигурация базы данных

```rust
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub name: String,
    pub data_directory: PathBuf,
    pub max_connections: usize,
    pub connection_timeout: Duration,
    pub query_timeout: Duration,
    pub storage: StorageConfig,
    pub logging: LoggingConfig,
    pub network: NetworkConfig,
    pub performance: PerformanceConfig,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            name: "rustdb".to_string(),
            data_directory: PathBuf::from("./data"),
            max_connections: 100,
            connection_timeout: Duration::from_secs(30),
            query_timeout: Duration::from_secs(60),
            storage: StorageConfig::default(),
            logging: LoggingConfig::default(),
            network: NetworkConfig::default(),
            performance: PerformanceConfig::default(),
        }
    }
}
```

### Конфигурация хранения

```rust
#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub page_size: usize,
    pub buffer_pool_size: usize,
    pub checkpoint_interval: Duration,
    pub wal_enabled: bool,
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
```

### Конфигурация производительности

```rust
#[derive(Debug, Clone)]
pub struct PerformanceConfig {
    pub lock_timeout: Duration,
    pub deadlock_detection_interval: Duration,
    pub max_query_plan_cache_size: usize,
    pub enable_query_optimization: bool,
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
```

### Загрузка конфигурации

```rust
impl DatabaseConfig {
    /// Загрузить конфигурацию из TOML файла
    pub fn from_file(path: &Path) -> Result<Self, Error>;
    
    /// Загрузить конфигурацию из переменных окружения
    pub fn from_env() -> Result<Self, Error>;
    
    /// Объединить с другой конфигурацией
    pub fn merge(self, other: Self) -> Self;
    
    /// Валидировать конфигурацию
    pub fn validate(&self) -> Result<(), Error>;
}
```

## Примеры

### Полный пример приложения

```rust
use rustdb::{Database, DatabaseConfig, IsolationLevel};
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Загрузка конфигурации
    let config = DatabaseConfig::from_file("config.toml")?;
    
    // Подключение к базе данных
    let db = Database::connect_with_config("localhost:8080", config).await?;
    
    // Создание таблиц
    db.execute("CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(255) UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )").await?;
    
    // Вставка данных с транзакцией
    let tx = db.begin_transaction(IsolationLevel::ReadCommitted).await?;
    tx.execute("INSERT INTO users (name, email) VALUES ('Иван Иванов', 'ivan@example.com')").await?;
    tx.execute("INSERT INTO users (name, email) VALUES ('Петр Петров', 'petr@example.com')").await?;
    tx.commit().await?;
    
    // Запрос данных
    let result = db.execute("SELECT * FROM users ORDER BY name").await?;
    for row in result.rows {
        let id: i32 = row.get("id")?;
        let name: String = row.get("name")?;
        let email: String = row.get("email")?;
        println!("ID: {}, Имя: {}, Email: {}", id, name, email);
    }
    
    // Получение статистики базы данных
    let stats = db.get_stats().await?;
    println!("Статистика базы данных: {:?}", stats);
    
    Ok(())
}
```

### Продвинутый пример транзакции

```rust
async fn transfer_money(
    db: &Database, 
    from_account: i32, 
    to_account: i32, 
    amount: f64
) -> Result<(), Box<dyn std::error::Error>> {
    let tx = db.begin_transaction(IsolationLevel::Serializable).await?;
    
    // Проверить баланс отправителя
    let result = tx.execute(&format!(
        "SELECT balance FROM accounts WHERE id = {}", from_account
    )).await?;
    
    if result.rows.is_empty() {
        tx.rollback().await?;
        return Err("Счет отправителя не найден".into());
    }
    
    let current_balance: f64 = result.rows[0].get("balance")?;
    if current_balance < amount {
        tx.rollback().await?;
        return Err("Недостаточно средств".into());
    }
    
    // Списать с отправителя
    tx.execute(&format!(
        "UPDATE accounts SET balance = balance - {} WHERE id = {}", 
        amount, from_account
    )).await?;
    
    // Добавить получателю
    tx.execute(&format!(
        "UPDATE accounts SET balance = balance + {} WHERE id = {}", 
        amount, to_account
    )).await?;
    
    // Записать транзакцию
    tx.execute(&format!(
        "INSERT INTO transactions (from_account, to_account, amount, timestamp) 
         VALUES ({}, {}, {}, CURRENT_TIMESTAMP)", 
        from_account, to_account, amount
    )).await?;
    
    tx.commit().await?;
    Ok(())
}
```

Эта справочная документация API предоставляет исчерпывающую документацию для всех основных компонентов RustDB. Для получения более подробной информации о конкретных модулях обратитесь к встроенной документации, генерируемой `cargo doc`.
