# RustDB API Reference

## Table of Contents
1. [Core Types](#core-types)
2. [Database Connection](#database-connection)
3. [Transaction Management](#transaction-management)
4. [Query Execution](#query-execution)
5. [Storage Layer](#storage-layer)
6. [Logging and Recovery](#logging-and-recovery)
7. [Concurrency Control](#concurrency-control)
8. [Debugging and Profiling](#debugging-and-profiling)
9. [Error Handling](#error-handling)
10. [Configuration](#configuration)

## Core Types

### Database
The main database connection type.

```rust
pub struct Database {
    // Private fields
}

impl Database {
    /// Connect to a database instance
    pub async fn connect(connection_string: &str) -> Result<Self, Error>;
    
    /// Connect with custom configuration
    pub async fn connect_with_config(
        connection_string: &str, 
        config: DatabaseConfig
    ) -> Result<Self, Error>;
    
    /// Execute a SQL statement
    pub async fn execute(&self, sql: &str) -> Result<QueryResult, Error>;
    
    /// Execute a prepared statement
    pub async fn execute_prepared(
        &self, 
        stmt: &PreparedStatement, 
        params: &[Value]
    ) -> Result<QueryResult, Error>;
    
    /// Begin a new transaction
    pub async fn begin_transaction(
        &self, 
        isolation_level: IsolationLevel
    ) -> Result<Transaction, Error>;
    
    /// Get database statistics
    pub async fn get_stats(&self) -> Result<DatabaseStats, Error>;
}
```

### Transaction
Represents a database transaction.

```rust
pub struct Transaction {
    // Private fields
}

impl Transaction {
    /// Execute a SQL statement within the transaction
    pub async fn execute(&self, sql: &str) -> Result<QueryResult, Error>;
    
    /// Commit the transaction
    pub async fn commit(self) -> Result<(), Error>;
    
    /// Rollback the transaction
    pub async fn rollback(self) -> Result<(), Error>;
    
    /// Get transaction ID
    pub fn id(&self) -> TransactionId;
    
    /// Get isolation level
    pub fn isolation_level(&self) -> IsolationLevel;
}
```

### QueryResult
Result of a query execution.

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

## Database Connection

### Connection Methods

```rust
// Basic connection
let db = Database::connect("localhost:8080").await?;

// Connection with configuration
let config = DatabaseConfig::default()
    .with_timeout(Duration::from_secs(30))
    .with_max_connections(100);
let db = Database::connect_with_config("localhost:8080", config).await?;

// Connection with authentication
let db = Database::connect("user:password@localhost:8080/database").await?;
```

### Connection Pool

```rust
pub struct ConnectionPool {
    // Private fields
}

impl ConnectionPool {
    /// Create a new connection pool
    pub fn new(config: PoolConfig) -> Self;
    
    /// Get a connection from the pool
    pub async fn get_connection(&self) -> Result<PooledConnection, Error>;
    
    /// Get pool statistics
    pub fn stats(&self) -> PoolStats;
}

pub struct PooledConnection {
    connection: Database,
    pool: Arc<ConnectionPool>,
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        // Return connection to pool
    }
}
```

## Transaction Management

### Isolation Levels

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}
```

### Transaction Operations

```rust
// Begin transaction with specific isolation level
let tx = db.begin_transaction(IsolationLevel::ReadCommitted).await?;

// Execute operations within transaction
tx.execute("INSERT INTO users (name) VALUES ('John')").await?;
tx.execute("UPDATE accounts SET balance = balance - 100").await?;

// Commit or rollback
match some_condition {
    true => tx.commit().await?,
    false => tx.rollback().await?,
}
```

### Savepoints

```rust
impl Transaction {
    /// Create a savepoint
    pub async fn create_savepoint(&self, name: &str) -> Result<(), Error>;
    
    /// Rollback to a savepoint
    pub async fn rollback_to_savepoint(&self, name: &str) -> Result<(), Error>;
    
    /// Release a savepoint
    pub async fn release_savepoint(&self, name: &str) -> Result<(), Error>;
}

// Example usage
let tx = db.begin_transaction(IsolationLevel::ReadCommitted).await?;
tx.execute("INSERT INTO users (name) VALUES ('John')").await?;
tx.create_savepoint("after_insert").await?;

tx.execute("UPDATE accounts SET balance = balance - 100").await?;
if some_error_condition {
    tx.rollback_to_savepoint("after_insert").await?;
}
tx.commit().await?;
```

## Query Execution

### Basic Query Execution

```rust
// Execute a SELECT query
let result = db.execute("SELECT * FROM users WHERE age > 18").await?;
for row in result.rows {
    let name: String = row.get("name")?;
    let age: i32 = row.get("age")?;
    println!("Name: {}, Age: {}", name, age);
}

// Execute an INSERT query
let result = db.execute("INSERT INTO users (name, age) VALUES ('John', 25)").await?;
println!("Inserted {} rows", result.affected_rows);

// Execute an UPDATE query
let result = db.execute("UPDATE users SET age = 26 WHERE name = 'John'").await?;
println!("Updated {} rows", result.affected_rows);
```

### Prepared Statements

```rust
pub struct PreparedStatement {
    // Private fields
}

impl Database {
    /// Prepare a SQL statement
    pub async fn prepare(&self, sql: &str) -> Result<PreparedStatement, Error>;
}

// Example usage
let stmt = db.prepare("INSERT INTO users (name, age) VALUES (?, ?)").await?;
let result = db.execute_prepared(&stmt, &[Value::String("John".to_string()), Value::Integer(25)]).await?;
```

### Batch Operations

```rust
impl Database {
    /// Execute multiple statements in a batch
    pub async fn execute_batch(&self, statements: &[String]) -> Result<Vec<QueryResult>, Error>;
    
    /// Execute a batch with parameters
    pub async fn execute_batch_prepared(
        &self, 
        stmt: &PreparedStatement, 
        param_sets: &[Vec<Value>]
    ) -> Result<Vec<QueryResult>, Error>;
}

// Example usage
let statements = vec![
    "INSERT INTO users (name) VALUES ('John')".to_string(),
    "INSERT INTO users (name) VALUES ('Jane')".to_string(),
    "INSERT INTO users (name) VALUES ('Bob')".to_string(),
];
let results = db.execute_batch(&statements).await?;
```

## Storage Layer

### Page Manager

```rust
pub struct PageManager {
    // Private fields
}

impl PageManager {
    /// Create a new page manager
    pub fn new(config: PageManagerConfig) -> Self;
    
    /// Read a page
    pub async fn read_page(&self, page_id: PageId) -> Result<Page, Error>;
    
    /// Write a page
    pub async fn write_page(&self, page: &Page) -> Result<(), Error>;
    
    /// Allocate a new page
    pub async fn allocate_page(&self) -> Result<PageId, Error>;
    
    /// Deallocate a page
    pub async fn deallocate_page(&self, page_id: PageId) -> Result<(), Error>;
}
```

### Buffer Manager

```rust
pub struct BufferManager {
    // Private fields
}

impl BufferManager {
    /// Create a new buffer manager
    pub fn new(config: BufferConfig) -> Self;
    
    /// Pin a page in the buffer pool
    pub async fn pin_page(&self, page_id: PageId) -> Result<PageRef, Error>;
    
    /// Unpin a page
    pub async fn unpin_page(&self, page_id: PageId) -> Result<(), Error>;
    
    /// Flush dirty pages to disk
    pub async fn flush_pages(&self) -> Result<(), Error>;
    
    /// Get buffer pool statistics
    pub fn get_stats(&self) -> BufferStats;
}
```

### Index Management

```rust
pub struct IndexManager {
    // Private fields
}

impl IndexManager {
    /// Create a new index
    pub async fn create_index(
        &self, 
        table_name: &str, 
        columns: &[String], 
        index_type: IndexType
    ) -> Result<IndexId, Error>;
    
    /// Drop an index
    pub async fn drop_index(&self, index_id: IndexId) -> Result<(), Error>;
    
    /// Insert a key-value pair into an index
    pub async fn insert(&self, index_id: IndexId, key: &Value, value: &Value) -> Result<(), Error>;
    
    /// Delete a key from an index
    pub async fn delete(&self, index_id: IndexId, key: &Value) -> Result<(), Error>;
    
    /// Search for a key in an index
    pub async fn search(&self, index_id: IndexId, key: &Value) -> Result<Option<Value>, Error>;
    
    /// Range scan on an index
    pub async fn range_scan(
        &self, 
        index_id: IndexId, 
        start: &Value, 
        end: &Value
    ) -> Result<IndexIterator, Error>;
}
```

## Logging and Recovery

### Write-Ahead Log (WAL)

```rust
pub struct WriteAheadLog {
    // Private fields
}

impl WriteAheadLog {
    /// Create a new WAL
    pub fn new(config: WalConfig) -> Self;
    
    /// Write a log record
    pub async fn write_record(&self, record: LogRecord) -> Result<LogSequenceNumber, Error>;
    
    /// Flush log to disk
    pub async fn flush(&self) -> Result<(), Error>;
    
    /// Read log records from a specific LSN
    pub async fn read_from(&self, lsn: LogSequenceNumber) -> Result<LogIterator, Error>;
    
    /// Get current LSN
    pub fn current_lsn(&self) -> LogSequenceNumber;
}
```

### Recovery Manager

```rust
pub struct RecoveryManager {
    // Private fields
}

impl RecoveryManager {
    /// Create a new recovery manager
    pub fn new(config: RecoveryConfig) -> Self;
    
    /// Perform crash recovery
    pub async fn recover(&self) -> Result<RecoveryResult, Error>;
    
    /// Create a checkpoint
    pub async fn checkpoint(&self) -> Result<(), Error>;
    
    /// Get recovery statistics
    pub fn get_stats(&self) -> RecoveryStats;
}
```

### Log Record Types

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

## Concurrency Control

### Lock Manager

```rust
pub struct LockManager {
    // Private fields
}

impl LockManager {
    /// Create a new lock manager
    pub fn new(config: LockConfig) -> Self;
    
    /// Acquire a lock
    pub async fn acquire_lock(
        &self, 
        transaction_id: TransactionId, 
        resource: ResourceId, 
        lock_mode: LockMode
    ) -> Result<(), Error>;
    
    /// Release a lock
    pub async fn release_lock(
        &self, 
        transaction_id: TransactionId, 
        resource: ResourceId
    ) -> Result<(), Error>;
    
    /// Release all locks for a transaction
    pub async fn release_all_locks(&self, transaction_id: TransactionId) -> Result<(), Error>;
    
    /// Check for deadlocks
    pub async fn detect_deadlock(&self) -> Result<Option<Vec<TransactionId>>, Error>;
}
```

### Lock Modes

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

### MVCC (Multi-Version Concurrency Control)

```rust
pub struct MVCCManager {
    // Private fields
}

impl MVCCManager {
    /// Create a new MVCC manager
    pub fn new(config: MVCCConfig) -> Self;
    
    /// Create a new version of a row
    pub async fn create_version(
        &self, 
        table_id: TableId, 
        row_id: RowId, 
        data: Vec<u8>
    ) -> Result<VersionId, Error>;
    
    /// Get the visible version of a row for a transaction
    pub async fn get_visible_version(
        &self, 
        table_id: TableId, 
        row_id: RowId, 
        transaction_id: TransactionId
    ) -> Result<Option<Version>, Error>;
    
    /// Clean up old versions
    pub async fn vacuum(&self, min_transaction_id: TransactionId) -> Result<(), Error>;
}
```

## Debugging and Profiling

### Debug Logger

```rust
pub struct DebugLogger {
    // Private fields
}

impl DebugLogger {
    /// Create a new debug logger
    pub fn new(config: DebugLoggerConfig) -> Self;
    
    /// Log a debug message
    pub fn log(&self, level: LogLevel, category: LogCategory, message: &str);
    
    /// Log with additional data
    pub fn log_with_data(
        &self, 
        level: LogLevel, 
        category: LogCategory, 
        message: &str, 
        data: &[u8]
    );
    
    /// Get log entries
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

### Query Tracer

```rust
pub struct QueryTracer {
    // Private fields
}

impl QueryTracer {
    /// Create a new query tracer
    pub fn new(config: QueryTracerConfig) -> Self;
    
    /// Start tracing a query
    pub fn start_query(&self, query_id: QueryId, sql: &str) -> QueryTrace;
    
    /// Add an event to a query trace
    pub fn add_event(&self, trace: &mut QueryTrace, event: QueryEvent);
    
    /// Complete a query trace
    pub fn complete_query(&self, trace: &mut QueryTrace, status: QueryStatus);
    
    /// Get query statistics
    pub fn get_stats(&self) -> QueryStats;
}
```

### Profiler

```rust
pub struct Profiler {
    // Private fields
}

impl Profiler {
    /// Create a new profiler
    pub fn new(config: ProfilerConfig) -> Self;
    
    /// Start profiling
    pub fn start(&self);
    
    /// Stop profiling
    pub fn stop(&self);
    
    /// Take a performance snapshot
    pub fn take_snapshot(&self) -> PerformanceSnapshot;
    
    /// Get performance statistics
    pub fn get_stats(&self) -> ProfilerStats;
    
    /// Analyze performance trends
    pub fn analyze_trends(&self, duration: Duration) -> TrendAnalysis;
}
```

### Performance Analyzer

```rust
pub struct PerformanceAnalyzer {
    // Private fields
}

impl PerformanceAnalyzer {
    /// Create a new performance analyzer
    pub fn new(config: PerformanceAnalyzerConfig) -> Self;
    
    /// Analyze performance metrics
    pub fn analyze_metrics(&self, metrics: &[PerformanceMetric]) -> AnalysisResult;
    
    /// Detect bottlenecks
    pub fn detect_bottlenecks(&self, metrics: &[PerformanceMetric]) -> Vec<Bottleneck>;
    
    /// Generate recommendations
    pub fn generate_recommendations(&self, bottlenecks: &[Bottleneck]) -> Vec<String>;
}
```

## Error Handling

### Error Types

```rust
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Connection error: {0}")]
    Connection(String),
    
    #[error("SQL parsing error: {0}")]
    SqlParsing(String),
    
    #[error("Execution error: {0}")]
    Execution(String),
    
    #[error("Transaction error: {0}")]
    Transaction(String),
    
    #[error("Lock timeout: {0}")]
    LockTimeout(String),
    
    #[error("Deadlock detected: {0}")]
    Deadlock(String),
    
    #[error("Storage error: {0}")]
    Storage(String),
    
    #[error("Recovery error: {0}")]
    Recovery(String),
    
    #[error("Configuration error: {0}")]
    Configuration(String),
    
    #[error("Internal error: {0}")]
    Internal(String),
}
```

### Result Type

```rust
pub type Result<T> = std::result::Result<T, Error>;
```

### Error Handling Examples

```rust
// Basic error handling
match db.execute("SELECT * FROM users").await {
    Ok(result) => {
        println!("Query executed successfully: {} rows", result.rows.len());
    }
    Err(Error::SqlParsing(msg)) => {
        eprintln!("SQL parsing error: {}", msg);
    }
    Err(Error::Execution(msg)) => {
        eprintln!("Execution error: {}", msg);
    }
    Err(e) => {
        eprintln!("Unexpected error: {}", e);
    }
}

// Using ? operator
let result = db.execute("SELECT * FROM users").await?;
println!("Found {} users", result.rows.len());
```

## Configuration

### Database Configuration

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

### Storage Configuration

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

### Performance Configuration

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

### Configuration Loading

```rust
impl DatabaseConfig {
    /// Load configuration from TOML file
    pub fn from_file(path: &Path) -> Result<Self, Error>;
    
    /// Load configuration from environment variables
    pub fn from_env() -> Result<Self, Error>;
    
    /// Merge with another configuration
    pub fn merge(self, other: Self) -> Self;
    
    /// Validate configuration
    pub fn validate(&self) -> Result<(), Error>;
}
```

## Examples

### Complete Application Example

```rust
use rustdb::{Database, DatabaseConfig, IsolationLevel};
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration
    let config = DatabaseConfig::from_file("config.toml")?;
    
    // Connect to database
    let db = Database::connect_with_config("localhost:8080", config).await?;
    
    // Create tables
    db.execute("CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(255) UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )").await?;
    
    // Insert data with transaction
    let tx = db.begin_transaction(IsolationLevel::ReadCommitted).await?;
    tx.execute("INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com')").await?;
    tx.execute("INSERT INTO users (name, email) VALUES ('Jane Smith', 'jane@example.com')").await?;
    tx.commit().await?;
    
    // Query data
    let result = db.execute("SELECT * FROM users ORDER BY name").await?;
    for row in result.rows {
        let id: i32 = row.get("id")?;
        let name: String = row.get("name")?;
        let email: String = row.get("email")?;
        println!("ID: {}, Name: {}, Email: {}", id, name, email);
    }
    
    // Get database statistics
    let stats = db.get_stats().await?;
    println!("Database statistics: {:?}", stats);
    
    Ok(())
}
```

### Advanced Transaction Example

```rust
async fn transfer_money(
    db: &Database, 
    from_account: i32, 
    to_account: i32, 
    amount: f64
) -> Result<(), Box<dyn std::error::Error>> {
    let tx = db.begin_transaction(IsolationLevel::Serializable).await?;
    
    // Check sender balance
    let result = tx.execute(&format!(
        "SELECT balance FROM accounts WHERE id = {}", from_account
    )).await?;
    
    if result.rows.is_empty() {
        tx.rollback().await?;
        return Err("Sender account not found".into());
    }
    
    let current_balance: f64 = result.rows[0].get("balance")?;
    if current_balance < amount {
        tx.rollback().await?;
        return Err("Insufficient funds".into());
    }
    
    // Deduct from sender
    tx.execute(&format!(
        "UPDATE accounts SET balance = balance - {} WHERE id = {}", 
        amount, from_account
    )).await?;
    
    // Add to receiver
    tx.execute(&format!(
        "UPDATE accounts SET balance = balance + {} WHERE id = {}", 
        amount, to_account
    )).await?;
    
    // Record transaction
    tx.execute(&format!(
        "INSERT INTO transactions (from_account, to_account, amount, timestamp) 
         VALUES ({}, {}, {}, CURRENT_TIMESTAMP)", 
        from_account, to_account, amount
    )).await?;
    
    tx.commit().await?;
    Ok(())
}
```

This API reference provides comprehensive documentation for all major components of RustDB. For more detailed information about specific modules, refer to the inline documentation generated by `cargo doc`.