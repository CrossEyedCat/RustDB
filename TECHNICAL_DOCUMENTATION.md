# RustDB Technical Documentation

## Table of Contents
1. [System Architecture](#system-architecture)
2. [Storage Engine](#storage-engine)
3. [Query Processing](#query-processing)
4. [Transaction Management](#transaction-management)
5. [Concurrency Control](#concurrency-control)
6. [Recovery System](#recovery-system)
7. [Network Layer](#network-layer)
8. [Performance Optimization](#performance-optimization)
9. [Security](#security)
10. [Development Guidelines](#development-guidelines)

## System Architecture

### High-Level Architecture

RustDB follows a layered architecture pattern with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                        │
├─────────────────────────────────────────────────────────────┤
│                    Network Layer                            │
├─────────────────────────────────────────────────────────────┤
│                    Query Processing Layer                   │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐           │
│  │   Parser    │ │  Planner    │ │  Executor   │           │
│  └─────────────┘ └─────────────┘ └─────────────┘           │
├─────────────────────────────────────────────────────────────┤
│                    Transaction Layer                        │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐           │
│  │   ACID      │ │   Lock      │ │    MVCC     │           │
│  │  Manager    │ │  Manager    │ │  Manager    │           │
│  └─────────────┘ └─────────────┘ └─────────────┘           │
├─────────────────────────────────────────────────────────────┤
│                    Storage Layer                            │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐           │
│  │   Buffer    │ │    Page     │ │   Index     │           │
│  │  Manager    │ │  Manager    │ │  Manager    │           │
│  └─────────────┘ └─────────────┘ └─────────────┘           │
├─────────────────────────────────────────────────────────────┤
│                    File System Layer                        │
└─────────────────────────────────────────────────────────────┘
```

### Core Components

#### 1. Storage Engine
- **Page Manager**: Manages database pages and their allocation
- **Buffer Manager**: Implements LRU caching for frequently accessed pages
- **File Manager**: Handles low-level file I/O operations
- **Index Manager**: Manages B+ trees and hash indexes

#### 2. Query Processing
- **SQL Parser**: Converts SQL text into Abstract Syntax Trees (AST)
- **Semantic Analyzer**: Validates queries and resolves object references
- **Query Planner**: Generates execution plans
- **Query Optimizer**: Optimizes execution plans for better performance
- **Executor**: Executes optimized plans using various operators

#### 3. Transaction Management
- **ACID Manager**: Ensures atomicity, consistency, isolation, and durability
- **Lock Manager**: Implements granular locking with deadlock detection
- **MVCC Manager**: Provides multi-version concurrency control
- **Recovery Manager**: Handles crash recovery and checkpointing

#### 4. Network Layer
- **Connection Manager**: Manages client connections
- **Protocol Handler**: Implements database protocol
- **Authentication**: Handles user authentication and authorization

## Storage Engine

### Page Structure

Each database page has a fixed size (default 8KB) and contains:

```rust
pub struct Page {
    pub header: PageHeader,
    pub data: [u8; PAGE_SIZE],
    pub free_space: usize,
    pub slot_directory: Vec<SlotEntry>,
}

pub struct PageHeader {
    pub page_id: PageId,
    pub page_type: PageType,
    pub lsn: LogSequenceNumber,
    pub checksum: u32,
    pub free_space_offset: u16,
    pub slot_count: u16,
}
```

### Buffer Pool Management

The buffer pool uses an LRU (Least Recently Used) eviction policy:

```rust
pub struct BufferManager {
    pool: HashMap<PageId, Arc<Mutex<Page>>>,
    lru_list: LinkedList<PageId>,
    max_pages: usize,
    stats: BufferStats,
}

impl BufferManager {
    pub async fn pin_page(&self, page_id: PageId) -> Result<PageRef, Error> {
        // Check if page is in buffer pool
        if let Some(page) = self.pool.get(&page_id) {
            self.update_lru(page_id);
            return Ok(PageRef::new(page.clone()));
        }
        
        // Load page from disk
        let page = self.load_page_from_disk(page_id).await?;
        self.insert_page(page_id, page).await
    }
}
```

### Index Implementation

#### B+ Tree Index

```rust
pub struct BPlusTree<K, V> {
    root: Option<BTreeNode<K, V>>,
    order: usize,
    height: usize,
}

pub enum BTreeNode<K, V> {
    Internal {
        keys: Vec<K>,
        children: Vec<Box<BTreeNode<K, V>>>,
    },
    Leaf {
        keys: Vec<K>,
        values: Vec<V>,
        next: Option<Box<BTreeNode<K, V>>>,
    },
}
```

#### Hash Index

```rust
pub struct HashIndex<K, V> {
    buckets: Vec<Vec<(K, V)>>,
    hash_function: fn(&K) -> u64,
    load_factor: f64,
    size: usize,
}
```

## Query Processing

### SQL Parsing

The parser uses a recursive descent approach:

```rust
pub struct SqlParser {
    tokens: Vec<Token>,
    current: usize,
}

impl SqlParser {
    pub fn parse(&mut self) -> Result<SqlStatement, Error> {
        match self.peek() {
            Some(Token::Select) => self.parse_select(),
            Some(Token::Insert) => self.parse_insert(),
            Some(Token::Update) => self.parse_update(),
            Some(Token::Delete) => self.parse_delete(),
            Some(Token::Create) => self.parse_create(),
            _ => Err(Error::UnexpectedToken),
        }
    }
}
```

### Query Planning

The planner generates execution plans using a cost-based approach:

```rust
pub struct QueryPlanner {
    statistics: Arc<StatisticsManager>,
    optimizer: QueryOptimizer,
}

impl QueryPlanner {
    pub fn create_plan(&self, query: &SelectStatement) -> Result<ExecutionPlan, Error> {
        // 1. Generate base plan
        let base_plan = self.create_base_plan(query)?;
        
        // 2. Apply optimizations
        let optimized_plan = self.optimizer.optimize(base_plan)?;
        
        // 3. Estimate costs
        let cost = self.estimate_cost(&optimized_plan)?;
        
        Ok(ExecutionPlan {
            root: optimized_plan,
            cost,
            metadata: PlanMetadata::new(),
        })
    }
}
```

### Execution Operators

#### Table Scan Operator

```rust
pub struct TableScanOperator {
    table_name: String,
    page_manager: Arc<PageManager>,
    current_page: Option<PageId>,
    current_slot: usize,
}

impl Operator for TableScanOperator {
    async fn next(&mut self) -> Result<Option<Row>, Error> {
        loop {
            if let Some(page_id) = self.current_page {
                let page = self.page_manager.read_page(page_id).await?;
                
                if self.current_slot < page.slot_count() {
                    let row = page.get_row(self.current_slot)?;
                    self.current_slot += 1;
                    return Ok(Some(row));
                } else {
                    // Move to next page
                    self.current_page = page.next_page();
                    self.current_slot = 0;
                }
            } else {
                return Ok(None);
            }
        }
    }
}
```

#### Join Operators

```rust
pub struct NestedLoopJoinOperator {
    left_child: Box<dyn Operator>,
    right_child: Box<dyn Operator>,
    join_condition: JoinCondition,
    left_row: Option<Row>,
}

impl Operator for NestedLoopJoinOperator {
    async fn next(&mut self) -> Result<Option<Row>, Error> {
        loop {
            if self.left_row.is_none() {
                self.left_row = self.left_child.next().await?;
                if self.left_row.is_none() {
                    return Ok(None);
                }
            }
            
            if let Some(right_row) = self.right_child.next().await? {
                if self.join_condition.evaluate(&self.left_row.as_ref().unwrap(), &right_row)? {
                    return Ok(Some(self.combine_rows(&self.left_row.as_ref().unwrap(), &right_row)));
                }
            } else {
                // Reset right child and move to next left row
                self.right_child.reset().await?;
                self.left_row = None;
            }
        }
    }
}
```

## Transaction Management

### ACID Properties Implementation

#### Atomicity
Ensured through the Write-Ahead Log (WAL):

```rust
impl AcidManager {
    pub async fn begin_transaction(&self, isolation_level: IsolationLevel) -> Result<TransactionId, Error> {
        let tx_id = self.generate_transaction_id();
        
        // Log transaction begin
        let log_record = LogRecord {
            lsn: self.get_next_lsn(),
            transaction_id: tx_id,
            record_type: LogRecordType::BeginTransaction,
            timestamp: SystemTime::now(),
        };
        
        self.wal.write_record(log_record).await?;
        self.register_transaction(tx_id, isolation_level);
        
        Ok(tx_id)
    }
}
```

#### Consistency
Maintained through constraint checking:

```rust
impl AcidManager {
    pub async fn check_constraints(&self, table_id: TableId, row: &Row) -> Result<(), Error> {
        let schema = self.schema_manager.get_table_schema(table_id)?;
        
        for constraint in &schema.constraints {
            match constraint {
                Constraint::NotNull(column_index) => {
                    if row.get_value(*column_index).is_null() {
                        return Err(Error::ConstraintViolation("NOT NULL constraint violated"));
                    }
                }
                Constraint::Unique(columns) => {
                    if self.check_unique_constraint(table_id, columns, row).await? {
                        return Err(Error::ConstraintViolation("UNIQUE constraint violated"));
                    }
                }
                _ => {}
            }
        }
        
        Ok(())
    }
}
```

#### Isolation
Implemented through locking and MVCC:

```rust
impl LockManager {
    pub async fn acquire_lock(
        &self,
        transaction_id: TransactionId,
        resource: ResourceId,
        lock_mode: LockMode,
    ) -> Result<(), Error> {
        let mut lock_table = self.lock_table.lock().await;
        
        // Check for lock conflicts
        if self.has_lock_conflict(&lock_table, &resource, &lock_mode) {
            // Add to waiting queue
            self.add_to_waiting_queue(transaction_id, resource, lock_mode).await?;
            
            // Check for deadlock
            if self.detect_deadlock(transaction_id).await? {
                return Err(Error::Deadlock);
            }
            
            // Wait for lock
            self.wait_for_lock(transaction_id, resource).await?;
        }
        
        // Grant lock
        self.grant_lock(&mut lock_table, transaction_id, resource, lock_mode);
        Ok(())
    }
}
```

#### Durability
Guaranteed through WAL and checkpointing:

```rust
impl AcidManager {
    pub async fn commit_transaction(&self, transaction_id: TransactionId) -> Result<(), Error> {
        // Log transaction commit
        let log_record = LogRecord {
            lsn: self.get_next_lsn(),
            transaction_id,
            record_type: LogRecordType::CommitTransaction,
            timestamp: SystemTime::now(),
        };
        
        self.wal.write_record(log_record).await?;
        self.wal.flush().await?; // Ensure durability
        
        // Release locks
        self.lock_manager.release_all_locks(transaction_id).await?;
        
        // Remove from active transactions
        self.remove_transaction(transaction_id);
        
        Ok(())
    }
}
```

## Concurrency Control

### Lock Granularity

RustDB supports multiple levels of locking:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockGranularity {
    Database,
    Table,
    Page,
    Row,
}
```

### Lock Compatibility Matrix

| Lock Mode | IS | IX | S | SIX | X |
|-----------|----|----|----|-----|---|
| IS        | ✓  | ✓  | ✓  | ✓   | ✗ |
| IX        | ✓  | ✓  | ✗  | ✗   | ✗ |
| S         | ✓  | ✗  | ✓  | ✗   | ✗ |
| SIX       | ✓  | ✗  | ✗  | ✗   | ✗ |
| X         | ✗  | ✗  | ✗  | ✗   | ✗ |

### Deadlock Detection

Uses a wait-for graph to detect deadlocks:

```rust
impl LockManager {
    pub async fn detect_deadlock(&self, transaction_id: TransactionId) -> Result<bool, Error> {
        let wait_for_graph = self.build_wait_for_graph().await?;
        
        // Use DFS to detect cycles
        let mut visited = HashSet::new();
        let mut recursion_stack = HashSet::new();
        
        for &tx_id in wait_for_graph.keys() {
            if !visited.contains(&tx_id) {
                if self.has_cycle(&wait_for_graph, tx_id, &mut visited, &mut recursion_stack) {
                    return Ok(true);
                }
            }
        }
        
        Ok(false)
    }
}
```

### MVCC Implementation

```rust
pub struct MVCCManager {
    version_chains: HashMap<RowId, VersionChain>,
    active_transactions: HashMap<TransactionId, TransactionInfo>,
}

pub struct Version {
    pub version_id: VersionId,
    pub transaction_id: TransactionId,
    pub begin_timestamp: Timestamp,
    pub end_timestamp: Option<Timestamp>,
    pub data: Vec<u8>,
    pub next_version: Option<VersionId>,
}

impl MVCCManager {
    pub async fn get_visible_version(
        &self,
        row_id: RowId,
        transaction_id: TransactionId,
    ) -> Result<Option<Version>, Error> {
        let chain = self.version_chains.get(&row_id)?;
        let tx_info = self.active_transactions.get(&transaction_id)?;
        
        for version in chain.versions() {
            if self.is_version_visible(version, tx_info) {
                return Ok(Some(version.clone()));
            }
        }
        
        Ok(None)
    }
}
```

## Recovery System

### Write-Ahead Logging

All changes are logged before being applied to data pages:

```rust
impl WriteAheadLog {
    pub async fn write_record(&self, record: LogRecord) -> Result<LogSequenceNumber, Error> {
        let lsn = self.get_next_lsn();
        let log_entry = LogEntry {
            lsn,
            record,
            checksum: self.calculate_checksum(&record),
        };
        
        // Write to log file
        self.log_file.write(&log_entry.serialize()).await?;
        
        // Update in-memory log buffer
        self.log_buffer.push(log_entry);
        
        Ok(lsn)
    }
}
```

### Checkpointing

Periodic checkpoints reduce recovery time:

```rust
impl CheckpointManager {
    pub async fn create_checkpoint(&self) -> Result<(), Error> {
        // 1. Flush all dirty pages
        self.buffer_manager.flush_all_dirty_pages().await?;
        
        // 2. Write checkpoint record
        let checkpoint_record = LogRecord {
            lsn: self.wal.current_lsn(),
            transaction_id: TransactionId::new(0),
            record_type: LogRecordType::Checkpoint {
                active_transactions: self.get_active_transactions(),
                dirty_pages: self.get_dirty_pages(),
            },
            timestamp: SystemTime::now(),
        };
        
        self.wal.write_record(checkpoint_record).await?;
        self.wal.flush().await?;
        
        // 3. Update checkpoint file
        self.update_checkpoint_file().await?;
        
        Ok(())
    }
}
```

### Crash Recovery

Three-phase recovery process:

```rust
impl RecoveryManager {
    pub async fn recover(&self) -> Result<RecoveryResult, Error> {
        // Phase 1: Analysis
        let analysis_result = self.analyze_log().await?;
        
        // Phase 2: Redo
        let redo_result = self.redo_phase(&analysis_result).await?;
        
        // Phase 3: Undo
        let undo_result = self.undo_phase(&analysis_result).await?;
        
        Ok(RecoveryResult {
            analysis: analysis_result,
            redo: redo_result,
            undo: undo_result,
        })
    }
    
    async fn analyze_log(&self) -> Result<AnalysisResult, Error> {
        let mut active_transactions = HashSet::new();
        let mut dirty_pages = HashSet::new();
        
        // Find last checkpoint
        let checkpoint_lsn = self.find_last_checkpoint().await?;
        
        // Scan log from checkpoint
        let log_iter = self.wal.read_from(checkpoint_lsn).await?;
        
        for log_entry in log_iter {
            match &log_entry.record.record_type {
                LogRecordType::BeginTransaction { transaction_id } => {
                    active_transactions.insert(*transaction_id);
                }
                LogRecordType::CommitTransaction { transaction_id } => {
                    active_transactions.remove(transaction_id);
                }
                LogRecordType::AbortTransaction { transaction_id } => {
                    active_transactions.remove(transaction_id);
                }
                LogRecordType::Insert { table_id, .. } => {
                    dirty_pages.insert(*table_id);
                }
                _ => {}
            }
        }
        
        Ok(AnalysisResult {
            active_transactions,
            dirty_pages,
        })
    }
}
```

## Network Layer

### Connection Management

```rust
pub struct ConnectionManager {
    connections: HashMap<ConnectionId, Arc<Connection>>,
    max_connections: usize,
    connection_timeout: Duration,
}

impl ConnectionManager {
    pub async fn accept_connection(&self, stream: TcpStream) -> Result<ConnectionId, Error> {
        if self.connections.len() >= self.max_connections {
            return Err(Error::TooManyConnections);
        }
        
        let connection_id = ConnectionId::new();
        let connection = Arc::new(Connection::new(stream, connection_id));
        
        // Start connection handler
        let connection_clone = connection.clone();
        tokio::spawn(async move {
            connection_clone.handle_requests().await;
        });
        
        self.connections.insert(connection_id, connection);
        Ok(connection_id)
    }
}
```

### Protocol Implementation

```rust
pub struct ProtocolHandler {
    database: Arc<Database>,
}

impl ProtocolHandler {
    pub async fn handle_request(&self, request: Request) -> Result<Response, Error> {
        match request {
            Request::Query { sql } => {
                let result = self.database.execute(&sql).await?;
                Ok(Response::QueryResult(result))
            }
            Request::Prepare { sql } => {
                let stmt = self.database.prepare(&sql).await?;
                Ok(Response::PreparedStatement(stmt))
            }
            Request::Execute { stmt_id, params } => {
                let stmt = self.get_prepared_statement(stmt_id)?;
                let result = self.database.execute_prepared(&stmt, &params).await?;
                Ok(Response::QueryResult(result))
            }
            Request::BeginTransaction { isolation_level } => {
                let tx = self.database.begin_transaction(isolation_level).await?;
                Ok(Response::TransactionStarted(tx.id()))
            }
            Request::Commit { transaction_id } => {
                let tx = self.get_transaction(transaction_id)?;
                tx.commit().await?;
                Ok(Response::TransactionCommitted)
            }
            Request::Rollback { transaction_id } => {
                let tx = self.get_transaction(transaction_id)?;
                tx.rollback().await?;
                Ok(Response::TransactionRolledBack)
            }
        }
    }
}
```

## Performance Optimization

### Query Optimization

#### Cost-Based Optimization

```rust
impl QueryOptimizer {
    pub fn optimize(&self, plan: ExecutionPlan) -> Result<ExecutionPlan, Error> {
        let mut optimized_plan = plan;
        
        // Apply optimization rules
        optimized_plan = self.push_down_selections(optimized_plan)?;
        optimized_plan = self.push_down_projections(optimized_plan)?;
        optimized_plan = self.reorder_joins(optimized_plan)?;
        optimized_plan = self.choose_join_algorithms(optimized_plan)?;
        
        Ok(optimized_plan)
    }
    
    fn reorder_joins(&self, plan: ExecutionPlan) -> Result<ExecutionPlan, Error> {
        // Use dynamic programming to find optimal join order
        let join_nodes = self.extract_join_nodes(&plan);
        let optimal_order = self.find_optimal_join_order(join_nodes)?;
        
        self.rebuild_plan_with_join_order(plan, optimal_order)
    }
}
```

#### Statistics Collection

```rust
pub struct StatisticsManager {
    table_stats: HashMap<TableId, TableStatistics>,
    column_stats: HashMap<ColumnId, ColumnStatistics>,
}

pub struct TableStatistics {
    pub row_count: usize,
    pub page_count: usize,
    pub average_row_size: f64,
    pub last_updated: SystemTime,
}

pub struct ColumnStatistics {
    pub distinct_values: usize,
    pub null_count: usize,
    pub min_value: Option<Value>,
    pub max_value: Option<Value>,
    pub histogram: Histogram,
}
```

### Index Selection

```rust
impl QueryOptimizer {
    pub fn choose_best_index(
        &self,
        table_id: TableId,
        conditions: &[Condition],
    ) -> Result<Option<IndexId>, Error> {
        let available_indexes = self.index_manager.get_indexes_for_table(table_id)?;
        let mut best_index = None;
        let mut best_cost = f64::INFINITY;
        
        for index_id in available_indexes {
            let cost = self.estimate_index_cost(index_id, conditions)?;
            if cost < best_cost {
                best_cost = cost;
                best_index = Some(index_id);
            }
        }
        
        // Compare with table scan cost
        let table_scan_cost = self.estimate_table_scan_cost(table_id, conditions)?;
        if table_scan_cost < best_cost {
            return Ok(None);
        }
        
        Ok(best_index)
    }
}
```

## Security

### Authentication

```rust
pub struct AuthenticationManager {
    users: HashMap<String, UserInfo>,
    password_hasher: PasswordHasher,
}

pub struct UserInfo {
    pub username: String,
    pub password_hash: String,
    pub permissions: Vec<Permission>,
    pub created_at: SystemTime,
    pub last_login: Option<SystemTime>,
}

impl AuthenticationManager {
    pub async fn authenticate(&self, username: &str, password: &str) -> Result<UserInfo, Error> {
        let user = self.users.get(username)
            .ok_or(Error::InvalidCredentials)?;
        
        if self.password_hasher.verify(password, &user.password_hash)? {
            Ok(user.clone())
        } else {
            Err(Error::InvalidCredentials)
        }
    }
}
```

### Authorization

```rust
pub struct AuthorizationManager {
    permissions: HashMap<String, Vec<Permission>>,
}

#[derive(Debug, Clone)]
pub enum Permission {
    Select { table: String },
    Insert { table: String },
    Update { table: String },
    Delete { table: String },
    CreateTable,
    DropTable,
    CreateIndex,
    DropIndex,
}

impl AuthorizationManager {
    pub fn check_permission(&self, user: &UserInfo, permission: &Permission) -> bool {
        user.permissions.contains(permission) || 
        user.permissions.contains(&Permission::All)
    }
}
```

## Development Guidelines

### Code Organization

```
src/
├── lib.rs                 # Library entry point
├── main.rs               # Binary entry point
├── common/               # Common utilities and types
│   ├── error.rs         # Error definitions
│   ├── types.rs         # Common type definitions
│   └── utils.rs         # Utility functions
├── storage/             # Storage engine
│   ├── page.rs         # Page management
│   ├── buffer.rs       # Buffer pool
│   ├── file.rs         # File I/O
│   └── index/          # Index implementations
├── parser/              # SQL parsing
│   ├── lexer.rs        # Tokenization
│   ├── parser.rs       # Syntax analysis
│   └── ast.rs          # Abstract syntax tree
├── planner/             # Query planning
│   ├── planner.rs      # Query planning
│   └── optimizer.rs    # Query optimization
├── executor/            # Query execution
│   ├── operators.rs    # Execution operators
│   └── executor.rs     # Execution engine
├── core/               # Core database functionality
│   ├── transaction.rs  # Transaction management
│   ├── lock.rs         # Locking
│   └── mvcc.rs         # Multi-version concurrency control
├── logging/            # Logging and recovery
│   ├── wal.rs         # Write-ahead log
│   └── recovery.rs    # Crash recovery
├── network/            # Network layer
│   ├── server.rs      # Database server
│   └── connection.rs  # Connection handling
└── debug/              # Debugging and profiling
    ├── profiler.rs    # Performance profiling
    └── logger.rs      # Debug logging
```

### Error Handling

Use the `thiserror` crate for error definitions:

```rust
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("SQL parsing error: {message}")]
    SqlParsing { message: String },
    
    #[error("Execution error: {message}")]
    Execution { message: String },
    
    #[error("Transaction error: {message}")]
    Transaction { message: String },
    
    #[error("Storage error: {message}")]
    Storage { message: String },
    
    #[error("Internal error: {message}")]
    Internal { message: String },
}
```

### Testing Strategy

#### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_page_allocation() {
        let page_manager = PageManager::new(PageManagerConfig::default());
        let page_id = page_manager.allocate_page().await.unwrap();
        
        assert!(page_id > 0);
        
        let page = page_manager.read_page(page_id).await.unwrap();
        assert_eq!(page.header.page_id, page_id);
    }
}
```

#### Integration Tests

```rust
#[tokio::test]
async fn test_transaction_isolation() {
    let db = Database::connect(":memory:").await.unwrap();
    
    // Create test table
    db.execute("CREATE TABLE test (id INTEGER, value INTEGER)").await.unwrap();
    db.execute("INSERT INTO test VALUES (1, 100)").await.unwrap();
    
    // Start two transactions
    let tx1 = db.begin_transaction(IsolationLevel::ReadCommitted).await.unwrap();
    let tx2 = db.begin_transaction(IsolationLevel::ReadCommitted).await.unwrap();
    
    // Transaction 1 reads
    let result1 = tx1.execute("SELECT value FROM test WHERE id = 1").await.unwrap();
    assert_eq!(result1.rows[0].get::<i32>("value").unwrap(), 100);
    
    // Transaction 2 updates
    tx2.execute("UPDATE test SET value = 200 WHERE id = 1").await.unwrap();
    tx2.commit().await.unwrap();
    
    // Transaction 1 reads again (should see updated value in ReadCommitted)
    let result2 = tx1.execute("SELECT value FROM test WHERE id = 1").await.unwrap();
    assert_eq!(result2.rows[0].get::<i32>("value").unwrap(), 200);
    
    tx1.commit().await.unwrap();
}
```

### Performance Considerations

1. **Memory Management**: Use `Arc` and `Mutex` judiciously to avoid unnecessary cloning and locking
2. **Async I/O**: Use `tokio::fs` for file operations and `tokio::net` for network operations
3. **Buffer Pool**: Keep frequently accessed pages in memory
4. **Index Usage**: Create appropriate indexes for common query patterns
5. **Query Optimization**: Use statistics to make informed optimization decisions

### Documentation Standards

1. **API Documentation**: Use `///` for public API documentation
2. **Code Comments**: Explain complex algorithms and business logic
3. **Examples**: Provide usage examples for public APIs
4. **Architecture**: Document design decisions and trade-offs

This technical documentation provides a comprehensive overview of RustDB's internal architecture and implementation details. For more specific information about individual components, refer to the inline documentation and source code.
