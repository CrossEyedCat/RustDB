# RustDB User Guide

## Table of Contents
1. [Introduction](#introduction)
2. [Installation](#installation)
3. [Quick Start](#quick-start)
4. [Configuration](#configuration)
5. [SQL Operations](#sql-operations)
6. [Transaction Management](#transaction-management)
7. [Performance Tuning](#performance-tuning)
8. [Troubleshooting](#troubleshooting)
9. [Examples](#examples)

## Introduction

RustDB is a high-performance, ACID-compliant database management system written in Rust. It provides a robust foundation for applications requiring reliable data storage with strong consistency guarantees.

### Key Features
- **ACID Compliance**: Full support for Atomicity, Consistency, Isolation, and Durability
- **High Performance**: Optimized for concurrent operations with advanced locking mechanisms
- **SQL Support**: Comprehensive SQL query language support
- **Transaction Management**: Multiple isolation levels and deadlock detection
- **Recovery System**: Automatic crash recovery with WAL (Write-Ahead Logging)
- **Debugging Tools**: Built-in profiling and debugging capabilities

## Installation

### Prerequisites
- Rust 1.70+ (stable channel)
- 4GB+ RAM recommended
- 1GB+ disk space

### Building from Source

```bash
# Clone the repository
git clone https://github.com/your-org/rustdb.git
cd rustdb

# Build the project
cargo build --release

# Run tests to verify installation
cargo test
```

### Docker Installation

```bash
# Build Docker image
docker build -t rustdb .

# Run container
docker run -p 8080:8080 -v /path/to/data:/data rustdb
```

## Quick Start

### Starting the Database

```bash
# Start with default configuration
./target/release/rustdb

# Start with custom configuration
./target/release/rustdb --config config.toml
```

### Basic Connection

```rust
use rustdb::Database;

// Connect to database
let db = Database::connect("localhost:8080").await?;

// Create a table
db.execute("CREATE TABLE users (id INTEGER, name VARCHAR(100), email VARCHAR(255))").await?;

// Insert data
db.execute("INSERT INTO users VALUES (1, 'John Doe', 'john@example.com')").await?;

// Query data
let results = db.query("SELECT * FROM users WHERE id = 1").await?;
```

## Configuration

### Configuration File (config.toml)

```toml
[database]
name = "mydb"
data_directory = "./data"
max_connections = 100

[storage]
page_size = 8192
buffer_pool_size = 1000
checkpoint_interval = 300

[logging]
level = "info"
file = "./logs/rustdb.log"
max_file_size = "100MB"
max_files = 10

[network]
host = "0.0.0.0"
port = 8080
max_connections = 1000

[performance]
query_timeout = 30
lock_timeout = 10
deadlock_detection_interval = 1000
```

### Environment Variables

```bash
export RUSTDB_DATA_DIR="/var/lib/rustdb"
export RUSTDB_LOG_LEVEL="debug"
export RUSTDB_PORT="8080"
```

## SQL Operations

### Data Definition Language (DDL)

#### Creating Tables

```sql
-- Basic table creation
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table with constraints
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    amount DECIMAL(10,2) CHECK (amount > 0),
    status VARCHAR(20) DEFAULT 'pending'
);
```

#### Modifying Tables

```sql
-- Add column
ALTER TABLE users ADD COLUMN age INTEGER;

-- Drop column
ALTER TABLE users DROP COLUMN age;

-- Create index
CREATE INDEX idx_users_email ON users(email);

-- Drop index
DROP INDEX idx_users_email;
```

### Data Manipulation Language (DML)

#### Insert Operations

```sql
-- Single insert
INSERT INTO users (name, email) VALUES ('Jane Smith', 'jane@example.com');

-- Multiple inserts
INSERT INTO users (name, email) VALUES 
    ('Bob Johnson', 'bob@example.com'),
    ('Alice Brown', 'alice@example.com');

-- Insert with subquery
INSERT INTO users (name, email)
SELECT name, email FROM temp_users WHERE verified = true;
```

#### Update Operations

```sql
-- Update single row
UPDATE users SET email = 'newemail@example.com' WHERE id = 1;

-- Update multiple rows
UPDATE users SET status = 'active' WHERE created_at < '2023-01-01';

-- Update with subquery
UPDATE orders SET status = 'shipped' 
WHERE user_id IN (SELECT id FROM users WHERE premium = true);
```

#### Delete Operations

```sql
-- Delete specific rows
DELETE FROM users WHERE id = 1;

-- Delete with condition
DELETE FROM orders WHERE status = 'cancelled' AND created_at < '2023-01-01';

-- Truncate table
TRUNCATE TABLE temp_data;
```

### Query Operations

#### Basic Queries

```sql
-- Select all columns
SELECT * FROM users;

-- Select specific columns
SELECT id, name, email FROM users;

-- Select with condition
SELECT * FROM users WHERE age > 18;

-- Select with ordering
SELECT * FROM users ORDER BY name ASC, created_at DESC;

-- Select with limit
SELECT * FROM users LIMIT 10 OFFSET 20;
```

#### Joins

```sql
-- Inner join
SELECT u.name, o.amount
FROM users u
INNER JOIN orders o ON u.id = o.user_id;

-- Left join
SELECT u.name, COUNT(o.id) as order_count
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.id, u.name;

-- Self join
SELECT e1.name as employee, e2.name as manager
FROM employees e1
LEFT JOIN employees e2 ON e1.manager_id = e2.id;
```

#### Aggregation

```sql
-- Basic aggregation
SELECT COUNT(*) as total_users FROM users;
SELECT AVG(amount) as avg_order FROM orders;
SELECT MAX(created_at) as latest_order FROM orders;

-- Group by
SELECT status, COUNT(*) as count
FROM orders
GROUP BY status;

-- Having clause
SELECT user_id, COUNT(*) as order_count
FROM orders
GROUP BY user_id
HAVING COUNT(*) > 5;
```

## Transaction Management

### Transaction Basics

```rust
use rustdb::{Database, IsolationLevel};

let db = Database::connect("localhost:8080").await?;

// Begin transaction
let tx = db.begin_transaction(IsolationLevel::ReadCommitted).await?;

// Execute operations within transaction
tx.execute("INSERT INTO users (name, email) VALUES ('John', 'john@example.com')").await?;
tx.execute("UPDATE accounts SET balance = balance - 100 WHERE user_id = 1").await?;

// Commit transaction
tx.commit().await?;
```

### Isolation Levels

```rust
use rustdb::IsolationLevel;

// Read Committed (default)
let tx1 = db.begin_transaction(IsolationLevel::ReadCommitted).await?;

// Repeatable Read
let tx2 = db.begin_transaction(IsolationLevel::RepeatableRead).await?;

// Serializable
let tx3 = db.begin_transaction(IsolationLevel::Serializable).await?;
```

### Error Handling and Rollback

```rust
let tx = db.begin_transaction(IsolationLevel::ReadCommitted).await?;

match tx.execute("INSERT INTO users (name, email) VALUES ('John', 'john@example.com')").await {
    Ok(_) => {
        // Continue with more operations
        match tx.execute("UPDATE accounts SET balance = balance - 100 WHERE user_id = 1").await {
            Ok(_) => tx.commit().await?,
            Err(e) => {
                // Rollback on error
                tx.rollback().await?;
                return Err(e);
            }
        }
    }
    Err(e) => {
        tx.rollback().await?;
        return Err(e);
    }
}
```

## Performance Tuning

### Index Optimization

```sql
-- Create indexes for frequently queried columns
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_orders_user_status ON orders(user_id, status);

-- Composite indexes for complex queries
CREATE INDEX idx_orders_date_status ON orders(created_at, status);

-- Partial indexes for filtered queries
CREATE INDEX idx_active_users ON users(email) WHERE status = 'active';
```

### Query Optimization

```sql
-- Use EXPLAIN to analyze query plans
EXPLAIN SELECT * FROM users WHERE email = 'john@example.com';

-- Optimize joins by ensuring proper indexes
EXPLAIN SELECT u.name, o.amount
FROM users u
INNER JOIN orders o ON u.id = o.user_id
WHERE u.status = 'active';
```

### Configuration Tuning

```toml
[performance]
# Increase buffer pool for better caching
buffer_pool_size = 2000

# Adjust checkpoint frequency
checkpoint_interval = 600

# Tune lock timeouts
lock_timeout = 5
deadlock_detection_interval = 500

[storage]
# Optimize page size for your workload
page_size = 16384

# Enable compression for large datasets
compression_enabled = true
```

## Troubleshooting

### Common Issues

#### Connection Issues

```bash
# Check if database is running
ps aux | grep rustdb

# Check port availability
netstat -tlnp | grep 8080

# Check logs
tail -f logs/rustdb.log
```

#### Performance Issues

```sql
-- Check active connections
SHOW CONNECTIONS;

-- Check lock status
SHOW LOCKS;

-- Check transaction status
SHOW TRANSACTIONS;

-- Analyze slow queries
SHOW SLOW_QUERIES;
```

#### Deadlock Resolution

```rust
use rustdb::DeadlockResolution;

// Configure deadlock resolution strategy
let config = DatabaseConfig::default()
    .with_deadlock_resolution(DeadlockResolution::YoungestTransactionWins)
    .with_deadlock_detection_interval(Duration::from_millis(1000));

let db = Database::connect_with_config("localhost:8080", config).await?;
```

### Log Analysis

```bash
# Monitor error logs
grep "ERROR" logs/rustdb.log

# Check performance metrics
grep "SLOW_QUERY" logs/rustdb.log

# Monitor deadlock events
grep "DEADLOCK" logs/rustdb.log
```

## Examples

### Web Application Integration

```rust
use rustdb::Database;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize database
    let db = Database::connect("localhost:8080").await?;
    
    // Create tables
    db.execute("CREATE TABLE IF NOT EXISTS posts (
        id INTEGER PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        content TEXT,
        author_id INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )").await?;
    
    // Start web server
    let listener = TcpListener::bind("0.0.0.0:3000").await?;
    
    loop {
        let (stream, _) = listener.accept().await?;
        let db = db.clone();
        
        tokio::spawn(async move {
            handle_connection(stream, db).await;
        });
    }
}

async fn handle_connection(stream: TcpStream, db: Database) {
    // Handle HTTP requests and database operations
    // Implementation details...
}
```

### Batch Processing

```rust
use rustdb::Database;

async fn batch_insert_users(db: &Database, users: Vec<User>) -> Result<(), Box<dyn std::error::Error>> {
    let tx = db.begin_transaction(IsolationLevel::ReadCommitted).await?;
    
    for user in users {
        tx.execute(&format!(
            "INSERT INTO users (name, email, age) VALUES ('{}', '{}', {})",
            user.name, user.email, user.age
        )).await?;
    }
    
    tx.commit().await?;
    Ok(())
}
```

### Data Migration

```rust
use rustdb::Database;

async fn migrate_data(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    // Create new table structure
    db.execute("CREATE TABLE users_v2 (
        id INTEGER PRIMARY KEY,
        username VARCHAR(50) UNIQUE NOT NULL,
        email VARCHAR(255) UNIQUE NOT NULL,
        full_name VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )").await?;
    
    // Migrate data
    db.execute("INSERT INTO users_v2 (id, username, email, full_name)
        SELECT id, name, email, name FROM users").await?;
    
    // Drop old table
    db.execute("DROP TABLE users").await?;
    
    // Rename new table
    db.execute("ALTER TABLE users_v2 RENAME TO users").await?;
    
    Ok(())
}
```

## Support and Resources

### Getting Help
- **Documentation**: [Full API Reference](API_REFERENCE.md)
- **Issues**: [GitHub Issues](https://github.com/your-org/rustdb/issues)
- **Discussions**: [GitHub Discussions](https://github.com/your-org/rustdb/discussions)

### Contributing
- **Code**: [Contributing Guide](CONTRIBUTING.md)
- **Architecture**: [Architecture Guide](ARCHITECTURE_GUIDE.md)
- **Testing**: Run `cargo test` to verify your changes

### License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
