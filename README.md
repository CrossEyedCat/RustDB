# RustDB - Relational Database Implementation in Rust

This project is an implementation of a custom relational database in Rust.

## 🎯 Project Goal

Create a fully functional relational database with support for SQL-like query language, ACID transactions, and efficient data storage.

## 🏗️ System Architecture

### 1. Database Core
- **Memory Manager** - buffer and cache management
- **Transaction Manager** - ensuring ACID properties
- **Lock Manager** - managing concurrent access
- **Recovery Manager** - logging and recovery after failures

### 2. Data Storage
- **Page Manager** - working with data blocks on disk
- **File Manager** - organizing database file structure
- **Index Manager** - B+ trees, hash indexes
- **Table Manager** - data structures for storing tables

### 3. Parser and Planner
- **Lexical Analyzer** - SQL query tokenization
- **Syntax Analyzer** - building AST
- **Semantic Analyzer** - query correctness validation
- **Query Optimizer** - selecting optimal execution plan
- **Planner** - creating query execution plans

### 4. Query Executor
- **Scan Operators** - TableScan, IndexScan
- **Join Operators** - NestedLoop, HashJoin, MergeJoin
- **Aggregation Operators** - GroupBy, Aggregate
- **Sort Operators** - OrderBy, TopK

### 5. Metadata Catalog
- **Database Schema** - information about tables, columns, indexes
- **Statistics** - information about data distribution
- **Access Rights** - user and permission management

## 🚀 Main Components for Implementation

### Phase 1: Basic Infrastructure
- [ ] Data structures for pages and blocks
- [ ] Buffer manager with LRU caching
- [ ] Basic file manager
- [ ] Simple logging system

### Phase 2: Data Storage
- [ ] Table structures (Tuple, Schema)
- [ ] Page manager with CRUD operations support
- [ ] Basic indexes (B+ tree)
- [ ] Transaction manager with simple 2PL

### Phase 3: Parsing and Planning
- [ ] SQL lexical and syntax analyzer
- [ ] AST construction for queries
- [ ] Simple query planner
- [ ] Basic execution operators

### Phase 4: Query Execution
- [ ] Table scan operators
- [ ] Join operators
- [ ] Aggregation and sort operators
- [ ] Simple query optimization

### Phase 5: Advanced Features
- [ ] ACID transaction support
- [ ] Recovery system
- [ ] Concurrent access management
- [ ] Complex query optimization

## 🚀 Quick Start

### Requirements
- **Rust 1.70+** - for main code
- **Cargo** - for dependency management

### Installation and Running
```bash
# Clone repository
git clone <your-repo-url>
cd RustDB

# Build project
cargo build

# Run CLI
cargo run -- --help

# Show database information
cargo run -- info

# Run tests
cargo test
```

## 🛠️ Technical Requirements

### Programming Language
- **Rust** - for main code
- **Cargo** - for dependency management

### Main Dependencies
- `serde` - serialization/deserialization
- `tokio` - asynchronous execution
- `clap` - CLI interface
- `log` - logging
- `anyhow` - error handling

### Project Structure
```
src/
├── core/           # Database core
├── storage/        # Data storage
├── parser/         # SQL parser
├── planner/        # Query planner
├── executor/       # Query executor
├── catalog/        # Metadata
├── network/        # Network layer (optional)
└── main.rs         # Entry point
```

## 📚 Documentation

- [System Architecture](ARCHITECTURE.md)
- [Architecture Guide](ARCHITECTURE_GUIDE.md)
- [Development Guide](DEVELOPMENT.md)
- [Coding Standards](CODING_STANDARDS.md)
- [API Reference](API_REFERENCE.md)
- [Usage Examples](EXAMPLES.md)
- [Rustdoc Guide](RUSTDOC_GUIDE.md)
- [Testing Guide](TESTING_GUIDE.md)
- [CI/CD Guide](CI_CD_GUIDE.md)
- [Deployment Guide](DEPLOYMENT.md)
- [Contributing Guide](CONTRIBUTING.md)

## 📚 Educational Resources

### Books
- "Database System Concepts" - Silberschatz, Korth, Sudarshan
- "Database Management Systems" - Raghu Ramakrishnan, Johannes Gehrke
- "Transaction Processing: Concepts and Techniques" - Jim Gray, Andreas Reuter

### Articles and Documents
- PostgreSQL Architecture
- MySQL InnoDB Architecture
- B+ Tree Documentation
- Table Join Algorithms

## 🎯 Success Criteria

- [ ] Support for basic SQL operations (SELECT, INSERT, UPDATE, DELETE)
- [ ] ACID transactions
- [ ] Efficient indexes
- [ ] Query optimization
- [ ] Concurrent query processing
- [ ] Recovery after failures

## 🚧 Current Status

The project is in the early stages of development.

### ✅ What's Already Implemented:
- **Basic project structure** with modular architecture
- **Error handling system** using thiserror
- **Basic data types** (DataType, Column, Schema, Row)
- **Utilities** for validation and calculations
- **CLI interface** with basic commands
- **Test infrastructure** with 10 passing tests
- **Stubs for all main modules** according to architecture

### 🔄 Next Steps:
1. **Buffer manager implementation** with LRU caching
2. **Creating page structures** and data blocks
3. **File manager implementation** for disk operations
4. **Adding logging system** and monitoring

## 🤝 Contributing

Contributions are welcome! Please create issues to discuss new features and submit pull requests with improvements.

## 📄 License

MIT License

---

**Note**: This is an educational project for learning the internals of relational databases. Not recommended for production use.
