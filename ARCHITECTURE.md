# RustDB Component Interaction Architecture

## Architecture Overview

RustDB is built on a multi-layered architecture where each layer is responsible for specific functionality and interacts with other layers through well-defined interfaces.

## Architecture Layers

### 1. Client Layer
**Purpose**: Provides interfaces for interacting with the database

**Components**:
- **CLI Client** - command line for executing SQL queries
- **Network Client** - network clients (drivers for various languages)

**Interaction**: Sends SQL queries to the server and receives results

### 2. Network Layer
**Purpose**: Handles network connections and protocols

**Components**:
- **Network Server** - accepts incoming connections
- **Connection Pool** - manages connection pool for efficiency

**Interaction**: 
- Accepts requests from clients
- Forwards requests to main server
- Sends results back to clients

### 3. Main Server
**Purpose**: Coordinates query execution and manages sessions

**Components**:
- **Main Server** - central coordinator
- **Session Manager** - manages user sessions

**Interaction**:
- Receives requests from network layer
- Creates and manages user sessions
- Coordinates query execution

### 4. Parser and Planner
**Purpose**: Analyzes SQL queries and creates execution plans

**Components**:
- **SQL Parser** - parses SQL into tokens
- **AST Builder** - builds abstract syntax tree
- **Query Planner** - creates query execution plan
- **Query Optimizer** - optimizes plan for better performance

**Interaction**:
- Receives SQL queries from Session Manager
- Accesses Schema Manager for metadata
- Creates optimized execution plan
- Passes plan to Query Executor

### 5. Query Executor
**Purpose**: Executes queries according to plan

**Components**:
- **Query Executor** - coordinates operator execution
- **Operator Tree** - tree of operators for execution
- **Result Set** - formats query results

**Interaction**:
- Receives plan from Query Optimizer
- Creates operator tree
- Interacts with Transaction Manager for transaction management
- Accesses Buffer Manager for data retrieval
- Formats and returns results

### 6. Database Core
**Purpose**: Provides core DBMS functionality

**Components**:
- **Transaction Manager** - manages ACID transactions
- **Lock Manager** - ensures transaction isolation
- **Recovery Manager** - logs operations and recovers system
- **Buffer Manager** - manages page caching in memory

**Interaction**:
- Transaction Manager coordinates all operations
- Lock Manager locks resources for isolation
- Recovery Manager logs all changes
- Buffer Manager caches frequently used pages

### 7. Data Storage
**Purpose**: Manages physical data storage

**Components**:
- **Page Manager** - works with data pages
- **File Manager** - manages database files
- **Index Manager** - supports indexes (B+ trees, hashes)
- **Table Manager** - manages table structure

**Interaction**:
- Page Manager interacts with Buffer Manager
- File Manager reads and writes data to disk
- Index Manager provides fast search
- Table Manager manages data schema

### 8. Metadata Catalog
**Purpose**: Stores information about database structure

**Components**:
- **Schema Manager** - manages table and column schema
- **Statistics Manager** - collects data statistics
- **Access Control** - manages access rights

**Interaction**:
- Provides metadata for parser and planner
- Updates when database structure changes
- Controls user access to objects

### 9. Physical Storage
**Purpose**: Files on disk

**Components**:
- **Data Files** - files with table data
- **Index Files** - index files
- **Log Files** - transaction log files
- **System Catalog** - metadata files

## Query Execution Flow

### 1. Request Reception
```
CLI Client → Network Server → Connection Pool → Main Server → Session Manager
```

### 2. Parsing and Planning
```
Session Manager → SQL Parser → AST Builder → Query Planner → Query Optimizer
```

### 3. Query Execution
```
Query Optimizer → Query Executor → Operator Tree → Buffer Manager → Page Manager
```

### 4. Result Return
```
Page Manager → Buffer Manager → Query Executor → Result Set → Main Server → Network Server → CLI Client
```

## Key Interaction Principles

### 1. Separation of Concerns
Each component is responsible for its own area and does not interfere with others

### 2. Loose Coupling
Components interact through well-defined interfaces

### 3. High Cohesion
Related functionality is grouped in a single component

### 4. Scalability
The architecture allows adding new components without changing existing ones

### 5. Fault Tolerance
The system can continue operating when individual components fail

## Extension Points

### 1. New Index Types
New index managers can be added implementing a common interface

### 2. Additional Operators
Query Executor supports adding new operator types

### 3. Alternative Planners
Query Optimizer can be replaced with more advanced algorithms

### 4. Different Protocols
Network Server supports connecting different protocols

This architecture ensures modularity, extensibility, and reliability of the system, which is critically important for a relational database.
