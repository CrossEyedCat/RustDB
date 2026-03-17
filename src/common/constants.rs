//! Constants for rustdb

/// Magic number for database files
pub const DB_MAGIC: u32 = 0x52535442; // "RSTB" in hex

/// File format version
pub const DB_VERSION: u32 = 1;

/// Database file header size in bytes
pub const DB_HEADER_SIZE: usize = 128;

/// Maximum number of tables in the database
pub const MAX_TABLES: usize = 1000;

/// Maximum number of columns in a table
pub const MAX_COLUMNS: usize = 100;

/// Maximum table name length
pub const MAX_TABLE_NAME_LENGTH: usize = 64;

/// Maximum column name length
pub const MAX_COLUMN_NAME_LENGTH: usize = 64;

/// Maximum index name length
pub const MAX_INDEX_NAME_LENGTH: usize = 64;

/// Maximum VARCHAR string size
pub const MAX_VARCHAR_LENGTH: usize = 65535;

/// Maximum TEXT field size
pub const MAX_TEXT_LENGTH: usize = 4294967295; // 2^32 - 1

/// Maximum BLOB field size
pub const MAX_BLOB_LENGTH: usize = 4294967295; // 2^32 - 1

/// Default buffer size
pub const DEFAULT_BUFFER_SIZE: usize = 8192;

/// Maximum buffer size
pub const MAX_BUFFER_SIZE: usize = 1048576; // 1MB

/// Default lock timeout (in seconds)
pub const DEFAULT_LOCK_TIMEOUT: u64 = 30;

/// Maximum lock timeout (in seconds)
pub const MAX_LOCK_TIMEOUT: u64 = 300;

/// Default transaction timeout (in seconds)
pub const DEFAULT_TRANSACTION_TIMEOUT: u64 = 60;

/// Maximum transaction timeout (in seconds)
pub const MAX_TRANSACTION_TIMEOUT: u64 = 3600;

/// Default log file size (in bytes)
pub const DEFAULT_LOG_FILE_SIZE: usize = 16777216; // 16MB

/// Maximum log file size (in bytes)
pub const MAX_LOG_FILE_SIZE: usize = 1073741824; // 1GB

/// Default number of log files
pub const DEFAULT_LOG_FILES_COUNT: usize = 10;

/// Maximum number of log files
pub const MAX_LOG_FILES_COUNT: usize = 100;

/// Default page size
pub const DEFAULT_PAGE_SIZE: usize = 4096;

/// Supported page sizes
pub const SUPPORTED_PAGE_SIZES: &[usize] = &[1024, 2048, 4096, 8192, 16384, 32768, 65536];

/// Minimum page size
pub const MIN_PAGE_SIZE: usize = 1024;

/// Maximum page size
pub const MAX_PAGE_SIZE: usize = 65536;

/// Page header size (as percentage of page size)
pub const PAGE_HEADER_PERCENTAGE: f64 = 0.1;

/// Minimum number of records per page
pub const MIN_RECORDS_PER_PAGE: usize = 2;

/// Maximum number of records per page
pub const MAX_RECORDS_PER_PAGE: usize = 1000;

/// Page fill threshold for splitting (as percentage)
pub const PAGE_SPLIT_THRESHOLD: f64 = 0.9;

/// Page fill threshold for merging (as percentage)
pub const PAGE_MERGE_THRESHOLD: f64 = 0.3;

/// Default B+ tree key size
pub const DEFAULT_BTREE_KEY_SIZE: usize = 256;

/// Maximum B+ tree key size
pub const MAX_BTREE_KEY_SIZE: usize = 1024;

/// Default B+ tree order
pub const DEFAULT_BTREE_ORDER: usize = 50;

/// Minimum B+ tree order
pub const MIN_BTREE_ORDER: usize = 3;

/// Maximum B+ tree order
pub const MAX_BTREE_ORDER: usize = 1000;

/// Default hash table size
pub const DEFAULT_HASH_TABLE_SIZE: usize = 1024;

/// Maximum hash table size
pub const MAX_HASH_TABLE_SIZE: usize = 1048576; // 1M

/// Default hash table load factor
pub const DEFAULT_HASH_LOAD_FACTOR: f64 = 0.75;

/// Maximum hash table load factor
pub const MAX_HASH_LOAD_FACTOR: f64 = 0.95;

/// Minimum hash table load factor
pub const MIN_HASH_LOAD_FACTOR: f64 = 0.25;

/// Default connection pool size
pub const DEFAULT_CONNECTION_POOL_SIZE: usize = 10;

/// Maximum connection pool size
pub const MAX_CONNECTION_POOL_SIZE: usize = 1000;

/// Default connection timeout (in seconds)
pub const DEFAULT_CONNECTION_TIMEOUT: u64 = 30;

/// Maximum connection timeout (in seconds)
pub const MAX_CONNECTION_TIMEOUT: u64 = 300;

/// Default network I/O buffer size
pub const DEFAULT_NETWORK_BUFFER_SIZE: usize = 8192;

/// Maximum network I/O buffer size
pub const MAX_NETWORK_BUFFER_SIZE: usize = 1048576; // 1MB

/// Default number of worker threads
pub const DEFAULT_WORKER_THREADS: usize = 4;

/// Maximum number of worker threads
pub const MAX_WORKER_THREADS: usize = 64;

/// Default thread stack size (in bytes)
pub const DEFAULT_THREAD_STACK_SIZE: usize = 2097152; // 2MB

/// Maximum thread stack size (in bytes)
pub const MAX_THREAD_STACK_SIZE: usize = 67108864; // 64MB

/// Default cleanup timeout (in seconds)
pub const DEFAULT_CLEANUP_TIMEOUT: u64 = 300; // 5 minutes

/// Maximum cleanup timeout (in seconds)
pub const MAX_CLEANUP_TIMEOUT: u64 = 3600; // 1 hour

/// Default cleanup interval (in seconds)
pub const DEFAULT_CLEANUP_INTERVAL: u64 = 60; // 1 minute

/// Maximum cleanup interval (in seconds)
pub const MAX_CLEANUP_INTERVAL: u64 = 3600; // 1 hour
