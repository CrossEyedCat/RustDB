//! Константы для rustdb

/// Магическое число для файлов базы данных
pub const DB_MAGIC: u32 = 0x52535442; // "RSTB" в hex

/// Версия формата файла
pub const DB_VERSION: u32 = 1;

/// Размер заголовка файла БД в байтах
pub const DB_HEADER_SIZE: usize = 128;

/// Максимальное количество таблиц в базе данных
pub const MAX_TABLES: usize = 1000;

/// Максимальное количество колонок в таблице
pub const MAX_COLUMNS: usize = 100;

/// Максимальная длина имени таблицы
pub const MAX_TABLE_NAME_LENGTH: usize = 64;

/// Максимальная длина имени колонки
pub const MAX_COLUMN_NAME_LENGTH: usize = 64;

/// Максимальная длина имени индекса
pub const MAX_INDEX_NAME_LENGTH: usize = 64;

/// Максимальный размер строки VARCHAR
pub const MAX_VARCHAR_LENGTH: usize = 65535;

/// Максимальный размер TEXT поля
pub const MAX_TEXT_LENGTH: usize = 4294967295; // 2^32 - 1

/// Максимальный размер BLOB поля
pub const MAX_BLOB_LENGTH: usize = 4294967295; // 2^32 - 1

/// Размер буфера по умолчанию
pub const DEFAULT_BUFFER_SIZE: usize = 8192;

/// Максимальный размер буфера
pub const MAX_BUFFER_SIZE: usize = 1048576; // 1MB

/// Таймаут блокировки по умолчанию (в секундах)
pub const DEFAULT_LOCK_TIMEOUT: u64 = 30;

/// Максимальный таймаут блокировки (в секундах)
pub const MAX_LOCK_TIMEOUT: u64 = 300;

/// Таймаут транзакции по умолчанию (в секундах)
pub const DEFAULT_TRANSACTION_TIMEOUT: u64 = 60;

/// Максимальный таймаут транзакции (в секундах)
pub const MAX_TRANSACTION_TIMEOUT: u64 = 3600;

/// Размер лог-файла по умолчанию (в байтах)
pub const DEFAULT_LOG_FILE_SIZE: usize = 16777216; // 16MB

/// Максимальный размер лог-файла (в байтах)
pub const MAX_LOG_FILE_SIZE: usize = 1073741824; // 1GB

/// Количество лог-файлов по умолчанию
pub const DEFAULT_LOG_FILES_COUNT: usize = 10;

/// Максимальное количество лог-файлов
pub const MAX_LOG_FILES_COUNT: usize = 100;

/// Размер страницы по умолчанию
pub const DEFAULT_PAGE_SIZE: usize = 4096;

/// Поддерживаемые размеры страниц
pub const SUPPORTED_PAGE_SIZES: &[usize] = &[1024, 2048, 4096, 8192, 16384, 32768, 65536];

/// Минимальный размер страницы
pub const MIN_PAGE_SIZE: usize = 1024;

/// Максимальный размер страницы
pub const MAX_PAGE_SIZE: usize = 65536;

/// Размер заголовка страницы (в процентах от размера страницы)
pub const PAGE_HEADER_PERCENTAGE: f64 = 0.1;

/// Минимальное количество записей на странице
pub const MIN_RECORDS_PER_PAGE: usize = 2;

/// Максимальное количество записей на странице
pub const MAX_RECORDS_PER_PAGE: usize = 1000;

/// Порог заполнения страницы для разделения (в процентах)
pub const PAGE_SPLIT_THRESHOLD: f64 = 0.9;

/// Порог заполнения страницы для объединения (в процентах)
pub const PAGE_MERGE_THRESHOLD: f64 = 0.3;

/// Размер ключа B+ дерева по умолчанию
pub const DEFAULT_BTREE_KEY_SIZE: usize = 256;

/// Максимальный размер ключа B+ дерева
pub const MAX_BTREE_KEY_SIZE: usize = 1024;

/// Порядок B+ дерева по умолчанию
pub const DEFAULT_BTREE_ORDER: usize = 50;

/// Минимальный порядок B+ дерева
pub const MIN_BTREE_ORDER: usize = 3;

/// Максимальный порядок B+ дерева
pub const MAX_BTREE_ORDER: usize = 1000;

/// Размер хеш-таблицы по умолчанию
pub const DEFAULT_HASH_TABLE_SIZE: usize = 1024;

/// Максимальный размер хеш-таблицы
pub const MAX_HASH_TABLE_SIZE: usize = 1048576; // 1M

/// Коэффициент загрузки хеш-таблицы по умолчанию
pub const DEFAULT_HASH_LOAD_FACTOR: f64 = 0.75;

/// Максимальный коэффициент загрузки хеш-таблицы
pub const MAX_HASH_LOAD_FACTOR: f64 = 0.95;

/// Минимальный коэффициент загрузки хеш-таблицы
pub const MIN_HASH_LOAD_FACTOR: f64 = 0.25;

/// Размер пула соединений по умолчанию
pub const DEFAULT_CONNECTION_POOL_SIZE: usize = 10;

/// Максимальный размер пула соединений
pub const MAX_CONNECTION_POOL_SIZE: usize = 1000;

/// Таймаут соединения по умолчанию (в секундах)
pub const DEFAULT_CONNECTION_TIMEOUT: u64 = 30;

/// Максимальный таймаут соединения (в секундах)
pub const MAX_CONNECTION_TIMEOUT: u64 = 300;

/// Размер буфера сетевого I/O по умолчанию
pub const DEFAULT_NETWORK_BUFFER_SIZE: usize = 8192;

/// Максимальный размер буфера сетевого I/O
pub const MAX_NETWORK_BUFFER_SIZE: usize = 1048576; // 1MB

/// Количество рабочих потоков по умолчанию
pub const DEFAULT_WORKER_THREADS: usize = 4;

/// Максимальное количество рабочих потоков
pub const MAX_WORKER_THREADS: usize = 64;

/// Размер стека потока по умолчанию (в байтах)
pub const DEFAULT_THREAD_STACK_SIZE: usize = 2097152; // 2MB

/// Максимальный размер стека потока (в байтах)
pub const MAX_THREAD_STACK_SIZE: usize = 67108864; // 64MB

/// Таймаут очистки по умолчанию (в секундах)
pub const DEFAULT_CLEANUP_TIMEOUT: u64 = 300; // 5 минут

/// Максимальный таймаут очистки (в секундах)
pub const MAX_CLEANUP_TIMEOUT: u64 = 3600; // 1 час

/// Интервал очистки по умолчанию (в секундах)
pub const DEFAULT_CLEANUP_INTERVAL: u64 = 60; // 1 минута

/// Максимальный интервал очистки (в секундах)
pub const MAX_CLEANUP_INTERVAL: u64 = 3600; // 1 час
