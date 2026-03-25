//! Additional unit tests to increase coverage (errors, config, types).

use crate::common::config::{
    DatabaseConfig, LoggingConfig, NetworkConfig, PerformanceConfig, StorageConfig,
};
use crate::common::error::Error;
use crate::common::i18n::Language;
use crate::common::i18n::MessageKey;
use crate::common::types::PAGE_SIZE;
use crate::common::types::{ColumnValue, DataType};
use crate::common::utils::{
    calculate_hash, calculate_max_record_size, calculate_optimal_page_size,
    calculate_page_header_size, calculate_pages_needed, can_fit_record_on_page, format_bytes,
    format_duration, is_power_of_two, is_valid_column_name, is_valid_index_name,
    is_valid_page_size, is_valid_table_name, next_power_of_two, prev_power_of_two,
};
use std::path::PathBuf;

#[test]
fn test_error_constructors_and_display() {
    let cases: Vec<Error> = vec![
        Error::database("db"),
        Error::sql_parsing("sql"),
        Error::parser("p"),
        Error::timeout("t"),
        Error::conflict("c"),
        Error::semantic_analysis("s"),
        Error::query_planning("qp"),
        Error::query_execution("qe"),
        Error::transaction("tr"),
        Error::lock("lk"),
        Error::validation("v"),
        Error::configuration("cfg"),
        Error::unsupported("op"),
        Error::internal("in"),
        Error::localized_database(MessageKey::Welcome),
        Error::localized_database_with_params(MessageKey::Welcome, &["a"]),
        Error::localized_sql_parsing(MessageKey::Error),
        Error::localized_sql_parsing_with_params(MessageKey::Error, &["x"]),
        Error::localized_transaction(MessageKey::TransactionError),
        Error::localized_transaction_with_params(MessageKey::TransactionError, &["y"]),
        Error::localized_lock(MessageKey::LockTimeout),
        Error::localized_lock_with_params(MessageKey::LockTimeout, &["z"]),
        Error::localized_validation(MessageKey::InvalidQuery),
        Error::localized_validation_with_params(MessageKey::InvalidQuery, &["v"]),
        Error::localized_configuration(MessageKey::DatabaseError),
        Error::localized_configuration_with_params(MessageKey::DatabaseError, &["c"]),
        Error::localized_internal(MessageKey::InternalError),
        Error::localized_internal_with_params(MessageKey::InternalError, &["i"]),
        Error::TransactionError("te".into()),
        Error::LockError("le".into()),
        Error::DeadlockDetected("d".into()),
    ];
    for e in cases {
        let s = format!("{}", e);
        assert!(!s.is_empty());
    }
}

#[test]
fn test_config_defaults_and_merge() {
    let _ = DatabaseConfig::default();
    let _ = StorageConfig::default();
    let _ = LoggingConfig::default();
    let _ = NetworkConfig::default();
    let _ = PerformanceConfig::default();
    let mut db = DatabaseConfig::default();
    db.name = "x".into();
    let merged = db.merge(DatabaseConfig::default());
    assert_eq!(merged.name, "x");
}

#[test]
fn test_database_config_file_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db.toml");
    let original = DatabaseConfig {
        name: "mydb".into(),
        data_directory: "/var/data".into(),
        max_connections: 42,
        connection_timeout: 11,
        query_timeout: 22,
        language: Language::English,
        network: NetworkConfig::default(),
    };
    original.to_file(&path)?;
    let loaded = DatabaseConfig::from_file(&path)?;
    assert_eq!(loaded.name, original.name);
    assert_eq!(loaded.data_directory, original.data_directory);
    assert_eq!(loaded.max_connections, original.max_connections);
    assert_eq!(loaded.connection_timeout, original.connection_timeout);
    assert_eq!(loaded.query_timeout, original.query_timeout);
    assert_eq!(loaded.network.host, original.network.host);
    assert_eq!(loaded.network.port, original.network.port);
    Ok(())
}

#[test]
fn test_database_config_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let keys = [
        "RUSTDB_NAME",
        "RUSTDB_DATA_DIR",
        "RUSTDB_MAX_CONNECTIONS",
        "RUSTDB_LANGUAGE",
    ];
    let mut saved: Vec<(String, Option<String>)> = Vec::new();
    for k in keys {
        saved.push((k.to_string(), std::env::var(k).ok()));
    }
    std::env::set_var("RUSTDB_NAME", "envdb");
    std::env::set_var("RUSTDB_DATA_DIR", "/tmp/envdata");
    std::env::set_var("RUSTDB_MAX_CONNECTIONS", "7");
    std::env::set_var("RUSTDB_LANGUAGE", "en");
    let cfg = DatabaseConfig::from_env()?;
    assert_eq!(cfg.name, "envdb");
    assert_eq!(cfg.data_directory, "/tmp/envdata");
    assert_eq!(cfg.max_connections, 7);
    assert_eq!(cfg.language, Language::English);
    for (k, v) in saved {
        match v {
            Some(val) => std::env::set_var(&k, val),
            None => std::env::remove_var(&k),
        }
    }
    Ok(())
}

#[test]
fn test_database_config_merge_non_defaults() {
    let base = DatabaseConfig::default();
    let other = DatabaseConfig {
        name: "merged".into(),
        data_directory: "/other".into(),
        max_connections: 5,
        connection_timeout: 99,
        query_timeout: 88,
        language: Language::English,
        ..Default::default()
    };
    let m = base.merge(other);
    assert_eq!(m.name, "merged");
    assert_eq!(m.data_directory, "/other");
    assert_eq!(m.max_connections, 5);
    assert_eq!(m.connection_timeout, 99);
    assert_eq!(m.query_timeout, 88);
}

#[test]
fn test_storage_logging_network_performance_serde_roundtrip() {
    let s = StorageConfig::default();
    let json = serde_json::to_string(&s).unwrap();
    let s2: StorageConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(s2.page_size, s.page_size);

    let l = LoggingConfig::default();
    let lj = serde_json::to_string(&l).unwrap();
    let l2: LoggingConfig = serde_json::from_str(&lj).unwrap();
    assert_eq!(l2.level, l.level);

    let n = NetworkConfig::default();
    let nj = serde_json::to_string(&n).unwrap();
    let n2: NetworkConfig = serde_json::from_str(&nj).unwrap();
    assert_eq!(n2.port, n.port);

    let p = PerformanceConfig::default();
    let pj = serde_json::to_string(&p).unwrap();
    let p2: PerformanceConfig = serde_json::from_str(&pj).unwrap();
    assert_eq!(p2.max_query_plan_cache_size, p.max_query_plan_cache_size);
}

#[test]
fn test_logging_config_pathbuf_clone() {
    let mut l = LoggingConfig::default();
    l.file = PathBuf::from("./custom.log");
    let j = serde_json::to_string(&l).unwrap();
    let l2: LoggingConfig = serde_json::from_str(&j).unwrap();
    assert!(l2.file.to_string_lossy().contains("custom"));
}

#[test]
fn test_data_type_size_and_column_value() {
    let types = [
        DataType::Null,
        DataType::Boolean(true),
        DataType::TinyInt(1),
        DataType::SmallInt(2),
        DataType::Integer(3),
        DataType::BigInt(4),
        DataType::Float(1.0),
        DataType::Double(2.0),
        DataType::Char("a".into()),
        DataType::Varchar("hi".into()),
        DataType::Text("t".into()),
        DataType::Date("2020-01-01".into()),
        DataType::Time("12:00:00".into()),
        DataType::Timestamp("2020-01-01 00:00:00".into()),
        DataType::Blob(vec![1, 2]),
    ];
    for dt in types {
        let _ = dt.size();
        let _ = dt.is_null();
        let _ = dt.is_numeric();
        let _ = dt.is_string();
    }
    let cv = ColumnValue::new(DataType::Varchar("hi".into()));
    assert!(!format!("{:?}", cv).is_empty());
    let _ = ColumnValue::null();
    assert!(ColumnValue::null().is_null());
}

#[test]
fn test_error_from_io_and_json() {
    let io_err: Error = std::io::Error::new(std::io::ErrorKind::NotFound, "nope").into();
    assert!(!format!("{}", io_err).is_empty());
    let json_err: Error = serde_json::from_str::<i32>("not json").unwrap_err().into();
    assert!(!format!("{}", json_err).is_empty());
}

#[test]
fn test_database_config_validate() {
    let ok = DatabaseConfig::default();
    assert!(ok.validate().is_ok());
    let mut bad = DatabaseConfig::default();
    bad.name = "".into();
    assert!(bad.validate().is_err());
    let mut bad2 = DatabaseConfig::default();
    bad2.max_connections = 0;
    assert!(bad2.validate().is_err());
}

#[test]
fn test_utils_functions() {
    assert!(is_power_of_two(8));
    assert!(!is_power_of_two(7));
    assert_eq!(next_power_of_two(5), 8);
    assert_eq!(prev_power_of_two(5), 4);
    assert!(is_valid_table_name("t1"));
    assert!(is_valid_column_name("col_a"));
    assert!(is_valid_index_name("idx_a"));
    assert!(is_valid_page_size(PAGE_SIZE));
    let _ = calculate_hash(&"x");
    let _ = format_bytes(1024);
    let _ = format_duration(2);
    let ph = calculate_page_header_size(PAGE_SIZE);
    let mr = calculate_max_record_size(PAGE_SIZE);
    assert!(can_fit_record_on_page(10, PAGE_SIZE));
    assert_eq!(calculate_pages_needed(100, PAGE_SIZE) >= 1, true);
    let _ = calculate_optimal_page_size(100);
    assert!(ph > 0 && mr > 0);
}

#[test]
fn test_utils_page_and_hash_heuristics() {
    use crate::common::utils::{
        calculate_optimal_btree_order, calculate_optimal_hash_table_size, should_expand_hash_table,
        should_merge_pages, should_shrink_hash_table, should_split_page,
    };
    let _ = should_split_page(100, 4096);
    let _ = should_merge_pages(50, 4096);
    let _ = calculate_optimal_btree_order(16);
    let _ = calculate_optimal_hash_table_size(100);
    let _ = should_expand_hash_table(16, 20);
    let _ = should_shrink_hash_table(1024, 2);
}
