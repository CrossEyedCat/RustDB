//! Тесты для структур лог-записей

use crate::logging::log_record::*;
use std::collections::HashMap;

#[test]
fn test_log_record_type_variants() {
    let types = vec![
        LogRecordType::TransactionBegin,
        LogRecordType::TransactionCommit,
        LogRecordType::TransactionAbort,
        LogRecordType::DataInsert,
        LogRecordType::DataUpdate,
        LogRecordType::DataDelete,
        LogRecordType::Checkpoint,
        LogRecordType::CheckpointEnd,
    ];

    assert_eq!(types.len(), 8);
}

#[test]
fn test_log_priority_ordering() {
    assert!(LogPriority::Low < LogPriority::Normal);
    assert!(LogPriority::Normal < LogPriority::High);
    assert!(LogPriority::High < LogPriority::Critical);
}

#[test]
fn test_log_priority_values() {
    assert_eq!(LogPriority::Low as i32, 0);
    assert_eq!(LogPriority::Normal as i32, 1);
    assert_eq!(LogPriority::High as i32, 2);
    assert_eq!(LogPriority::Critical as i32, 3);
}

#[test]
fn test_record_operation_creation() {
    let op = RecordOperation {
        file_id: 1,
        page_id: 100,
        record_offset: 50,
        record_size: 128,
        old_data: Some(vec![1, 2, 3]),
        new_data: Some(vec![4, 5, 6]),
    };

    assert_eq!(op.file_id, 1);
    assert_eq!(op.page_id, 100);
    assert_eq!(op.record_offset, 50);
    assert_eq!(op.record_size, 128);
    assert!(op.old_data.is_some());
    assert!(op.new_data.is_some());
}

#[test]
fn test_transaction_operation_creation() {
    let op = TransactionOperation {
        dirty_pages: vec![(1, 100), (1, 200)],
        locked_resources: vec!["table1".to_string(), "table2".to_string()],
        start_time: 1000,
        isolation_level: IsolationLevel::ReadCommitted,
    };

    assert_eq!(op.dirty_pages.len(), 2);
    assert_eq!(op.locked_resources.len(), 2);
    assert_eq!(op.start_time, 1000);
}

#[test]
fn test_isolation_levels() {
    let levels = vec![
        IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted,
        IsolationLevel::RepeatableRead,
        IsolationLevel::Serializable,
    ];

    assert_eq!(levels.len(), 4);
}

#[test]
fn test_log_operation_data_variants() {
    let variants = vec![
        LogOperationData::Empty,
        LogOperationData::Record(RecordOperation {
            file_id: 1,
            page_id: 100,
            record_offset: 0,
            record_size: 64,
            old_data: None,
            new_data: Some(vec![1, 2, 3]),
        }),
        LogOperationData::Transaction(TransactionOperation {
            dirty_pages: vec![],
            locked_resources: vec![],
            start_time: 1000,
            isolation_level: IsolationLevel::ReadCommitted,
        }),
    ];

    assert_eq!(variants.len(), 3);
}

#[test]
fn test_log_record_creation_begin() {
    let record = LogRecord {
        lsn: 1,
        transaction_id: Some(100),
        record_type: LogRecordType::TransactionBegin,
        priority: LogPriority::High,
        timestamp: 1000,
        record_size: 64,
        checksum: 12345,
        operation_data: LogOperationData::Empty,
        prev_lsn: None,
        metadata: HashMap::new(),
    };

    assert_eq!(record.lsn, 1);
    assert_eq!(record.transaction_id, Some(100));
    assert_eq!(record.record_type, LogRecordType::TransactionBegin);
    assert_eq!(record.priority, LogPriority::High);
}

#[test]
fn test_log_record_creation_commit() {
    let record = LogRecord {
        lsn: 10,
        transaction_id: Some(100),
        record_type: LogRecordType::TransactionCommit,
        priority: LogPriority::High,
        timestamp: 2000,
        record_size: 64,
        checksum: 12345,
        operation_data: LogOperationData::Empty,
        prev_lsn: Some(9),
        metadata: HashMap::new(),
    };

    assert_eq!(record.lsn, 10);
    assert_eq!(record.prev_lsn, Some(9));
    assert_eq!(record.record_type, LogRecordType::TransactionCommit);
}

#[test]
fn test_log_record_with_metadata() {
    let mut metadata = HashMap::new();
    metadata.insert("key1".to_string(), "value1".to_string());
    metadata.insert("key2".to_string(), "value2".to_string());

    let record = LogRecord {
        lsn: 5,
        transaction_id: Some(50),
        record_type: LogRecordType::DataUpdate,
        priority: LogPriority::Normal,
        timestamp: 1500,
        record_size: 128,
        checksum: 54321,
        operation_data: LogOperationData::Empty,
        prev_lsn: Some(4),
        metadata,
    };

    assert_eq!(record.metadata.len(), 2);
    assert_eq!(record.metadata.get("key1"), Some(&"value1".to_string()));
}

#[test]
fn test_log_record_with_operation_data() {
    let op_data = LogOperationData::Record(RecordOperation {
        file_id: 2,
        page_id: 500,
        record_offset: 100,
        record_size: 256,
        old_data: Some(vec![1, 2, 3, 4]),
        new_data: Some(vec![5, 6, 7, 8]),
    });

    let record = LogRecord {
        lsn: 20,
        transaction_id: Some(200),
        record_type: LogRecordType::DataUpdate,
        priority: LogPriority::Normal,
        timestamp: 3000,
        record_size: 300,
        checksum: 99999,
        operation_data: op_data,
        prev_lsn: Some(19),
        metadata: HashMap::new(),
    };

    match record.operation_data {
        LogOperationData::Record(op) => {
            assert_eq!(op.file_id, 2);
            assert_eq!(op.page_id, 500);
        }
        _ => panic!("Expected Record operation data"),
    }
}

#[test]
fn test_log_record_checksum() {
    let record1 = LogRecord {
        lsn: 1,
        transaction_id: Some(1),
        record_type: LogRecordType::DataInsert,
        priority: LogPriority::Normal,
        timestamp: 1000,
        record_size: 100,
        checksum: 11111,
        operation_data: LogOperationData::Empty,
        prev_lsn: None,
        metadata: HashMap::new(),
    };

    let record2 = LogRecord {
        lsn: 1,
        transaction_id: Some(1),
        record_type: LogRecordType::DataInsert,
        priority: LogPriority::Normal,
        timestamp: 1000,
        record_size: 100,
        checksum: 22222, // Разная контрольная сумма
        operation_data: LogOperationData::Empty,
        prev_lsn: None,
        metadata: HashMap::new(),
    };

    assert_ne!(record1.checksum, record2.checksum);
}

#[test]
fn test_log_record_chain() {
    let records = vec![
        LogRecord {
            lsn: 1,
            transaction_id: Some(100),
            record_type: LogRecordType::TransactionBegin,
            priority: LogPriority::High,
            timestamp: 1000,
            record_size: 64,
            checksum: 1000,
            operation_data: LogOperationData::Empty,
            prev_lsn: None,
            metadata: HashMap::new(),
        },
        LogRecord {
            lsn: 2,
            transaction_id: Some(100),
            record_type: LogRecordType::DataInsert,
            priority: LogPriority::Normal,
            timestamp: 1001,
            record_size: 128,
            checksum: 2000,
            operation_data: LogOperationData::Empty,
            prev_lsn: Some(1),
            metadata: HashMap::new(),
        },
        LogRecord {
            lsn: 3,
            transaction_id: Some(100),
            record_type: LogRecordType::TransactionCommit,
            priority: LogPriority::High,
            timestamp: 1002,
            record_size: 64,
            checksum: 3000,
            operation_data: LogOperationData::Empty,
            prev_lsn: Some(2),
            metadata: HashMap::new(),
        },
    ];

    assert_eq!(records[0].prev_lsn, None);
    assert_eq!(records[1].prev_lsn, Some(1));
    assert_eq!(records[2].prev_lsn, Some(2));
}

#[test]
fn test_log_record_size_calculation() {
    let record = LogRecord {
        lsn: 1,
        transaction_id: Some(1),
        record_type: LogRecordType::DataUpdate,
        priority: LogPriority::Normal,
        timestamp: 1000,
        record_size: 1024,
        checksum: 5000,
        operation_data: LogOperationData::Record(RecordOperation {
            file_id: 1,
            page_id: 100,
            record_offset: 0,
            record_size: 512,
            old_data: Some(vec![0; 256]),
            new_data: Some(vec![1; 256]),
        }),
        prev_lsn: None,
        metadata: HashMap::new(),
    };

    assert_eq!(record.record_size, 1024);
}
