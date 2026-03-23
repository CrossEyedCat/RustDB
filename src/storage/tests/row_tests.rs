//! Tests for the Row structure

use crate::storage::row::{Row, RowHeader, RowStatus};
use crate::common::types::{PageId, ColumnValue};

#[test]
fn test_row_creation() {
    let row = Row::new(1, PageId(10), 100);
    
    assert_eq!(row.get_id(), 1);
    assert_eq!(row.get_page_id(), PageId(10));
    assert_eq!(row.get_offset(), 100);
    assert_eq!(row.get_status(), RowStatus::Active);
    assert!(row.is_dirty());
}

#[test]
fn test_row_header() {
    let row = Row::new(42, PageId(5), 200);
    let header = row.get_header();
    
    assert_eq!(header.row_id, 42);
    assert_eq!(header.page_id, PageId(5));
    assert_eq!(header.offset, 200);
    assert_eq!(header.status, RowStatus::Active);
}

#[test]
fn test_row_data_operations() {
    let mut row = Row::new(1, PageId(1), 0);
    
    // Adding data to a row
    row.add_column_value(ColumnValue::Integer(42));
    row.add_column_value(ColumnValue::Text("Hello".to_string()));
    row.add_column_value(ColumnValue::Boolean(true));
    
    assert_eq!(row.get_column_count(), 3);
    
    // Checking column values
    assert_eq!(row.get_column_value(0), Some(&ColumnValue::Integer(42)));
    assert_eq!(row.get_column_value(1), Some(&ColumnValue::Text("Hello".to_string())));
    assert_eq!(row.get_column_value(2), Some(&ColumnValue::Boolean(true)));
    assert_eq!(row.get_column_value(3), None);
}

#[test]
fn test_row_update_column() {
    let mut row = Row::new(1, PageId(1), 0);
    
    row.add_column_value(ColumnValue::Integer(10));
    row.add_column_value(ColumnValue::Text("Old".to_string()));
    
    // Updating column values
    let result1 = row.update_column_value(0, ColumnValue::Integer(20));
    let result2 = row.update_column_value(1, ColumnValue::Text("New".to_string()));
    
    assert!(result1.is_ok());
    assert!(result2.is_ok());
    
    assert_eq!(row.get_column_value(0), Some(&ColumnValue::Integer(20)));
    assert_eq!(row.get_column_value(1), Some(&ColumnValue::Text("New".to_string())));
}

#[test]
fn test_row_update_invalid_column() {
    let mut row = Row::new(1, PageId(1), 0);
    row.add_column_value(ColumnValue::Integer(1));
    
    // Trying to update a non-existent column
    let result = row.update_column_value(5, ColumnValue::Integer(2));
    assert!(result.is_err());
}

#[test]
fn test_row_status_changes() {
    let mut row = Row::new(1, PageId(1), 0);
    
    assert_eq!(row.get_status(), RowStatus::Active);
    
    // Mark the line as deleted
    row.mark_deleted();
    assert_eq!(row.get_status(), RowStatus::Deleted);
    assert!(row.is_dirty());
    
    // Restoring the line
    row.mark_active();
    assert_eq!(row.get_status(), RowStatus::Active);
    assert!(row.is_dirty());
}

#[test]
fn test_row_serialization() {
    let mut row = Row::new(42, PageId(10), 500);
    
    row.add_column_value(ColumnValue::Integer(123));
    row.add_column_value(ColumnValue::Text("Serialization test".to_string()));
    row.add_column_value(ColumnValue::Float(3.14));
    row.add_column_value(ColumnValue::Boolean(false));
    
    // Serialize a string
    let serialized = row.serialize();
    assert!(!serialized.is_empty());
    
    // Deserialize the string
    let deserialized = Row::deserialize(&serialized);
    assert!(deserialized.is_ok());
    
    let new_row = deserialized.unwrap();
    assert_eq!(new_row.get_id(), 42);
    assert_eq!(new_row.get_page_id(), PageId(10));
    assert_eq!(new_row.get_offset(), 500);
    assert_eq!(new_row.get_column_count(), 4);
    
    // Checking the data
    assert_eq!(new_row.get_column_value(0), Some(&ColumnValue::Integer(123)));
    assert_eq!(new_row.get_column_value(1), Some(&ColumnValue::Text("Serialization test".to_string())));
    assert_eq!(new_row.get_column_value(2), Some(&ColumnValue::Float(3.14)));
    assert_eq!(new_row.get_column_value(3), Some(&ColumnValue::Boolean(false)));
}

#[test]
fn test_row_different_data_types() {
    let mut row = Row::new(1, PageId(1), 0);
    
    // Testing different types of data
    row.add_column_value(ColumnValue::Integer(i64::MAX));
    row.add_column_value(ColumnValue::Integer(i64::MIN));
    row.add_column_value(ColumnValue::Float(f64::MAX));
    row.add_column_value(ColumnValue::Float(f64::MIN));
    row.add_column_value(ColumnValue::Text("".to_string()));
    row.add_column_value(ColumnValue::Text("🦀 Rust is awesome! 🚀".to_string())); // Unicode
    row.add_column_value(ColumnValue::Boolean(true));
    row.add_column_value(ColumnValue::Boolean(false));
    row.add_column_value(ColumnValue::Null);
    
    assert_eq!(row.get_column_count(), 9);
    
    // Checking all values
    assert_eq!(row.get_column_value(0), Some(&ColumnValue::Integer(i64::MAX)));
    assert_eq!(row.get_column_value(1), Some(&ColumnValue::Integer(i64::MIN)));
    assert_eq!(row.get_column_value(2), Some(&ColumnValue::Float(f64::MAX)));
    assert_eq!(row.get_column_value(3), Some(&ColumnValue::Float(f64::MIN)));
    assert_eq!(row.get_column_value(4), Some(&ColumnValue::Text("".to_string())));
    assert_eq!(row.get_column_value(5), Some(&ColumnValue::Text("🦀 Rust is awesome! 🚀".to_string())));
    assert_eq!(row.get_column_value(6), Some(&ColumnValue::Boolean(true)));
    assert_eq!(row.get_column_value(7), Some(&ColumnValue::Boolean(false)));
    assert_eq!(row.get_column_value(8), Some(&ColumnValue::Null));
}

#[test]
fn test_row_large_data() {
    let mut row = Row::new(1, PageId(1), 0);
    
    // Add a big line
    let large_text = "A".repeat(100000);
    row.add_column_value(ColumnValue::Text(large_text.clone()));
    
    assert_eq!(row.get_column_count(), 1);
    assert_eq!(row.get_column_value(0), Some(&ColumnValue::Text(large_text)));
    
    // Checking the string size
    let size = row.calculate_size();
    assert!(size > 100000);
}

#[test]
fn test_row_dirty_flag() {
    let mut row = Row::new(1, PageId(1), 0);
    assert!(row.is_dirty());
    
    row.mark_clean();
    assert!(!row.is_dirty());
    
    // Adding a column should mark the row as dirty
    row.add_column_value(ColumnValue::Integer(1));
    assert!(row.is_dirty());
    
    row.mark_clean();
    assert!(!row.is_dirty());
    
    // Updating a column should mark the row as dirty
    row.update_column_value(0, ColumnValue::Integer(2)).unwrap();
    assert!(row.is_dirty());
    
    row.mark_clean();
    assert!(!row.is_dirty());
    
    // Changing the status should mark the line as dirty
    row.mark_deleted();
    assert!(row.is_dirty());
}

#[test]
fn test_row_clone() {
    let mut original = Row::new(1, PageId(5), 100);
    original.add_column_value(ColumnValue::Integer(42));
    original.add_column_value(ColumnValue::Text("Original".to_string()));
    
    let cloned = original.clone();
    
    assert_eq!(cloned.get_id(), original.get_id());
    assert_eq!(cloned.get_page_id(), original.get_page_id());
    assert_eq!(cloned.get_offset(), original.get_offset());
    assert_eq!(cloned.get_status(), original.get_status());
    assert_eq!(cloned.get_column_count(), original.get_column_count());
    assert_eq!(cloned.get_column_value(0), original.get_column_value(0));
    assert_eq!(cloned.get_column_value(1), original.get_column_value(1));
}

#[test]
fn test_row_equality() {
    let mut row1 = Row::new(1, PageId(1), 0);
    row1.add_column_value(ColumnValue::Integer(42));
    
    let mut row2 = Row::new(1, PageId(1), 0);
    row2.add_column_value(ColumnValue::Integer(42));
    
    let mut row3 = Row::new(2, PageId(1), 0);
    row3.add_column_value(ColumnValue::Integer(42));
    
    assert_eq!(row1, row2);
    assert_ne!(row1, row3);
}

#[test]
fn test_row_move_to_different_page() {
    let mut row = Row::new(1, PageId(1), 100);
    row.add_column_value(ColumnValue::Text("Moving row".to_string()));
    
    // Move a line to another page
    row.set_page_id(PageId(2));
    row.set_offset(200);
    
    assert_eq!(row.get_page_id(), PageId(2));
    assert_eq!(row.get_offset(), 200);
    assert!(row.is_dirty());
    
    // Data must remain unchanged
    assert_eq!(row.get_column_value(0), Some(&ColumnValue::Text("Moving row".to_string())));
}

#[test]
fn test_row_status_enum() {
    // Checking various row statuses
    assert_eq!(RowStatus::Active as u8, 0);
    assert_eq!(RowStatus::Deleted as u8, 1);
    
    // Checking status serialization
    let statuses = vec![RowStatus::Active, RowStatus::Deleted];
    
    for status in statuses {
        let serialized = crate::common::bincode_io::serialize(&status).unwrap();
        let deserialized: RowStatus = crate::common::bincode_io::deserialize(&serialized).unwrap();
        assert_eq!(status, deserialized);
    }
}

#[test]
fn test_row_boundary_conditions() {
    // Test with maximum values
    let mut row = Row::new(u32::MAX, PageId(u64::MAX), usize::MAX);
    
    assert_eq!(row.get_id(), u32::MAX);
    assert_eq!(row.get_page_id(), PageId(u64::MAX));
    assert_eq!(row.get_offset(), usize::MAX);
    
    // Add the maximum number of columns (limited by memory)
    for i in 0..1000 {
        row.add_column_value(ColumnValue::Integer(i));
    }
    
    assert_eq!(row.get_column_count(), 1000);
}

#[test]
fn test_row_memory_efficiency() {
    use std::mem;
    
    let row = Row::new(1, PageId(1), 0);
    let size = mem::size_of_val(&row);
    
    // The string must be a reasonable size in memory
    assert!(size < 1024);
}

#[test]
fn test_row_persistence() {
    use tempfile::TempDir;
    use std::fs::File;
    use std::io::{Write, Read};
    
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test_row.dat");
    
    // Create a row with data
    let mut original_row = Row::new(42, PageId(10), 500);
    original_row.add_column_value(ColumnValue::Integer(123));
    original_row.add_column_value(ColumnValue::Text("Persistent row".to_string()));
    original_row.mark_deleted();
    
    // Write to file
    {
        let mut file = File::create(&file_path).unwrap();
        let serialized = original_row.serialize();
        file.write_all(&serialized).unwrap();
    }
    
    // Reading from a file
    {
        let mut file = File::open(&file_path).unwrap();
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).unwrap();
        
        let loaded_row = Row::deserialize(&buffer).unwrap();
        
        assert_eq!(loaded_row.get_id(), 42);
        assert_eq!(loaded_row.get_page_id(), PageId(10));
        assert_eq!(loaded_row.get_offset(), 500);
        assert_eq!(loaded_row.get_status(), RowStatus::Deleted);
        assert_eq!(loaded_row.get_column_count(), 2);
        assert_eq!(loaded_row.get_column_value(0), Some(&ColumnValue::Integer(123)));
        assert_eq!(loaded_row.get_column_value(1), Some(&ColumnValue::Text("Persistent row".to_string())));
    }
}

#[test]
fn test_row_concurrent_access_simulation() {
    use std::sync::{Arc, Mutex};
    use std::thread;
    
    let row = Arc::new(Mutex::new(Row::new(1, PageId(1), 0)));
    let mut handles = vec![];
    
    // Simulating competitive access
    for i in 0..10 {
        let row_clone = Arc::clone(&row);
        let handle = thread::spawn(move || {
            let mut row = row_clone.lock().unwrap();
            row.add_column_value(ColumnValue::Integer(i));
        });
        handles.push(handle);
    }
    
    // Waiting for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }
    
    let final_row = row.lock().unwrap();
    assert_eq!(final_row.get_column_count(), 10);
}

#[test]
fn test_row_validation() {
    let mut row = Row::new(1, PageId(1), 0);
    
    // Adding correct data
    row.add_column_value(ColumnValue::Integer(42));
    row.add_column_value(ColumnValue::Text("Valid".to_string()));
    
    // Checking that the string is valid
    assert!(row.validate().is_ok());
    
    // Mark the line as deleted
    row.mark_deleted();
    
    // The string must still be valid
    assert!(row.validate().is_ok());
}
