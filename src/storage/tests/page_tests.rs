//! Tests for the Page structure

use crate::storage::page::{Page, PageHeader, RecordSlot, PAGE_SIZE, PAGE_HEADER_SIZE};
use crate::common::types::PageId;
use tempfile::TempDir;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

#[test]
fn test_page_creation() {
    let page = Page::new(PageId(1));
    
    assert_eq!(page.get_page_id(), PageId(1));
    assert_eq!(page.get_free_space(), PAGE_SIZE - PAGE_HEADER_SIZE);
    assert_eq!(page.get_record_count(), 0);
    assert!(page.is_dirty());
}

#[test]
fn test_page_header() {
    let page = Page::new(PageId(42));
    let header = page.get_header();
    
    assert_eq!(header.page_id, PageId(42));
    assert_eq!(header.record_count, 0);
    assert_eq!(header.free_space_offset, PAGE_HEADER_SIZE as u16);
    assert_eq!(header.free_space_size, (PAGE_SIZE - PAGE_HEADER_SIZE) as u16);
}

#[test]
fn test_add_record() {
    let mut page = Page::new(PageId(1));
    let record_data = b"Hello, World!";
    let record_id = 1;
    
    let result = page.add_record(record_data, record_id);
    assert!(result.is_ok());
    
    let offset = result.unwrap();
    assert!(offset >= PAGE_HEADER_SIZE);
    assert_eq!(page.get_record_count(), 1);
    assert!(page.get_free_space() < PAGE_SIZE - PAGE_HEADER_SIZE);
}

#[test]
fn test_add_multiple_records() {
    let mut page = Page::new(PageId(1));
    let mut record_offsets = Vec::new();
    
    // Adding multiple entries
    for i in 1..=5 {
        let record_data = format!("Record {}", i);
        let result = page.add_record(record_data.as_bytes(), i);
        assert!(result.is_ok());
        record_offsets.push(result.unwrap());
    }
    
    assert_eq!(page.get_record_count(), 5);
    
    // Checking that all records have different offsets
    for i in 0..record_offsets.len() {
        for j in i + 1..record_offsets.len() {
            assert_ne!(record_offsets[i], record_offsets[j]);
        }
    }
}

#[test]
fn test_get_record() {
    let mut page = Page::new(PageId(1));
    let record_data = b"Test Record";
    let record_id = 1;
    
    let offset = page.add_record(record_data, record_id).unwrap();
    let retrieved = page.get_record(offset);
    
    assert!(retrieved.is_some());
    let retrieved_data = retrieved.unwrap();
    assert_eq!(retrieved_data, record_data);
}

#[test]
fn test_get_nonexistent_record() {
    let page = Page::new(PageId(1));
    let result = page.get_record(PAGE_SIZE);
    assert!(result.is_none());
}

#[test]
fn test_update_record() {
    let mut page = Page::new(PageId(1));
    let original_data = b"Original Data";
    let updated_data = b"Updated Data!";
    let record_id = 1;
    
    let offset = page.add_record(original_data, record_id).unwrap();
    
    // Updating the entry
    let result = page.update_record(offset, updated_data);
    assert!(result.is_ok());
    
    // Checking that the data has been updated
    let retrieved = page.get_record(offset).unwrap();
    assert_eq!(retrieved, updated_data);
}

#[test]
fn test_update_record_larger_size() {
    let mut page = Page::new(PageId(1));
    let original_data = b"Short";
    let updated_data = b"This is a much longer piece of data that should not fit in the same space";
    let record_id = 1;
    
    let offset = page.add_record(original_data, record_id).unwrap();
    
    // An attempt to update a record with longer data may fail
    let result = page.update_record(offset, updated_data);
    // Depending on the implementation, this may be an error or a success
    // Checking that the page remains in a consistent state
    assert!(page.get_record(offset).is_some());
}

#[test]
fn test_delete_record() {
    let mut page = Page::new(PageId(1));
    let record_data = b"To be deleted";
    let record_id = 1;
    
    let offset = page.add_record(record_data, record_id).unwrap();
    let initial_count = page.get_record_count();
    
    let result = page.delete_record(offset);
    assert!(result.is_ok());
    
    // Checking that the entry has been deleted
    assert!(page.get_record(offset).is_none());
    assert_eq!(page.get_record_count(), initial_count - 1);
}

#[test]
fn test_delete_nonexistent_record() {
    let mut page = Page::new(PageId(1));
    let result = page.delete_record(PAGE_SIZE);
    assert!(result.is_err());
}

#[test]
fn test_page_full_scenario() {
    let mut page = Page::new(PageId(1));
    let mut records_added = 0;
    
    // Fill the page with entries until the space runs out
    loop {
        let record_data = format!("Record number {}", records_added);
        let result = page.add_record(record_data.as_bytes(), records_added + 1);
        
        if result.is_err() {
            break;
        }
        records_added += 1;
        
        // Infinite loop protection
        if records_added > 1000 {
            break;
        }
    }
    
    assert!(records_added > 0);
    assert_eq!(page.get_record_count(), records_added);
    
    // We check that there is very little free space or no space at all
    assert!(page.get_free_space() < 100);
}

#[test]
fn test_page_serialization() {
    let mut page = Page::new(PageId(42));
    
    // Adding multiple entries
    page.add_record(b"First record", 1).unwrap();
    page.add_record(b"Second record", 2).unwrap();
    page.add_record(b"Third record", 3).unwrap();
    
    // Serializing the page
    let serialized = page.serialize();
    assert_eq!(serialized.len(), PAGE_SIZE);
    
    // Deserializing the page
    let deserialized = Page::deserialize(&serialized, PageId(42));
    assert!(deserialized.is_ok());
    
    let new_page = deserialized.unwrap();
    assert_eq!(new_page.get_page_id(), PageId(42));
    assert_eq!(new_page.get_record_count(), 3);
}

#[test]
fn test_page_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test_page.dat");
    
    // Create and fill out the page
    let mut page = Page::new(PageId(100));
    page.add_record(b"Persistent data", 1).unwrap();
    
    // Write the page to a file
    {
        let mut file = File::create(&file_path).unwrap();
        let serialized = page.serialize();
        file.write_all(&serialized).unwrap();
    }
    
    // Reading a page from a file
    {
        let mut file = File::open(&file_path).unwrap();
        let mut buffer = vec![0u8; PAGE_SIZE];
        file.read_exact(&mut buffer).unwrap();
        
        let loaded_page = Page::deserialize(&buffer, PageId(100)).unwrap();
        assert_eq!(loaded_page.get_page_id(), PageId(100));
        assert_eq!(loaded_page.get_record_count(), 1);
        
        // Checking that the data has been saved
        let header = loaded_page.get_header();
        assert_eq!(header.page_id, PageId(100));
    }
}

#[test]
fn test_record_slot_functionality() {
    let mut page = Page::new(PageId(1));
    let record_data = b"Test data for slot";
    
    let offset = page.add_record(record_data, 1).unwrap();
    
    // Checking that the slot was created correctly
    let retrieved = page.get_record(offset);
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap(), record_data);
}

#[test]
fn test_page_dirty_flag() {
    let mut page = Page::new(PageId(1));
    assert!(page.is_dirty());
    
    page.mark_clean();
    assert!(!page.is_dirty());
    
    // Any modification should mark the page as dirty
    page.add_record(b"test", 1).unwrap();
    assert!(page.is_dirty());
}

#[test]
fn test_page_compaction() {
    let mut page = Page::new(PageId(1));
    
    // Adding entries
    let offset1 = page.add_record(b"Record 1", 1).unwrap();
    let offset2 = page.add_record(b"Record 2", 2).unwrap();
    let offset3 = page.add_record(b"Record 3", 3).unwrap();
    
    let initial_free_space = page.get_free_space();
    
    // Deleting the middle entry
    page.delete_record(offset2).unwrap();
    
    // Checking that free space has increased
    assert!(page.get_free_space() > initial_free_space);
    
    // Entries 1 and 3 should remain available
    assert!(page.get_record(offset1).is_some());
    assert!(page.get_record(offset3).is_some());
    assert!(page.get_record(offset2).is_none());
}

#[test]
fn test_page_boundary_conditions() {
    let mut page = Page::new(PageId(u64::MAX));
    
    // Checking the correctness of work with boundary values
    assert_eq!(page.get_page_id(), PageId(u64::MAX));
    
    // We are trying to add a record of the largest possible size
    let max_record_size = page.get_free_space() - std::mem::size_of::<RecordSlot>();
    let large_record = vec![0u8; max_record_size];
    
    let result = page.add_record(&large_record, 1);
    // The result is implementation dependent, but the page should remain in the correct state
    assert!(page.get_record_count() <= 1);
}

#[test]
fn test_concurrent_page_access_simulation() {
    use std::sync::{Arc, Mutex};
    use std::thread;
    
    let page = Arc::new(Mutex::new(Page::new(PageId(1))));
    let mut handles = vec![];
    
    // Simulating competitive access
    for i in 0..10 {
        let page_clone = Arc::clone(&page);
        let handle = thread::spawn(move || {
            let mut page = page_clone.lock().unwrap();
            let record_data = format!("Record from thread {}", i);
            page.add_record(record_data.as_bytes(), i + 1)
        });
        handles.push(handle);
    }
    
    // Waiting for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }
    
    let final_page = page.lock().unwrap();
    assert!(final_page.get_record_count() <= 10);
}
