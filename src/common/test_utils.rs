//! Common Testing Utilities

use crate::common::{Error, Result};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// / Test Data Generator
pub struct TestDataGenerator {
    counter: Arc<Mutex<u64>>,
}

impl TestDataGenerator {
    // / Creates a new generator
    pub fn new() -> Self {
        Self {
            counter: Arc::new(Mutex::new(0)),
        }
    }

    // / Generates a unique ID
    pub fn next_id(&self) -> u64 {
        let mut counter = self.counter.lock().unwrap();
        *counter += 1;
        *counter
    }

    // / Generates a string of a given length
    pub fn generate_string(&self, length: usize) -> String {
        "a".repeat(length)
    }

    // / Generates random bytes
    pub fn generate_bytes(&self, size: usize) -> Vec<u8> {
        vec![0u8; size]
    }

    // / Generates a test table name
    pub fn table_name(&self) -> String {
        format!("test_table_{}", self.next_id())
    }

    // / Generates a test column name
    pub fn column_name(&self) -> String {
        format!("test_column_{}", self.next_id())
    }
}

impl Default for TestDataGenerator {
    fn default() -> Self {
        Self::new()
    }
}

// / Mock for storage operations
pub struct MockStorage {
    data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl MockStorage {
    // / Creates a new mock
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    // / Writes data
    pub fn write(&self, key: String, value: Vec<u8>) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        data.insert(key, value);
        Ok(())
    }

    // / Reads data
    pub fn read(&self, key: &str) -> Result<Vec<u8>> {
        let data = self.data.lock().unwrap();
        data.get(key)
            .cloned()
            .ok_or_else(|| Error::database(format!("Key not found: {}", key)))
    }

    // /Deletes data
    pub fn delete(&self, key: &str) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        data.remove(key);
        Ok(())
    }

    // / Clears storage
    pub fn clear(&self) {
        let mut data = self.data.lock().unwrap();
        data.clear();
    }

    // / Returns the number of records
    pub fn len(&self) -> usize {
        let data = self.data.lock().unwrap();
        data.len()
    }

    // / Checks if the storage is empty
    pub fn is_empty(&self) -> bool {
        let data = self.data.lock().unwrap();
        data.is_empty()
    }
}

impl Default for MockStorage {
    fn default() -> Self {
        Self::new()
    }
}

// / Test isolation - resource cleanup
pub struct TestCleanup {
    cleanup_functions: Vec<Box<dyn FnOnce() + Send>>,
}

impl TestCleanup {
    // / Creates a new cleanup object
    pub fn new() -> Self {
        Self {
            cleanup_functions: Vec::new(),
        }
    }

    // / Adds a cleaning function
    pub fn add<F>(&mut self, cleanup: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.cleanup_functions.push(Box::new(cleanup));
    }

    // / Performs all cleaning functions
    pub fn cleanup(mut self) {
        while let Some(cleanup_fn) = self.cleanup_functions.pop() {
            cleanup_fn();
        }
    }
}

impl Default for TestCleanup {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TestCleanup {
    fn drop(&mut self) {
        // We perform cleaning upon destruction
        while let Some(cleanup_fn) = self.cleanup_functions.pop() {
            cleanup_fn();
        }
    }
}

// / Test counter for checking calls
pub struct CallCounter {
    count: Arc<Mutex<usize>>,
}

impl CallCounter {
    // / Creates a new counter
    pub fn new() -> Self {
        Self {
            count: Arc::new(Mutex::new(0)),
        }
    }

    // / Increases the counter
    pub fn increment(&self) {
        let mut count = self.count.lock().unwrap();
        *count += 1;
    }

    // / Returns the current value
    pub fn get(&self) -> usize {
        let count = self.count.lock().unwrap();
        *count
    }

    // / Resets the counter
    pub fn reset(&self) {
        let mut count = self.count.lock().unwrap();
        *count = 0;
    }
}

impl Default for CallCounter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::i18n::{set_language, t, Language, MessageKey};

    #[test]
    fn test_data_generator() {
        let gen = TestDataGenerator::new();

        let id1 = gen.next_id();
        let id2 = gen.next_id();

        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
    }

    #[test]
    fn test_data_generator_string() {
        let gen = TestDataGenerator::new();
        let s = gen.generate_string(10);

        assert_eq!(s.len(), 10);
    }

    #[test]
    fn test_mock_storage() {
        let storage = MockStorage::new();

        storage.write("key1".to_string(), vec![1, 2, 3]).unwrap();
        let value = storage.read("key1").unwrap();

        assert_eq!(value, vec![1, 2, 3]);
    }

    #[test]
    fn test_mock_storage_delete() {
        let storage = MockStorage::new();

        storage.write("key1".to_string(), vec![1, 2, 3]).unwrap();
        storage.delete("key1").unwrap();

        assert!(storage.read("key1").is_err());
    }

    #[test]
    fn test_call_counter() {
        let counter = CallCounter::new();

        counter.increment();
        counter.increment();
        counter.increment();

        assert_eq!(counter.get(), 3);
    }

    #[test]
    fn test_call_counter_reset() {
        let counter = CallCounter::new();

        counter.increment();
        counter.increment();
        counter.reset();

        assert_eq!(counter.get(), 0);
    }

    #[test]
    fn test_i18n_in_test_utils() {
        // Test that i18n works in test utilities (only English is currently supported)
        set_language(Language::English).unwrap();
        assert_eq!(t(MessageKey::Welcome), "Welcome to RustDB");
    }
}
