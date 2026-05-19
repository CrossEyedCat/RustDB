//! Cached file manager with sync page cache (buffer pool)
//!
//! Wraps AdvancedFileManager with an in-memory LRU page cache to reduce disk I/O.

use crate::common::Result;
use crate::storage::advanced_file_manager::{AdvancedFileId, AdvancedFileManager, FileInfo};
use crate::storage::database_file::{DatabaseFileType, ExtensionStrategy, PageId};
use crate::storage::io_optimization::PageCache;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

/// Cached file manager with sync buffer pool
pub struct CachedFileManager {
    /// Base file manager
    inner: AdvancedFileManager,
    /// LRU page cache (buffer pool)
    cache: Mutex<PageCache>,
}

impl CachedFileManager {
    /// Creates a new cached file manager with specified buffer pool size
    pub fn new(root_dir: impl AsRef<std::path::Path>, buffer_pool_size: usize) -> Result<Self> {
        let inner = AdvancedFileManager::new(root_dir)?;
        let cache = Mutex::new(PageCache::new(buffer_pool_size));
        Ok(Self { inner, cache })
    }

    /// Creates a new database file
    pub fn create_database_file(
        &mut self,
        filename: &str,
        file_type: DatabaseFileType,
        database_id: u32,
        extension_strategy: ExtensionStrategy,
    ) -> Result<AdvancedFileId> {
        self.inner
            .create_database_file(filename, file_type, database_id, extension_strategy)
    }

    /// Opens an existing database file
    pub fn open_database_file(&mut self, filename: &str) -> Result<AdvancedFileId> {
        self.inner.open_database_file(filename)
    }

    /// Allocates pages in the file
    pub fn allocate_pages(&mut self, file_id: AdvancedFileId, page_count: u32) -> Result<PageId> {
        self.inner.allocate_pages(file_id, page_count)
    }

    /// Frees pages in the file
    pub fn free_pages(
        &mut self,
        file_id: AdvancedFileId,
        start_page: PageId,
        page_count: u32,
    ) -> Result<()> {
        self.inner.free_pages(file_id, start_page, page_count)
    }

    /// Reads a page (checks cache first, then disk)
    pub fn read_page(&mut self, file_id: AdvancedFileId, page_id: PageId) -> Result<Vec<u8>> {
        if let Some(data) = self.cache.lock().unwrap().get(file_id, page_id) {
            return Ok(data);
        }
        let data = self.inner.read_page(file_id, page_id)?;
        self.cache
            .lock()
            .unwrap()
            .put(file_id, page_id, data.clone());
        Ok(data)
    }

    /// Writes a page (to disk and updates cache)
    pub fn write_page(
        &mut self,
        file_id: AdvancedFileId,
        page_id: PageId,
        data: &[u8],
    ) -> Result<()> {
        self.inner.write_page(file_id, page_id, data)?;
        self.cache
            .lock()
            .unwrap()
            .put(file_id, page_id, data.to_vec());
        Ok(())
    }

    /// Writes multiple pages in one backend batch when io_uring batching is enabled.
    pub fn write_pages_batch(
        &mut self,
        file_id: AdvancedFileId,
        pages: &[(PageId, &[u8])],
    ) -> Result<()> {
        self.inner.write_pages_batch(file_id, pages)?;
        let mut cache = self.cache.lock().unwrap();
        for &(page_id, data) in pages {
            cache.put(file_id, page_id, data.to_vec());
        }
        Ok(())
    }

    /// Synchronizes a file to disk
    pub fn sync_file(&mut self, file_id: AdvancedFileId) -> Result<()> {
        self.inner.sync_file(file_id)
    }

    /// Returns file information
    pub fn get_file_info(&self, file_id: AdvancedFileId) -> Option<FileInfo> {
        self.inner.get_file_info(file_id)
    }

    /// Closes a file
    pub fn close_file(&mut self, file_id: AdvancedFileId) -> Result<()> {
        self.inner.close_file(file_id)
    }

    /// Returns cache statistics (hits, misses, hit_ratio)
    pub fn cache_stats(&self) -> (u64, u64, f64) {
        self.cache.lock().unwrap().get_stats()
    }

    /// Clears the page cache
    pub fn clear_cache(&self) {
        self.cache.lock().unwrap().clear();
    }

    /// Invalidates cached pages (e.g. after external flush)
    pub fn invalidate_pages(&self, file_id: AdvancedFileId, page_ids: &[PageId]) {
        let mut cache = self.cache.lock().unwrap();
        for &page_id in page_ids {
            cache.remove(file_id, page_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::database_file::{DatabaseFileType, ExtensionStrategy, BLOCK_SIZE};
    use tempfile::TempDir;

    #[test]
    fn test_write_pages_batch_updates_cache() -> crate::common::Result<()> {
        let temp_dir = TempDir::new().map_err(|e| crate::common::Error::database(e.to_string()))?;
        let mut manager = CachedFileManager::new(temp_dir.path(), 8)?;

        let file_id = manager.create_database_file(
            "cached_batch.db",
            DatabaseFileType::Data,
            7,
            ExtensionStrategy::Fixed,
        )?;

        let page_a = 1;
        let page_b = 2;
        let data_a = vec![0x11u8; BLOCK_SIZE];
        let data_b = vec![0x22u8; BLOCK_SIZE];
        manager.write_pages_batch(file_id, &[(page_a, &data_a), (page_b, &data_b)])?;

        assert_eq!(manager.read_page(file_id, page_a)?, data_a);
        assert_eq!(manager.read_page(file_id, page_b)?, data_b);

        Ok(())
    }
}
