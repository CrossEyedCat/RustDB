//! Page manager for rustdb
//!
//! This module provides a high-level interface for managing data pages,
//! including CRUD operations, page splitting/merging, and optimizations.

use crate::common::{
    types::{PageId, RecordId},
    Result,
};
use crate::storage::{
    cached_file_manager::CachedFileManager,
    database_file::{DatabaseFileType, ExtensionStrategy},
    page::Page,
};
use std::collections::HashMap;
use std::path::PathBuf;

/// Page manager configuration
#[derive(Debug, Clone)]
pub struct PageManagerConfig {
    /// Maximum page fill factor (0.0 - 1.0)
    pub max_fill_factor: f64,
    /// Minimum fill factor for page merging
    pub min_fill_factor: f64,
    /// Buffer size for page preallocation
    pub preallocation_buffer_size: u32,
    /// Enable data compression
    pub enable_compression: bool,
    /// Batch size for operations
    pub batch_size: u32,
    /// Buffer pool size (number of pages to cache in memory). 0 = no cache.
    pub buffer_pool_size: usize,
}

impl Default for PageManagerConfig {
    fn default() -> Self {
        Self {
            max_fill_factor: 0.9,
            min_fill_factor: 0.4,
            preallocation_buffer_size: 10,
            enable_compression: false,
            batch_size: 100,
            buffer_pool_size: 1000,
        }
    }
}

/// Insert operation result
#[derive(Debug, Clone)]
pub struct InsertResult {
    /// Record ID
    pub record_id: RecordId,
    /// ID of the page where the record was inserted
    pub page_id: PageId,
    /// Whether page split was performed
    pub page_split: bool,
}

/// Update operation result
#[derive(Debug, Clone)]
pub struct UpdateResult {
    /// Whether the update was performed in-place
    pub in_place: bool,
    /// ID of the new page (if the record was moved)
    pub new_page_id: Option<PageId>,
    /// Whether page split was performed
    pub page_split: bool,
}

/// Delete operation result
#[derive(Debug, Clone)]
pub struct DeleteResult {
    /// Whether the deletion was physical (true) or logical (false)
    pub physical_delete: bool,
    /// Whether page merge was performed
    pub page_merge: bool,
}

/// Page manager operation statistics
#[derive(Debug, Default, Clone)]
pub struct PageManagerStatistics {
    /// Number of insert operations
    pub insert_operations: u64,
    /// Number of select operations
    pub select_operations: u64,
    /// Number of update operations
    pub update_operations: u64,
    /// Number of delete operations
    pub delete_operations: u64,
    /// Number of page splits
    pub page_splits: u64,
    /// Number of page merges
    pub page_merges: u64,
    /// Number of defragmentation operations
    pub defragmentation_operations: u64,
}

/// Page information for the manager
#[derive(Debug, Clone)]
pub struct PageInfo {
    /// Page ID
    pub page_id: PageId,
    /// Fill factor (0.0 - 1.0)
    pub fill_factor: f64,
    /// Number of records on the page
    pub record_count: u32,
    /// Free space size in bytes
    pub free_space: u32,
    /// Whether the page needs defragmentation
    pub needs_defragmentation: bool,
}

/// Page manager
pub struct PageManager {
    /// File manager (with buffer pool)
    file_manager: CachedFileManager,
    /// Data file ID
    file_id: u32,
    /// Configuration
    config: PageManagerConfig,
    /// Page information cache
    page_cache: HashMap<PageId, PageInfo>,
    /// Pool of preallocated pages
    preallocated_pages: Vec<PageId>,
    /// Operation statistics
    statistics: PageManagerStatistics,
}

impl PageManager {
    /// Creates a new page manager
    pub fn new(data_dir: PathBuf, table_name: &str, config: PageManagerConfig) -> Result<Self> {
        let buffer_size = config.buffer_pool_size.max(1);
        let mut file_manager = CachedFileManager::new(data_dir, buffer_size)?;

        let filename = format!("{}.tbl", table_name);
        let file_id = file_manager.create_database_file(
            &filename,
            DatabaseFileType::Data,
            1, // database_id
            ExtensionStrategy::Linear,
        )?;

        let mut manager = Self {
            file_manager,
            file_id,
            config,
            page_cache: HashMap::new(),
            preallocated_pages: Vec::new(),
            statistics: PageManagerStatistics::default(),
        };

        // Preallocate initial pages
        manager.preallocate_pages()?;

        Ok(manager)
    }

    /// Opens an existing page manager
    pub fn open(data_dir: PathBuf, table_name: &str, config: PageManagerConfig) -> Result<Self> {
        let buffer_size = config.buffer_pool_size.max(1);
        let mut file_manager = CachedFileManager::new(data_dir, buffer_size)?;

        let filename = format!("{}.tbl", table_name);
        let file_id = file_manager.open_database_file(&filename)?;

        let mut manager = Self {
            file_manager,
            file_id,
            config,
            page_cache: HashMap::new(),
            preallocated_pages: Vec::new(),
            statistics: PageManagerStatistics::default(),
        };

        // Load information about existing pages
        manager.load_existing_pages()?;

        Ok(manager)
    }

    /// Inserts a new record
    pub fn insert(&mut self, data: &[u8]) -> Result<InsertResult> {
        self.statistics.insert_operations += 1;

        // Find a page with sufficient free space
        let page_id = self.find_page_with_space(data.len())?;

        // Load the page
        let page_data = self.file_manager.read_page(self.file_id, page_id)?;
        let mut page = Page::from_bytes(&page_data)?;

        // Generate record_id in advance
        let record_id = self.generate_record_id(page_id, 0); // Temporary offset, will be updated

        // Try to insert the record
        match page.add_record(data, record_id) {
            Ok(offset) => {
                // Record successfully inserted
                let final_record_id = self.generate_record_id(page_id, offset);

                // Save the page
                let serialized = page.to_bytes()?;
                self.file_manager
                    .write_page(self.file_id, page_id, &serialized)?;

                // Update cache
                self.update_page_cache(page_id, &page);

                Ok(InsertResult {
                    record_id: final_record_id,
                    page_id,
                    page_split: false,
                })
            }
            Err(e) => {
                // Page is full, need to split
                self.split_page_and_insert(page_id, data)
            }
        }
    }

    /// Selects records by condition
    pub fn select(
        &mut self,
        condition: Option<Box<dyn Fn(&[u8]) -> bool>>,
    ) -> Result<Vec<(RecordId, Vec<u8>)>> {
        self.statistics.select_operations += 1;

        let mut results = Vec::new();

        // Scan all pages
        let page_ids = self.get_all_page_ids()?;

        for page_id in page_ids {
            let page_data = self.file_manager.read_page(self.file_id, page_id)?;
            let page = Page::from_bytes(&page_data)?;

            // Scan records on the page
            let records = page.scan_records()?;

            for (offset, record_data) in records {
                let record_id = self.generate_record_id(page_id, offset);

                // Apply filter condition
                if let Some(ref cond) = condition {
                    if cond(&record_data) {
                        results.push((record_id, record_data));
                    }
                } else {
                    results.push((record_id, record_data));
                }
            }
        }

        Ok(results)
    }

    /// Updates a record
    pub fn update(&mut self, record_id: RecordId, new_data: &[u8]) -> Result<UpdateResult> {
        self.statistics.update_operations += 1;

        let (page_id, offset) = self.parse_record_id(record_id);

        // Load the page
        let page_data = self.file_manager.read_page(self.file_id, page_id)?;
        let mut page = Page::from_bytes(&page_data)?;

        // Try to update the record in-place
        match page.update_record_by_offset(offset, new_data) {
            Ok(_) => {
                // In-place update successful
                let serialized = page.to_bytes()?;
                self.file_manager
                    .write_page(self.file_id, page_id, &serialized)?;

                self.update_page_cache(page_id, &page);

                Ok(UpdateResult {
                    in_place: true,
                    new_page_id: None,
                    page_split: false,
                })
            }
            Err(_) => {
                // Need to delete old record and insert new one
                self.delete_record_internal(page_id, offset)?;
                let insert_result = self.insert(new_data)?;

                Ok(UpdateResult {
                    in_place: false,
                    new_page_id: Some(insert_result.page_id),
                    page_split: insert_result.page_split,
                })
            }
        }
    }

    /// Deletes a record
    pub fn delete(&mut self, record_id: RecordId) -> Result<DeleteResult> {
        self.statistics.delete_operations += 1;

        let (page_id, offset) = self.parse_record_id(record_id);
        self.delete_record_internal(page_id, offset)
    }

    /// Gets a record by ID
    pub fn get_record(&mut self, record_id: RecordId) -> Result<Option<Vec<u8>>> {
        let (page_id, _) = self.parse_record_id(record_id);
        let page_data = self.file_manager.read_page(self.file_id, page_id)?;
        let page = Page::from_bytes(&page_data)?;
        Ok(page.get_record(record_id).map(|s| s.to_vec()))
    }

    /// Gets operation statistics
    pub fn get_statistics(&self) -> &PageManagerStatistics {
        &self.statistics
    }

    /// Gets the file ID for WAL logging
    pub fn file_id(&self) -> u32 {
        self.file_id
    }

    /// Performs page defragmentation
    pub fn defragment(&mut self) -> Result<u32> {
        self.statistics.defragmentation_operations += 1;

        let mut defragmented_count = 0;

        // Collect page IDs that need defragmentation
        let pages_to_defrag: Vec<PageId> = self
            .page_cache
            .iter()
            .filter_map(|(&page_id, page_info)| {
                if page_info.needs_defragmentation {
                    Some(page_id)
                } else {
                    None
                }
            })
            .collect();

        // Defragment collected pages
        for page_id in pages_to_defrag {
            self.defragment_page(page_id)?;
            defragmented_count += 1;
        }

        Ok(defragmented_count)
    }

    /// Performs batch insert operations
    pub fn batch_insert(&mut self, records: Vec<Vec<u8>>) -> Result<Vec<InsertResult>> {
        let mut results = Vec::with_capacity(records.len());

        for chunk in records.chunks(self.config.batch_size as usize) {
            for record in chunk {
                results.push(self.insert(record)?);
            }
        }

        Ok(results)
    }

    // Private methods

    /// Preallocates pages
    fn preallocate_pages(&mut self) -> Result<()> {
        for _ in 0..self.config.preallocation_buffer_size {
            let page_id = self.file_manager.allocate_pages(self.file_id, 1)?;

            // Initialize empty page
            let page = Page::new(page_id);
            let serialized = page.to_bytes()?;
            self.file_manager
                .write_page(self.file_id, page_id, &serialized)?;

            self.preallocated_pages.push(page_id);
        }

        Ok(())
    }

    /// Loads information about existing pages
    fn load_existing_pages(&mut self) -> Result<()> {
        let page_ids = self.get_all_page_ids()?;

        for page_id in page_ids {
            // Load page and add its information to cache
            let page_data = self.file_manager.read_page(self.file_id, page_id)?;
            let page = Page::from_bytes(&page_data)?;
            self.update_page_cache(page_id, &page);
        }

        Ok(())
    }

    /// Finds a page with sufficient free space
    fn find_page_with_space(&mut self, required_size: usize) -> Result<PageId> {
        // First check cache
        for (&page_id, page_info) in &self.page_cache {
            if page_info.free_space as usize >= required_size {
                return Ok(page_id);
            }
        }

        // Use preallocated page
        if let Some(page_id) = self.preallocated_pages.pop() {
            return Ok(page_id);
        }

        // Allocate new page
        let page_id = self.file_manager.allocate_pages(self.file_id, 1)?;
        let page = Page::new(page_id);
        let serialized = page.to_bytes()?;
        self.file_manager
            .write_page(self.file_id, page_id, &serialized)?;

        Ok(page_id)
    }

    /// Splits a page and inserts a record
    fn split_page_and_insert(&mut self, page_id: PageId, data: &[u8]) -> Result<InsertResult> {
        self.statistics.page_splits += 1;

        // Load the overflowed page
        let page_data = self.file_manager.read_page(self.file_id, page_id)?;
        let mut old_page = Page::from_bytes(&page_data)?;

        // Create a new page
        let new_page_id = self.file_manager.allocate_pages(self.file_id, 1)?;
        let mut new_page = Page::new(new_page_id);

        // Redistribute records between pages
        let records = old_page.get_all_records()?;
        old_page.clear()?;

        let mid_point = records.len() / 2;

        // Records for the old page
        for (i, record_data) in records.iter().enumerate().take(mid_point) {
            old_page.add_record(record_data, i as u64)?;
        }

        // Records for the new page
        for (i, record_data) in records.iter().enumerate().skip(mid_point) {
            new_page.add_record(record_data, i as u64)?;
        }

        // Try to insert the new record
        let insert_page_id = if old_page.get_free_space() >= data.len() as u32 {
            old_page.add_record(data, 0u64)?;
            page_id
        } else {
            new_page.add_record(data, 0u64)?;
            new_page_id
        };

        // Save pages
        let old_serialized = old_page.to_bytes()?;
        self.file_manager
            .write_page(self.file_id, page_id, &old_serialized)?;

        let new_serialized = new_page.to_bytes()?;
        self.file_manager
            .write_page(self.file_id, new_page_id, &new_serialized)?;

        // Update cache
        self.update_page_cache(page_id, &old_page);
        self.update_page_cache(new_page_id, &new_page);

        let record_id = self.generate_record_id(insert_page_id, 0);

        Ok(InsertResult {
            record_id,
            page_id: insert_page_id,
            page_split: true,
        })
    }

    /// Deletes a record (internal method)
    fn delete_record_internal(&mut self, page_id: PageId, offset: u32) -> Result<DeleteResult> {
        let page_data = self.file_manager.read_page(self.file_id, page_id)?;
        let mut page = Page::from_bytes(&page_data)?;

        // Delete the record
        page.delete_record_by_offset(offset)?;

        // Save the page
        let serialized = page.to_bytes()?;
        self.file_manager
            .write_page(self.file_id, page_id, &serialized)?;

        // Update cache
        self.update_page_cache(page_id, &page);

        // Check if page merging is needed
        let needs_merge = if let Some(page_info) = self.page_cache.get(&page_id) {
            page_info.fill_factor < self.config.min_fill_factor
        } else {
            false
        };

        if needs_merge {
            // Try to merge the page with a neighbor
            if let Ok(merged) = self.try_merge_page(page_id) {
                if merged {
                    self.statistics.page_merges += 1;
                }
            }
        }

        Ok(DeleteResult {
            physical_delete: true,
            page_merge: needs_merge,
        })
    }

    /// Defragments a page
    fn defragment_page(&mut self, page_id: PageId) -> Result<()> {
        let page_data = self.file_manager.read_page(self.file_id, page_id)?;
        let mut page = Page::from_bytes(&page_data)?;

        // Perform defragmentation
        page.defragment()?;

        // Save the page
        let serialized = page.to_bytes()?;
        self.file_manager
            .write_page(self.file_id, page_id, &serialized)?;

        // Update cache
        self.update_page_cache(page_id, &page);

        Ok(())
    }

    /// Updates page information cache
    fn update_page_cache(&mut self, page_id: PageId, page: &Page) {
        let page_info = PageInfo {
            page_id,
            fill_factor: page.get_fill_factor(),
            record_count: page.get_record_count(),
            free_space: page.get_free_space(),
            needs_defragmentation: page.needs_defragmentation(),
        };

        self.page_cache.insert(page_id, page_info);
    }

    /// Generates a record ID
    fn generate_record_id(&self, page_id: PageId, offset: u32) -> RecordId {
        // Combine page_id and offset into one ID
        ((page_id as u64) << 32) | (offset as u64)
    }

    /// Parses a record ID
    fn parse_record_id(&self, record_id: RecordId) -> (PageId, u32) {
        let page_id = (record_id >> 32) as PageId;
        let offset = (record_id & 0xFFFFFFFF) as u32;
        (page_id, offset)
    }

    /// Gets all page IDs
    fn get_all_page_ids(&mut self) -> Result<Vec<PageId>> {
        // Get file information and count pages
        let file_info = self
            .file_manager
            .get_file_info(self.file_id)
            .ok_or_else(|| crate::common::Error::database("File info not found"))?;
        let total_pages = file_info.total_pages;

        let mut page_ids = Vec::new();
        // Pages start from 1, not 0
        for page_id in 1..=total_pages as PageId {
            // Check that the page exists and contains data
            if let Ok(page_data) = self.file_manager.read_page(self.file_id, page_id) {
                if !page_data.iter().all(|&b| b == 0) {
                    // Not an empty page
                    page_ids.push(page_id);
                }
            }
        }

        // Also check pages in cache that might have been created recently
        for cached_page_id in self.page_cache.keys() {
            if !page_ids.contains(cached_page_id) {
                // Check that the page actually exists
                if let Ok(page_data) = self.file_manager.read_page(self.file_id, *cached_page_id) {
                    if !page_data.iter().all(|&b| b == 0) {
                        page_ids.push(*cached_page_id);
                    }
                }
            }
        }

        Ok(page_ids)
    }

    /// Tries to merge a page with a neighbor
    fn try_merge_page(&mut self, page_id: PageId) -> Result<bool> {
        // Find a neighbor page for merging
        let neighbor_id = self.find_merge_candidate(page_id)?;

        if let Some(neighbor_page_id) = neighbor_id {
            self.merge_pages(page_id, neighbor_page_id)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Finds a candidate for page merging
    fn find_merge_candidate(&self, page_id: PageId) -> Result<Option<PageId>> {
        // Check neighbor pages (previous and next)
        let candidates = vec![
            if page_id > 0 { Some(page_id - 1) } else { None },
            Some(page_id + 1),
        ];

        for candidate in candidates.into_iter().flatten() {
            if let Some(candidate_info) = self.page_cache.get(&candidate) {
                // Check that merged pages fit into one
                if let Some(current_info) = self.page_cache.get(&page_id) {
                    let combined_records = current_info.record_count + candidate_info.record_count;
                    let page_capacity = 4096 / 64; // Rough estimate of page capacity

                    if combined_records <= page_capacity
                        && candidate_info.fill_factor < self.config.max_fill_factor
                    {
                        return Ok(Some(candidate));
                    }
                }
            }
        }

        Ok(None)
    }

    /// Merges two pages
    fn merge_pages(&mut self, page_id1: PageId, page_id2: PageId) -> Result<()> {
        // Load both pages
        let page1_data = self.file_manager.read_page(self.file_id, page_id1)?;
        let page2_data = self.file_manager.read_page(self.file_id, page_id2)?;

        let mut page1 = Page::from_bytes(&page1_data)?;
        let page2 = Page::from_bytes(&page2_data)?;

        // Get all records from the second page
        let page2_records = page2.get_all_records()?;

        // Move records from the second page to the first
        for (i, record_data) in page2_records.iter().enumerate() {
            page1.add_record(record_data, i as u64)?;
        }

        // Save the updated first page
        let serialized = page1.to_bytes()?;
        self.file_manager
            .write_page(self.file_id, page_id1, &serialized)?;

        // Clear the second page
        let empty_page = Page::new(page_id2);
        let empty_serialized = empty_page.to_bytes()?;
        self.file_manager
            .write_page(self.file_id, page_id2, &empty_serialized)?;

        // Update cache
        self.update_page_cache(page_id1, &page1);
        self.update_page_cache(page_id2, &empty_page);

        Ok(())
    }

    /// Compresses page data if compression is enabled
    fn compress_page_data(&self, data: &[u8]) -> Result<Vec<u8>> {
        if self.config.enable_compression {
            match lz4_flex::compress_prepend_size(data) {
                compressed if compressed.len() < data.len() => Ok(compressed),
                _ => Ok(data.to_vec()), // If compression is not effective, return original
            }
        } else {
            Ok(data.to_vec())
        }
    }

    /// Decompresses page data if it was compressed
    fn decompress_page_data(&self, data: &[u8]) -> Result<Vec<u8>> {
        if self.config.enable_compression && data.len() >= 4 {
            // Check if data starts with size (LZ4 signature)
            let size_bytes = [data[0], data[1], data[2], data[3]];
            let expected_size = u32::from_le_bytes(size_bytes) as usize;

            // If size is reasonable, try to decompress
            if expected_size > 0 && expected_size < 1024 * 1024 {
                // Maximum 1MB
                match lz4_flex::decompress_size_prepended(data) {
                    Ok(decompressed) => Ok(decompressed),
                    Err(_) => Ok(data.to_vec()), // If decompression failed, return as is
                }
            } else {
                Ok(data.to_vec())
            }
        } else {
            Ok(data.to_vec())
        }
    }

    /// Reads a page with automatic decompression
    fn read_page_with_decompression(&mut self, page_id: PageId) -> Result<Vec<u8>> {
        let raw_data = self.file_manager.read_page(self.file_id, page_id)?;
        self.decompress_page_data(&raw_data)
    }

    /// Writes a page with automatic compression
    fn write_page_with_compression(&mut self, page_id: PageId, data: &[u8]) -> Result<()> {
        let compressed_data = self.compress_page_data(data)?;
        self.file_manager
            .write_page(self.file_id, page_id, &compressed_data)
    }
}

impl Drop for PageManager {
    fn drop(&mut self) {
        // Synchronize and close file when manager is destroyed
        let _ = self.file_manager.sync_file(self.file_id);
        let _ = self.file_manager.close_file(self.file_id);
    }
}
