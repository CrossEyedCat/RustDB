//! Database file structures for rustdb
//!
//! This module contains extended structures for organizing database files:
//! - Extended database file header with metadata
//! - Free page map for efficient space management
//! - File extension management with optimization
//! - Structures for organizing data in the file

use crate::common::{Error, Result};
use serde::{Deserialize, Serialize};

/// Block size in bytes (4KB)
pub const BLOCK_SIZE: usize = 4096;

/// Page size (equals block size)
pub const PAGE_SIZE: usize = BLOCK_SIZE;

/// Database file ID
pub type DatabaseFileId = u32;

/// Page ID in the file
pub type PageId = u64;

/// Database file type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatabaseFileType {
    /// Main data file
    Data,
    /// Index file
    Index,
    /// Transaction log file
    Log,
    /// Temporary file
    Temporary,
    /// System metadata file
    System,
}

/// Database file state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatabaseFileState {
    /// File is active and ready
    Active,
    /// File is being created
    Creating,
    /// File is marked for deletion
    MarkedForDeletion,
    /// File is corrupted
    Corrupted,
    /// File is read-only
    ReadOnly,
}

/// Extended database file header
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseFileHeader {
    /// Magic number for identifying rustdb files
    pub magic: u32,
    /// File format version
    pub version: u16,
    /// Format subversion (for backward compatibility)
    pub subversion: u16,

    /// Block/page size
    pub page_size: u32,
    /// Total number of pages in the file
    pub total_pages: u64,
    /// Number of used pages
    pub used_pages: u64,
    /// Number of free pages
    pub free_pages: u64,

    /// Database file type
    pub file_type: DatabaseFileType,
    /// File state
    pub file_state: DatabaseFileState,

    /// Database ID (for linking files of the same DB)
    pub database_id: u32,
    /// Sequential file number in the DB
    pub file_sequence: u32,

    /// Pointer to the catalog root page
    pub catalog_root_page: Option<PageId>,
    /// Pointer to the first free page map page
    pub free_page_map_start: Option<PageId>,
    /// Number of pages occupied by the free page map
    pub free_page_map_pages: u32,

    /// Maximum file size in pages (0 = unlimited)
    pub max_pages: u64,
    /// File extension size in pages
    pub extension_size: u32,

    /// File creation time (Unix timestamp)
    pub created_at: u64,
    /// Last modification time
    pub modified_at: u64,
    /// Last integrity check time
    pub last_check_at: u64,

    /// Write operation counter
    pub write_count: u64,
    /// Read operation counter
    pub read_count: u64,

    /// File flags (bitmask)
    pub flags: u32,

    /// Header checksum (should be calculated last)
    pub checksum: u32,

    /// Reserved for future extensions (should be filled with zeros)
    pub reserved: Vec<u8>,
}

/// Database file flags
impl DatabaseFileHeader {
    /// Magic number for rustdb files
    pub const MAGIC: u32 = 0x52555354; // "RUST"

    /// Current file format version
    pub const VERSION: u16 = 2;
    /// Current file format subversion
    pub const SUBVERSION: u16 = 0;

    /// Flag: file is compressed
    pub const FLAG_COMPRESSED: u32 = 0x0001;
    /// Flag: file is encrypted
    pub const FLAG_ENCRYPTED: u32 = 0x0002;
    /// Flag: integrity check enabled
    pub const FLAG_CHECKSUM_ENABLED: u32 = 0x0004;
    /// Flag: file in debug mode
    pub const FLAG_DEBUG_MODE: u32 = 0x0008;

    /// Creates a new database file header
    pub fn new(file_type: DatabaseFileType, database_id: u32) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            magic: Self::MAGIC,
            version: Self::VERSION,
            subversion: Self::SUBVERSION,
            page_size: PAGE_SIZE as u32,
            total_pages: 0,
            used_pages: 0,
            free_pages: 0,
            file_type,
            file_state: DatabaseFileState::Creating,
            database_id,
            file_sequence: 0,
            catalog_root_page: None,
            free_page_map_start: None,
            free_page_map_pages: 0,
            max_pages: 0,         // No limit
            extension_size: 1024, // Extend by 1024 pages (4MB)
            created_at: now,
            modified_at: now,
            last_check_at: now,
            write_count: 0,
            read_count: 0,
            flags: Self::FLAG_CHECKSUM_ENABLED,
            checksum: 0,
            reserved: vec![0; 64],
        }
    }

    /// Updates the modification time
    pub fn touch(&mut self) {
        self.modified_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Increments the write counter
    pub fn increment_write_count(&mut self) {
        self.write_count = self.write_count.wrapping_add(1);
        self.touch();
    }

    /// Increments the read counter
    pub fn increment_read_count(&mut self) {
        self.read_count = self.read_count.wrapping_add(1);
    }

    /// Checks a flag
    pub fn has_flag(&self, flag: u32) -> bool {
        (self.flags & flag) != 0
    }

    /// Sets a flag
    pub fn set_flag(&mut self, flag: u32) {
        self.flags |= flag;
    }

    /// Clears a flag
    pub fn clear_flag(&mut self, flag: u32) {
        self.flags &= !flag;
    }

    /// Calculates the header checksum
    pub fn calculate_checksum(&self) -> u32 {
        let mut sum = 0u32;
        sum = sum.wrapping_add(self.magic);
        sum = sum.wrapping_add(self.version as u32);
        sum = sum.wrapping_add(self.subversion as u32);
        sum = sum.wrapping_add(self.page_size);
        sum = sum.wrapping_add(self.total_pages as u32);
        sum = sum.wrapping_add(self.used_pages as u32);
        sum = sum.wrapping_add(self.free_pages as u32);
        sum = sum.wrapping_add(self.database_id);
        sum = sum.wrapping_add(self.file_sequence);
        sum = sum.wrapping_add(self.free_page_map_pages);
        sum = sum.wrapping_add(self.max_pages as u32);
        sum = sum.wrapping_add(self.extension_size);
        sum = sum.wrapping_add(self.created_at as u32);
        sum = sum.wrapping_add(self.modified_at as u32);
        sum = sum.wrapping_add(self.write_count as u32);
        sum = sum.wrapping_add(self.read_count as u32);
        sum = sum.wrapping_add(self.flags);

        // Add bytes from reserved
        for &byte in &self.reserved {
            sum = sum.wrapping_add(byte as u32);
        }

        sum
    }

    /// Updates the checksum
    pub fn update_checksum(&mut self) {
        self.checksum = self.calculate_checksum();
    }

    /// Checks header validity
    pub fn is_valid(&self) -> bool {
        self.magic == Self::MAGIC
            && self.version == Self::VERSION
            && self.page_size == PAGE_SIZE as u32
            && self.checksum == self.calculate_checksum()
            && self.used_pages <= self.total_pages
            && self.free_pages <= self.total_pages
            && (self.used_pages + self.free_pages) <= self.total_pages
    }

    /// Returns file state description
    pub fn state_description(&self) -> &'static str {
        match self.file_state {
            DatabaseFileState::Active => "Active",
            DatabaseFileState::Creating => "Creating",
            DatabaseFileState::MarkedForDeletion => "Marked for deletion",
            DatabaseFileState::Corrupted => "Corrupted",
            DatabaseFileState::ReadOnly => "Read-only",
        }
    }

    /// Returns file type description
    pub fn type_description(&self) -> &'static str {
        match self.file_type {
            DatabaseFileType::Data => "Data",
            DatabaseFileType::Index => "Index",
            DatabaseFileType::Log => "Log",
            DatabaseFileType::Temporary => "Temporary",
            DatabaseFileType::System => "System",
        }
    }
}

impl Default for DatabaseFileHeader {
    fn default() -> Self {
        Self::new(DatabaseFileType::Data, 0)
    }
}

/// Free page map entry
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreePageMapEntry {
    /// Start page of free block
    pub start_page: PageId,
    /// Number of consecutive free pages
    pub page_count: u32,
    /// Usage priority (0 = highest)
    pub priority: u8,
    /// Entry flags
    pub flags: u8,
}

/// Free page map
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreePageMap {
    /// Map header
    pub header: FreePageMapHeader,
    /// Entries for free page blocks
    pub entries: Vec<FreePageMapEntry>,
    /// Bitmap for quick search (optional)
    pub bitmap: Option<Vec<u8>>,
}

/// Free page map header
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreePageMapHeader {
    /// Map magic number
    pub magic: u32,
    /// Map format version
    pub version: u16,
    /// Total number of entries in the map
    pub total_entries: u32,
    /// Number of active entries
    pub active_entries: u32,
    /// Total number of free pages
    pub total_free_pages: u64,
    /// Largest contiguous free page block
    pub largest_free_block: u32,
    /// Last map update time
    pub last_updated: u64,
    /// Map checksum
    pub checksum: u32,
}

impl FreePageMap {
    /// Magic number for free page map
    pub const MAGIC: u32 = 0x46524545; // "FREE"

    /// Map format version
    pub const VERSION: u16 = 1;

    /// Creates a new free page map
    pub fn new() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            header: FreePageMapHeader {
                magic: Self::MAGIC,
                version: Self::VERSION,
                total_entries: 0,
                active_entries: 0,
                total_free_pages: 0,
                largest_free_block: 0,
                last_updated: now,
                checksum: 0,
            },
            entries: Vec::new(),
            bitmap: None,
        }
    }

    /// Adds a free page block
    pub fn add_free_block(&mut self, start_page: PageId, page_count: u32) -> Result<()> {
        if page_count == 0 {
            return Err(Error::validation("Page count cannot be zero"));
        }

        // Check for intersections with existing blocks
        for entry in &self.entries {
            let entry_end = entry.start_page + entry.page_count as u64;
            let new_end = start_page + page_count as u64;

            if start_page < entry_end && new_end > entry.start_page {
                return Err(Error::validation(
                    "Free block intersects with existing block",
                ));
            }
        }

        // Try to merge with neighboring blocks
        let mut merged = false;
        for entry in &mut self.entries {
            // Merge with previous block
            if entry.start_page + entry.page_count as u64 == start_page {
                entry.page_count += page_count;
                merged = true;
                break;
            }
            // Merge with next block
            else if start_page + page_count as u64 == entry.start_page {
                entry.start_page = start_page;
                entry.page_count += page_count;
                merged = true;
                break;
            }
        }

        // If merge failed, add a new entry
        if !merged {
            self.entries.push(FreePageMapEntry {
                start_page,
                page_count,
                priority: 0,
                flags: 0,
            });
        }

        // Sort entries by start page
        self.entries.sort_by_key(|entry| entry.start_page);

        // Update statistics
        self.update_statistics();

        Ok(())
    }

    /// Allocates a block of free pages
    pub fn allocate_pages(&mut self, page_count: u32) -> Option<PageId> {
        if page_count == 0 {
            return None;
        }

        // Find a suitable block (first-fit algorithm)
        for i in 0..self.entries.len() {
            let entry = &self.entries[i];

            if entry.page_count >= page_count {
                let allocated_start = entry.start_page;

                if entry.page_count == page_count {
                    // Remove entry completely
                    self.entries.remove(i);
                } else {
                    // Decrease block size
                    self.entries[i].start_page += page_count as u64;
                    self.entries[i].page_count -= page_count;
                }

                self.update_statistics();
                // Make sure we don't return page_id = 0
                return Some(if allocated_start == 0 {
                    1
                } else {
                    allocated_start
                });
            }
        }

        None
    }

    /// Frees a block of pages
    pub fn free_pages(&mut self, start_page: PageId, page_count: u32) -> Result<()> {
        self.add_free_block(start_page, page_count)
    }

    /// Finds the largest contiguous free page block
    pub fn find_largest_free_block(&self) -> u32 {
        self.entries
            .iter()
            .map(|entry| entry.page_count)
            .max()
            .unwrap_or(0)
    }

    /// Returns total number of free pages
    pub fn total_free_pages(&self) -> u64 {
        self.entries
            .iter()
            .map(|entry| entry.page_count as u64)
            .sum()
    }

    /// Updates map statistics
    fn update_statistics(&mut self) {
        self.header.total_entries = self.entries.len() as u32;
        self.header.active_entries = self.entries.len() as u32;
        self.header.total_free_pages = self.total_free_pages();
        self.header.largest_free_block = self.find_largest_free_block();
        self.header.last_updated = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Validates map integrity
    pub fn validate(&self) -> Result<()> {
        if self.header.magic != Self::MAGIC {
            return Err(Error::validation("Invalid free page map magic number"));
        }

        if self.header.version != Self::VERSION {
            return Err(Error::validation("Unsupported free page map version"));
        }

        // Check for block intersections
        for (i, entry1) in self.entries.iter().enumerate() {
            for (j, entry2) in self.entries.iter().enumerate() {
                if i != j {
                    let end1 = entry1.start_page + entry1.page_count as u64;
                    let end2 = entry2.start_page + entry2.page_count as u64;

                    if entry1.start_page < end2 && end1 > entry2.start_page {
                        return Err(Error::validation("Overlapping blocks detected in map"));
                    }
                }
            }
        }

        Ok(())
    }

    /// Defragments the map (merges neighboring blocks)
    pub fn defragment(&mut self) {
        // Sort by start page
        self.entries.sort_by_key(|entry| entry.start_page);

        // Merge neighboring blocks
        let mut i = 0;
        while i < self.entries.len().saturating_sub(1) {
            let current_end = self.entries[i].start_page + self.entries[i].page_count as u64;
            let next_start = self.entries[i + 1].start_page;

            if current_end == next_start {
                // Merge blocks
                self.entries[i].page_count += self.entries[i + 1].page_count;
                self.entries.remove(i + 1);
            } else {
                i += 1;
            }
        }

        self.update_statistics();
    }

    /// Creates a bitmap for quick search
    pub fn create_bitmap(&mut self, total_pages: u64) {
        let bitmap_size = total_pages.div_ceil(8) as usize;
        let mut bitmap = vec![0u8; bitmap_size];

        // Mark free pages in the bitmap
        for entry in &self.entries {
            for page in entry.start_page..entry.start_page + entry.page_count as u64 {
                let byte_index = (page / 8) as usize;
                let bit_index = (page % 8) as u8;

                if byte_index < bitmap.len() {
                    bitmap[byte_index] |= 1 << bit_index;
                }
            }
        }

        self.bitmap = Some(bitmap);
    }
}

impl Default for FreePageMap {
    fn default() -> Self {
        Self::new()
    }
}

/// File extension manager
#[derive(Debug, Clone)]
pub struct FileExtensionManager {
    /// Extension strategy
    pub strategy: ExtensionStrategy,
    /// Minimum extension size (in pages)
    pub min_extension_size: u32,
    /// Maximum extension size (in pages)
    pub max_extension_size: u32,
    /// Growth factor for exponential strategy
    pub growth_factor: f64,
    /// Extension history for analysis
    pub extension_history: Vec<ExtensionRecord>,
}

/// File extension strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionStrategy {
    /// Fixed extension size
    Fixed,
    /// Linear increase
    Linear,
    /// Exponential increase
    Exponential,
    /// Adaptive extension based on usage patterns
    Adaptive,
}

/// File extension record
#[derive(Debug, Clone)]
pub struct ExtensionRecord {
    /// Extension time
    pub timestamp: u64,
    /// File size before extension
    pub old_size: u64,
    /// File size after extension
    pub new_size: u64,
    /// Extension reason
    pub reason: ExtensionReason,
}

/// File extension reason
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionReason {
    /// Out of space
    OutOfSpace,
    /// Preallocation
    Preallocation,
    /// Performance optimization
    Performance,
    /// Defragmentation
    Defragmentation,
}

impl FileExtensionManager {
    /// Creates a new file extension manager
    pub fn new(strategy: ExtensionStrategy) -> Self {
        Self {
            strategy,
            min_extension_size: 64,   // 256KB
            max_extension_size: 4096, // 16MB
            growth_factor: 1.5,
            extension_history: Vec::new(),
        }
    }

    /// Calculates the size of the next extension
    pub fn calculate_extension_size(&self, current_size: u64, required_size: u32) -> u32 {
        let base_size = match self.strategy {
            ExtensionStrategy::Fixed => self.min_extension_size,
            ExtensionStrategy::Linear => {
                // Increase by 10% of current size
                let linear_size = (current_size as f64 * 0.1) as u32;
                linear_size.max(self.min_extension_size)
            }
            ExtensionStrategy::Exponential => {
                // Exponential increase
                let exp_size = (current_size as f64 * (self.growth_factor - 1.0)) as u32;
                exp_size.max(self.min_extension_size)
            }
            ExtensionStrategy::Adaptive => self.calculate_adaptive_size(current_size),
        };

        // Make sure the size is sufficient for the required number of pages
        let final_size = base_size.max(required_size);

        // Limit by maximum size
        final_size.min(self.max_extension_size)
    }

    /// Calculates adaptive extension size
    fn calculate_adaptive_size(&self, _current_size: u64) -> u32 {
        if self.extension_history.is_empty() {
            return self.min_extension_size;
        }

        // Analyze extension history for the last hour
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let recent_extensions: Vec<_> = self
            .extension_history
            .iter()
            .filter(|record| now - record.timestamp < 3600) // Last hour
            .collect();

        if recent_extensions.is_empty() {
            return self.min_extension_size;
        }

        // If there were many extensions recently, increase size
        let extension_count = recent_extensions.len() as u32;
        let adaptive_multiplier = if extension_count > 5 {
            3.0
        } else if extension_count > 2 {
            2.0
        } else {
            1.0
        };

        let adaptive_size = (self.min_extension_size as f64 * adaptive_multiplier) as u32;
        adaptive_size.min(self.max_extension_size)
    }

    /// Records an extension in history
    pub fn record_extension(&mut self, old_size: u64, new_size: u64, reason: ExtensionReason) {
        let record = ExtensionRecord {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            old_size,
            new_size,
            reason,
        };

        self.extension_history.push(record);

        // Limit history size
        if self.extension_history.len() > 1000 {
            self.extension_history.drain(0..500); // Remove old entries
        }
    }

    /// Recommends file pre-extension
    pub fn should_preextend(&self, _current_size: u64, free_pages: u64, total_pages: u64) -> bool {
        if total_pages == 0 {
            return false;
        }

        let usage_ratio = (total_pages - free_pages) as f64 / total_pages as f64;

        // Pre-extend if more than 80% of space is used
        usage_ratio > 0.8
    }

    /// Returns extension statistics
    pub fn get_statistics(&self) -> ExtensionStatistics {
        ExtensionStatistics {
            total_extensions: self.extension_history.len(),
            average_extension_size: if self.extension_history.is_empty() {
                0.0
            } else {
                let total_growth: u64 = self
                    .extension_history
                    .iter()
                    .map(|record| record.new_size - record.old_size)
                    .sum();
                total_growth as f64 / self.extension_history.len() as f64
            },
            last_extension: self.extension_history.last().cloned(),
        }
    }
}

/// File extension statistics
#[derive(Debug, Clone)]
pub struct ExtensionStatistics {
    /// Total number of extensions
    pub total_extensions: usize,
    /// Average extension size
    pub average_extension_size: f64,
    /// Last extension
    pub last_extension: Option<ExtensionRecord>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_file_header_creation() {
        let header = DatabaseFileHeader::new(DatabaseFileType::Data, 123);

        assert_eq!(header.magic, DatabaseFileHeader::MAGIC);
        assert_eq!(header.version, DatabaseFileHeader::VERSION);
        assert_eq!(header.database_id, 123);
        assert_eq!(header.file_type, DatabaseFileType::Data);
        assert_eq!(header.file_state, DatabaseFileState::Creating);
        assert!(header.has_flag(DatabaseFileHeader::FLAG_CHECKSUM_ENABLED));
    }

    #[test]
    fn test_database_file_header_checksum() {
        let mut header = DatabaseFileHeader::new(DatabaseFileType::Index, 456);
        header.update_checksum();
        assert!(header.is_valid());

        // Modify data and ensure validation fails
        header.database_id = 999;
        assert!(!header.is_valid());

        // Update checksum and verify again
        header.update_checksum();
        assert!(header.is_valid());
    }

    #[test]
    fn test_free_page_map_basic_operations() -> Result<()> {
        let mut map = FreePageMap::new();

        // Add free blocks
        map.add_free_block(10, 5)?;
        map.add_free_block(20, 3)?;

        assert_eq!(map.total_free_pages(), 8);
        assert_eq!(map.find_largest_free_block(), 5);

        // Allocate pages
        let allocated = map.allocate_pages(3);
        assert_eq!(allocated, Some(10));
        assert_eq!(map.total_free_pages(), 5);

        Ok(())
    }

    #[test]
    fn test_free_page_map_merge_blocks() -> Result<()> {
        let mut map = FreePageMap::new();

        // Add adjacent blocks
        map.add_free_block(10, 5)?;
        map.add_free_block(15, 3)?; // Should merge with previous

        assert_eq!(map.entries.len(), 1);
        assert_eq!(map.entries[0].start_page, 10);
        assert_eq!(map.entries[0].page_count, 8);

        Ok(())
    }

    #[test]
    fn test_file_extension_manager() {
        let mut manager = FileExtensionManager::new(ExtensionStrategy::Fixed);

        let extension_size = manager.calculate_extension_size(1000, 50);
        assert_eq!(extension_size, manager.min_extension_size.max(50));

        // Record extension
        manager.record_extension(1000, 1064, ExtensionReason::OutOfSpace);

        let stats = manager.get_statistics();
        assert_eq!(stats.total_extensions, 1);
        assert_eq!(stats.average_extension_size, 64.0);
    }

    #[test]
    fn test_extension_strategies() {
        let manager_fixed = FileExtensionManager::new(ExtensionStrategy::Fixed);
        let manager_linear = FileExtensionManager::new(ExtensionStrategy::Linear);
        let manager_exp = FileExtensionManager::new(ExtensionStrategy::Exponential);

        let current_size = 1000;
        let required = 10;

        let fixed_size = manager_fixed.calculate_extension_size(current_size, required);
        let linear_size = manager_linear.calculate_extension_size(current_size, required);
        let exp_size = manager_exp.calculate_extension_size(current_size, required);

        // Fixed strategy should be minimal
        assert_eq!(fixed_size, manager_fixed.min_extension_size);

        // Linear strategy should be >= fixed
        assert!(linear_size >= fixed_size);

        // Exponential strategy should exceed linear for large files
        assert!(exp_size >= linear_size);
    }

    #[test]
    fn test_free_page_map_validation() -> Result<()> {
        let mut map = FreePageMap::new();

        // Add valid blocks
        map.add_free_block(10, 5)?;
        map.add_free_block(20, 3)?;

        assert!(map.validate().is_ok());

        // Add overlapping block
        let result = map.add_free_block(12, 3);
        assert!(result.is_err());

        Ok(())
    }
}
