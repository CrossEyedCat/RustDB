//! File manager for rustdb
//!
//! This module is responsible for basic database file operations:
//! - Creating, opening, and closing files
//! - Reading and writing data blocks
//! - Managing file sizes
//! - Synchronizing data to disk

use crate::common::{Error, Result};
use crate::storage::block::BlockId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Block size in bytes (4KB)
pub const BLOCK_SIZE: usize = 4096;

/// File ID
pub type FileId = u32;

/// Database file header
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileHeader {
    /// Magic number for identifying rustdb files
    pub magic: u32,
    /// File format version
    pub version: u16,
    /// Block size
    pub block_size: u32,
    /// Total number of blocks in the file
    pub total_blocks: u32,
    /// Number of used blocks
    pub used_blocks: u32,
    /// Pointer to the first free block
    pub first_free_block: Option<BlockId>,
    /// File creation time
    pub created_at: u64,
    /// Last modification time
    pub modified_at: u64,
    /// Header checksum
    pub checksum: u32,
}

impl FileHeader {
    /// Magic number for rustdb files
    pub const MAGIC: u32 = 0x52555354; // "RUST"

    /// Current file format version
    pub const VERSION: u16 = 1;

    /// Creates a new file header
    pub fn new() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            magic: Self::MAGIC,
            version: Self::VERSION,
            block_size: BLOCK_SIZE as u32,
            total_blocks: 0,
            used_blocks: 0,
            first_free_block: None,
            created_at: now,
            modified_at: now,
            checksum: 0,
        }
    }

    /// Updates the modification time
    pub fn touch(&mut self) {
        self.modified_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Calculates the header checksum
    pub fn calculate_checksum(&self) -> u32 {
        // Simple checksum (real DB uses CRC32)
        let mut sum = 0u32;
        sum = sum.wrapping_add(self.magic);
        sum = sum.wrapping_add(self.version as u32);
        sum = sum.wrapping_add(self.block_size);
        sum = sum.wrapping_add(self.total_blocks);
        sum = sum.wrapping_add(self.used_blocks);
        sum = sum.wrapping_add(self.created_at as u32);
        sum = sum.wrapping_add(self.modified_at as u32);
        sum
    }

    /// Checks header validity
    pub fn is_valid(&self) -> bool {
        self.magic == Self::MAGIC
            && self.version == Self::VERSION
            && self.block_size == BLOCK_SIZE as u32
            && self.checksum == self.calculate_checksum()
    }
}

impl Default for FileHeader {
    fn default() -> Self {
        Self::new()
    }
}

/// Handle for an open database file
pub struct DatabaseFile {
    /// File ID
    pub file_id: FileId,
    /// File path
    pub path: PathBuf,
    /// File descriptor
    pub file: File,
    /// File header
    pub header: FileHeader,
    /// Header modification flag
    pub header_dirty: bool,
    /// Read-only flag
    pub read_only: bool,
}

impl DatabaseFile {
    /// Creates a new database file
    pub fn create<P: AsRef<Path>>(file_id: FileId, path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Check that the file doesn't exist
        if path.exists() {
            return Err(Error::database(format!(
                "File {} already exists",
                path.display()
            )));
        }

        // Create the file
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&path)?;

        // Create the header
        let mut header = FileHeader::new();
        header.checksum = header.calculate_checksum();

        // Write the header to the file
        let header_bytes = bincode::serialize(&header)
            .map_err(|e| Error::database(format!("Header serialization error: {}", e)))?;

        file.write_all(&header_bytes)?;

        // Pad to block boundary so data starts at a clear position
        let header_size = header_bytes.len();
        let padding_size = BLOCK_SIZE - (header_size % BLOCK_SIZE);
        if padding_size < BLOCK_SIZE {
            let padding = vec![0u8; padding_size];
            file.write_all(&padding)?;
        }

        file.sync_all()?;

        Ok(Self {
            file_id,
            path,
            file,
            header,
            header_dirty: false,
            read_only: false,
        })
    }

    /// Opens an existing database file
    pub fn open<P: AsRef<Path>>(file_id: FileId, path: P, read_only: bool) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Check that the file exists
        if !path.exists() {
            return Err(Error::database(format!(
                "File {} not found",
                path.display()
            )));
        }

        // Open the file
        let mut file = if read_only {
            OpenOptions::new().read(true).open(&path)?
        } else {
            OpenOptions::new().read(true).write(true).open(&path)?
        };

        // Read the header
        let mut header_bytes = Vec::new();
        file.read_to_end(&mut header_bytes)?;

        if header_bytes.is_empty() {
            return Err(Error::database("File corrupted: empty file".to_string()));
        }

        let header: FileHeader = bincode::deserialize(&header_bytes)
            .map_err(|e| Error::database(format!("Header deserialization error: {}", e)))?;

        // Check header validity
        if !header.is_valid() {
            return Err(Error::database(
                "File corrupted: invalid header".to_string(),
            ));
        }

        Ok(Self {
            file_id,
            path,
            file,
            header,
            header_dirty: false,
            read_only,
        })
    }

    /// Reads a data block from the file
    pub fn read_block(&mut self, block_id: BlockId) -> Result<Vec<u8>> {
        if block_id >= self.header.total_blocks as u64 {
            return Err(Error::database(format!("Block {} does not exist", block_id)));
        }

        // Calculate block position in file (data starts from first block after header)
        let offset = BLOCK_SIZE as u64 + (block_id as u64 * BLOCK_SIZE as u64);

        // Seek to block position
        self.file.seek(SeekFrom::Start(offset))?;

        // Read block data
        let mut buffer = vec![0u8; BLOCK_SIZE];
        self.file.read_exact(&mut buffer)?;

        Ok(buffer)
    }

    /// Writes a data block to the file
    pub fn write_block(&mut self, block_id: BlockId, data: &[u8]) -> Result<()> {
        if self.read_only {
            return Err(Error::database("File is open for reading only".to_string()));
        }

        if data.len() != BLOCK_SIZE {
            return Err(Error::database(format!(
                "Invalid block size: {} (expected {})",
                data.len(),
                BLOCK_SIZE
            )));
        }

        // Extend file if necessary
        if block_id >= self.header.total_blocks as u64 {
            self.extend_file((block_id + 1) as u32)?;
        }

        // Calculate block position in file (data starts from first block after header)
        let offset = BLOCK_SIZE as u64 + (block_id as u64 * BLOCK_SIZE as u64);

        // Seek to block position
        self.file.seek(SeekFrom::Start(offset))?;

        // Write block data
        self.file.write_all(data)?;

        // Update used blocks counter
        if block_id >= self.header.used_blocks as u64 {
            self.header.used_blocks = (block_id + 1) as u32;
            self.header.touch();
            self.header_dirty = true;
        }

        Ok(())
    }

    /// Extends the file to the specified number of blocks
    pub fn extend_file(&mut self, new_block_count: u32) -> Result<()> {
        if self.read_only {
            return Err(Error::database("File is open for reading only".to_string()));
        }

        if new_block_count <= self.header.total_blocks {
            return Ok(()); // File is already large enough
        }

        // Calculate new file size (header + data blocks)
        let new_size = BLOCK_SIZE as u64 + (new_block_count as u64 * BLOCK_SIZE as u64);

        // Extend the file
        self.file.seek(SeekFrom::Start(new_size - 1))?;
        self.file.write_all(&[0])?;

        // Update header
        self.header.total_blocks = new_block_count;
        self.header.touch();
        self.header_dirty = true;

        Ok(())
    }

    /// Synchronizes data to disk
    pub fn sync(&mut self) -> Result<()> {
        if self.read_only {
            return Ok(());
        }

        // Write header if it changed
        if self.header_dirty {
            self.write_header()?;
            self.header_dirty = false;
        }

        // Synchronize all data
        self.file.sync_all()?;

        Ok(())
    }

    /// Writes the header to the file
    fn write_header(&mut self) -> Result<()> {
        // Update checksum
        self.header.checksum = self.header.calculate_checksum();

        // Serialize header
        let header_bytes = bincode::serialize(&self.header)
            .map_err(|e| Error::database(format!("Header serialization error: {}", e)))?;

        // Write to beginning of file
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header_bytes)?;

        Ok(())
    }

    /// Returns file size in blocks
    pub fn size_in_blocks(&self) -> u32 {
        self.header.total_blocks
    }

    /// Returns number of used blocks
    pub fn used_blocks(&self) -> u32 {
        self.header.used_blocks
    }

    /// Returns number of free blocks
    pub fn free_blocks(&self) -> u32 {
        self.header
            .total_blocks
            .saturating_sub(self.header.used_blocks)
    }
}

/// Database file manager
pub struct FileManager {
    /// Open files
    files: HashMap<FileId, DatabaseFile>,
    /// Root directory for database files
    root_dir: PathBuf,
    /// Counter for generating file IDs
    next_file_id: FileId,
}

impl FileManager {
    /// Creates a new file manager
    pub fn new<P: AsRef<Path>>(root_dir: P) -> Result<Self> {
        let root_dir = root_dir.as_ref().to_path_buf();

        // Create directory if it doesn't exist
        if !root_dir.exists() {
            std::fs::create_dir_all(&root_dir)?;
        }

        Ok(Self {
            files: HashMap::new(),
            root_dir,
            next_file_id: 1,
        })
    }

    /// Creates a new database file
    pub fn create_file(&mut self, filename: &str) -> Result<FileId> {
        let file_id = self.next_file_id;
        self.next_file_id += 1;

        let file_path = self.root_dir.join(filename);
        let db_file = DatabaseFile::create(file_id, file_path)?;

        self.files.insert(file_id, db_file);

        Ok(file_id)
    }

    /// Opens an existing database file
    pub fn open_file(&mut self, filename: &str, read_only: bool) -> Result<FileId> {
        let file_path = self.root_dir.join(filename);

        // Check if the file is already open
        for (id, file) in &self.files {
            if file.path == file_path {
                return Ok(*id);
            }
        }

        let file_id = self.next_file_id;
        self.next_file_id += 1;

        let db_file = DatabaseFile::open(file_id, file_path, read_only)?;
        self.files.insert(file_id, db_file);

        Ok(file_id)
    }

    /// Closes a database file
    pub fn close_file(&mut self, file_id: FileId) -> Result<()> {
        if let Some(mut file) = self.files.remove(&file_id) {
            file.sync()?;
        }
        Ok(())
    }

    /// Reads a block from a file
    pub fn read_block(&mut self, file_id: FileId, block_id: BlockId) -> Result<Vec<u8>> {
        let file = self
            .files
            .get_mut(&file_id)
            .ok_or_else(|| Error::database(format!("File {} is not open", file_id)))?;

        file.read_block(block_id)
    }

    /// Writes a block to a file
    pub fn write_block(&mut self, file_id: FileId, block_id: BlockId, data: &[u8]) -> Result<()> {
        let file = self
            .files
            .get_mut(&file_id)
            .ok_or_else(|| Error::database(format!("File {} is not open", file_id)))?;

        file.write_block(block_id, data)
    }

    /// Synchronizes a file to disk
    pub fn sync_file(&mut self, file_id: FileId) -> Result<()> {
        let file = self
            .files
            .get_mut(&file_id)
            .ok_or_else(|| Error::database(format!("File {} is not open", file_id)))?;

        file.sync()
    }

    /// Synchronizes all open files
    pub fn sync_all(&mut self) -> Result<()> {
        for file in self.files.values_mut() {
            file.sync()?;
        }
        Ok(())
    }

    /// Returns file information
    pub fn get_file_info(&self, file_id: FileId) -> Option<&DatabaseFile> {
        self.files.get(&file_id)
    }

    /// Returns list of open files
    pub fn list_open_files(&self) -> Vec<FileId> {
        self.files.keys().cloned().collect()
    }

    /// Closes all open files
    pub fn close_all(&mut self) -> Result<()> {
        let file_ids: Vec<FileId> = self.files.keys().cloned().collect();
        for file_id in file_ids {
            self.close_file(file_id)?;
        }
        Ok(())
    }

    /// Deletes a file from disk
    pub fn delete_file(&mut self, filename: &str) -> Result<()> {
        let file_path = self.root_dir.join(filename);

        // Close file if it's open
        let file_id_to_close = self
            .files
            .iter()
            .find(|(_, file)| file.path == file_path)
            .map(|(id, _)| *id);

        if let Some(file_id) = file_id_to_close {
            self.close_file(file_id)?;
        }

        // Delete the file
        if file_path.exists() {
            std::fs::remove_file(file_path)?;
        }

        Ok(())
    }
}

impl Drop for FileManager {
    fn drop(&mut self) {
        let _ = self.close_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_file_header_creation() {
        let header = FileHeader::new();
        assert_eq!(header.magic, FileHeader::MAGIC);
        assert_eq!(header.version, FileHeader::VERSION);
        assert_eq!(header.block_size, BLOCK_SIZE as u32);
        assert_eq!(header.total_blocks, 0);
        assert_eq!(header.used_blocks, 0);
    }

    #[test]
    fn test_file_header_checksum() {
        let mut header = FileHeader::new();
        let checksum = header.calculate_checksum();
        header.checksum = checksum;
        assert!(header.is_valid());
    }

    #[test]
    fn test_create_database_file() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.db");

        let db_file = DatabaseFile::create(1, &file_path)?;
        assert_eq!(db_file.file_id, 1);
        assert_eq!(db_file.path, file_path);
        assert!(!db_file.read_only);
        assert!(file_path.exists());

        Ok(())
    }

    #[test]
    fn test_open_database_file() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.db");

        // Create file
        let _db_file = DatabaseFile::create(1, &file_path)?;

        // Open file
        let db_file = DatabaseFile::open(2, &file_path, false)?;
        assert_eq!(db_file.file_id, 2);
        assert_eq!(db_file.path, file_path);
        assert!(!db_file.read_only);

        Ok(())
    }

    #[test]
    fn test_write_read_block() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.db");

        let mut db_file = DatabaseFile::create(1, &file_path)?;

        // Prepare test data
        let test_data = vec![42u8; BLOCK_SIZE];

        // Write block
        db_file.write_block(0, &test_data)?;

        // Read block
        let read_data = db_file.read_block(0)?;
        assert_eq!(read_data, test_data);

        Ok(())
    }

    #[test]
    fn test_file_manager() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut manager = FileManager::new(temp_dir.path())?;

        // Create file
        let file_id = manager.create_file("test.db")?;

        // Write data
        let test_data = vec![123u8; BLOCK_SIZE];
        manager.write_block(file_id, 0, &test_data)?;

        // Read data
        let read_data = manager.read_block(file_id, 0)?;
        assert_eq!(read_data, test_data);

        // Sync to disk
        manager.sync_file(file_id)?;

        // Close file
        manager.close_file(file_id)?;

        Ok(())
    }

    #[test]
    fn test_file_extension() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.db");

        let mut db_file = DatabaseFile::create(1, &file_path)?;

        // File initially empty
        assert_eq!(db_file.size_in_blocks(), 0);

        // Write block with higher index
        let test_data = vec![1u8; BLOCK_SIZE];
        db_file.write_block(10, &test_data)?;

        // File should expand
        assert_eq!(db_file.size_in_blocks(), 11);
        assert_eq!(db_file.used_blocks(), 11);

        Ok(())
    }
}
