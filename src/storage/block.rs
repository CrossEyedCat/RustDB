//! Block structures for rustdb

use crate::common::bincode_io;
use crate::common::{types::PageId, Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Block identifier
pub type BlockId = u64;

/// Block type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BlockType {
    /// Data block
    Data,
    /// Index block
    Index,
    /// Metadata block
    Metadata,
    /// Log block
    Log,
    /// Free space block
    FreeSpace,
}

/// Block header
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockHeader {
    /// Block ID
    pub block_id: BlockId,
    /// Block type
    pub block_type: BlockType,
    /// Block size in bytes
    pub size: u32,
    /// Number of pages in the block
    pub page_count: u32,
    /// Block creation time
    pub created_at: u64,
    /// Last modification time
    pub last_modified: u64,
    /// Dirty block flag
    pub is_dirty: bool,
    /// Pinned block flag
    pub is_pinned: bool,
}

impl BlockHeader {
    /// Creates a new block header
    pub fn new(block_id: BlockId, block_type: BlockType, size: u32) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            block_id,
            block_type,
            size,
            page_count: 0,
            created_at: now,
            last_modified: now,
            is_dirty: false,
            is_pinned: false,
        }
    }

    /// Updates the last modification time
    pub fn touch(&mut self) {
        self.last_modified = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Marks the block as modified
    pub fn mark_dirty(&mut self) {
        self.is_dirty = true;
        self.touch();
    }

    /// Marks the block as clean
    pub fn mark_clean(&mut self) {
        self.is_dirty = false;
    }

    /// Pins the block in memory
    pub fn pin(&mut self) {
        self.is_pinned = true;
    }

    /// Unpins the block from memory
    pub fn unpin(&mut self) {
        self.is_pinned = false;
    }
}

/// Block structure
#[derive(Debug, Clone)]
pub struct Block {
    /// Block header
    pub header: BlockHeader,
    /// Pages in the block
    pub pages: HashMap<PageId, Vec<u8>>,
    /// Links to other blocks
    pub links: BlockLinks,
    /// Block metadata
    pub metadata: HashMap<String, String>,
}

impl Block {
    /// Creates a new empty block
    pub fn new(block_id: BlockId, block_type: BlockType, size: u32) -> Self {
        Self {
            header: BlockHeader::new(block_id, block_type, size),
            pages: HashMap::new(),
            links: BlockLinks::new(),
            metadata: HashMap::new(),
        }
    }

    /// Adds a page to the block
    pub fn add_page(&mut self, page_id: PageId, page_data: Vec<u8>) -> Result<()> {
        // Check if data size exceeds block size
        let total_size: usize =
            self.pages.values().map(|p| p.len()).sum::<usize>() + page_data.len();
        if total_size > (self.header.size as usize - 256) {
            // leave space for metadata
            return Err(Error::validation("Block overflow"));
        }

        self.pages.insert(page_id, page_data);
        self.header.page_count = self.pages.len() as u32;
        self.header.mark_dirty();
        Ok(())
    }

    /// Removes a page from the block
    pub fn remove_page(&mut self, page_id: PageId) -> Option<Vec<u8>> {
        let page_data = self.pages.remove(&page_id);
        if page_data.is_some() {
            self.header.page_count = self.pages.len() as u32;
            self.header.mark_dirty();
        }
        page_data
    }

    /// Gets a page by ID
    pub fn get_page(&self, page_id: PageId) -> Option<&Vec<u8>> {
        self.pages.get(&page_id)
    }

    /// Gets a mutable reference to a page
    pub fn get_page_mut(&mut self, page_id: PageId) -> Option<&mut Vec<u8>> {
        self.pages.get_mut(&page_id)
    }

    /// Checks if the block contains a page
    pub fn contains_page(&self, page_id: PageId) -> bool {
        self.pages.contains_key(&page_id)
    }

    /// Returns the number of pages in the block
    pub fn page_count(&self) -> u32 {
        self.header.page_count
    }

    /// Checks if the block is empty
    pub fn is_empty(&self) -> bool {
        self.header.page_count == 0
    }

    /// Checks if the block is full
    pub fn is_full(&self) -> bool {
        self.pages.len() >= self.header.page_count as usize
    }

    /// Clears the block
    pub fn clear(&mut self) {
        self.pages.clear();
        self.header.page_count = 0;
        self.header.mark_dirty();
    }

    /// Serializes the block to bytes
    ///
    /// Layout: `bincode(header)` · `u32 page_count` · repeated (`u64 page_id` · `u32 len` · payload) ·
    /// `u32 links_len` · `bincode(links)` · `u32 meta_len` · `bincode(metadata)`.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();

        let header_bytes = bincode_io::serialize(&self.header).map_err(Error::from)?;
        bytes.extend_from_slice(&header_bytes);

        bytes.extend_from_slice(&(self.pages.len() as u32).to_le_bytes());

        for (page_id, page_data) in &self.pages {
            bytes.extend_from_slice(&page_id.to_le_bytes());
            bytes.extend_from_slice(&(page_data.len() as u32).to_le_bytes());
            bytes.extend_from_slice(page_data);
        }

        let links_bytes = bincode_io::serialize(&self.links).map_err(Error::from)?;
        let meta_bytes = bincode_io::serialize(&self.metadata).map_err(Error::from)?;
        bytes.extend_from_slice(&(links_bytes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&links_bytes);
        bytes.extend_from_slice(&(meta_bytes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&meta_bytes);

        Ok(bytes)
    }

    /// Creates a block from bytes (deserialization)
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        use std::io::Cursor;

        // Minimum check will be done after calculating header_size

        // Deserialize header
        // First, try to deserialize directly from the beginning of the array
        // bincode with default config may try to read more than needed
        // Therefore, use Cursor to control read position
        // Determine header size by serializing a test header
        let test_header = BlockHeader::new(0, BlockType::Data, 0);
        let test_header_bytes = bincode_io::serialize(&test_header).map_err(Error::from)?;
        let header_size = test_header_bytes.len();

        if bytes.len() < header_size {
            return Err(Error::validation(&format!(
                "Invalid block size: requires at least {} bytes, got {}",
                header_size,
                bytes.len()
            )));
        }

        // Deserialize header from the beginning of the array
        let mut cursor = Cursor::new(&bytes[..header_size]);

        let header: BlockHeader =
            bincode_io::deserialize_from_reader(&mut cursor).map_err(Error::from)?;

        // Get position after reading header
        // If we used direct deserialization, we need to determine header size
        let header_end = header_size;

        // Check if there's enough data to read page count
        if bytes.len() < header_end + 4 {
            return Err(Error::validation(&format!(
                "Invalid block size: requires at least {} bytes, got {}",
                header_end + 4,
                bytes.len()
            )));
        }

        // Read page count
        let page_count = u32::from_le_bytes([
            bytes[header_end],
            bytes[header_end + 1],
            bytes[header_end + 2],
            bytes[header_end + 3],
        ]);

        // Deserialize pages
        let mut pages = HashMap::new();
        let mut offset = header_end + 4;

        for _ in 0..page_count {
            if offset + 12 > bytes.len() {
                return Err(Error::validation(
                    "Invalid block size: not enough data for page",
                ));
            }

            // Read page_id (u64, 8 bytes)
            let page_id = u64::from_le_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
                bytes[offset + 4],
                bytes[offset + 5],
                bytes[offset + 6],
                bytes[offset + 7],
            ]);

            // Read page data size (u32, 4 bytes)
            let page_data_len = u32::from_le_bytes([
                bytes[offset + 8],
                bytes[offset + 9],
                bytes[offset + 10],
                bytes[offset + 11],
            ]) as usize;

            offset += 12;

            // Check if there's enough data
            if offset + page_data_len > bytes.len() {
                return Err(Error::validation(
                    "Invalid block size: not enough data for page",
                ));
            }

            // Read page data
            let page_data = bytes[offset..offset + page_data_len].to_vec();
            pages.insert(page_id, page_data);
            offset += page_data_len;
        }

        let mut links = BlockLinks::new();
        let mut metadata = HashMap::new();

        if offset < bytes.len() {
            if bytes.len() < offset + 4 {
                return Err(Error::validation(
                    "Invalid block: truncated links length field",
                ));
            }
            let links_len = u32::from_le_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
            ]) as usize;
            offset += 4;
            if offset + links_len > bytes.len() {
                return Err(Error::validation("Invalid block: truncated links payload"));
            }
            if links_len > 0 {
                links = bincode_io::deserialize(&bytes[offset..offset + links_len])
                    .map_err(Error::from)?;
            }
            offset += links_len;

            if offset < bytes.len() {
                if bytes.len() < offset + 4 {
                    return Err(Error::validation(
                        "Invalid block: truncated metadata length field",
                    ));
                }
                let meta_len = u32::from_le_bytes([
                    bytes[offset],
                    bytes[offset + 1],
                    bytes[offset + 2],
                    bytes[offset + 3],
                ]) as usize;
                offset += 4;
                if offset + meta_len > bytes.len() {
                    return Err(Error::validation(
                        "Invalid block: truncated metadata payload",
                    ));
                }
                if meta_len > 0 {
                    metadata = bincode_io::deserialize(&bytes[offset..offset + meta_len])
                        .map_err(Error::from)?;
                }
                offset += meta_len;
            }
        }

        if offset != bytes.len() {
            return Err(Error::validation(
                "Invalid block: trailing bytes after metadata",
            ));
        }

        let block = Self {
            header,
            pages,
            links,
            metadata,
        };

        Ok(block)
    }
}

/// Block links to other blocks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockLinks {
    /// Next block in the chain
    pub next_block: Option<BlockId>,
    /// Previous block in the chain
    pub prev_block: Option<BlockId>,
    /// Parent block (for hierarchical structures)
    pub parent_block: Option<BlockId>,
    /// Child blocks
    pub child_blocks: Vec<BlockId>,
}

impl BlockLinks {
    /// Creates new block links
    pub fn new() -> Self {
        Self {
            next_block: None,
            prev_block: None,
            parent_block: None,
            child_blocks: Vec::new(),
        }
    }

    /// Sets the next block
    pub fn set_next(&mut self, block_id: BlockId) {
        self.next_block = Some(block_id);
    }

    /// Sets the previous block
    pub fn set_prev(&mut self, block_id: BlockId) {
        self.prev_block = Some(block_id);
    }

    /// Sets the parent block
    pub fn set_parent(&mut self, block_id: BlockId) {
        self.parent_block = Some(block_id);
    }

    /// Adds a child block
    pub fn add_child(&mut self, block_id: BlockId) {
        if !self.child_blocks.contains(&block_id) {
            self.child_blocks.push(block_id);
        }
    }

    /// Removes a child block
    pub fn remove_child(&mut self, block_id: BlockId) -> bool {
        if let Some(pos) = self.child_blocks.iter().position(|&id| id == block_id) {
            self.child_blocks.remove(pos);
            true
        } else {
            false
        }
    }

    /// Checks if the block is a leaf
    pub fn is_leaf(&self) -> bool {
        self.child_blocks.is_empty()
    }

    /// Returns the number of child blocks
    pub fn child_count(&self) -> usize {
        self.child_blocks.len()
    }
}

/// Block manager
pub struct BlockManager {
    /// Block cache
    blocks: HashMap<BlockId, Block>,
    /// Maximum number of blocks in the cache
    max_blocks: usize,
}

impl BlockManager {
    /// Creates a new block manager
    pub fn new(max_blocks: usize) -> Self {
        Self {
            blocks: HashMap::new(),
            max_blocks,
        }
    }

    /// Gets a block by ID
    pub fn get_block(&self, block_id: BlockId) -> Option<&Block> {
        self.blocks.get(&block_id)
    }

    /// Gets a mutable reference to a block
    pub fn get_block_mut(&mut self, block_id: BlockId) -> Option<&mut Block> {
        self.blocks.get_mut(&block_id)
    }

    /// Adds a block to the cache
    pub fn add_block(&mut self, block: Block) {
        let block_id = block.header.block_id;

        // If limit exceeded, remove the oldest block
        if self.blocks.len() >= self.max_blocks {
            self.evict_oldest_block();
        }

        self.blocks.insert(block_id, block);
    }

    /// Removes a block from the cache
    pub fn remove_block(&mut self, block_id: BlockId) -> Option<Block> {
        self.blocks.remove(&block_id)
    }

    /// Removes the oldest block from the cache
    fn evict_oldest_block(&mut self) {
        if let Some((&oldest_id, _)) = self
            .blocks
            .iter()
            .filter(|(_, block)| !block.header.is_pinned)
            .min_by_key(|(id, block)| (block.header.last_modified, *id))
        {
            self.blocks.remove(&oldest_id);
        }
    }

    /// Returns the number of blocks in the cache
    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    /// Checks if the cache contains a block
    pub fn contains_block(&self, block_id: BlockId) -> bool {
        self.blocks.contains_key(&block_id)
    }

    /// Creates a new block
    pub fn create_block(&mut self, block_type: BlockType, size: u32) -> BlockId {
        let block_id = self.generate_block_id();
        let block = Block::new(block_id, block_type, size);
        self.add_block(block);
        block_id
    }

    /// Generates a unique block ID
    fn generate_block_id(&self) -> BlockId {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        now as BlockId
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_creation() {
        let block = Block::new(1, BlockType::Data, 1024);
        assert_eq!(block.header.block_id, 1);
        assert_eq!(block.header.block_type, BlockType::Data);
        assert_eq!(block.header.size, 1024);
        assert_eq!(block.pages.len(), 0);
    }

    #[test]
    fn test_add_page() {
        let mut block = Block::new(1, BlockType::Data, 1024);
        let page_data = vec![1, 2, 3, 4];

        block.add_page(1, page_data.clone()).unwrap();
        assert_eq!(block.page_count(), 1);
        assert!(block.contains_page(1));

        let retrieved = block.get_page(1).unwrap();
        assert_eq!(retrieved, &page_data);
    }

    #[test]
    fn test_block_links() {
        let mut links = BlockLinks::new();
        links.set_next(2);
        links.set_prev(0);
        links.add_child(3);
        links.add_child(4);

        assert_eq!(links.next_block, Some(2));
        assert_eq!(links.prev_block, Some(0));
        assert_eq!(links.child_count(), 2);
        assert!(!links.is_leaf());
    }

    #[test]
    fn test_block_manager() {
        let mut manager = BlockManager::new(2);
        let block1 = Block::new(1, BlockType::Data, 1024);
        let block2 = Block::new(2, BlockType::Data, 1024);
        let block3 = Block::new(3, BlockType::Data, 1024);

        manager.add_block(block1);
        manager.add_block(block2);
        assert_eq!(manager.block_count(), 2);

        manager.add_block(block3);
        assert_eq!(manager.block_count(), 2); // Oldest block should have been removed

        assert!(manager.contains_block(2));
        assert!(manager.contains_block(3));
    }

    #[test]
    fn test_block_serialization_deserialization() {
        use crate::common::types::PAGE_SIZE;
        let block = Block::new(1, BlockType::Data, PAGE_SIZE as u32);
        let block_data = block.to_bytes().unwrap();
        println!("Serialized block size: {}", block_data.len());
        match Block::from_bytes(&block_data) {
            Ok(deserialized) => {
                assert_eq!(deserialized.header.block_id, block.header.block_id);
                assert_eq!(deserialized.header.block_type, block.header.block_type);
                assert_eq!(deserialized.header.size, block.header.size);
                assert_eq!(deserialized.pages.len(), block.pages.len());
            }
            Err(e) => {
                println!("Deserialization error: {:?}", e);
                panic!("Failed to deserialize: {:?}", e);
            }
        }
    }

    #[test]
    fn test_block_roundtrip_links_and_metadata() {
        use crate::common::types::PAGE_SIZE;
        let mut block = Block::new(42, BlockType::Index, PAGE_SIZE as u32);
        block.add_page(1, vec![7, 8, 9]).unwrap();
        block.links.set_next(100);
        block.links.add_child(200);
        block.metadata.insert("k".to_string(), "v".to_string());

        let raw = block.to_bytes().unwrap();
        let out = Block::from_bytes(&raw).unwrap();
        assert_eq!(out.header.block_id, 42);
        assert_eq!(out.pages.len(), 1);
        assert_eq!(out.get_page(1).unwrap().as_slice(), &[7, 8, 9]);
        assert_eq!(out.links.next_block, Some(100));
        assert_eq!(out.links.child_blocks, vec![200]);
        assert_eq!(out.metadata.get("k"), Some(&"v".to_string()));
    }
}
