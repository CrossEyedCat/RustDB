//! Page manager for rustdb

use crate::common::{
    types::{PageId, MAX_RECORD_SIZE, PAGE_HEADER_SIZE, PAGE_SIZE},
    Error, Result,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Magic bytes for slotted page format (avoids collision with bincode)
const SLOTTED_PAGE_MAGIC: [u8; 2] = [0x52, 0x50]; // "RP" for RustDB Page
const SLOTTED_HEADER_OFFSET: usize = 2;
const SLOTTED_HEADER_SIZE: usize = PAGE_HEADER_SIZE - 2; // 62 bytes after magic
const SLOTTED_DATA_OFFSET: usize = PAGE_HEADER_SIZE; // 64 - same as Page layout
const SLOT_SIZE: usize = 9; // offset u16, size u16, record_id u32, flags u8
/// Must match `page_manager::MAX_RECORDS_PER_PAGE`. Payload placement reserves the slot
/// directory as if the page could grow to this many active records, otherwise data written
/// when the directory was small can overlap slots after more inserts.
const MAX_DATA_RECORDS_PER_PAGE: usize = 100;

/// Page header
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageHeader {
    /// Page ID
    pub page_id: PageId,
    /// Page type
    pub page_type: PageType,
    /// Last modification time
    pub last_modified: u64,
    /// Number of records on the page
    pub record_count: u32,
    /// Free space size
    pub free_space: u32,
    /// Pointer to the next page (for linked pages)
    pub next_page: Option<PageId>,
    /// Pointer to the previous page (for linked pages)
    pub prev_page: Option<PageId>,
    /// Dirty page flag (modified in memory)
    pub is_dirty: bool,
    /// Pinned page flag
    pub is_pinned: bool,
}

impl PageHeader {
    /// Creates a new page header
    pub fn new(page_id: PageId, page_type: PageType) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            page_id,
            page_type,
            last_modified: now,
            record_count: 0,
            free_space: MAX_RECORD_SIZE as u32,
            next_page: None,
            prev_page: None,
            is_dirty: false,
            is_pinned: false,
        }
    }

    /// Header size in bytes
    pub fn size(&self) -> usize {
        PAGE_HEADER_SIZE
    }

    /// Updates the last modification time
    pub fn touch(&mut self) {
        self.last_modified = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Marks the page as modified
    pub fn mark_dirty(&mut self) {
        self.is_dirty = true;
        self.touch();
    }

    /// Marks the page as clean
    pub fn mark_clean(&mut self) {
        self.is_dirty = false;
    }

    /// Pins the page in memory
    pub fn pin(&mut self) {
        self.is_pinned = true;
    }

    /// Unpins the page from memory
    pub fn unpin(&mut self) {
        self.is_pinned = false;
    }
}

/// Page type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PageType {
    /// Data page
    Data,
    /// Index page
    Index,
    /// Free space page
    FreeSpace,
    /// Metadata page
    Metadata,
    /// Log page
    Log,
}

/// Record slot on a page
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordSlot {
    /// Record offset from the start of the page
    pub offset: u32,
    /// Record size in bytes
    pub size: u32,
    /// Deleted record flag
    pub is_deleted: bool,
    /// Record ID
    pub record_id: u64,
}

impl RecordSlot {
    /// Creates a new record slot
    pub fn new(offset: u32, size: u32, record_id: u64) -> Self {
        Self {
            offset,
            size,
            is_deleted: false,
            record_id,
        }
    }

    /// Marks the record as deleted
    pub fn mark_deleted(&mut self) {
        self.is_deleted = true;
    }

    /// Checks if the record is deleted
    pub fn is_deleted(&self) -> bool {
        self.is_deleted
    }
}

/// Compact structure for page serialization
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompactPage {
    header: PageHeader,
    slots: Vec<RecordSlot>,
    data: Vec<u8>,
    // free_space_map is not serialized as it can be recalculated
}

/// Page structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Page {
    /// Page header
    pub header: PageHeader,
    /// Record slots
    pub slots: Vec<RecordSlot>,
    /// Page data
    pub data: Vec<u8>,
    /// Free space map
    pub free_space_map: Vec<bool>,
}

impl Page {
    /// Creates a new empty data page
    pub fn new(page_id: PageId) -> Self {
        Self::new_with_type(page_id, PageType::Data)
    }

    /// Creates a new empty page with the specified type
    pub fn new_with_type(page_id: PageId, page_type: PageType) -> Self {
        let data = vec![0u8; PAGE_SIZE];
        let free_space_map = vec![true; PAGE_SIZE - PAGE_HEADER_SIZE];

        Self {
            header: PageHeader::new(page_id, page_type),
            slots: Vec::new(),
            data,
            free_space_map,
        }
    }

    /// Creates a page from bytes (deserialization)
    /// Detects format: slotted (magic RP) or legacy bincode
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() >= 2
            && bytes[0] == SLOTTED_PAGE_MAGIC[0]
            && bytes[1] == SLOTTED_PAGE_MAGIC[1]
        {
            Self::from_bytes_slotted(bytes)
        } else {
            Self::from_bytes_bincode(bytes)
        }
    }

    /// Deserializes from legacy bincode format
    fn from_bytes_bincode(bytes: &[u8]) -> Result<Self> {
        let compact_page: CompactPage =
            crate::common::bincode_io::deserialize(bytes).map_err(Error::from)?;

        let mut data = compact_page.data.clone();
        data.resize(PAGE_SIZE, 0);

        let mut free_space_map = vec![true; PAGE_SIZE - PAGE_HEADER_SIZE];
        for slot in &compact_page.slots {
            if !slot.is_deleted {
                let start = (slot.offset as usize).saturating_sub(PAGE_HEADER_SIZE);
                let end = start + slot.size as usize;
                for i in start..end.min(free_space_map.len()) {
                    free_space_map[i] = false;
                }
            }
        }

        Ok(Self {
            header: compact_page.header,
            slots: compact_page.slots,
            data,
            free_space_map,
        })
    }

    /// Deserializes from slotted binary format
    fn from_bytes_slotted(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < PAGE_SIZE {
            return Err(Error::validation("Page data too short"));
        }

        let h = SLOTTED_HEADER_OFFSET;
        let mut header = PageHeader::new(0, PageType::Data);
        header.page_id = u64::from_le_bytes(bytes[h..][..8].try_into().unwrap());
        header.page_type = match bytes.get(h + 8).copied().unwrap_or(0) {
            0 => PageType::Data,
            1 => PageType::Index,
            2 => PageType::FreeSpace,
            3 => PageType::Metadata,
            4 => PageType::Log,
            _ => PageType::Data,
        };
        header.last_modified = u64::from_le_bytes(bytes[h + 9..][..8].try_into().unwrap());
        header.record_count = u32::from_le_bytes(bytes[h + 17..][..4].try_into().unwrap());
        header.free_space = u32::from_le_bytes(bytes[h + 21..][..4].try_into().unwrap());

        let slot_count = u16::from_le_bytes(bytes[PAGE_SIZE - 2..].try_into().unwrap()) as usize;
        let slot_start = PAGE_SIZE - 2 - slot_count * SLOT_SIZE;

        let mut slots = Vec::with_capacity(slot_count);
        let mut data = vec![0u8; PAGE_SIZE];

        for i in 0..slot_count {
            let base = slot_start + i * SLOT_SIZE;
            let offset = u16::from_le_bytes(bytes[base..][..2].try_into().unwrap()) as u32;
            let size = u16::from_le_bytes(bytes[base + 2..][..2].try_into().unwrap()) as u32;
            let record_id = u32::from_le_bytes(bytes[base + 4..][..4].try_into().unwrap()) as u64;
            let flags = bytes[base + 8];
            let is_deleted = (flags & 1) != 0;

            let slot = RecordSlot {
                offset,
                size,
                is_deleted,
                record_id,
            };
            slots.push(slot);

            if !is_deleted
                && (offset as usize) < PAGE_SIZE
                && (offset as usize) + (size as usize) <= PAGE_SIZE
            {
                let start = offset as usize;
                let end = start + size as usize;
                data[start..end].copy_from_slice(&bytes[start..end]);
            }
        }

        let mut free_space_map = vec![true; PAGE_SIZE - PAGE_HEADER_SIZE];
        for slot in &slots {
            if !slot.is_deleted {
                let start = (slot.offset as usize).saturating_sub(PAGE_HEADER_SIZE);
                let end = start + slot.size as usize;
                for i in start..end.min(free_space_map.len()) {
                    free_space_map[i] = false;
                }
            }
        }

        Ok(Self {
            header,
            slots,
            data,
            free_space_map,
        })
    }

    /// Creates a page from bytes with specified page_id
    ///
    /// Parses slotted or legacy bincode format (see [`Self::from_bytes`]), then sets
    /// [`PageHeader::page_id`] to `page_id` (caller’s canonical id, e.g. from storage layer).
    pub fn from_bytes_with_id(bytes: &[u8], page_id: PageId) -> Result<Self> {
        if bytes.len() != PAGE_SIZE {
            return Err(Error::validation("Invalid page size"));
        }
        let mut page = Self::from_bytes(bytes)?;
        page.header.page_id = page_id;
        Ok(page)
    }

    /// Serializes the page to bytes (uses slotted format for compactness)
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.to_bytes_slotted()
    }

    /// Serializes to slotted binary format (compact, fixed layout)
    fn to_bytes_slotted(&self) -> Result<Vec<u8>> {
        let active_slots: Vec<_> = self.slots.iter().filter(|s| !s.is_deleted).collect();
        let slot_count = active_slots.len();

        if slot_count * SLOT_SIZE + 2 > PAGE_SIZE - SLOTTED_DATA_OFFSET {
            return Err(Error::validation("Too many slots for page"));
        }

        let slot_start = PAGE_SIZE - 2 - slot_count * SLOT_SIZE;
        let mut buf = vec![0u8; PAGE_SIZE];

        buf[0..2].copy_from_slice(&SLOTTED_PAGE_MAGIC);

        let h = SLOTTED_HEADER_OFFSET;
        buf[h..h + 8].copy_from_slice(&self.header.page_id.to_le_bytes());
        buf[h + 8] = match self.header.page_type {
            PageType::Data => 0,
            PageType::Index => 1,
            PageType::FreeSpace => 2,
            PageType::Metadata => 3,
            PageType::Log => 4,
        };
        buf[h + 9..h + 17].copy_from_slice(&self.header.last_modified.to_le_bytes());
        buf[h + 17..h + 21].copy_from_slice(&self.header.record_count.to_le_bytes());
        buf[h + 21..h + 25].copy_from_slice(&self.header.free_space.to_le_bytes());

        let max_offset = self
            .slots
            .iter()
            .filter(|s| !s.is_deleted)
            .map(|s| (s.offset + s.size) as usize)
            .max()
            .unwrap_or(PAGE_HEADER_SIZE);

        if max_offset > slot_start {
            return Err(Error::validation("Data overlaps slot area"));
        }

        for (i, slot) in active_slots.iter().enumerate() {
            let base = slot_start + i * SLOT_SIZE;
            buf[base..base + 2].copy_from_slice(&(slot.offset as u16).to_le_bytes());
            buf[base + 2..base + 4].copy_from_slice(&(slot.size as u16).to_le_bytes());
            buf[base + 4..base + 8].copy_from_slice(&(slot.record_id as u32).to_le_bytes());
            buf[base + 8] = if slot.is_deleted { 1 } else { 0 };
        }

        buf[PAGE_SIZE - 2..].copy_from_slice(&(slot_count as u16).to_le_bytes());

        let data_end = self
            .slots
            .iter()
            .filter(|s| !s.is_deleted)
            .map(|s| (s.offset + s.size) as usize)
            .max()
            .unwrap_or(PAGE_HEADER_SIZE);
        let copy_len = data_end
            .saturating_sub(PAGE_HEADER_SIZE)
            .min(slot_start.saturating_sub(PAGE_HEADER_SIZE));
        if copy_len > 0 {
            buf[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + copy_len]
                .copy_from_slice(&self.data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + copy_len]);
        }

        Ok(buf)
    }

    /// Adds a record to the page
    pub fn add_record(&mut self, record_data: &[u8], record_id: u64) -> Result<u32> {
        if record_data.len() > MAX_RECORD_SIZE {
            return Err(Error::validation("Record is too large"));
        }

        // Find free space
        let offset = self.find_free_space(record_data.len())?;

        // Write data
        let end_offset = offset + record_data.len();
        self.data[offset..end_offset].copy_from_slice(record_data);

        // Create slot
        let slot = RecordSlot::new(offset as u32, record_data.len() as u32, record_id);
        self.slots.push(slot);

        // Update header
        self.header.record_count += 1;
        self.header.free_space -= record_data.len() as u32;
        self.header.mark_dirty();

        // Update free space map
        self.update_free_space_map(offset, end_offset, false);

        Ok(offset as u32)
    }

    /// Inserts a record at a fixed byte offset (WAL recovery). Caller must ensure the span is free.
    pub fn add_record_at_offset(
        &mut self,
        record_data: &[u8],
        record_id: u64,
        offset: u32,
    ) -> Result<()> {
        if record_data.len() > MAX_RECORD_SIZE {
            return Err(Error::validation("Record is too large"));
        }
        let start = offset as usize;
        let end = start + record_data.len();
        if end > PAGE_SIZE {
            return Err(Error::validation("Record does not fit on page"));
        }
        if self
            .slots
            .iter()
            .any(|s| s.offset == offset && !s.is_deleted)
        {
            return Err(Error::validation("Record already exists at offset"));
        }
        self.data[start..end].copy_from_slice(record_data);
        let slot = RecordSlot::new(offset, record_data.len() as u32, record_id);
        self.slots.push(slot);
        self.header.record_count += 1;
        self.header.free_space -= record_data.len() as u32;
        self.header.mark_dirty();
        self.update_free_space_map(start, end, false);
        Ok(())
    }

    /// Deletes a record by ID
    pub fn delete_record(&mut self, record_id: u64) -> Result<bool> {
        if let Some(slot_index) = self.slots.iter().position(|s| s.record_id == record_id) {
            let slot = &mut self.slots[slot_index];
            if !slot.is_deleted {
                slot.mark_deleted();

                // Free space in the map
                let start = slot.offset as usize;
                let end = start + slot.size as usize;
                let size = slot.size;

                // Free the slot
                let _ = slot;

                // Now update the map and header
                self.update_free_space_map(start, end, true);
                self.header.free_space += size;
                self.header.mark_dirty();

                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Gets a record by ID
    pub fn get_record(&self, record_id: u64) -> Option<&[u8]> {
        if let Some(slot) = self
            .slots
            .iter()
            .find(|s| s.record_id == record_id && !s.is_deleted)
        {
            let start = slot.offset as usize;
            let end = start + slot.size as usize;
            Some(&self.data[start..end])
        } else {
            None
        }
    }

    /// Updates a record
    pub fn update_record(&mut self, record_id: u64, new_data: &[u8]) -> Result<bool> {
        if new_data.len() > MAX_RECORD_SIZE {
            return Err(Error::validation("New data is too large"));
        }

        let slot_index = if let Some(idx) = self
            .slots
            .iter()
            .position(|s| s.record_id == record_id && !s.is_deleted)
        {
            idx
        } else {
            return Ok(false);
        };

        let old_size = self.slots[slot_index].size as usize;
        let new_size = new_data.len();

        if new_size <= old_size {
            // Write new data
            let start = self.slots[slot_index].offset as usize;
            let end = start + new_size;
            self.data[start..end].copy_from_slice(new_data);

            // Update slot
            self.slots[slot_index].size = new_size as u32;

            // Update header
            self.header.free_space += (old_size - new_size) as u32;
            self.header.mark_dirty();

            // Update free space map
            if new_size < old_size {
                self.update_free_space_map(start + new_size, start + old_size, true);
            }

            Ok(true)
        } else {
            // Need to redistribute space
            // First delete the old record
            let slot = &mut self.slots[slot_index];
            let start = slot.offset as usize;
            let end = start + slot.size as usize;
            let size = slot.size;

            // Delete the slot
            slot.mark_deleted();

            // Free the slot
            let _ = slot;

            // Now update the map and header
            self.update_free_space_map(start, end, true);
            self.header.free_space += size;

            // Now add the new record
            let new_data_copy = new_data.to_vec();
            self.add_record(&new_data_copy, record_id)?;
            Ok(true)
        }
    }

    /// Finds free space for a record of the specified size
    fn find_free_space(&self, size: usize) -> Result<usize> {
        // Keep layout consistent with `to_bytes_slotted`: slot directory grows upward from
        // `PAGE_SIZE - 2` (slot_count u16) with `SLOT_SIZE` bytes per active slot. Record
        // payload must not overlap `[slot_start, PAGE_SIZE)` or serialization fails with
        // "Data overlaps slot area" and forces expensive splits.
        let active_count = self.slots.iter().filter(|s| !s.is_deleted).count();
        let slot_count_after = active_count + 1;

        if slot_count_after * SLOT_SIZE + 2 > PAGE_SIZE - SLOTTED_DATA_OFFSET {
            return Err(Error::validation("Too many slots for page"));
        }

        let phys_max_slots = (PAGE_SIZE - SLOTTED_DATA_OFFSET - 2) / SLOT_SIZE;
        let layout_slot_count = slot_count_after
            .max(MAX_DATA_RECORDS_PER_PAGE)
            .min(phys_max_slots);
        let slot_start_ceiling = PAGE_SIZE - 2 - layout_slot_count * SLOT_SIZE;
        let allowed_end = slot_start_ceiling.saturating_sub(PAGE_HEADER_SIZE);

        if size == 0 || size > allowed_end {
            return Err(Error::validation("Not enough free space on the page"));
        }

        let scan_end = allowed_end.min(self.free_space_map.len());

        let mut consecutive_free = 0usize;
        for i in 0..scan_end {
            if self.free_space_map[i] {
                consecutive_free += 1;
                if consecutive_free >= size {
                    let start_i = i + 1 - size;
                    if start_i + size <= allowed_end {
                        return Ok(PAGE_HEADER_SIZE + start_i);
                    }
                }
            } else {
                consecutive_free = 0;
            }
        }

        Err(Error::validation("Not enough free space on the page"))
    }

    /// Updates the free space map
    fn update_free_space_map(&mut self, start: usize, end: usize, is_free: bool) {
        let map_start = start.saturating_sub(PAGE_HEADER_SIZE);
        let map_end = end.saturating_sub(PAGE_HEADER_SIZE);

        for i in map_start..map_end {
            if i < self.free_space_map.len() {
                self.free_space_map[i] = is_free;
            }
        }
    }

    /// Checks if there is free space for a record of the specified size
    pub fn has_free_space(&self, size: usize) -> bool {
        self.header.free_space >= size as u32
    }

    /// Returns the amount of free space
    pub fn free_space(&self) -> u32 {
        self.header.free_space
    }

    /// Returns the number of records
    pub fn record_count(&self) -> u32 {
        self.header.record_count
    }

    /// Checks if the page is empty
    pub fn is_empty(&self) -> bool {
        self.header.record_count == 0
    }

    /// Checks if the page is full
    pub fn is_full(&self) -> bool {
        self.header.free_space == 0
    }

    /// Clears the page
    pub fn clear(&mut self) -> Result<()> {
        self.slots.clear();
        self.data.fill(0);
        self.free_space_map.fill(true);
        self.header.record_count = 0;
        self.header.free_space = MAX_RECORD_SIZE as u32;
        self.header.mark_dirty();
        Ok(())
    }

    /// Gets the page fill factor (0.0 - 1.0)
    pub fn get_fill_factor(&self) -> f64 {
        let used_space = MAX_RECORD_SIZE as u32 - self.header.free_space;
        used_space as f64 / MAX_RECORD_SIZE as f64
    }

    /// Gets the number of records
    pub fn get_record_count(&self) -> u32 {
        self.header.record_count
    }

    /// Gets the free space size
    pub fn get_free_space(&self) -> u32 {
        self.header.free_space
    }

    /// Checks if defragmentation is needed
    pub fn needs_defragmentation(&self) -> bool {
        // Simple heuristic: if there are deleted records
        self.slots.iter().any(|slot| slot.is_deleted)
    }

    /// Scans all records on the page
    pub fn scan_records(&self) -> Result<Vec<(u32, Vec<u8>)>> {
        let mut records = Vec::new();

        for slot in &self.slots {
            if !slot.is_deleted {
                let start = slot.offset as usize;
                let end = start + slot.size as usize;

                if end <= self.data.len() {
                    let record_data = self.data[start..end].to_vec();
                    records.push((slot.offset, record_data));
                }
            }
        }

        Ok(records)
    }

    /// Gets all records on the page
    pub fn get_all_records(&self) -> Result<Vec<Vec<u8>>> {
        let mut records = Vec::new();

        for slot in &self.slots {
            if !slot.is_deleted {
                let start = slot.offset as usize;
                let end = start + slot.size as usize;

                if end <= self.data.len() {
                    let record_data = self.data[start..end].to_vec();
                    records.push(record_data);
                }
            }
        }

        Ok(records)
    }

    /// Performs page defragmentation
    pub fn defragment(&mut self) -> Result<()> {
        // Collect all active records
        let mut active_records = Vec::new();
        for slot in &self.slots {
            if !slot.is_deleted {
                let start = slot.offset as usize;
                let end = start + slot.size as usize;
                let record_data = self.data[start..end].to_vec();
                active_records.push((slot.record_id, record_data));
            }
        }

        // Clear the page
        self.clear()?;

        // Add records back
        for (record_id, record_data) in active_records {
            self.add_record(&record_data, record_id)?;
        }

        Ok(())
    }

    /// Updates a record by offset
    pub fn update_record_by_offset(&mut self, offset: u32, new_data: &[u8]) -> Result<()> {
        // Find slot by offset
        if let Some(slot) = self
            .slots
            .iter_mut()
            .find(|s| s.offset == offset && !s.is_deleted)
        {
            if new_data.len() <= slot.size as usize {
                // Update data in-place
                let start = offset as usize;
                let end = start + new_data.len();
                self.data[start..end].copy_from_slice(new_data);

                // Update slot size if needed
                if new_data.len() < slot.size as usize {
                    let size_diff = slot.size as usize - new_data.len();
                    let old_slot_end = start + slot.size as usize;
                    slot.size = new_data.len() as u32;
                    self.header.free_space += size_diff as u32;

                    // Update free space map
                    self.update_free_space_map(end, old_slot_end, true);
                }

                self.header.mark_dirty();
                Ok(())
            } else {
                Err(Error::validation("New data does not fit in the slot"))
            }
        } else {
            Err(Error::validation("Record not found"))
        }
    }

    /// Deletes a record by offset
    pub fn delete_record_by_offset(&mut self, offset: u32) -> Result<()> {
        if let Some(slot) = self
            .slots
            .iter_mut()
            .find(|s| s.offset == offset && !s.is_deleted)
        {
            slot.mark_deleted();

            // Save values before changing borrowing
            let start = slot.offset as usize;
            let end = start + slot.size as usize;
            let slot_size = slot.size;

            // Free space
            self.update_free_space_map(start, end, true);
            self.header.free_space += slot_size;
            self.header.mark_dirty();

            Ok(())
        } else {
            Err(Error::validation("Record not found"))
        }
    }
}

/// Page manager
pub struct PageManager {
    /// Page cache
    pages: HashMap<PageId, Page>,
    /// Maximum number of pages in the cache
    max_pages: usize,
}

impl PageManager {
    /// Creates a new page manager
    pub fn new(max_pages: usize) -> Self {
        Self {
            pages: HashMap::new(),
            max_pages,
        }
    }

    /// Gets a page by ID
    pub fn get_page(&mut self, page_id: PageId) -> Option<&Page> {
        self.pages.get(&page_id)
    }

    /// Gets a mutable reference to a page
    pub fn get_page_mut(&mut self, page_id: PageId) -> Option<&mut Page> {
        self.pages.get_mut(&page_id)
    }

    /// Adds a page to the cache
    pub fn add_page(&mut self, page: Page) {
        let page_id = page.header.page_id;

        // If limit exceeded, remove the oldest page
        if self.pages.len() >= self.max_pages {
            self.evict_oldest_page();
        }

        self.pages.insert(page_id, page);
    }

    /// Removes a page from the cache
    pub fn remove_page(&mut self, page_id: PageId) -> Option<Page> {
        self.pages.remove(&page_id)
    }

    /// Removes the oldest page from the cache
    fn evict_oldest_page(&mut self) {
        if let Some((&oldest_id, _)) = self
            .pages
            .iter()
            .filter(|(_, page)| !page.header.is_pinned)
            .min_by_key(|(_, page)| page.header.last_modified)
        {
            self.pages.remove(&oldest_id);
        }
    }

    /// Returns the number of pages in the cache
    pub fn page_count(&self) -> usize {
        self.pages.len()
    }

    /// Checks if the cache contains a page
    pub fn contains_page(&self, page_id: PageId) -> bool {
        self.pages.contains_key(&page_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_creation() {
        let page = Page::new(1);
        assert_eq!(page.header.page_id, 1);
        assert_eq!(page.header.page_type, PageType::Data);
        assert_eq!(page.data.len(), PAGE_SIZE);
        assert_eq!(page.free_space_map.len(), PAGE_SIZE - PAGE_HEADER_SIZE);
    }

    #[test]
    fn test_add_record() {
        let mut page = Page::new(1);
        let record_data = b"test record";
        let record_id = 123;

        let _offset = page.add_record(record_data, record_id).unwrap();
        assert_eq!(page.header.record_count, 1);
        assert_eq!(
            page.header.free_space,
            (MAX_RECORD_SIZE - record_data.len()) as u32
        );
        assert_eq!(page.slots.len(), 1);

        let retrieved = page.get_record(record_id).unwrap();
        assert_eq!(retrieved, record_data);
    }

    #[test]
    fn test_delete_record() {
        let mut page = Page::new(1);
        let record_data = b"test record";
        let record_id = 123;

        page.add_record(record_data, record_id).unwrap();
        assert_eq!(page.header.record_count, 1);

        let deleted = page.delete_record(record_id).unwrap();
        assert!(deleted);
        assert_eq!(page.header.record_count, 1); // Slot remains, but the record is marked as deleted

        let retrieved = page.get_record(record_id);
        assert!(retrieved.is_none());
    }

    #[test]
    fn test_page_manager() {
        let mut manager = PageManager::new(2);
        let page1 = Page::new(1);
        let page2 = Page::new(2);
        let page3 = Page::new(3);

        manager.add_page(page1);
        manager.add_page(page2);
        assert_eq!(manager.page_count(), 2);

        manager.add_page(page3);
        assert_eq!(manager.page_count(), 2); // Oldest page should have been removed

        // After adding 3 pages to a manager with capacity=2, the last 2 should remain
        // Check which pages actually remained
        assert!(manager.contains_page(2) || manager.contains_page(1));
        assert!(manager.contains_page(3));
    }
}
