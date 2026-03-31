//! Buffer manager for rustdb

use crate::common::{types::PageId, Error, Result};
use crate::storage::page::Page;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Persists serialized page bytes (e.g. write to `.tbl` via file manager). Return `Ok(())` on success.
pub type PageFlushCallback = Arc<dyn Fn(PageId, Vec<u8>) -> Result<()> + Send + Sync>;

/// Buffer statistics
#[derive(Debug, Clone)]
pub struct BufferStats {
    /// Number of cache accesses
    pub total_accesses: u64,
    /// Number of cache hits
    pub cache_hits: u64,
    /// Number of cache misses
    pub cache_misses: u64,
    /// Number of write operations
    pub write_operations: u64,
    /// Number of read operations
    pub read_operations: u64,
    /// Time of last statistics reset
    pub last_reset: Instant,
}

impl BufferStats {
    /// Creates new statistics
    pub fn new() -> Self {
        Self {
            total_accesses: 0,
            cache_hits: 0,
            cache_misses: 0,
            write_operations: 0,
            read_operations: 0,
            last_reset: Instant::now(),
        }
    }

    /// Returns hit ratio (ratio of hits to total accesses)
    pub fn hit_ratio(&self) -> f64 {
        if self.total_accesses == 0 {
            0.0
        } else {
            self.cache_hits as f64 / self.total_accesses as f64
        }
    }

    /// Returns miss ratio (ratio of misses to total accesses)
    pub fn miss_ratio(&self) -> f64 {
        if self.total_accesses == 0 {
            0.0
        } else {
            self.cache_misses as f64 / self.total_accesses as f64
        }
    }

    /// Resets statistics
    pub fn reset(&mut self) {
        self.total_accesses = 0;
        self.cache_hits = 0;
        self.cache_misses = 0;
        self.write_operations = 0;
        self.read_operations = 0;
        self.last_reset = Instant::now();
    }

    /// Records cache access
    pub fn record_access(&mut self, is_hit: bool) {
        self.total_accesses += 1;
        if is_hit {
            self.cache_hits += 1;
        } else {
            self.cache_misses += 1;
        }
    }

    /// Records write operation
    pub fn record_write(&mut self) {
        self.write_operations += 1;
    }

    /// Records read operation
    pub fn record_read(&mut self) {
        self.read_operations += 1;
    }
}

/// LRU cache entry
#[derive(Debug, Clone)]
struct LRUEntry {
    /// Page
    page: Page,
    /// Last access time
    last_access: Instant,
    /// Access count
    access_count: u32,
    /// Dirty page flag
    is_dirty: bool,
}

impl LRUEntry {
    /// Creates new LRU entry
    fn new(page: Page) -> Self {
        Self {
            page,
            last_access: Instant::now(),
            access_count: 1,
            is_dirty: false,
        }
    }

    /// Updates access time
    fn touch(&mut self) {
        self.last_access = Instant::now();
        self.access_count += 1;
    }

    /// Marks page as modified
    fn mark_dirty(&mut self) {
        self.is_dirty = true;
        self.page.header.mark_dirty();
    }

    /// Marks page as clean
    fn mark_clean(&mut self) {
        self.is_dirty = false;
        self.page.header.mark_clean();
    }
}

/// Eviction strategy
#[derive(Debug, Clone, PartialEq)]
pub enum EvictionStrategy {
    /// LRU (Least Recently Used)
    LRU,
    /// Clock algorithm
    Clock,
    /// Adaptive strategy
    Adaptive,
}

/// Buffer manager with LRU cache
pub struct BufferManager {
    /// LRU page cache
    cache: HashMap<PageId, LRUEntry>,
    /// LRU queue for tracking access order
    lru_queue: VecDeque<PageId>,
    /// Maximum number of pages in cache
    max_pages: usize,
    /// Eviction strategy
    strategy: EvictionStrategy,
    /// Buffer statistics
    stats: BufferStats,
    /// Pointer for Clock algorithm
    clock_pointer: usize,
    /// Access counter for adaptive strategy
    access_counter: u64,
    /// When set, dirty pages are serialized and passed here on eviction and flush.
    dirty_flush: Option<PageFlushCallback>,
}

impl BufferManager {
    /// Creates a new buffer manager
    pub fn new(max_pages: usize, strategy: EvictionStrategy) -> Self {
        Self {
            cache: HashMap::new(),
            lru_queue: VecDeque::new(),
            max_pages,
            strategy,
            stats: BufferStats::new(),
            clock_pointer: 0,
            access_counter: 0,
            dirty_flush: None,
        }
    }

    /// Install a hook to persist dirty page contents. Without it, eviction/flush only clears dirty state (with a warning on eviction).
    pub fn set_dirty_flush_hook(&mut self, hook: Option<PageFlushCallback>) {
        self.dirty_flush = hook;
    }

    fn run_flush_hook(&mut self, page_id: PageId, bytes: Vec<u8>) -> Result<()> {
        let Some(ref hook) = self.dirty_flush else {
            return Ok(());
        };
        hook(page_id, bytes)?;
        self.stats.record_write();
        Ok(())
    }

    /// Gets page by ID
    pub fn get_page(&mut self, page_id: PageId) -> Option<&Page> {
        self.stats.record_read();

        if !self.cache.contains_key(&page_id) {
            self.stats.record_access(false);
            return None;
        }

        self.stats.record_access(true);

        // First update LRU order
        self.update_lru_order(page_id);

        // Then get reference to page
        if let Some(entry) = self.cache.get_mut(&page_id) {
            entry.touch();
            Some(&entry.page)
        } else {
            None
        }
    }

    /// Gets mutable reference to page
    pub fn get_page_mut(&mut self, page_id: PageId) -> Option<&mut Page> {
        self.stats.record_write();

        if !self.cache.contains_key(&page_id) {
            self.stats.record_access(false);
            return None;
        }

        self.stats.record_access(true);

        // First update LRU order
        self.update_lru_order(page_id);

        // Then get mutable reference to page
        if let Some(entry) = self.cache.get_mut(&page_id) {
            entry.touch();
            entry.mark_dirty();
            Some(&mut entry.page)
        } else {
            None
        }
    }

    /// Adds page to cache
    pub fn add_page(&mut self, page: Page) -> Result<()> {
        let page_id = page.header.page_id;

        // If page already exists, update it
        if self.cache.contains_key(&page_id) {
            self.update_page(page)?;
            return Ok(());
        }

        // If limit exceeded, evict a page
        if self.cache.len() >= self.max_pages {
            self.evict_page()?;
        }

        // Add page to cache
        let entry = LRUEntry::new(page);
        self.cache.insert(page_id, entry);
        self.lru_queue.push_back(page_id);

        Ok(())
    }

    /// Updates existing page
    pub fn update_page(&mut self, page: Page) -> Result<()> {
        let page_id = page.header.page_id;

        if !self.cache.contains_key(&page_id) {
            return Ok(());
        }

        // First update LRU order
        self.update_lru_order(page_id);

        // Then update page
        if let Some(entry) = self.cache.get_mut(&page_id) {
            entry.page = page;
            entry.touch();
            entry.mark_dirty();
        }

        Ok(())
    }

    /// Removes page from cache
    pub fn remove_page(&mut self, page_id: PageId) -> Option<Page> {
        if let Some(entry) = self.cache.remove(&page_id) {
            // Remove from LRU queue
            if let Some(pos) = self.lru_queue.iter().position(|&id| id == page_id) {
                self.lru_queue.remove(pos);
            }
            Some(entry.page)
        } else {
            None
        }
    }

    /// Evicts page from cache
    fn evict_page(&mut self) -> Result<()> {
        let page_id = match self.strategy {
            EvictionStrategy::LRU => self.evict_lru()?,
            EvictionStrategy::Clock => self.evict_clock()?,
            EvictionStrategy::Adaptive => self.evict_adaptive()?,
        };

        let dirty = self
            .cache
            .get(&page_id)
            .map(|e| e.is_dirty || e.page.header.is_dirty)
            .unwrap_or(false);
        if dirty {
            if self.dirty_flush.is_none() {
                log::warn!(
                    "Evicting dirty page {} without flush hook; data may not be persisted",
                    page_id
                );
            } else {
                let bytes = self
                    .cache
                    .get(&page_id)
                    .unwrap()
                    .page
                    .to_bytes()?;
                self.run_flush_hook(page_id, bytes)?;
            }
        }

        let _ = self.remove_page(page_id);

        Ok(())
    }

    /// Evicts page using LRU algorithm
    fn evict_lru(&mut self) -> Result<PageId> {
        if let Some(page_id) = self.lru_queue.front().copied() {
            // Check if page is pinned
            if let Some(entry) = self.cache.get(&page_id) {
                if entry.page.header.is_pinned {
                    // Find next unpinned page
                    for &id in self.lru_queue.iter().skip(1) {
                        if let Some(entry) = self.cache.get(&id) {
                            if !entry.page.header.is_pinned {
                                return Ok(id);
                            }
                        }
                    }
                    return Err(Error::validation("All pages are pinned"));
                }
            }
            Ok(page_id)
        } else {
            Err(Error::validation("Cache is empty"))
        }
    }

    /// Evicts page using Clock algorithm
    fn evict_clock(&mut self) -> Result<PageId> {
        let mut attempts = 0;
        let max_attempts = self.cache.len() * 2;

        while attempts < max_attempts {
            let page_ids: Vec<PageId> = self.cache.keys().copied().collect();
            if page_ids.is_empty() {
                return Err(Error::validation("Cache is empty"));
            }

            let page_id = page_ids[self.clock_pointer % page_ids.len()];
            self.clock_pointer = (self.clock_pointer + 1) % page_ids.len();

            if let Some(entry) = self.cache.get(&page_id) {
                if !entry.page.header.is_pinned {
                    if entry.access_count == 0 {
                        return Ok(page_id);
                    } else {
                        // Decrease access counter
                        if let Some(entry_mut) = self.cache.get_mut(&page_id) {
                            entry_mut.access_count = entry_mut.access_count.saturating_sub(1);
                        }
                    }
                }
            }

            attempts += 1;
        }

        Err(Error::validation("Failed to find page for eviction"))
    }

    /// Evicts page using adaptive strategy
    fn evict_adaptive(&mut self) -> Result<PageId> {
        // Adaptive strategy: combination of LRU and Clock
        let hit_ratio = self.stats.hit_ratio();

        if hit_ratio > 0.8 {
            // High hit ratio - use LRU
            self.evict_lru()
        } else if hit_ratio < 0.2 {
            // Low hit ratio - use Clock
            self.evict_clock()
        } else {
            // Medium hit ratio - use LRU
            self.evict_lru()
        }
    }

    /// Updates LRU order
    fn update_lru_order(&mut self, page_id: PageId) {
        // Remove page from current position
        if let Some(pos) = self.lru_queue.iter().position(|&id| id == page_id) {
            self.lru_queue.remove(pos);
        }
        // Add to end (most recently used)
        self.lru_queue.push_back(page_id);
    }

    /// Pins page in memory
    pub fn pin_page(&mut self, page_id: PageId) -> Result<()> {
        if let Some(entry) = self.cache.get_mut(&page_id) {
            entry.page.header.pin();
        } else {
            return Err(Error::validation("Page not found in cache"));
        }
        Ok(())
    }

    /// Unpins page from memory
    pub fn unpin_page(&mut self, page_id: PageId) -> Result<()> {
        if let Some(entry) = self.cache.get_mut(&page_id) {
            entry.page.header.unpin();
        } else {
            return Err(Error::validation("Page not found in cache"));
        }
        Ok(())
    }

    /// Returns buffer statistics
    pub fn get_stats(&self) -> BufferStats {
        self.stats.clone()
    }

    /// Resets statistics
    pub fn reset_stats(&mut self) {
        self.stats.reset();
    }

    /// Returns number of pages in cache
    pub fn page_count(&self) -> usize {
        self.cache.len()
    }

    /// Checks if cache contains page
    pub fn contains_page(&self, page_id: PageId) -> bool {
        self.cache.contains_key(&page_id)
    }

    /// Returns number of dirty pages
    pub fn dirty_page_count(&self) -> usize {
        self.cache.values().filter(|entry| entry.is_dirty).count()
    }

    /// Forces write of all dirty pages
    pub fn flush_dirty_pages(&mut self) -> Result<usize> {
        let mut flushed_count = 0;
        let dirty_pages: Vec<PageId> = self
            .cache
            .iter()
            .filter(|(_, entry)| entry.is_dirty)
            .map(|(&id, _)| id)
            .collect();

        for page_id in dirty_pages {
            let need_flush = self
                .cache
                .get(&page_id)
                .map(|e| e.is_dirty || e.page.header.is_dirty)
                .unwrap_or(false);
            if need_flush && self.dirty_flush.is_some() {
                let bytes = self
                    .cache
                    .get(&page_id)
                    .unwrap()
                    .page
                    .to_bytes()?;
                self.run_flush_hook(page_id, bytes)?;
            }
            if let Some(entry) = self.cache.get_mut(&page_id) {
                entry.mark_clean();
                flushed_count += 1;
            }
        }

        Ok(flushed_count)
    }

    /// Changes eviction strategy
    pub fn set_eviction_strategy(&mut self, strategy: EvictionStrategy) {
        self.strategy = strategy;
        // Reset pointer for Clock algorithm
        self.clock_pointer = 0;
    }

    /// Returns current eviction strategy
    pub fn get_eviction_strategy(&self) -> EvictionStrategy {
        self.strategy.clone()
    }
}

/// Thread-safe buffer manager
pub type SharedBufferManager = Arc<Mutex<BufferManager>>;

/// Creates thread-safe buffer manager
pub fn create_shared_buffer_manager(
    max_pages: usize,
    strategy: EvictionStrategy,
) -> SharedBufferManager {
    Arc::new(Mutex::new(BufferManager::new(max_pages, strategy)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::{Page, PageType};
    use std::sync::Arc;

    #[test]
    fn test_buffer_manager_creation() {
        let manager = BufferManager::new(100, EvictionStrategy::LRU);
        assert_eq!(manager.max_pages, 100);
        assert_eq!(manager.strategy, EvictionStrategy::LRU);
        assert_eq!(manager.page_count(), 0);
    }

    #[test]
    fn test_add_and_get_page() {
        let mut manager = BufferManager::new(10, EvictionStrategy::LRU);
        let page = Page::new(1);

        manager.add_page(page).unwrap();
        assert_eq!(manager.page_count(), 1);
        assert!(manager.contains_page(1));

        let retrieved = manager.get_page(1);
        assert!(retrieved.is_some());
    }

    #[test]
    fn test_lru_eviction() {
        let mut manager = BufferManager::new(2, EvictionStrategy::LRU);

        // Add 3 pages
        manager.add_page(Page::new(1)).unwrap();
        manager.add_page(Page::new(2)).unwrap();
        manager.add_page(Page::new(3)).unwrap();

        // First page should be evicted
        assert_eq!(manager.page_count(), 2);
        assert!(!manager.contains_page(1));
        assert!(manager.contains_page(2));
        assert!(manager.contains_page(3));
    }

    #[test]
    fn test_page_pinning() {
        let mut manager = BufferManager::new(2, EvictionStrategy::LRU);

        manager.add_page(Page::new(1)).unwrap();
        manager.pin_page(1).unwrap();

        // Add 2 more pages
        manager.add_page(Page::new(2)).unwrap();
        manager.add_page(Page::new(3)).unwrap();

        // Pinned page should not be evicted
        assert!(manager.contains_page(1));
        // Buffer should have maximum 2 pages (buffer size)
        assert_eq!(manager.page_count(), 2);
        // Page 3 should be in buffer (last added)
        assert!(manager.contains_page(3));
    }

    #[test]
    fn test_buffer_stats() {
        let mut manager = BufferManager::new(10, EvictionStrategy::LRU);
        let page = Page::new(1);

        manager.add_page(page).unwrap();
        manager.get_page(1);
        manager.get_page(1);
        manager.get_page(999); // Miss

        let stats = manager.get_stats();
        assert_eq!(stats.total_accesses, 3);
        assert_eq!(stats.cache_hits, 2);
        assert_eq!(stats.cache_misses, 1);
        assert_eq!(stats.hit_ratio(), 2.0 / 3.0);
    }

    #[test]
    fn test_eviction_strategies() {
        let mut manager = BufferManager::new(2, EvictionStrategy::LRU);

        // Test strategy change
        manager.set_eviction_strategy(EvictionStrategy::Clock);
        assert_eq!(manager.get_eviction_strategy(), EvictionStrategy::Clock);

        manager.set_eviction_strategy(EvictionStrategy::Adaptive);
        assert_eq!(manager.get_eviction_strategy(), EvictionStrategy::Adaptive);
    }

    #[test]
    fn test_dirty_flush_hook_on_eviction() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let flushed = Arc::new(AtomicUsize::new(0));
        let f = flushed.clone();
        let hook: PageFlushCallback = Arc::new(move |_pid, _bytes| {
            f.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });

        let mut manager = BufferManager::new(1, EvictionStrategy::LRU);
        manager.set_dirty_flush_hook(Some(hook));

        let mut p1 = Page::new(1);
        p1.header.mark_dirty();
        manager.add_page(p1).unwrap();
        let mut p2 = Page::new(2);
        p2.header.mark_dirty();
        manager.add_page(p2).unwrap();

        assert_eq!(flushed.load(Ordering::SeqCst), 1);
    }
}
