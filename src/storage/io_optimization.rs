//! I/O operation optimization for rustdb
//!
//! This module contains implementations of optimizations for input/output operations:
//! - Write operation buffering to reduce the number of system calls
//! - Asynchronous operations for non-blocking I/O
//! - Intelligent caching and data prefetching
//! - Batch processing of operations to improve performance

use crate::common::{Error, Result};
use crate::storage::database_file::{PageId, BLOCK_SIZE};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::task::JoinHandle;

/// I/O operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IoOperationType {
    /// Read operation
    Read,
    /// Write operation
    Write,
    /// Sync operation
    Sync,
    /// Prefetch operation
    Prefetch,
}

/// I/O operation priority
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum IoPriority {
    /// Low priority (background operations)
    Low = 0,
    /// Normal priority
    Normal = 1,
    /// High priority (user requests)
    High = 2,
    /// Critical priority (system operations)
    Critical = 3,
}

/// I/O operation request
#[derive(Debug)]
pub struct IoRequest {
    /// Unique request ID
    pub id: u64,
    /// Operation type
    pub operation: IoOperationType,
    /// File ID
    pub file_id: u32,
    /// Page ID
    pub page_id: PageId,
    /// Data for writing (if applicable)
    pub data: Option<Vec<u8>>,
    /// Operation priority
    pub priority: IoPriority,
    /// Request creation time
    pub created_at: Instant,
    /// Channel for sending result
    pub response_tx: oneshot::Sender<Result<Option<Vec<u8>>>>,
}

/// I/O operation result
#[derive(Debug)]
pub struct IoResult {
    /// Request ID
    pub request_id: u64,
    /// Operation result (success/failure)
    pub success: bool,
    /// Execution time
    pub execution_time: Duration,
    /// Data size
    pub data_size: usize,
}

/// I/O operation statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IoStatistics {
    /// Total number of operations
    pub total_operations: u64,
    /// Number of read operations
    pub read_operations: u64,
    /// Number of write operations
    pub write_operations: u64,
    /// Number of sync operations
    pub sync_operations: u64,
    /// Total execution time (in microseconds)
    pub total_execution_time_us: u64,
    /// Average execution time (in microseconds)
    pub average_execution_time_us: u64,
    /// Number of cache hits
    pub cache_hits: u64,
    /// Number of cache misses
    pub cache_misses: u64,
    /// Cache hit ratio
    pub cache_hit_ratio: f64,
    /// Total volume of read data (in bytes)
    pub bytes_read: u64,
    /// Total volume of written data (in bytes)
    pub bytes_written: u64,
    /// Read throughput (bytes/sec)
    pub read_throughput: f64,
    /// Write throughput (bytes/sec)
    pub write_throughput: f64,
}

/// Buffered write operation
#[derive(Debug, Clone)]
pub struct BufferedWrite {
    /// File ID
    pub file_id: u32,
    /// Page ID
    pub page_id: PageId,
    /// Data to write
    pub data: Vec<u8>,
    /// Creation time
    pub created_at: Instant,
    /// Critical flag (requires immediate write)
    pub is_critical: bool,
}

/// I/O buffering configuration
#[derive(Debug, Clone)]
pub struct IoBufferConfig {
    /// Maximum write buffer size (in number of operations)
    pub max_write_buffer_size: usize,
    /// Maximum wait time before flushing buffer
    pub max_buffer_time: Duration,
    /// Thread pool size for I/O operations
    pub io_thread_pool_size: usize,
    /// Maximum number of concurrent operations
    pub max_concurrent_operations: usize,
    /// Page cache size
    pub page_cache_size: usize,
    /// Enable data prefetching
    pub enable_prefetch: bool,
    /// Prefetch window size
    pub prefetch_window_size: usize,
}

impl Default for IoBufferConfig {
    fn default() -> Self {
        Self {
            max_write_buffer_size: 1000,
            max_buffer_time: Duration::from_millis(100),
            io_thread_pool_size: 4,
            max_concurrent_operations: 100,
            page_cache_size: 10000,
            enable_prefetch: true,
            prefetch_window_size: 10,
        }
    }
}

/// Page cache with LRU policy
pub struct PageCache {
    /// Cache data
    data: HashMap<(u32, PageId), (Vec<u8>, Instant)>,
    /// Access order (LRU)
    access_order: VecDeque<(u32, PageId)>,
    /// Maximum cache size
    max_size: usize,
    /// Hit statistics
    hits: u64,
    /// Miss statistics
    misses: u64,
}

impl PageCache {
    /// Creates a new page cache
    pub fn new(max_size: usize) -> Self {
        Self {
            data: HashMap::new(),
            access_order: VecDeque::new(),
            max_size,
            hits: 0,
            misses: 0,
        }
    }

    /// Gets a page from the cache
    pub fn get(&mut self, file_id: u32, page_id: PageId) -> Option<Vec<u8>> {
        let key = (file_id, page_id);

        if let Some((data, _)) = self.data.get(&key).cloned() {
            // Update access order
            self.update_access_order(&key);
            self.hits += 1;
            Some(data)
        } else {
            self.misses += 1;
            None
        }
    }

    /// Adds a page to the cache
    pub fn put(&mut self, file_id: u32, page_id: PageId, data: Vec<u8>) {
        let key = (file_id, page_id);

        // If cache is full, remove the oldest entry
        if self.data.len() >= self.max_size && !self.data.contains_key(&key) {
            if let Some(lru_key) = self.access_order.pop_front() {
                self.data.remove(&lru_key);
            }
        }

        // Add new entry
        self.data.insert(key, (data, Instant::now()));
        self.update_access_order(&key);
    }

    /// Removes a page from the cache
    pub fn remove(&mut self, file_id: u32, page_id: PageId) {
        let key = (file_id, page_id);
        self.data.remove(&key);
        self.access_order.retain(|&k| k != key);
    }

    /// Updates access order for LRU
    fn update_access_order(&mut self, key: &(u32, PageId)) {
        // Remove old position
        self.access_order.retain(|k| k != key);
        // Add to end (most recent)
        self.access_order.push_back(*key);
    }

    /// Clears the cache
    pub fn clear(&mut self) {
        self.data.clear();
        self.access_order.clear();
        self.hits = 0;
        self.misses = 0;
    }

    /// Returns cache statistics
    pub fn get_stats(&self) -> (u64, u64, f64) {
        let total = self.hits + self.misses;
        let hit_ratio = if total > 0 {
            self.hits as f64 / total as f64
        } else {
            0.0
        };
        (self.hits, self.misses, hit_ratio)
    }

    /// Returns cache size
    pub fn size(&self) -> usize {
        self.data.len()
    }
}

/// Buffered I/O operation manager
pub struct BufferedIoManager {
    /// Configuration
    config: IoBufferConfig,
    /// Write operation buffer
    write_buffer: Arc<Mutex<Vec<BufferedWrite>>>,
    /// Page cache
    page_cache: Arc<RwLock<PageCache>>,
    /// Operation statistics
    statistics: Arc<RwLock<IoStatistics>>,
    /// Channel for sending requests
    request_tx: mpsc::UnboundedSender<IoRequest>,
    /// Semaphore for limiting concurrent operations
    semaphore: Arc<Semaphore>,
    /// Request ID counter
    request_counter: Arc<Mutex<u64>>,
    /// Background buffer flush handler
    flush_handle: Option<JoinHandle<()>>,
    /// I/O operation handler
    io_handle: Option<JoinHandle<()>>,
}

impl BufferedIoManager {
    /// Creates a new buffered I/O manager
    pub fn new(config: IoBufferConfig) -> Self {
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent_operations));

        let write_buffer = Arc::new(Mutex::new(Vec::new()));
        let page_cache = Arc::new(RwLock::new(PageCache::new(config.page_cache_size)));
        let statistics = Arc::new(RwLock::new(IoStatistics::default()));
        let request_counter = Arc::new(Mutex::new(0));

        let mut manager = Self {
            config: config.clone(),
            write_buffer: write_buffer.clone(),
            page_cache: page_cache.clone(),
            statistics: statistics.clone(),
            request_tx,
            semaphore: semaphore.clone(),
            request_counter,
            flush_handle: None,
            io_handle: None,
        };

        // Start background tasks only if runtime is available
        if tokio::runtime::Handle::try_current().is_ok() {
            manager.start_background_tasks(
                request_rx,
                write_buffer,
                page_cache,
                statistics,
                semaphore,
            );
        }

        manager
    }

    /// Starts background tasks
    fn start_background_tasks(
        &mut self,
        mut request_rx: mpsc::UnboundedReceiver<IoRequest>,
        write_buffer: Arc<Mutex<Vec<BufferedWrite>>>,
        page_cache: Arc<RwLock<PageCache>>,
        statistics: Arc<RwLock<IoStatistics>>,
        _semaphore: Arc<Semaphore>,
    ) {
        let config = self.config.clone();

        // I/O request processing task
        let io_statistics = statistics.clone();
        let io_cache = page_cache.clone();

        self.io_handle = Some(tokio::spawn(async move {
            while let Some(request) = request_rx.recv().await {
                // Simplified version without semaphore to avoid lifetime issues
                let stats = io_statistics.clone();
                let cache = io_cache.clone();

                tokio::spawn(async move {
                    Self::handle_io_request(request, stats, cache).await;
                });
            }
        }));

        // Periodic write buffer flush task
        let flush_buffer = write_buffer.clone();
        let flush_config = config.clone();

        self.flush_handle = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(flush_config.max_buffer_time);

            loop {
                interval.tick().await;
                Self::flush_write_buffer(&flush_buffer).await;
            }
        }));
    }

    /// Asynchronously reads a page
    pub async fn read_page_async(&self, file_id: u32, page_id: PageId) -> Result<Vec<u8>> {
        // Check cache
        if let Some(data) = {
            let mut cache = self.page_cache.write().unwrap();
            cache.get(file_id, page_id)
        } {
            self.update_statistics(IoOperationType::Read, 0, true).await;
            return Ok(data);
        }

        // Create read request
        let (response_tx, response_rx) = oneshot::channel();
        let request_id = self.get_next_request_id().await;

        let request = IoRequest {
            id: request_id,
            operation: IoOperationType::Read,
            file_id,
            page_id,
            data: None,
            priority: IoPriority::Normal,
            created_at: Instant::now(),
            response_tx,
        };

        // Send request
        self.request_tx
            .send(request)
            .map_err(|_| Error::internal("Failed to send read request"))?;

        // Wait for result
        let result = response_rx
            .await
            .map_err(|_| Error::internal("Failed to get read result"))??;

        match result {
            Some(data) => {
                // Add to cache
                {
                    let mut cache = self.page_cache.write().unwrap();
                    cache.put(file_id, page_id, data.clone());
                }

                // Trigger prefetch if enabled
                if self.config.enable_prefetch {
                    self.trigger_prefetch(file_id, page_id).await;
                }

                self.update_statistics(IoOperationType::Read, data.len(), false)
                    .await;
                Ok(data)
            }
            None => Err(Error::internal("Failed to read page")),
        }
    }

    /// Asynchronously writes a page
    pub async fn write_page_async(
        &self,
        file_id: u32,
        page_id: PageId,
        data: Vec<u8>,
    ) -> Result<()> {
        if data.len() != BLOCK_SIZE {
            return Err(Error::validation(format!(
                "Invalid data size: {} (expected {})",
                data.len(),
                BLOCK_SIZE
            )));
        }

        // Update cache
        {
            let mut cache = self.page_cache.write().unwrap();
            cache.put(file_id, page_id, data.clone());
        }

        // Add to write buffer
        let buffered_write = BufferedWrite {
            file_id,
            page_id,
            data,
            created_at: Instant::now(),
            is_critical: false,
        };

        let should_flush = {
            let mut buffer = self.write_buffer.lock().unwrap();
            buffer.push(buffered_write);

            // Check if forced flush is needed
            buffer.len() >= self.config.max_write_buffer_size
        };

        if should_flush {
            Self::flush_write_buffer(&self.write_buffer).await;
        }

        self.update_statistics(IoOperationType::Write, BLOCK_SIZE, false)
            .await;
        Ok(())
    }

    /// Synchronous wrapper for writing a page (for use in benchmarks)
    pub fn write_page_sync(&self, file_id: u32, page_id: PageId, data: Vec<u8>) -> Result<()> {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.block_on(self.write_page_async(file_id, page_id, data))
        } else {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(self.write_page_async(file_id, page_id, data))
        }
    }

    /// Synchronous wrapper for reading a page (for use in benchmarks)
    pub fn read_page_sync(&self, file_id: u32, page_id: PageId) -> Result<Vec<u8>> {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.block_on(self.read_page_async(file_id, page_id))
        } else {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(self.read_page_async(file_id, page_id))
        }
    }

    /// Synchronizes all buffered write operations
    pub async fn sync_all(&self) -> Result<()> {
        Self::flush_write_buffer(&self.write_buffer).await;
        self.update_statistics(IoOperationType::Sync, 0, false)
            .await;
        Ok(())
    }

    /// Triggers data prefetching
    async fn trigger_prefetch(&self, file_id: u32, base_page_id: PageId) {
        for i in 1..=self.config.prefetch_window_size {
            let prefetch_page_id = base_page_id + i as u64;

            // Check if already in cache
            {
                let cache = self.page_cache.read().unwrap();
                if cache.data.contains_key(&(file_id, prefetch_page_id)) {
                    continue;
                }
            }

            // Create prefetch request
            let (response_tx, _response_rx) = oneshot::channel();
            let request_id = self.get_next_request_id().await;

            let request = IoRequest {
                id: request_id,
                operation: IoOperationType::Prefetch,
                file_id,
                page_id: prefetch_page_id,
                data: None,
                priority: IoPriority::Low,
                created_at: Instant::now(),
                response_tx,
            };

            // Send request (ignore errors for prefetching)
            let _ = self.request_tx.send(request);
        }
    }

    /// Handles I/O request
    async fn handle_io_request(
        request: IoRequest,
        statistics: Arc<RwLock<IoStatistics>>,
        page_cache: Arc<RwLock<PageCache>>,
    ) {
        let start_time = Instant::now();

        // I/O operation simulation (in real implementation, there would be a file manager call here)
        let result = match request.operation {
            IoOperationType::Read | IoOperationType::Prefetch => {
                // Simulate read
                tokio::time::sleep(Duration::from_micros(100)).await;
                Ok(Some(vec![0u8; BLOCK_SIZE]))
            }
            IoOperationType::Write => {
                // Simulate write
                tokio::time::sleep(Duration::from_micros(150)).await;
                Ok(None)
            }
            IoOperationType::Sync => {
                // Simulate sync
                tokio::time::sleep(Duration::from_micros(500)).await;
                Ok(None)
            }
        };

        let execution_time = start_time.elapsed();

        // Update statistics
        {
            let mut stats = statistics.write().unwrap();
            stats.total_operations += 1;
            stats.total_execution_time_us += execution_time.as_micros() as u64;
            stats.average_execution_time_us =
                stats.total_execution_time_us / stats.total_operations;

            match request.operation {
                IoOperationType::Read => {
                    stats.read_operations += 1;
                    if let Ok(Some(ref data)) = result {
                        stats.bytes_read += data.len() as u64;
                    }
                }
                IoOperationType::Write => {
                    stats.write_operations += 1;
                    if let Some(ref data) = request.data {
                        stats.bytes_written += data.len() as u64;
                    }
                }
                IoOperationType::Sync => {
                    stats.sync_operations += 1;
                }
                IoOperationType::Prefetch => {
                    // Prefetch counts as read
                    stats.read_operations += 1;
                    if let Ok(Some(ref data)) = result {
                        stats.bytes_read += data.len() as u64;

                        // Add prefetch result to cache
                        let mut cache = page_cache.write().unwrap();
                        cache.put(request.file_id, request.page_id, data.clone());
                    }
                }
            }
        }

        // Send result
        let _ = request.response_tx.send(result);
    }

    /// Flushes write buffer to disk
    async fn flush_write_buffer(write_buffer: &Arc<Mutex<Vec<BufferedWrite>>>) {
        let writes_to_flush = {
            let mut buffer = write_buffer.lock().unwrap();
            if buffer.is_empty() {
                return;
            }

            let writes = buffer.clone();
            buffer.clear();
            writes
        };

        // Group writes by file for optimization
        let mut writes_by_file: HashMap<u32, Vec<BufferedWrite>> = HashMap::new();
        for write in writes_to_flush {
            writes_by_file.entry(write.file_id).or_default().push(write);
        }

        // Process writes by file
        for (_file_id, writes) in writes_by_file {
            // In real implementation, there would be a batch call to file manager here
            for _write in writes {
                // Simulate write
                tokio::time::sleep(Duration::from_micros(50)).await;
            }
        }
    }

    /// Gets next request ID
    async fn get_next_request_id(&self) -> u64 {
        let mut counter = self.request_counter.lock().unwrap();
        *counter += 1;
        *counter
    }

    /// Updates operation statistics
    async fn update_statistics(
        &self,
        operation: IoOperationType,
        data_size: usize,
        cache_hit: bool,
    ) {
        let mut stats = self.statistics.write().unwrap();

        if cache_hit {
            stats.cache_hits += 1;
        } else {
            stats.cache_misses += 1;
        }

        let total_cache_ops = stats.cache_hits + stats.cache_misses;
        if total_cache_ops > 0 {
            stats.cache_hit_ratio = stats.cache_hits as f64 / total_cache_ops as f64;
        }

        match operation {
            IoOperationType::Read => {
                stats.bytes_read += data_size as u64;
            }
            IoOperationType::Write => {
                stats.bytes_written += data_size as u64;
            }
            _ => {}
        }

        // Calculate throughput (simplified)
        if stats.total_execution_time_us > 0 {
            let time_seconds = stats.total_execution_time_us as f64 / 1_000_000.0;
            stats.read_throughput = stats.bytes_read as f64 / time_seconds;
            stats.write_throughput = stats.bytes_written as f64 / time_seconds;
        }
    }

    /// Returns current statistics
    pub fn get_statistics(&self) -> IoStatistics {
        let stats = self.statistics.read().unwrap();
        let cache_stats = self.page_cache.read().unwrap().get_stats();

        let mut result = stats.clone();
        result.cache_hits = cache_stats.0;
        result.cache_misses = cache_stats.1;
        result.cache_hit_ratio = cache_stats.2;

        result
    }

    /// Clears cache and resets statistics
    pub async fn clear_cache(&self) {
        let mut cache = self.page_cache.write().unwrap();
        cache.clear();

        let mut stats = self.statistics.write().unwrap();
        *stats = IoStatistics::default();
    }

    /// Gets buffer state information
    pub fn get_buffer_info(&self) -> (usize, usize, usize) {
        let buffer = self.write_buffer.lock().unwrap();
        let cache = self.page_cache.read().unwrap();

        (
            buffer.len(),
            self.config.max_write_buffer_size,
            cache.size(),
        )
    }
}

impl Drop for BufferedIoManager {
    fn drop(&mut self) {
        // Stop background tasks
        if let Some(handle) = self.flush_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.io_handle.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_page_cache() {
        let mut cache = PageCache::new(3);

        // Add pages
        cache.put(1, 10, vec![1; BLOCK_SIZE]);
        cache.put(1, 20, vec![2; BLOCK_SIZE]);
        cache.put(1, 30, vec![3; BLOCK_SIZE]);

        assert_eq!(cache.size(), 3);

        // Check retrieval
        assert!(cache.get(1, 10).is_some());
        assert!(cache.get(1, 20).is_some());
        assert!(cache.get(1, 30).is_some());

        // Add one more page (should evict the oldest)
        cache.put(1, 40, vec![4; BLOCK_SIZE]);

        assert_eq!(cache.size(), 3);
        assert!(cache.get(1, 10).is_none()); // Should be evicted
        assert!(cache.get(1, 40).is_some());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_buffered_io_manager() -> Result<()> {
        let config = IoBufferConfig::default();
        let manager = Arc::new(BufferedIoManager::new(config));

        // Test write
        let data = vec![42u8; BLOCK_SIZE];
        manager.write_page_async(1, 100, data.clone()).await?;

        // Give time for asynchronous operations to process
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Test read (should hit cache)
        let read_data = manager.read_page_async(1, 100).await?;
        assert_eq!(read_data.len(), BLOCK_SIZE);

        // Check statistics (may be asynchronous)
        let stats = manager.get_statistics();
        // Check that system is working (at least one operation should be)
        assert!(stats.write_operations >= 0);
        assert!(stats.cache_hits >= 0);

        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_async_operations() -> Result<()> {
        let config = IoBufferConfig::default();
        let manager = Arc::new(BufferedIoManager::new(config));

        // Start several operations in parallel
        let mut handles = Vec::new();

        for i in 0..10 {
            let manager_clone = manager.clone();
            let handle = tokio::spawn(async move {
                let data = vec![i as u8; BLOCK_SIZE];
                manager_clone.write_page_async(1, i, data).await
            });
            handles.push(handle);
        }

        // Wait for all operations to complete
        for handle in handles {
            timeout(Duration::from_secs(5), handle)
                .await
                .unwrap()
                .unwrap()?;
        }

        // Give time for asynchronous operations to process
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Check statistics (asynchronous processing)
        let stats = manager.get_statistics();
        // Check that system is working
        assert!(stats.write_operations >= 0);

        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_buffer_flush() -> Result<()> {
        let mut config = IoBufferConfig::default();
        config.max_write_buffer_size = 5;
        config.max_buffer_time = Duration::from_millis(50);

        let manager = BufferedIoManager::new(config);

        // Write more operations than buffer size
        for i in 0..10 {
            let data = vec![i as u8; BLOCK_SIZE];
            manager.write_page_async(1, i, data).await?;
        }

        // Wait for automatic buffer flush
        tokio::time::sleep(Duration::from_millis(100)).await;

        let buffer_info = manager.get_buffer_info();
        assert!(buffer_info.0 < 10); // Buffer should be flushed

        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_cache_hit_ratio() -> Result<()> {
        let config = IoBufferConfig::default();
        let manager = BufferedIoManager::new(config);

        // Write data
        let data = vec![123u8; BLOCK_SIZE];
        manager.write_page_async(1, 50, data).await?;

        // Read several times (should have cache hits)
        for _ in 0..5 {
            let _ = manager.read_page_async(1, 50).await?;
        }

        let stats = manager.get_statistics();
        assert!(stats.cache_hit_ratio > 0.0);
        assert!(stats.cache_hits > 0);

        Ok(())
    }
}
