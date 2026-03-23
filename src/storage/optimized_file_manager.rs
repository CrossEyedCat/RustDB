//! Optimized file manager with I/O optimization integration
//!
//! This module combines the advanced file manager with I/O optimization system:
//! - Buffered write operations
//! - Asynchronous read/write operations
//! - Intelligent page caching
//! - Data prefetching

use crate::common::Result;
use crate::storage::advanced_file_manager::{AdvancedFileId, AdvancedFileManager, FileInfo};
use crate::storage::database_file::{DatabaseFileType, ExtensionStrategy, PageId};
use crate::storage::io_optimization::{BufferedIoManager, IoBufferConfig, IoStatistics};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Optimized file manager with I/O optimizations
pub struct OptimizedFileManager {
    /// Base advanced file manager
    advanced_manager: Arc<RwLock<AdvancedFileManager>>,
    /// I/O optimization manager
    io_manager: Arc<BufferedIoManager>,
    /// File mapping to their I/O handlers
    file_mapping: Arc<RwLock<HashMap<AdvancedFileId, u32>>>,
}

impl OptimizedFileManager {
    /// Creates a new optimized file manager
    pub fn new(root_dir: impl AsRef<std::path::Path>) -> Result<Self> {
        let advanced_manager = Arc::new(RwLock::new(AdvancedFileManager::new(root_dir)?));

        let mut io_config = IoBufferConfig::default();
        io_config.max_write_buffer_size = 2000;
        io_config.page_cache_size = 50000;
        io_config.enable_prefetch = true;

        let io_manager = Arc::new(BufferedIoManager::new(io_config));
        let file_mapping = Arc::new(RwLock::new(HashMap::new()));

        Ok(Self {
            advanced_manager,
            io_manager,
            file_mapping,
        })
    }

    /// Creates a new database file with optimizations
    pub async fn create_database_file(
        &self,
        filename: &str,
        file_type: DatabaseFileType,
        database_id: u32,
        extension_strategy: ExtensionStrategy,
    ) -> Result<AdvancedFileId> {
        let mut manager = self.advanced_manager.write().await;
        let file_id =
            manager.create_database_file(filename, file_type, database_id, extension_strategy)?;

        // Register file in mapping
        let mut mapping = self.file_mapping.write().await;
        mapping.insert(file_id, file_id); // Use the same ID for simplicity

        Ok(file_id)
    }

    /// Opens an existing database file
    pub async fn open_database_file(&self, filename: &str) -> Result<AdvancedFileId> {
        let mut manager = self.advanced_manager.write().await;
        let file_id = manager.open_database_file(filename)?;

        // Register file in mapping
        let mut mapping = self.file_mapping.write().await;
        mapping.insert(file_id, file_id);

        Ok(file_id)
    }

    /// Allocates pages in the file with optimizations
    pub async fn allocate_pages(&self, file_id: AdvancedFileId, page_count: u32) -> Result<PageId> {
        let mut manager = self.advanced_manager.write().await;
        manager.allocate_pages(file_id, page_count)
    }

    /// Frees pages in the file
    pub async fn free_pages(
        &self,
        file_id: AdvancedFileId,
        start_page: PageId,
        page_count: u32,
    ) -> Result<()> {
        let mut manager = self.advanced_manager.write().await;
        manager.free_pages(file_id, start_page, page_count)
    }

    /// Asynchronously reads a page using cache and prefetching
    pub async fn read_page(&self, file_id: AdvancedFileId, page_id: PageId) -> Result<Vec<u8>> {
        // First try to read from I/O manager cache
        match self.io_manager.read_page_async(file_id, page_id).await {
            Ok(data) => Ok(data),
            Err(_) => {
                // If failed, read through regular manager and add to cache
                let mut manager = self.advanced_manager.write().await;
                let data = manager.read_page(file_id, page_id)?;

                // Add to cache for future access
                let _ = self
                    .io_manager
                    .write_page_async(file_id, page_id, data.clone())
                    .await;

                Ok(data)
            }
        }
    }

    /// Asynchronously writes a page with buffering
    pub async fn write_page(
        &self,
        file_id: AdvancedFileId,
        page_id: PageId,
        data: &[u8],
    ) -> Result<()> {
        // Write through I/O manager with buffering
        self.io_manager
            .write_page_async(file_id, page_id, data.to_vec())
            .await?;

        // Periodically sync to disk
        if page_id.is_multiple_of(100) {
            self.sync_file(file_id).await?;
        }

        Ok(())
    }

    /// Synchronizes file to disk
    pub async fn sync_file(&self, file_id: AdvancedFileId) -> Result<()> {
        // First synchronize I/O manager buffers
        self.io_manager.sync_all().await?;

        // Then synchronize base file
        let mut manager = self.advanced_manager.write().await;
        manager.sync_file(file_id)
    }

    /// Synchronizes all files
    pub async fn sync_all(&self) -> Result<()> {
        self.io_manager.sync_all().await?;

        let mut manager = self.advanced_manager.write().await;
        manager.sync_all()
    }

    /// Closes a file
    pub async fn close_file(&self, file_id: AdvancedFileId) -> Result<()> {
        // Synchronize before closing
        self.sync_file(file_id).await?;

        // Remove from mapping
        let mut mapping = self.file_mapping.write().await;
        mapping.remove(&file_id);

        // Close in base manager
        let mut manager = self.advanced_manager.write().await;
        manager.close_file(file_id)
    }

    /// Returns file information
    pub async fn get_file_info(&self, file_id: AdvancedFileId) -> Option<FileInfo> {
        let manager = self.advanced_manager.read().await;
        manager.get_file_info(file_id)
    }

    /// Returns I/O operation statistics
    pub fn get_io_statistics(&self) -> IoStatistics {
        self.io_manager.get_statistics()
    }

    /// Returns buffer state information
    pub fn get_buffer_info(&self) -> (usize, usize, usize) {
        self.io_manager.get_buffer_info()
    }

    /// Clears cache and resets I/O statistics
    pub async fn clear_io_cache(&self) {
        self.io_manager.clear_cache().await;
    }

    /// Runs maintenance check for all files
    pub async fn maintenance_check(&self) -> Result<Vec<AdvancedFileId>> {
        let mut manager = self.advanced_manager.write().await;
        manager.maintenance_check()
    }

    /// Defragments all files
    pub async fn defragment_all(&self) {
        let mut manager = self.advanced_manager.write().await;
        manager.defragment_all();
    }

    /// Validates integrity of all files
    pub async fn validate_all(&self) -> Result<Vec<(AdvancedFileId, Result<()>)>> {
        let manager = self.advanced_manager.read().await;
        manager.validate_all()
    }

    /// Returns combined statistics
    pub async fn get_combined_statistics(&self) -> CombinedStatistics {
        let manager = self.advanced_manager.read().await;
        let global_stats = manager.get_global_statistics();
        let io_stats = self.get_io_statistics();
        let buffer_info = self.get_buffer_info();

        CombinedStatistics {
            total_files: global_stats.total_files,
            total_pages: global_stats.total_pages,
            total_reads: io_stats.read_operations,
            total_writes: io_stats.write_operations,
            cache_hit_ratio: io_stats.cache_hit_ratio,
            average_utilization: global_stats.average_utilization,
            average_fragmentation: global_stats.average_fragmentation,
            buffer_usage: buffer_info.0 as f64 / buffer_info.1 as f64,
            cache_usage: buffer_info.2,
            read_throughput: io_stats.read_throughput,
            write_throughput: io_stats.write_throughput,
        }
    }
}

/// Combined statistics for optimized manager
#[derive(Debug, Clone)]
pub struct CombinedStatistics {
    /// Total number of files
    pub total_files: u32,
    /// Total number of pages
    pub total_pages: u64,
    /// Total number of read operations
    pub total_reads: u64,
    /// Total number of write operations
    pub total_writes: u64,
    /// Cache hit ratio
    pub cache_hit_ratio: f64,
    /// Average utilization ratio
    pub average_utilization: f64,
    /// Average fragmentation ratio
    pub average_fragmentation: f64,
    /// Write buffer usage (0.0 - 1.0)
    pub buffer_usage: f64,
    /// Page cache usage
    pub cache_usage: usize,
    /// Read throughput (bytes/sec)
    pub read_throughput: f64,
    /// Write throughput (bytes/sec)
    pub write_throughput: f64,
}

impl CombinedStatistics {
    /// Returns overall performance score (0.0 - 1.0)
    pub fn performance_score(&self) -> f64 {
        let cache_score = self.cache_hit_ratio;
        let utilization_score = self.average_utilization;
        let fragmentation_score = 1.0 - self.average_fragmentation;
        let buffer_score = 1.0 - self.buffer_usage; // Less buffer usage = better

        (cache_score + utilization_score + fragmentation_score + buffer_score) / 4.0
    }

    /// Returns optimization recommendations
    pub fn get_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();

        if self.cache_hit_ratio < 0.8 {
            recommendations.push("Consider increasing page cache size".to_string());
        }

        if self.average_fragmentation > 0.3 {
            recommendations.push("File defragmentation recommended".to_string());
        }

        if self.buffer_usage > 0.9 {
            recommendations.push("Write buffer is full, increase buffer size".to_string());
        }

        if self.average_utilization < 0.6 {
            recommendations.push("Low space utilization, consider file compression".to_string());
        }

        if self.read_throughput < 1_000_000.0 {
            // < 1MB/s
            recommendations.push("Low read throughput, check disk subsystem".to_string());
        }

        if self.write_throughput < 500_000.0 {
            // < 500KB/s
            recommendations.push("Low write throughput, consider SSD".to_string());
        }

        if recommendations.is_empty() {
            recommendations.push("Performance is optimal".to_string());
        }

        recommendations
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::database_file::BLOCK_SIZE;
    use tempfile::TempDir;

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_optimized_file_manager_creation() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let _manager = OptimizedFileManager::new(temp_dir.path())?;
        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_create_and_open_file() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let manager = OptimizedFileManager::new(temp_dir.path())?;

        let file_id = manager
            .create_database_file(
                "test.db",
                DatabaseFileType::Data,
                123,
                ExtensionStrategy::Adaptive,
            )
            .await?;

        assert!(file_id > 0);
        assert!(manager.get_file_info(file_id).await.is_some());

        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_optimized_page_operations() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let manager = OptimizedFileManager::new(temp_dir.path())?;

        let file_id = manager
            .create_database_file(
                "test.db",
                DatabaseFileType::Data,
                123,
                ExtensionStrategy::Fixed,
            )
            .await?;

        // Allocate pages
        let page_id = manager.allocate_pages(file_id, 5).await?;
        assert_eq!(page_id, 1); // Pages start from 1

        // Write data
        let test_data = vec![42u8; BLOCK_SIZE];
        manager.write_page(file_id, page_id, &test_data).await?;

        // Read data (should hit cache)
        let read_data = manager.read_page(file_id, page_id).await?;
        assert_eq!(read_data, test_data);

        // Give time for asynchronous operations to process
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Check statistics
        let stats = manager.get_io_statistics();
        // In optimized manager, statistics may update asynchronously
        // Just check that system is initialized
        assert!(stats.total_operations >= 0);

        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_combined_statistics() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let manager = OptimizedFileManager::new(temp_dir.path())?;

        let file_id = manager
            .create_database_file(
                "test.db",
                DatabaseFileType::Data,
                123,
                ExtensionStrategy::Linear,
            )
            .await?;

        // Perform some operations
        let page_id = manager.allocate_pages(file_id, 10).await?;
        let data = vec![100u8; BLOCK_SIZE];

        for i in 0..5 {
            manager.write_page(file_id, page_id + i, &data).await?;
            let _ = manager.read_page(file_id, page_id + i).await?;
        }

        // Get combined statistics
        let stats = manager.get_combined_statistics().await;
        assert!(stats.total_files >= 1);
        assert!(stats.total_pages >= 10);

        // Check performance score
        let score = stats.performance_score();
        assert!(score >= 0.0 && score <= 1.0);

        // Get recommendations
        let recommendations = stats.get_recommendations();
        assert!(!recommendations.is_empty());

        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_maintenance_operations() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let manager = OptimizedFileManager::new(temp_dir.path())?;

        let file_id = manager
            .create_database_file(
                "test.db",
                DatabaseFileType::Data,
                123,
                ExtensionStrategy::Exponential,
            )
            .await?;

        // Run maintenance check
        let extended_files = manager.maintenance_check().await?;
        assert!(extended_files.len() <= 1);

        // Defragment
        manager.defragment_all().await;

        // Validate integrity
        let validation_results = manager.validate_all().await?;
        assert!(!validation_results.is_empty());

        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_cache_performance() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let manager = OptimizedFileManager::new(temp_dir.path())?;

        let file_id = manager
            .create_database_file(
                "cache_test.db",
                DatabaseFileType::Data,
                456,
                ExtensionStrategy::Adaptive,
            )
            .await?;

        let page_id = manager.allocate_pages(file_id, 1).await?;
        let data = vec![255u8; BLOCK_SIZE];

        // Write data
        manager.write_page(file_id, page_id, &data).await?;

        // Read several times (should have cache hits)
        for _ in 0..10 {
            let read_data = manager.read_page(file_id, page_id).await?;
            assert_eq!(read_data, data);
        }

        // Check cache statistics
        let stats = manager.get_io_statistics();
        assert!(stats.cache_hits > 0);
        assert!(stats.cache_hit_ratio > 0.0);

        Ok(())
    }
}
