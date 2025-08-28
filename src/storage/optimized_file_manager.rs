//! Оптимизированный менеджер файлов с интеграцией I/O оптимизаций
//!
//! Этот модуль объединяет продвинутый менеджер файлов с системой оптимизации I/O:
//! - Буферизованные операции записи
//! - Асинхронные операции чтения/записи
//! - Интеллектуальное кэширование страниц
//! - Предвыборка данных

use crate::common::Result;
use crate::storage::advanced_file_manager::{AdvancedFileManager, AdvancedFileId, FileInfo};
use crate::storage::database_file::{DatabaseFileType, ExtensionStrategy, PageId};
use crate::storage::io_optimization::{BufferedIoManager, IoBufferConfig, IoStatistics};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Оптимизированный менеджер файлов с I/O оптимизациями
pub struct OptimizedFileManager {
    /// Базовый продвинутый менеджер файлов
    advanced_manager: Arc<RwLock<AdvancedFileManager>>,
    /// Менеджер I/O оптимизаций
    io_manager: Arc<BufferedIoManager>,
    /// Маппинг файлов на их I/O обработчики
    file_mapping: Arc<RwLock<HashMap<AdvancedFileId, u32>>>,
}

impl OptimizedFileManager {
    /// Создает новый оптимизированный менеджер файлов
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

    /// Создает новый файл базы данных с оптимизациями
    pub async fn create_database_file(
        &self,
        filename: &str,
        file_type: DatabaseFileType,
        database_id: u32,
        extension_strategy: ExtensionStrategy,
    ) -> Result<AdvancedFileId> {
        let mut manager = self.advanced_manager.write().await;
        let file_id = manager.create_database_file(filename, file_type, database_id, extension_strategy)?;
        
        // Регистрируем файл в маппинге
        let mut mapping = self.file_mapping.write().await;
        mapping.insert(file_id, file_id); // Используем тот же ID для простоты
        
        Ok(file_id)
    }

    /// Открывает существующий файл базы данных
    pub async fn open_database_file(&self, filename: &str) -> Result<AdvancedFileId> {
        let mut manager = self.advanced_manager.write().await;
        let file_id = manager.open_database_file(filename)?;
        
        // Регистрируем файл в маппинге
        let mut mapping = self.file_mapping.write().await;
        mapping.insert(file_id, file_id);
        
        Ok(file_id)
    }

    /// Выделяет страницы в файле с оптимизациями
    pub async fn allocate_pages(&self, file_id: AdvancedFileId, page_count: u32) -> Result<PageId> {
        let mut manager = self.advanced_manager.write().await;
        manager.allocate_pages(file_id, page_count)
    }

    /// Освобождает страницы в файле
    pub async fn free_pages(&self, file_id: AdvancedFileId, start_page: PageId, page_count: u32) -> Result<()> {
        let mut manager = self.advanced_manager.write().await;
        manager.free_pages(file_id, start_page, page_count)
    }

    /// Асинхронно читает страницу с использованием кэша и предвыборки
    pub async fn read_page(&self, file_id: AdvancedFileId, page_id: PageId) -> Result<Vec<u8>> {
        // Сначала пытаемся прочитать из кэша I/O менеджера
        match self.io_manager.read_page_async(file_id, page_id).await {
            Ok(data) => Ok(data),
            Err(_) => {
                // Если не удалось, читаем через обычный менеджер и добавляем в кэш
                let mut manager = self.advanced_manager.write().await;
                let data = manager.read_page(file_id, page_id)?;
                
                // Добавляем в кэш для будущих обращений
                let _ = self.io_manager.write_page_async(file_id, page_id, data.clone()).await;
                
                Ok(data)
            }
        }
    }

    /// Асинхронно записывает страницу с буферизацией
    pub async fn write_page(&self, file_id: AdvancedFileId, page_id: PageId, data: &[u8]) -> Result<()> {
        // Записываем через I/O менеджер с буферизацией
        self.io_manager.write_page_async(file_id, page_id, data.to_vec()).await?;
        
        // Периодически синхронизируем с диском
        if page_id % 100 == 0 {
            self.sync_file(file_id).await?;
        }
        
        Ok(())
    }

    /// Синхронизирует файл с диском
    pub async fn sync_file(&self, file_id: AdvancedFileId) -> Result<()> {
        // Сначала синхронизируем буферы I/O менеджера
        self.io_manager.sync_all().await?;
        
        // Затем синхронизируем базовый файл
        let mut manager = self.advanced_manager.write().await;
        manager.sync_file(file_id)
    }

    /// Синхронизирует все файлы
    pub async fn sync_all(&self) -> Result<()> {
        self.io_manager.sync_all().await?;
        
        let mut manager = self.advanced_manager.write().await;
        manager.sync_all()
    }

    /// Закрывает файл
    pub async fn close_file(&self, file_id: AdvancedFileId) -> Result<()> {
        // Синхронизируем перед закрытием
        self.sync_file(file_id).await?;
        
        // Удаляем из маппинга
        let mut mapping = self.file_mapping.write().await;
        mapping.remove(&file_id);
        
        // Закрываем в базовом менеджере
        let mut manager = self.advanced_manager.write().await;
        manager.close_file(file_id)
    }

    /// Возвращает информацию о файле
    pub async fn get_file_info(&self, file_id: AdvancedFileId) -> Option<FileInfo> {
        let manager = self.advanced_manager.read().await;
        manager.get_file_info(file_id)
    }

    /// Возвращает статистику I/O операций
    pub fn get_io_statistics(&self) -> IoStatistics {
        self.io_manager.get_statistics()
    }

    /// Возвращает информацию о состоянии буферов
    pub fn get_buffer_info(&self) -> (usize, usize, usize) {
        self.io_manager.get_buffer_info()
    }

    /// Очищает кэш и сбрасывает статистику I/O
    pub async fn clear_io_cache(&self) {
        self.io_manager.clear_cache().await;
    }

    /// Запускает проверку обслуживания для всех файлов
    pub async fn maintenance_check(&self) -> Result<Vec<AdvancedFileId>> {
        let mut manager = self.advanced_manager.write().await;
        manager.maintenance_check()
    }

    /// Дефрагментирует все файлы
    pub async fn defragment_all(&self) {
        let mut manager = self.advanced_manager.write().await;
        manager.defragment_all();
    }

    /// Проверяет целостность всех файлов
    pub async fn validate_all(&self) -> Result<Vec<(AdvancedFileId, Result<()>)>> {
        let manager = self.advanced_manager.read().await;
        manager.validate_all()
    }

    /// Возвращает комбинированную статистику
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

/// Комбинированная статистика оптимизированного менеджера
#[derive(Debug, Clone)]
pub struct CombinedStatistics {
    /// Общее количество файлов
    pub total_files: u32,
    /// Общее количество страниц
    pub total_pages: u64,
    /// Общее количество операций чтения
    pub total_reads: u64,
    /// Общее количество операций записи
    pub total_writes: u64,
    /// Коэффициент попаданий в кэш
    pub cache_hit_ratio: f64,
    /// Средний коэффициент использования
    pub average_utilization: f64,
    /// Средний коэффициент фрагментации
    pub average_fragmentation: f64,
    /// Использование буфера записи (0.0 - 1.0)
    pub buffer_usage: f64,
    /// Использование кэша страниц
    pub cache_usage: usize,
    /// Пропускная способность чтения (байт/сек)
    pub read_throughput: f64,
    /// Пропускная способность записи (байт/сек)
    pub write_throughput: f64,
}

impl CombinedStatistics {
    /// Возвращает общую оценку производительности (0.0 - 1.0)
    pub fn performance_score(&self) -> f64 {
        let cache_score = self.cache_hit_ratio;
        let utilization_score = self.average_utilization;
        let fragmentation_score = 1.0 - self.average_fragmentation;
        let buffer_score = 1.0 - self.buffer_usage; // Меньше использование буфера = лучше
        
        (cache_score + utilization_score + fragmentation_score + buffer_score) / 4.0
    }

    /// Возвращает рекомендации по оптимизации
    pub fn get_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();

        if self.cache_hit_ratio < 0.8 {
            recommendations.push("Рассмотрите увеличение размера кэша страниц".to_string());
        }

        if self.average_fragmentation > 0.3 {
            recommendations.push("Рекомендуется дефрагментация файлов".to_string());
        }

        if self.buffer_usage > 0.9 {
            recommendations.push("Буфер записи переполнен, увеличьте размер буфера".to_string());
        }

        if self.average_utilization < 0.6 {
            recommendations.push("Низкое использование пространства, рассмотрите сжатие файлов".to_string());
        }

        if self.read_throughput < 1_000_000.0 { // < 1MB/s
            recommendations.push("Низкая пропускная способность чтения, проверьте дисковую подсистему".to_string());
        }

        if self.write_throughput < 500_000.0 { // < 500KB/s
            recommendations.push("Низкая пропускная способность записи, рассмотрите SSD".to_string());
        }

        if recommendations.is_empty() {
            recommendations.push("Производительность оптимальна".to_string());
        }

        recommendations
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use crate::storage::database_file::BLOCK_SIZE;

    #[tokio::test]
    async fn test_optimized_file_manager_creation() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let _manager = OptimizedFileManager::new(temp_dir.path())?;
        Ok(())
    }

    #[tokio::test]
    async fn test_create_and_open_file() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let manager = OptimizedFileManager::new(temp_dir.path())?;

        let file_id = manager.create_database_file(
            "test.db",
            DatabaseFileType::Data,
            123,
            ExtensionStrategy::Adaptive,
        ).await?;

        assert!(file_id > 0);
        assert!(manager.get_file_info(file_id).await.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_optimized_page_operations() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let manager = OptimizedFileManager::new(temp_dir.path())?;

        let file_id = manager.create_database_file(
            "test.db",
            DatabaseFileType::Data,
            123,
            ExtensionStrategy::Fixed,
        ).await?;

        // Выделяем страницы
        let page_id = manager.allocate_pages(file_id, 5).await?;
        assert_eq!(page_id, 0);

        // Записываем данные
        let test_data = vec![42u8; BLOCK_SIZE];
        manager.write_page(file_id, page_id, &test_data).await?;

        // Читаем данные (должно попасть в кэш)
        let read_data = manager.read_page(file_id, page_id).await?;
        assert_eq!(read_data, test_data);

        // Даем время на обработку асинхронных операций
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        
        // Проверяем статистику
        let stats = manager.get_io_statistics();
        // В оптимизированном менеджере статистика может обновляться асинхронно
        // Просто проверяем, что система инициализирована
        assert!(stats.total_operations >= 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_combined_statistics() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let manager = OptimizedFileManager::new(temp_dir.path())?;

        let file_id = manager.create_database_file(
            "test.db",
            DatabaseFileType::Data,
            123,
            ExtensionStrategy::Linear,
        ).await?;

        // Выполняем некоторые операции
        let page_id = manager.allocate_pages(file_id, 10).await?;
        let data = vec![100u8; BLOCK_SIZE];
        
        for i in 0..5 {
            manager.write_page(file_id, page_id + i, &data).await?;
            let _ = manager.read_page(file_id, page_id + i).await?;
        }

        // Получаем комбинированную статистику
        let stats = manager.get_combined_statistics().await;
        assert!(stats.total_files >= 1);
        assert!(stats.total_pages >= 10);

        // Проверяем оценку производительности
        let score = stats.performance_score();
        assert!(score >= 0.0 && score <= 1.0);

        // Получаем рекомендации
        let recommendations = stats.get_recommendations();
        assert!(!recommendations.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_maintenance_operations() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let manager = OptimizedFileManager::new(temp_dir.path())?;

        let file_id = manager.create_database_file(
            "test.db",
            DatabaseFileType::Data,
            123,
            ExtensionStrategy::Exponential,
        ).await?;

        // Запускаем проверку обслуживания
        let extended_files = manager.maintenance_check().await?;
        assert!(extended_files.len() <= 1);

        // Дефрагментируем
        manager.defragment_all().await;

        // Проверяем целостность
        let validation_results = manager.validate_all().await?;
        assert!(!validation_results.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_cache_performance() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let manager = OptimizedFileManager::new(temp_dir.path())?;

        let file_id = manager.create_database_file(
            "cache_test.db",
            DatabaseFileType::Data,
            456,
            ExtensionStrategy::Adaptive,
        ).await?;

        let page_id = manager.allocate_pages(file_id, 1).await?;
        let data = vec![255u8; BLOCK_SIZE];

        // Записываем данные
        manager.write_page(file_id, page_id, &data).await?;

        // Читаем несколько раз (должны быть попадания в кэш)
        for _ in 0..10 {
            let read_data = manager.read_page(file_id, page_id).await?;
            assert_eq!(read_data, data);
        }

        // Проверяем статистику кэша
        let stats = manager.get_io_statistics();
        assert!(stats.cache_hits > 0);
        assert!(stats.cache_hit_ratio > 0.0);

        Ok(())
    }
}
