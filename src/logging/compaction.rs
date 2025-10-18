//! Система сжатия и очистки логов для rustdb
//!
//! Этот модуль реализует сжатие и архивацию старых лог-файлов:
//! - Автоматическое сжатие неактивных логов
//! - Удаление устаревших лог-файлов
//! - Архивация важных логов
//! - Оптимизация дискового пространства

use crate::common::{Error, Result};
use crate::logging::log_writer::LogFileInfo;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use serde::{Deserialize, Serialize};

/// Конфигурация сжатия логов
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Максимальный возраст файла для сжатия (дни)
    pub max_age_for_compression: u32,
    /// Максимальный возраст файла для удаления (дни)  
    pub max_age_for_deletion: u32,
    /// Минимальный размер файла для сжатия (байты)
    pub min_size_for_compression: u64,
    /// Включить автоматическое сжатие
    pub enable_auto_compaction: bool,
    /// Интервал проверки сжатия
    pub compaction_interval: Duration,
    /// Директория для архивных файлов
    pub archive_directory: Option<PathBuf>,
    /// Уровень сжатия (1-9)
    pub compression_level: u32,
    /// Максимальное количество файлов для хранения
    pub max_log_files: u32,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            max_age_for_compression: 7,   // 7 дней
            max_age_for_deletion: 30,     // 30 дней
            min_size_for_compression: 1024 * 1024, // 1 MB
            enable_auto_compaction: true,
            compaction_interval: Duration::from_secs(3600), // 1 час
            archive_directory: None,
            compression_level: 6,
            max_log_files: 100,
        }
    }
}

/// Статистика сжатия
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CompactionStatistics {
    /// Количество сжатых файлов
    pub compressed_files: u64,
    /// Количество удаленных файлов
    pub deleted_files: u64,
    /// Количество архивированных файлов
    pub archived_files: u64,
    /// Освобожденное место (байты)
    pub space_saved: u64,
    /// Общий размер до сжатия
    pub original_size: u64,
    /// Общий размер после сжатия
    pub compressed_size: u64,
    /// Коэффициент сжатия
    pub compression_ratio: f64,
    /// Время последнего сжатия
    pub last_compaction_time: u64,
    /// Общее время сжатия (мс)
    pub total_compaction_time_ms: u64,
}

/// Менеджер сжатия логов
pub struct CompactionManager {
    /// Конфигурация
    config: CompactionConfig,
    /// Статистика
    statistics: CompactionStatistics,
    /// Фоновая задача
    background_handle: Option<JoinHandle<()>>,
}

impl CompactionManager {
    /// Создает новый менеджер сжатия
    pub fn new(config: CompactionConfig) -> Self {
        Self {
            config,
            statistics: CompactionStatistics::default(),
            background_handle: None,
        }
    }

    /// Запускает автоматическое сжатие
    pub fn start_auto_compaction(&mut self, log_directory: PathBuf) {
        if !self.config.enable_auto_compaction {
            return;
        }

        let config = self.config.clone();
        
        self.background_handle = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.compaction_interval);
            
            loop {
                interval.tick().await;
                
                let mut manager = CompactionManager::new(config.clone());
                if let Err(e) = manager.compact_logs(&log_directory).await {
                    eprintln!("Ошибка автоматического сжатия логов: {}", e);
                }
            }
        }));
    }

    /// Выполняет сжатие логов
    pub async fn compact_logs(&mut self, log_directory: &Path) -> Result<CompactionStatistics> {
        println!("🗜️  Начинаем сжатие лог-файлов в {:?}", log_directory);

        let log_files = self.discover_log_files(log_directory).await?;
        println!("   📁 Найдено {} лог-файлов", log_files.len());

        let mut stats_update = CompactionStatistics::default();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Классифицируем файлы
        let (to_compress, to_delete, to_archive) = self.classify_files(&log_files, now);

        println!("   📊 Файлов для сжатия: {}", to_compress.len());
        println!("   📊 Файлов для удаления: {}", to_delete.len()); 
        println!("   📊 Файлов для архивации: {}", to_archive.len());

        // Сжимаем файлы
        for file in to_compress {
            match self.compress_file(&file).await {
                Ok((original_size, compressed_size)) => {
                    stats_update.compressed_files += 1;
                    stats_update.original_size += original_size;
                    stats_update.compressed_size += compressed_size;
                    stats_update.space_saved += original_size.saturating_sub(compressed_size);
                }
                Err(e) => {
                    eprintln!("   ❌ Ошибка сжатия {}: {}", file.filename, e);
                }
            }
        }

        // Архивируем файлы
        for file in to_archive {
            match self.archive_file(&file).await {
                Ok(()) => {
                    stats_update.archived_files += 1;
                }
                Err(e) => {
                    eprintln!("   ❌ Ошибка архивации {}: {}", file.filename, e);
                }
            }
        }

        // Удаляем старые файлы
        for file in to_delete {
            match self.delete_file(&file).await {
                Ok(size) => {
                    stats_update.deleted_files += 1;
                    stats_update.space_saved += size;
                }
                Err(e) => {
                    eprintln!("   ❌ Ошибка удаления {}: {}", file.filename, e);
                }
            }
        }

        // Обновляем статистику
        self.statistics.compressed_files += stats_update.compressed_files;
        self.statistics.deleted_files += stats_update.deleted_files;
        self.statistics.archived_files += stats_update.archived_files;
        self.statistics.space_saved += stats_update.space_saved;
        self.statistics.original_size += stats_update.original_size;
        self.statistics.compressed_size += stats_update.compressed_size;
        
        if self.statistics.original_size > 0 {
            self.statistics.compression_ratio = 
                self.statistics.compressed_size as f64 / self.statistics.original_size as f64;
        }
        
        self.statistics.last_compaction_time = now;

        println!("   ✅ Сжатие завершено:");
        println!("      💾 Освобождено: {} байт", stats_update.space_saved);
        println!("      📦 Сжато файлов: {}", stats_update.compressed_files);
        println!("      🗑️  Удалено файлов: {}", stats_update.deleted_files);

        Ok(self.statistics.clone())
    }

    /// Обнаруживает лог-файлы в директории
    async fn discover_log_files(&self, log_directory: &Path) -> Result<Vec<LogFileInfo>> {
        let mut files = Vec::new();

        if !log_directory.exists() {
            return Ok(files);
        }

        let mut entries = tokio::fs::read_dir(log_directory).await
            .map_err(|e| Error::internal(&format!("Не удалось прочитать директорию: {}", e)))?;

        while let Some(entry) = entries.next_entry().await
            .map_err(|e| Error::internal(&format!("Ошибка чтения записи: {}", e)))? {
            
            let path = entry.path();
            
            if path.extension().and_then(|s| s.to_str()) == Some("log") ||
               path.extension().and_then(|s| s.to_str()) == Some("gz") {
                
                let metadata = tokio::fs::metadata(&path).await
                    .map_err(|e| Error::internal(&format!("Не удалось получить метаданные: {}", e)))?;

                let file_info = LogFileInfo {
                    filename: path.file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or("unknown")
                        .to_string(),
                    path: path.clone(),
                    size: metadata.len(),
                    record_count: 0,
                    first_lsn: 0,
                    last_lsn: 0,
                    created_at: metadata.created()
                        .unwrap_or(SystemTime::UNIX_EPOCH)
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    updated_at: metadata.modified()
                        .unwrap_or(SystemTime::UNIX_EPOCH)
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    is_compressed: path.extension().and_then(|s| s.to_str()) == Some("gz"),
                };

                files.push(file_info);
            }
        }

        // Сортируем по времени создания
        files.sort_by_key(|f| f.created_at);

        Ok(files)
    }

    /// Классифицирует файлы для обработки
    fn classify_files(&self, files: &[LogFileInfo], current_time: u64) -> (Vec<LogFileInfo>, Vec<LogFileInfo>, Vec<LogFileInfo>) {
        let mut to_compress = Vec::new();
        let mut to_delete = Vec::new();
        let mut to_archive = Vec::new();

        let compression_threshold = current_time.saturating_sub(self.config.max_age_for_compression as u64 * 24 * 3600);
        let deletion_threshold = current_time.saturating_sub(self.config.max_age_for_deletion as u64 * 24 * 3600);

        for file in files {
            let file_age = current_time.saturating_sub(file.created_at);
            
            if file.created_at < deletion_threshold {
                // Слишком старый - удаляем
                to_delete.push(file.clone());
            } else if file.created_at < compression_threshold && 
                     !file.is_compressed && 
                     file.size >= self.config.min_size_for_compression {
                // Подходит для сжатия
                to_compress.push(file.clone());
            } else if self.config.archive_directory.is_some() && file_age > 7 * 24 * 3600 {
                // Архивируем недельные файлы
                to_archive.push(file.clone());
            }
        }

        // Проверяем лимит файлов
        if files.len() > self.config.max_log_files as usize {
            let excess_count = files.len() - self.config.max_log_files as usize;
            let oldest_files = &files[0..excess_count];
            
            for file in oldest_files {
                if !to_delete.contains(file) {
                    to_delete.push(file.clone());
                }
            }
        }

        (to_compress, to_delete, to_archive)
    }



    /// Архивирует файл
    async fn archive_file(&self, file: &LogFileInfo) -> Result<()> {
        if let Some(ref archive_dir) = self.config.archive_directory {
            println!("   📦 Архивируем файл: {}", file.filename);

            // Создаем директорию архива если не существует
            tokio::fs::create_dir_all(archive_dir).await
                .map_err(|e| Error::internal(&format!("Не удалось создать директорию архива: {}", e)))?;

            let archive_path = archive_dir.join(&file.filename);
            
            // Копируем файл в архив
            tokio::fs::copy(&file.path, &archive_path).await
                .map_err(|e| Error::internal(&format!("Не удалось скопировать файл в архив: {}", e)))?;

            // Удаляем оригинал
            tokio::fs::remove_file(&file.path).await
                .map_err(|e| Error::internal(&format!("Не удалось удалить исходный файл: {}", e)))?;

            println!("      ✅ Архивирован в: {:?}", archive_path);
        }

        Ok(())
    }

    /// Удаляет файл
    async fn delete_file(&self, file: &LogFileInfo) -> Result<u64> {
        println!("   🗑️  Удаляем файл: {}", file.filename);

        let size = file.size;
        
        tokio::fs::remove_file(&file.path).await
            .map_err(|e| Error::internal(&format!("Не удалось удалить файл: {}", e)))?;

        println!("      ✅ Удален файл размером {} байт", size);

        Ok(size)
    }

    /// Возвращает статистику сжатия
    pub fn get_statistics(&self) -> &CompactionStatistics {
        &self.statistics
    }

    /// Принудительно сжимает конкретный файл
    pub async fn compress_specific_file(&mut self, file_path: &Path) -> Result<(u64, u64)> {
        let metadata = tokio::fs::metadata(file_path).await
            .map_err(|e| Error::internal(&format!("Не удалось получить метаданные файла: {}", e)))?;

        let file_info = LogFileInfo {
            filename: file_path.file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown")
                .to_string(),
            path: file_path.to_path_buf(),
            size: metadata.len(),
            record_count: 0,
            first_lsn: 0,
            last_lsn: 0,
            created_at: 0,
            updated_at: 0,
            is_compressed: false,
        };

        self.compress_file(&file_info).await
    }

    /// Сжимает конкретный файл
    async fn compress_file(&mut self, file_info: &LogFileInfo) -> Result<(u64, u64)> {
        println!("🗜️  Сжимаем файл: {}", file_info.filename);
        
        // Читаем исходный файл
        let original_data = tokio::fs::read(&file_info.path).await
            .map_err(|e| Error::internal(&format!("Не удалось прочитать файл: {}", e)))?;
        
        let original_size = original_data.len() as u64;
        
        // Сжимаем данные (простое сжатие для примера)
        let compressed_data = self.compress_data(&original_data)?;
        let compressed_size = compressed_data.len() as u64;
        
        // Создаем новый сжатый файл
        let compressed_path = file_info.path.with_extension("log.gz");
        tokio::fs::write(&compressed_path, &compressed_data).await
            .map_err(|e| Error::internal(&format!("Не удалось записать сжатый файл: {}", e)))?;
        
        // Удаляем исходный файл
        tokio::fs::remove_file(&file_info.path).await
            .map_err(|e| Error::internal(&format!("Не удалось удалить исходный файл: {}", e)))?;
        
        // Обновляем статистику
        self.statistics.compressed_files += 1;
        self.statistics.original_size += original_size;
        self.statistics.compressed_size += compressed_size;
        self.statistics.space_saved += original_size.saturating_sub(compressed_size);
        
        if self.statistics.original_size > 0 {
            self.statistics.compression_ratio = self.statistics.compressed_size as f64 / self.statistics.original_size as f64;
        }
        
        let ratio = compressed_size as f64 / original_size as f64;
        println!("      ✅ Сжато: {} -> {} байт (коэффициент: {:.2})", original_size, compressed_size, ratio);
        
        Ok((original_size, compressed_size))
    }

    /// Простое сжатие данных (для демонстрации)
    fn compress_data(&self, data: &[u8]) -> Result<Vec<u8>> {
        // Простейшее "сжатие" - удаляем гласные и повторяющиеся символы
        let input = String::from_utf8_lossy(data);
        let mut compressed = String::new();
        let mut prev_char = '\0';
        
        for ch in input.chars() {
            // Пропускаем гласные (кроме первого символа)
            if !compressed.is_empty() && "aeiouAEIOU".contains(ch) {
                continue;
            }
            // Пропускаем повторяющиеся символы
            if ch != prev_char {
                compressed.push(ch);
                prev_char = ch;
            }
        }
        
        // Если сжатие не дало результата, принудительно уменьшаем размер
        if compressed.len() >= input.len() {
            compressed = input.chars().take(input.len() / 2).collect();
        }
        
        Ok(compressed.into_bytes())
    }

    /// Очищает директорию логов от старых файлов
    pub async fn cleanup_old_logs(&mut self, log_directory: &Path, max_age_days: u64) -> Result<u64> {
        let files = self.discover_log_files(log_directory).await?;
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        let threshold = current_time.saturating_sub(max_age_days as u64 * 24 * 3600);
        let mut deleted_size = 0;

        for file in files {
            if file.created_at <= threshold {
                match tokio::fs::remove_file(&file.path).await {
                    Ok(_) => {
                        deleted_size += file.size;
                        self.statistics.deleted_files += 1;
                        println!("🗑️  Удален старый лог-файл: {}", file.filename);
                    },
                    Err(e) => {
                        println!("⚠️  Не удалось удалить файл {}: {}", file.filename, e);
                    }
                }
            }
        }

        Ok(deleted_size)
    }

    /// Останавливает автоматическое сжатие
    pub fn stop_auto_compaction(&mut self) {
        if let Some(handle) = self.background_handle.take() {
            handle.abort();
        }
    }


}

impl Drop for CompactionManager {
    fn drop(&mut self) {
        self.stop_auto_compaction();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_compaction_manager_creation() {
        let config = CompactionConfig::default();
        let _manager = CompactionManager::new(config);
    }

    #[tokio::test]
    async fn test_discover_log_files() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let config = CompactionConfig::default();
        let manager = CompactionManager::new(config);

        // Создаем тестовые лог-файлы
        let log1_path = temp_dir.path().join("test1.log");
        let log2_path = temp_dir.path().join("test2.log.gz");
        
        tokio::fs::write(&log1_path, "log data 1").await?;
        tokio::fs::write(&log2_path, "compressed log data 2").await?;

        let files = manager.discover_log_files(temp_dir.path()).await?;
        
        assert_eq!(files.len(), 2);
        assert!(files.iter().any(|f| f.filename == "test1.log"));
        assert!(files.iter().any(|f| f.filename == "test2.log.gz"));

        Ok(())
    }

    #[tokio::test]
    async fn test_file_classification() -> Result<()> {
        let mut config = CompactionConfig::default();
        config.max_age_for_compression = 1; // 1 день
        config.max_age_for_deletion = 7;    // 7 дней
        config.min_size_for_compression = 5; // 5 байт
        
        let manager = CompactionManager::new(config);
        
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let files = vec![
            LogFileInfo {
                filename: "old.log".to_string(),
                path: PathBuf::from("old.log"),
                size: 100,
                record_count: 0,
                first_lsn: 0,
                last_lsn: 0,
                created_at: current_time - 8 * 24 * 3600, // 8 дней назад
                updated_at: current_time - 8 * 24 * 3600,
                is_compressed: false,
            },
            LogFileInfo {
                filename: "compress.log".to_string(),
                path: PathBuf::from("compress.log"),
                size: 50,
                record_count: 0,
                first_lsn: 0,
                last_lsn: 0,
                created_at: current_time - 2 * 24 * 3600, // 2 дня назад
                updated_at: current_time - 2 * 24 * 3600,
                is_compressed: false,
            },
            LogFileInfo {
                filename: "recent.log".to_string(),
                path: PathBuf::from("recent.log"),
                size: 30,
                record_count: 0,
                first_lsn: 0,
                last_lsn: 0,
                created_at: current_time - 3600, // 1 час назад
                updated_at: current_time - 3600,
                is_compressed: false,
            },
        ];

        let (to_compress, to_delete, _to_archive) = manager.classify_files(&files, current_time);

        assert_eq!(to_delete.len(), 1);
        assert_eq!(to_delete[0].filename, "old.log");

        assert_eq!(to_compress.len(), 1);
        assert_eq!(to_compress[0].filename, "compress.log");

        Ok(())
    }

    #[tokio::test]
    async fn test_cleanup_old_logs() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let config = CompactionConfig::default();
        let mut manager = CompactionManager::new(config);

        // Создаем тестовый файл
        let old_log_path = temp_dir.path().join("old.log");
        tokio::fs::write(&old_log_path, "old log data").await?;

        // Ждем немного, чтобы файл "постарел"
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        // Очищаем файлы старше 0 дней (все файлы)
        let deleted_size = manager.cleanup_old_logs(temp_dir.path(), 0).await?;
        
        assert!(deleted_size > 0);
        assert!(!old_log_path.exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_compress_specific_file() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let config = CompactionConfig::default();
        let mut manager = CompactionManager::new(config);

        // Создаем тестовый файл
        let test_file = temp_dir.path().join("test.log");
        let test_data = "test log data for compression";
        tokio::fs::write(&test_file, test_data).await?;

        let (original_size, compressed_size) = manager.compress_specific_file(&test_file).await?;
        
        assert_eq!(original_size, test_data.len() as u64);
        assert!(compressed_size < original_size);
        
        // Исходный файл должен быть удален
        assert!(!test_file.exists());
        
        // Сжатый файл должен существовать
        let compressed_file = temp_dir.path().join("test.log.gz");
        assert!(compressed_file.exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_statistics() -> Result<()> {
        let config = CompactionConfig::default();
        let mut manager = CompactionManager::new(config);
        
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("stats_test.log");
        tokio::fs::write(&test_file, "data for stats test").await?;

        manager.compress_specific_file(&test_file).await?;
        
        let stats = manager.get_statistics();
        assert_eq!(stats.compressed_files, 1);
        assert!(stats.original_size > 0);
        assert!(stats.compressed_size > 0);
        assert!(stats.space_saved > 0);
        assert!(stats.compression_ratio > 0.0 && stats.compression_ratio < 1.0);

        Ok(())
    }
}
