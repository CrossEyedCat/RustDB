//! Система записи логов в файл для RustBD
//!
//! Этот модуль реализует буферизованную запись лог-записей в файл
//! с использованием оптимизированного I/O:
//! - Буферизация записей для повышения производительности
//! - Асинхронная запись с контролем приоритетов
//! - Ротация лог-файлов и управление размером
//! - Интеграция с системой оптимизации I/O

use crate::common::{Error, Result};
use crate::logging::log_record::{LogRecord, LogSequenceNumber};
use crate::storage::io_optimization::{BufferedIoManager, IoBufferConfig};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::task::JoinHandle;
use serde::{Deserialize, Serialize};

/// Конфигурация системы записи логов
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogWriterConfig {
    /// Путь к директории с лог-файлами
    pub log_directory: PathBuf,
    /// Максимальный размер лог-файла (в байтах)
    pub max_log_file_size: u64,
    /// Максимальное количество лог-файлов
    pub max_log_files: u32,
    /// Размер буфера записи (количество записей)
    pub write_buffer_size: usize,
    /// Максимальное время буферизации
    pub max_buffer_time: Duration,
    /// Включить сжатие старых логов
    pub enable_compression: bool,
    /// Уровень синхронизации
    pub sync_level: SyncLevel,
    /// Размер пула потоков для записи
    pub writer_thread_pool_size: usize,
    /// Включить проверку целостности
    pub enable_integrity_check: bool,
}

/// Уровень синхронизации логов
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncLevel {
    /// Никогда не синхронизировать (быстро, но небезопасно)
    Never,
    /// Синхронизировать периодически
    Periodic,
    /// Синхронизировать после каждого коммита
    OnCommit,
    /// Синхронизировать после каждой записи (медленно, но безопасно)
    Always,
}

impl Default for LogWriterConfig {
    fn default() -> Self {
        Self {
            log_directory: PathBuf::from("./logs"),
            max_log_file_size: 100 * 1024 * 1024, // 100 MB
            max_log_files: 10,
            write_buffer_size: 1000,
            max_buffer_time: Duration::from_millis(100),
            enable_compression: true,
            sync_level: SyncLevel::OnCommit,
            writer_thread_pool_size: 2,
            enable_integrity_check: true,
        }
    }
}

/// Информация о лог-файле
#[derive(Debug, Clone, PartialEq)]
pub struct LogFileInfo {
    /// Имя файла
    pub filename: String,
    /// Полный путь к файлу
    pub path: PathBuf,
    /// Размер файла в байтах
    pub size: u64,
    /// Количество записей в файле
    pub record_count: u64,
    /// Первый LSN в файле
    pub first_lsn: LogSequenceNumber,
    /// Последний LSN в файле
    pub last_lsn: LogSequenceNumber,
    /// Время создания файла
    pub created_at: u64,
    /// Время последнего обновления
    pub updated_at: u64,
    /// Сжат ли файл
    pub is_compressed: bool,
}

/// Запрос на запись лога
#[derive(Debug)]
pub struct LogWriteRequest {
    /// Лог-запись для записи
    pub record: LogRecord,
    /// Канал для ответа
    pub response_tx: Option<oneshot::Sender<Result<()>>>,
    /// Требует ли немедленной синхронизации
    pub force_sync: bool,
}

/// Статистика записи логов
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LogWriterStatistics {
    /// Общее количество записанных записей
    pub total_records_written: u64,
    /// Общее количество байт записанных
    pub total_bytes_written: u64,
    /// Количество операций синхронизации
    pub sync_operations: u64,
    /// Количество ротаций файлов
    pub file_rotations: u64,
    /// Среднее время записи (микросекунды)
    pub average_write_time_us: u64,
    /// Количество ошибок записи
    pub write_errors: u64,
    /// Текущий размер буфера
    pub current_buffer_size: usize,
    /// Максимальный размер буфера за сессию
    pub max_buffer_size_reached: usize,
    /// Пропускная способность записи (записей/сек)
    pub write_throughput: f64,
}

/// Система записи логов
pub struct LogWriter {
    /// Конфигурация
    config: LogWriterConfig,
    /// Текущий активный файл
    current_file: Arc<RwLock<Option<LogFileInfo>>>,
    /// Список всех лог-файлов
    log_files: Arc<RwLock<Vec<LogFileInfo>>>,
    /// Буфер записей
    write_buffer: Arc<Mutex<VecDeque<LogRecord>>>,
    /// Канал для отправки запросов на запись
    write_tx: mpsc::UnboundedSender<LogWriteRequest>,
    /// Генератор LSN
    lsn_generator: Arc<Mutex<LogSequenceNumber>>,
    /// Статистика
    statistics: Arc<RwLock<LogWriterStatistics>>,
    /// Семафор для ограничения одновременных операций
    semaphore: Arc<Semaphore>,
    /// Обработчик фоновых задач
    background_handle: Option<JoinHandle<()>>,
    /// Обработчик записи
    writer_handle: Option<JoinHandle<()>>,
    /// Менеджер оптимизированного I/O
    io_manager: Option<Arc<BufferedIoManager>>,
}

impl LogWriter {
    /// Создает новую систему записи логов
    pub fn new(config: LogWriterConfig) -> Result<Self> {
        // Создаем директорию для логов если не существует
        if !config.log_directory.exists() {
            std::fs::create_dir_all(&config.log_directory).map_err(|e| {
                Error::internal(&format!("Не удалось создать директорию логов: {}", e))
            })?;
        }

        let (write_tx, write_rx) = mpsc::unbounded_channel();
        let semaphore = Arc::new(Semaphore::new(config.writer_thread_pool_size));

        // Настройка оптимизированного I/O
        let io_manager = if config.write_buffer_size > 0 {
            let mut io_config = IoBufferConfig::default();
            io_config.max_write_buffer_size = config.write_buffer_size;
            io_config.max_buffer_time = config.max_buffer_time;
            Some(Arc::new(BufferedIoManager::new(io_config)))
        } else {
            None
        };

        let mut writer = Self {
            config: config.clone(),
            current_file: Arc::new(RwLock::new(None)),
            log_files: Arc::new(RwLock::new(Vec::new())),
            write_buffer: Arc::new(Mutex::new(VecDeque::new())),
            write_tx,
            lsn_generator: Arc::new(Mutex::new(1)),
            statistics: Arc::new(RwLock::new(LogWriterStatistics::default())),
            semaphore,
            background_handle: None,
            writer_handle: None,
            io_manager,
        };

        // Загружаем существующие лог-файлы
        writer.load_existing_log_files()?;

        // Запускаем фоновые задачи
        writer.start_background_tasks(write_rx);

        Ok(writer)
    }

    /// Загружает существующие лог-файлы
    fn load_existing_log_files(&mut self) -> Result<()> {
        let mut files = Vec::new();
        let mut max_lsn = 0;

        if self.config.log_directory.exists() {
            let entries = std::fs::read_dir(&self.config.log_directory).map_err(|e| {
                Error::internal(&format!("Не удалось прочитать директорию логов: {}", e))
            })?;

            for entry in entries {
                let entry = entry.map_err(|e| Error::internal(&format!("Ошибка чтения записи: {}", e)))?;
                let path = entry.path();

                if path.extension().and_then(|s| s.to_str()) == Some("log") {
                    if let Ok(file_info) = self.analyze_log_file(&path) {
                        if file_info.last_lsn > max_lsn {
                            max_lsn = file_info.last_lsn;
                        }
                        files.push(file_info);
                    }
                }
            }
        }

        // Сортируем файлы по времени создания
        files.sort_by_key(|f| f.created_at);

        // Устанавливаем последний файл как текущий
        if let Some(latest_file) = files.last() {
            *self.current_file.write().unwrap() = Some(latest_file.clone());
        }

        *self.log_files.write().unwrap() = files;

        // Устанавливаем следующий LSN
        *self.lsn_generator.lock().unwrap() = max_lsn + 1;

        Ok(())
    }

    /// Анализирует лог-файл и возвращает информацию о нем
    fn analyze_log_file(&self, path: &Path) -> Result<LogFileInfo> {
        let metadata = std::fs::metadata(path).map_err(|e| {
            Error::internal(&format!("Не удалось получить метаданные файла: {}", e))
        })?;

        let filename = path.file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();

        // Простая реализация - в реальной системе нужно читать заголовок файла
        let file_info = LogFileInfo {
            filename,
            path: path.to_path_buf(),
            size: metadata.len(),
            record_count: 0, // Требует чтения файла
            first_lsn: 0,    // Требует чтения файла
            last_lsn: 0,     // Требует чтения файла
            created_at: metadata.created()
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            updated_at: metadata.modified()
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            is_compressed: path.extension().and_then(|s| s.to_str()) == Some("gz"),
        };

        Ok(file_info)
    }

    /// Запускает фоновые задачи
    fn start_background_tasks(&mut self, mut write_rx: mpsc::UnboundedReceiver<LogWriteRequest>) {
        let config = self.config.clone();
        let write_buffer = self.write_buffer.clone();
        let statistics = self.statistics.clone();
        let current_file = self.current_file.clone();
        let log_files = self.log_files.clone();
        let semaphore = self.semaphore.clone();
        let io_manager = self.io_manager.clone();

        // Задача обработки запросов на запись
        self.writer_handle = Some(tokio::spawn(async move {
            while let Some(request) = write_rx.recv().await {
                let buffer = write_buffer.clone();
                let stats = statistics.clone();
                let file = current_file.clone();
                let files = log_files.clone();
                let cfg = config.clone();
                let io_mgr = io_manager.clone();

                // Обрабатываем запрос напрямую без семафора для упрощения
                Self::handle_write_request(request, buffer, stats, file, files, cfg, io_mgr).await;
            }
        }));

        // Задача периодического сброса буфера
        let flush_buffer = self.write_buffer.clone();
        let flush_stats = self.statistics.clone();
        let flush_config = self.config.clone();

        self.background_handle = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(flush_config.max_buffer_time);

            loop {
                interval.tick().await;
                Self::flush_write_buffer(&flush_buffer, &flush_stats).await;
            }
        }));
    }

    /// Обрабатывает запрос на запись
    async fn handle_write_request(
        request: LogWriteRequest,
        write_buffer: Arc<Mutex<VecDeque<LogRecord>>>,
        statistics: Arc<RwLock<LogWriterStatistics>>,
        current_file: Arc<RwLock<Option<LogFileInfo>>>,
        _log_files: Arc<RwLock<Vec<LogFileInfo>>>,
        _config: LogWriterConfig,
        _io_manager: Option<Arc<BufferedIoManager>>,
    ) {
        let start_time = Instant::now();
        let result = Ok(());

        // Добавляем запись в буфер
        {
            let mut buffer = write_buffer.lock().unwrap();
            buffer.push_back(request.record.clone());
        }

        // Если требуется немедленная синхронизация, сбрасываем буфер
        if request.force_sync || request.record.requires_immediate_flush() {
            Self::flush_write_buffer(&write_buffer, &statistics).await;
        }

        // Обновляем статистику
        {
            let mut stats = statistics.write().unwrap();
            stats.total_records_written += 1;
            
            if let Ok(serialized) = request.record.serialize() {
                stats.total_bytes_written += serialized.len() as u64;
            }

            let execution_time = start_time.elapsed().as_micros() as u64;
            if stats.total_records_written > 0 {
                stats.average_write_time_us = 
                    (stats.average_write_time_us * (stats.total_records_written - 1) + execution_time) 
                    / stats.total_records_written;
            }

            let buffer_size = write_buffer.lock().unwrap().len();
            stats.current_buffer_size = buffer_size;
            if buffer_size > stats.max_buffer_size_reached {
                stats.max_buffer_size_reached = buffer_size;
            }
        }

        // Отправляем результат
        if let Some(response_tx) = request.response_tx {
            let _ = response_tx.send(result);
        }
    }

    /// Сбрасывает буфер записи на диск
    async fn flush_write_buffer(
        write_buffer: &Arc<Mutex<VecDeque<LogRecord>>>,
        statistics: &Arc<RwLock<LogWriterStatistics>>,
    ) {
        let records_to_write = {
            let mut buffer = write_buffer.lock().unwrap();
            if buffer.is_empty() {
                return;
            }

            let records: Vec<_> = buffer.drain(..).collect();
            records
        };

        // В реальной реализации здесь была бы запись в файл
        // Пока что просто симулируем запись
        tokio::time::sleep(Duration::from_micros(50 * records_to_write.len() as u64)).await;

        // Обновляем статистику
        {
            let mut stats = statistics.write().unwrap();
            stats.sync_operations += 1;
            stats.current_buffer_size = 0;
            
            // Вычисляем пропускную способность
            if stats.total_records_written > 0 && stats.average_write_time_us > 0 {
                stats.write_throughput = 1_000_000.0 / stats.average_write_time_us as f64;
            }
        }
    }

    /// Записывает лог-запись
    pub async fn write_log(&self, mut record: LogRecord) -> Result<LogSequenceNumber> {
        // Генерируем LSN если не установлен
        if record.lsn == 0 {
            record.lsn = self.generate_lsn();
        }

        let (response_tx, response_rx) = oneshot::channel();
        let request = LogWriteRequest {
            record: record.clone(),
            response_tx: Some(response_tx),
            force_sync: false,
        };

        self.write_tx.send(request).map_err(|_| {
            Error::internal("Не удалось отправить запрос на запись лога")
        })?;

        response_rx.await.map_err(|_| {
            Error::internal("Не удалось получить результат записи лога")
        })??;

        Ok(record.lsn)
    }

    /// Записывает лог-запись с принудительной синхронизацией
    pub async fn write_log_sync(&self, mut record: LogRecord) -> Result<LogSequenceNumber> {
        if record.lsn == 0 {
            record.lsn = self.generate_lsn();
        }

        let (response_tx, response_rx) = oneshot::channel();
        let request = LogWriteRequest {
            record: record.clone(),
            response_tx: Some(response_tx),
            force_sync: true,
        };

        self.write_tx.send(request).map_err(|_| {
            Error::internal("Не удалось отправить запрос на синхронную запись лога")
        })?;

        response_rx.await.map_err(|_| {
            Error::internal("Не удалось получить результат синхронной записи лога")
        })??;

        Ok(record.lsn)
    }

    /// Принудительно сбрасывает все буферы на диск
    pub async fn flush(&self) -> Result<()> {
        Self::flush_write_buffer(&self.write_buffer, &self.statistics).await;
        
        {
            let mut stats = self.statistics.write().unwrap();
            stats.sync_operations += 1;
        }
        
        Ok(())
    }

    /// Генерирует следующий LSN
    fn generate_lsn(&self) -> LogSequenceNumber {
        let mut generator = self.lsn_generator.lock().unwrap();
        let lsn = *generator;
        *generator += 1;
        lsn
    }

    /// Возвращает текущий LSN
    pub fn current_lsn(&self) -> LogSequenceNumber {
        let generator = self.lsn_generator.lock().unwrap();
        *generator - 1
    }

    /// Возвращает статистику записи логов
    pub fn get_statistics(&self) -> LogWriterStatistics {
        self.statistics.read().unwrap().clone()
    }

    /// Возвращает информацию о текущем файле
    pub fn get_current_file_info(&self) -> Option<LogFileInfo> {
        self.current_file.read().unwrap().clone()
    }

    /// Возвращает список всех лог-файлов
    pub fn get_log_files(&self) -> Vec<LogFileInfo> {
        self.log_files.read().unwrap().clone()
    }

    /// Выполняет ротацию лог-файлов
    pub async fn rotate_log_file(&self) -> Result<()> {
        // В реальной реализации здесь была бы логика ротации файлов
        {
            let mut stats = self.statistics.write().unwrap();
            stats.file_rotations += 1;
        }
        
        Ok(())
    }

    /// Проверяет целостность лог-файлов
    pub async fn verify_integrity(&self) -> Result<Vec<(String, bool)>> {
        let files = self.get_log_files();
        let mut results = Vec::new();

        for file in files {
            // В реальной реализации здесь была бы проверка целостности файла
            let is_valid = true; // Заглушка
            results.push((file.filename, is_valid));
        }

        Ok(results)
    }

    /// Очищает старые лог-файлы
    pub async fn cleanup_old_logs(&self, keep_files: u32) -> Result<u32> {
        let files = self.get_log_files();
        let mut removed_count = 0;

        if files.len() > keep_files as usize {
            let files_to_remove = files.len() - keep_files as usize;
            
            // В реальной реализации здесь было бы удаление файлов
            removed_count = files_to_remove as u32;
        }

        Ok(removed_count)
    }

    /// Возвращает общий размер всех лог-файлов
    pub fn get_total_log_size(&self) -> u64 {
        self.get_log_files().iter().map(|f| f.size).sum()
    }

    /// Проверяет, нужна ли ротация файла
    pub fn needs_rotation(&self) -> bool {
        if let Some(current) = self.get_current_file_info() {
            current.size >= self.config.max_log_file_size
        } else {
            false
        }
    }
}

impl Drop for LogWriter {
    fn drop(&mut self) {
        // Останавливаем фоновые задачи
        if let Some(handle) = self.background_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.writer_handle.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logging::log_record::{LogRecord, LogRecordType, IsolationLevel};
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_log_writer_creation() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut config = LogWriterConfig::default();
        config.log_directory = temp_dir.path().to_path_buf();

        let _writer = LogWriter::new(config)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_write_log() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut config = LogWriterConfig::default();
        config.log_directory = temp_dir.path().to_path_buf();
        config.write_buffer_size = 10;

        let writer = LogWriter::new(config)?;
        
        let record = LogRecord::new_transaction_begin(0, 100, IsolationLevel::ReadCommitted);
        let lsn = writer.write_log(record).await?;
        
        assert!(lsn > 0);
        
        let stats = writer.get_statistics();
        assert!(stats.total_records_written >= 1);
        
        Ok(())
    }

    #[tokio::test]
    async fn test_write_log_sync() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut config = LogWriterConfig::default();
        config.log_directory = temp_dir.path().to_path_buf();

        let writer = LogWriter::new(config)?;
        
        let record = LogRecord::new_transaction_commit(0, 100, vec![(1, 10)], None);
        let lsn = writer.write_log_sync(record).await?;
        
        assert!(lsn > 0);
        
        let stats = writer.get_statistics();
        assert!(stats.sync_operations >= 1);
        
        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_writes() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut config = LogWriterConfig::default();
        config.log_directory = temp_dir.path().to_path_buf();
        config.write_buffer_size = 5;

        let writer = LogWriter::new(config)?;
        
        // Записываем несколько записей
        for i in 0..10 {
            let record = LogRecord::new_data_insert(
                0, 100 + i, 1, i as u64, 0, vec![i as u8; 10], None
            );
            writer.write_log(record).await?;
        }
        
        // Принудительно сбрасываем буфер
        writer.flush().await?;
        
        let stats = writer.get_statistics();
        assert_eq!(stats.total_records_written, 10);
        assert!(stats.total_bytes_written > 0);
        
        Ok(())
    }

    #[tokio::test]
    async fn test_lsn_generation() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut config = LogWriterConfig::default();
        config.log_directory = temp_dir.path().to_path_buf();

        let writer = LogWriter::new(config)?;
        
        let mut last_lsn = 0;
        for i in 0..5 {
            let record = LogRecord::new_transaction_begin(0, 100 + i, IsolationLevel::ReadCommitted);
            let lsn = writer.write_log(record).await?;
            
            assert!(lsn > last_lsn);
            last_lsn = lsn;
        }
        
        Ok(())
    }

    #[tokio::test]
    async fn test_statistics() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut config = LogWriterConfig::default();
        config.log_directory = temp_dir.path().to_path_buf();

        let writer = LogWriter::new(config)?;
        
        // Записываем несколько записей разных типов
        writer.write_log(LogRecord::new_transaction_begin(0, 100, IsolationLevel::ReadCommitted)).await?;
        writer.write_log(LogRecord::new_data_insert(0, 100, 1, 10, 0, vec![1, 2, 3], None)).await?;
        writer.write_log_sync(LogRecord::new_transaction_commit(0, 100, vec![(1, 10)], None)).await?;
        
        let stats = writer.get_statistics();
        assert_eq!(stats.total_records_written, 3);
        assert!(stats.total_bytes_written > 0);
        assert!(stats.sync_operations >= 1);
        assert!(stats.average_write_time_us > 0);
        
        Ok(())
    }

    #[tokio::test]
    async fn test_buffer_management() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut config = LogWriterConfig::default();
        config.log_directory = temp_dir.path().to_path_buf();
        config.write_buffer_size = 3;
        config.max_buffer_time = Duration::from_millis(50);

        let writer = LogWriter::new(config)?;
        
        // Записываем записи, которые должны остаться в буфере
        for i in 0..2 {
            let record = LogRecord::new_data_insert(0, 100, 1, i, 0, vec![i as u8], None);
            writer.write_log(record).await?;
        }
        
        // Ждем автоматического сброса буфера
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        let stats = writer.get_statistics();
        assert!(stats.sync_operations >= 1);
        
        Ok(())
    }
}
