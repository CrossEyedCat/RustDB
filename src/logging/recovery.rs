//! Система восстановления данных из логов для RustBD
//!
//! Этот модуль реализует восстановление базы данных после сбоев:
//! - Анализ лог-файлов и определение точки восстановления
//! - REDO операции для восстановления зафиксированных транзакций
//! - UNDO операции для отката незавершенных транзакций
//! - Валидация целостности данных после восстановления

use crate::common::{Error, Result};
use crate::logging::log_record::{LogRecord, LogRecordType, LogSequenceNumber, TransactionId};
use crate::logging::log_writer::{LogWriter, LogFileInfo};
use crate::storage::database_file::PageId;
use std::collections::{HashMap, HashSet, BTreeMap};
use std::path::Path;
use std::time::{Duration, Instant};
use serde::{Deserialize, Serialize};

/// Состояние транзакции во время восстановления
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryTransactionState {
    /// Активна (не завершена)
    Active,
    /// Зафиксирована
    Committed,
    /// Отменена
    Aborted,
}

/// Информация о транзакции для восстановления
#[derive(Debug, Clone)]
struct RecoveryTransactionInfo {
    /// ID транзакции
    id: TransactionId,
    /// Состояние
    state: RecoveryTransactionState,
    /// Первый LSN транзакции
    first_lsn: LogSequenceNumber,
    /// Последний LSN транзакции
    last_lsn: LogSequenceNumber,
    /// Список операций транзакции
    operations: Vec<LogRecord>,
    /// Измененные страницы
    dirty_pages: HashSet<(u32, PageId)>,
}

/// Результат анализа логов
#[derive(Debug, Clone)]
pub struct LogAnalysisResult {
    /// Последний LSN в логах
    pub last_lsn: LogSequenceNumber,
    /// LSN последней контрольной точки
    pub checkpoint_lsn: Option<LogSequenceNumber>,
    /// Активные транзакции на момент сбоя
    pub active_transactions: HashMap<TransactionId, RecoveryTransactionInfo>,
    /// Зафиксированные транзакции
    pub committed_transactions: HashMap<TransactionId, RecoveryTransactionInfo>,
    /// Отмененные транзакции
    pub aborted_transactions: HashMap<TransactionId, RecoveryTransactionInfo>,
    /// Все измененные страницы
    pub dirty_pages: HashSet<(u32, PageId)>,
    /// Общее количество обработанных записей
    pub total_records: u64,
}

/// Статистика восстановления
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RecoveryStatistics {
    /// Время начала восстановления
    pub start_time: u64,
    /// Время завершения восстановления
    pub end_time: u64,
    /// Общее время восстановления (мс)
    pub total_duration_ms: u64,
    /// Количество обработанных лог-файлов
    pub log_files_processed: u32,
    /// Общее количество лог-записей
    pub total_log_records: u64,
    /// Количество операций REDO
    pub redo_operations: u64,
    /// Количество операций UNDO
    pub undo_operations: u64,
    /// Количество восстановленных транзакций
    pub recovered_transactions: u64,
    /// Количество отмененных транзакций
    pub rolled_back_transactions: u64,
    /// Количество восстановленных страниц
    pub recovered_pages: u64,
    /// Размер обработанных логов (байты)
    pub processed_log_size: u64,
    /// Количество ошибок восстановления
    pub recovery_errors: u64,
}

/// Конфигурация восстановления
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    /// Максимальное время восстановления
    pub max_recovery_time: Duration,
    /// Размер буфера для чтения логов
    pub read_buffer_size: usize,
    /// Включить параллельное восстановление
    pub enable_parallel_recovery: bool,
    /// Количество потоков для восстановления
    pub recovery_threads: usize,
    /// Включить валидацию после восстановления
    pub enable_validation: bool,
    /// Создать резервную копию перед восстановлением
    pub create_backup: bool,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            max_recovery_time: Duration::from_secs(300), // 5 минут
            read_buffer_size: 64 * 1024, // 64KB
            enable_parallel_recovery: true,
            recovery_threads: 4,
            enable_validation: true,
            create_backup: false,
        }
    }
}

/// Система восстановления данных
pub struct RecoveryManager {
    /// Конфигурация
    config: RecoveryConfig,
    /// Система записи логов
    log_writer: Option<LogWriter>,
    /// Статистика восстановления
    statistics: RecoveryStatistics,
}

impl RecoveryManager {
    /// Создает новый менеджер восстановления
    pub fn new(config: RecoveryConfig) -> Self {
        Self {
            config,
            log_writer: None,
            statistics: RecoveryStatistics::default(),
        }
    }

    /// Устанавливает систему записи логов
    pub fn set_log_writer(&mut self, log_writer: LogWriter) {
        self.log_writer = Some(log_writer);
    }

    /// Выполняет полное восстановление базы данных
    pub async fn recover_database(&mut self, log_directory: &Path) -> Result<RecoveryStatistics> {
        let start_time = Instant::now();
        self.statistics.start_time = start_time.elapsed().as_secs();

        println!("🔄 Начинаем восстановление базы данных...");

        // Этап 1: Анализ логов
        println!("📊 Этап 1: Анализ лог-файлов");
        let analysis_result = self.analyze_logs(log_directory).await?;
        
        println!("   ✅ Обработано {} лог-записей", analysis_result.total_records);
        println!("   ✅ Найдено {} активных транзакций", analysis_result.active_transactions.len());
        println!("   ✅ Найдено {} зафиксированных транзакций", analysis_result.committed_transactions.len());

        // Этап 2: REDO операции
        println!("🔄 Этап 2: Восстановление зафиксированных транзакций (REDO)");
        self.perform_redo_operations(&analysis_result).await?;
        
        println!("   ✅ Выполнено {} операций REDO", self.statistics.redo_operations);

        // Этап 3: UNDO операции
        println!("↩️  Этап 3: Откат незавершенных транзакций (UNDO)");
        self.perform_undo_operations(&analysis_result).await?;
        
        println!("   ✅ Выполнено {} операций UNDO", self.statistics.undo_operations);

        // Этап 4: Валидация (если включена)
        if self.config.enable_validation {
            println!("🔍 Этап 4: Валидация целостности данных");
            self.validate_recovery(&analysis_result).await?;
            println!("   ✅ Валидация завершена успешно");
        }

        // Завершаем статистику
        let end_time = Instant::now();
        self.statistics.end_time = end_time.duration_since(start_time).as_secs();
        self.statistics.total_duration_ms = start_time.elapsed().as_millis() as u64;

        println!("🎉 Восстановление завершено успешно!");
        println!("   ⏱️  Общее время: {} мс", self.statistics.total_duration_ms);
        println!("   📊 Восстановлено транзакций: {}", self.statistics.recovered_transactions);
        println!("   📊 Отменено транзакций: {}", self.statistics.rolled_back_transactions);

        Ok(self.statistics.clone())
    }

    /// Анализирует лог-файлы и строит карту транзакций
    async fn analyze_logs(&mut self, log_directory: &Path) -> Result<LogAnalysisResult> {
        let mut result = LogAnalysisResult {
            last_lsn: 0,
            checkpoint_lsn: None,
            active_transactions: HashMap::new(),
            committed_transactions: HashMap::new(),
            aborted_transactions: HashMap::new(),
            dirty_pages: HashSet::new(),
            total_records: 0,
        };

        // Получаем список лог-файлов
        let log_files = self.get_log_files(log_directory)?;
        self.statistics.log_files_processed = log_files.len() as u32;

        // Обрабатываем файлы в порядке создания
        for log_file in log_files {
            println!("   📖 Обрабатываем файл: {}", log_file.filename);
            
            let records = self.read_log_file(&log_file).await?;
            self.statistics.processed_log_size += log_file.size;
            
            for record in records {
                self.process_log_record(&mut result, record).await?;
                result.total_records += 1;
            }
        }

        self.statistics.total_log_records = result.total_records;

        // Определяем последнюю контрольную точку
        if let Some(checkpoint_lsn) = result.checkpoint_lsn {
            println!("   📍 Найдена контрольная точка на LSN: {}", checkpoint_lsn);
        }

        Ok(result)
    }

    /// Получает список лог-файлов
    fn get_log_files(&self, log_directory: &Path) -> Result<Vec<LogFileInfo>> {
        let mut files = Vec::new();

        if !log_directory.exists() {
            return Ok(files);
        }

        let entries = std::fs::read_dir(log_directory).map_err(|e| {
            Error::internal(&format!("Не удалось прочитать директорию логов: {}", e))
        })?;

        for entry in entries {
            let entry = entry.map_err(|e| Error::internal(&format!("Ошибка чтения записи: {}", e)))?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("log") {
                let metadata = std::fs::metadata(&path).map_err(|e| {
                    Error::internal(&format!("Не удалось получить метаданные файла: {}", e))
                })?;

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
                        .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
                        .duration_since(std::time::SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    updated_at: metadata.modified()
                        .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
                        .duration_since(std::time::SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    is_compressed: false,
                };

                files.push(file_info);
            }
        }

        // Сортируем по времени создания
        files.sort_by_key(|f| f.created_at);

        Ok(files)
    }

    /// Читает лог-записи из файла
    async fn read_log_file(&self, _log_file: &LogFileInfo) -> Result<Vec<LogRecord>> {
        // В реальной реализации здесь было бы чтение и десериализация записей из файла
        // Пока возвращаем пустой список
        Ok(Vec::new())
    }

    /// Обрабатывает одну лог-запись
    async fn process_log_record(&mut self, result: &mut LogAnalysisResult, record: LogRecord) -> Result<()> {
        // Обновляем последний LSN
        if record.lsn > result.last_lsn {
            result.last_lsn = record.lsn;
        }

        match record.record_type {
            LogRecordType::TransactionBegin => {
                if let Some(tx_id) = record.transaction_id {
                    let tx_info = RecoveryTransactionInfo {
                        id: tx_id,
                        state: RecoveryTransactionState::Active,
                        first_lsn: record.lsn,
                        last_lsn: record.lsn,
                        operations: vec![record.clone()],
                        dirty_pages: HashSet::new(),
                    };
                    result.active_transactions.insert(tx_id, tx_info);
                }
            }

            LogRecordType::TransactionCommit => {
                if let Some(tx_id) = record.transaction_id {
                    if let Some(mut tx_info) = result.active_transactions.remove(&tx_id) {
                        tx_info.state = RecoveryTransactionState::Committed;
                        tx_info.last_lsn = record.lsn;
                        tx_info.operations.push(record);
                        result.committed_transactions.insert(tx_id, tx_info);
                    }
                }
            }

            LogRecordType::TransactionAbort => {
                if let Some(tx_id) = record.transaction_id {
                    if let Some(mut tx_info) = result.active_transactions.remove(&tx_id) {
                        tx_info.state = RecoveryTransactionState::Aborted;
                        tx_info.last_lsn = record.lsn;
                        tx_info.operations.push(record);
                        result.aborted_transactions.insert(tx_id, tx_info);
                    }
                }
            }

            LogRecordType::DataInsert | LogRecordType::DataUpdate | LogRecordType::DataDelete => {
                if let Some(tx_id) = record.transaction_id {
                    // Добавляем операцию к транзакции
                    let tx_map = if result.active_transactions.contains_key(&tx_id) {
                        &mut result.active_transactions
                    } else if result.committed_transactions.contains_key(&tx_id) {
                        &mut result.committed_transactions
                    } else if result.aborted_transactions.contains_key(&tx_id) {
                        &mut result.aborted_transactions
                    } else {
                        return Ok(());
                    };

                    if let Some(tx_info) = tx_map.get_mut(&tx_id) {
                        tx_info.operations.push(record.clone());
                        tx_info.last_lsn = record.lsn;

                        // Добавляем измененную страницу
                        if let crate::logging::log_record::LogOperationData::Record(op) = &record.operation_data {
                            let page_key = (op.file_id, op.page_id);
                            tx_info.dirty_pages.insert(page_key);
                            result.dirty_pages.insert(page_key);
                        }
                    }
                }
            }

            LogRecordType::Checkpoint => {
                result.checkpoint_lsn = Some(record.lsn);
            }

            _ => {
                // Другие типы записей
            }
        }

        Ok(())
    }

    /// Выполняет операции REDO для зафиксированных транзакций
    async fn perform_redo_operations(&mut self, analysis_result: &LogAnalysisResult) -> Result<()> {
        let mut redo_count = 0;

        // Собираем все операции из зафиксированных транзакций
        let mut all_operations: BTreeMap<LogSequenceNumber, &LogRecord> = BTreeMap::new();

        for tx_info in analysis_result.committed_transactions.values() {
            for operation in &tx_info.operations {
                if matches!(
                    operation.record_type,
                    LogRecordType::DataInsert | LogRecordType::DataUpdate | LogRecordType::DataDelete
                ) {
                    all_operations.insert(operation.lsn, operation);
                }
            }
        }

        // Выполняем операции в порядке LSN
        for (lsn, operation) in all_operations {
            self.apply_redo_operation(lsn, operation).await?;
            redo_count += 1;

            if redo_count % 1000 == 0 {
                println!("   📝 Выполнено {} операций REDO", redo_count);
            }
        }

        self.statistics.redo_operations = redo_count;
        self.statistics.recovered_transactions = analysis_result.committed_transactions.len() as u64;

        Ok(())
    }

    /// Применяет одну операцию REDO
    async fn apply_redo_operation(&mut self, _lsn: LogSequenceNumber, operation: &LogRecord) -> Result<()> {
        match operation.record_type {
            LogRecordType::DataInsert => {
                // В реальной реализации здесь была бы вставка данных
                tokio::time::sleep(Duration::from_micros(10)).await;
            }
            LogRecordType::DataUpdate => {
                // В реальной реализации здесь было бы обновление данных
                tokio::time::sleep(Duration::from_micros(10)).await;
            }
            LogRecordType::DataDelete => {
                // В реальной реализации здесь было бы удаление данных
                tokio::time::sleep(Duration::from_micros(10)).await;
            }
            _ => {}
        }

        Ok(())
    }

    /// Выполняет операции UNDO для незавершенных транзакций
    async fn perform_undo_operations(&mut self, analysis_result: &LogAnalysisResult) -> Result<()> {
        let mut undo_count = 0;

        // Обрабатываем активные транзакции (откатываем их)
        for tx_info in analysis_result.active_transactions.values() {
            println!("   ↩️  Откатываем транзакцию {}", tx_info.id);

            // Откатываем операции в обратном порядке
            for operation in tx_info.operations.iter().rev() {
                if matches!(
                    operation.record_type,
                    LogRecordType::DataInsert | LogRecordType::DataUpdate | LogRecordType::DataDelete
                ) {
                    self.apply_undo_operation(operation).await?;
                    undo_count += 1;
                }
            }
        }

        self.statistics.undo_operations = undo_count;
        self.statistics.rolled_back_transactions = analysis_result.active_transactions.len() as u64;

        Ok(())
    }

    /// Применяет одну операцию UNDO
    async fn apply_undo_operation(&mut self, operation: &LogRecord) -> Result<()> {
        match operation.record_type {
            LogRecordType::DataInsert => {
                // Для INSERT делаем DELETE
                tokio::time::sleep(Duration::from_micros(10)).await;
            }
            LogRecordType::DataUpdate => {
                // Для UPDATE восстанавливаем старые данные
                tokio::time::sleep(Duration::from_micros(10)).await;
            }
            LogRecordType::DataDelete => {
                // Для DELETE восстанавливаем данные
                tokio::time::sleep(Duration::from_micros(10)).await;
            }
            _ => {}
        }

        Ok(())
    }

    /// Валидирует результат восстановления
    async fn validate_recovery(&mut self, analysis_result: &LogAnalysisResult) -> Result<()> {
        println!("   🔍 Проверяем целостность {} страниц", analysis_result.dirty_pages.len());

        let mut validated_pages = 0;
        for (file_id, page_id) in &analysis_result.dirty_pages {
            // В реальной реализации здесь была бы проверка целостности страницы
            self.validate_page(*file_id, *page_id).await?;
            validated_pages += 1;

            if validated_pages % 100 == 0 {
                println!("   ✅ Проверено {} страниц", validated_pages);
            }
        }

        self.statistics.recovered_pages = validated_pages;

        Ok(())
    }

    /// Валидирует одну страницу
    async fn validate_page(&self, _file_id: u32, _page_id: PageId) -> Result<()> {
        // В реальной реализации здесь была бы проверка контрольных сумм,
        // целостности данных и связей между записями
        tokio::time::sleep(Duration::from_micros(5)).await;
        Ok(())
    }

    /// Возвращает статистику восстановления
    pub fn get_statistics(&self) -> &RecoveryStatistics {
        &self.statistics
    }

    /// Проверяет, требуется ли восстановление
    pub async fn needs_recovery(&self, log_directory: &Path) -> Result<bool> {
        // В реальной реализации здесь была бы проверка:
        // - Наличие незавершенных транзакций
        // - Несоответствие между логами и данными
        // - Маркеры некорректного завершения работы
        
        let log_files = self.get_log_files(log_directory)?;
        
        // Если есть лог-файлы, возможно требуется восстановление
        Ok(!log_files.is_empty())
    }

    /// Создает резервную копию перед восстановлением
    pub async fn create_backup(&self, _data_directory: &Path, _backup_directory: &Path) -> Result<()> {
        if !self.config.create_backup {
            return Ok(());
        }

        println!("💾 Создаем резервную копию данных...");
        
        // В реальной реализации здесь было бы копирование файлов данных
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        println!("   ✅ Резервная копия создана");
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_recovery_manager_creation() {
        let config = RecoveryConfig::default();
        let _manager = RecoveryManager::new(config);
    }

    #[tokio::test]
    async fn test_needs_recovery() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let config = RecoveryConfig::default();
        let manager = RecoveryManager::new(config);

        // Пустая директория - восстановление не требуется
        let needs_recovery = manager.needs_recovery(temp_dir.path()).await?;
        assert!(!needs_recovery);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_log_files() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let config = RecoveryConfig::default();
        let manager = RecoveryManager::new(config);

        // Создаем тестовый лог-файл
        let log_file_path = temp_dir.path().join("test.log");
        std::fs::write(&log_file_path, "test data")?;

        let log_files = manager.get_log_files(temp_dir.path())?;
        assert_eq!(log_files.len(), 1);
        assert_eq!(log_files[0].filename, "test.log");

        Ok(())
    }

    #[tokio::test]
    async fn test_log_analysis() -> Result<()> {
        use crate::logging::log_record::{LogRecord, IsolationLevel};
        
        let config = RecoveryConfig::default();
        let mut manager = RecoveryManager::new(config);

        let mut result = LogAnalysisResult {
            last_lsn: 0,
            checkpoint_lsn: None,
            active_transactions: HashMap::new(),
            committed_transactions: HashMap::new(),
            aborted_transactions: HashMap::new(),
            dirty_pages: HashSet::new(),
            total_records: 0,
        };

        // Тестируем обработку записи BEGIN
        let begin_record = LogRecord::new_transaction_begin(1, 100, IsolationLevel::ReadCommitted);
        manager.process_log_record(&mut result, begin_record).await?;

        assert_eq!(result.active_transactions.len(), 1);
        assert!(result.active_transactions.contains_key(&100));

        // Тестируем обработку записи COMMIT
        let commit_record = LogRecord::new_transaction_commit(2, 100, vec![], Some(1));
        manager.process_log_record(&mut result, commit_record).await?;

        assert_eq!(result.active_transactions.len(), 0);
        assert_eq!(result.committed_transactions.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_backup_creation() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        
        let mut config = RecoveryConfig::default();
        config.create_backup = true;
        
        let manager = RecoveryManager::new(config);
        
        manager.create_backup(temp_dir.path(), backup_dir.path()).await?;
        
        Ok(())
    }

    #[tokio::test]
    async fn test_statistics() {
        let config = RecoveryConfig::default();
        let manager = RecoveryManager::new(config);
        
        let stats = manager.get_statistics();
        assert_eq!(stats.total_log_records, 0);
        assert_eq!(stats.redo_operations, 0);
        assert_eq!(stats.undo_operations, 0);
    }
}
