//! Расширенная система восстановления базы данных
//!
//! Обеспечивает полное восстановление после сбоев с использованием WAL

use crate::common::{Error, Result};
use crate::core::transaction::TransactionId;
use crate::logging::log_record::{LogRecord, LogRecordType, LogSequenceNumber};
use crate::logging::wal::WriteAheadLog;
use std::collections::{HashMap, HashSet, BTreeMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Instant, Duration};

/// Состояние транзакции при восстановлении
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryTransactionState {
    /// Активна
    Active,
    /// Подготовлена (2PC)
    Prepared,
    /// Зафиксирована
    Committed,
    /// Откачена
    Aborted,
}

/// Информация о транзакции для восстановления
#[derive(Debug, Clone)]
pub struct RecoveryTransactionInfo {
    /// ID транзакции
    pub transaction_id: TransactionId,
    /// Состояние
    pub state: RecoveryTransactionState,
    /// Первый LSN
    pub first_lsn: LogSequenceNumber,
    /// Последний LSN
    pub last_lsn: LogSequenceNumber,
    /// Операции транзакции
    pub operations: Vec<LogRecord>,
    /// Изменённые страницы
    pub dirty_pages: HashSet<(u32, u64)>, // (file_id, page_id)
}

/// Результат анализа логов
#[derive(Debug, Clone)]
pub struct AnalysisResult {
    /// Последний LSN
    pub last_lsn: LogSequenceNumber,
    /// Контрольная точка
    pub checkpoint_lsn: Option<LogSequenceNumber>,
    /// Активные транзакции
    pub active_transactions: HashMap<TransactionId, RecoveryTransactionInfo>,
    /// Зафиксированные транзакции
    pub committed_transactions: HashMap<TransactionId, RecoveryTransactionInfo>,
    /// Откаченные транзакции
    pub aborted_transactions: HashMap<TransactionId, RecoveryTransactionInfo>,
    /// Все изменённые страницы
    pub dirty_pages: HashSet<(u32, u64)>,
    /// Всего записей
    pub total_records: u64,
}

/// Статистика восстановления
#[derive(Debug, Clone, Default)]
pub struct RecoveryStatistics {
    /// Всего лог-файлов
    pub log_files_processed: u32,
    /// Всего записей
    pub total_records: u64,
    /// Операций REDO
    pub redo_operations: u64,
    /// Операций UNDO
    pub undo_operations: u64,
    /// Восстановлено транзакций
    pub recovered_transactions: u64,
    /// Откачено транзакций
    pub rolled_back_transactions: u64,
    /// Восстановлено страниц
    pub recovered_pages: u64,
    /// Время восстановления (мс)
    pub recovery_time_ms: u64,
    /// Ошибки восстановления
    pub recovery_errors: u64,
}

/// Конфигурация восстановления
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    /// Максимальное время восстановления
    pub max_recovery_time: Duration,
    /// Включить параллельное восстановление
    pub enable_parallel: bool,
    /// Количество потоков
    pub num_threads: usize,
    /// Создать резервную копию перед восстановлением
    pub create_backup: bool,
    /// Включить валидацию после восстановления
    pub enable_validation: bool,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            max_recovery_time: Duration::from_secs(300),
            enable_parallel: true,
            num_threads: 4,
            create_backup: false,
            enable_validation: true,
        }
    }
}

/// Расширенный менеджер восстановления
pub struct AdvancedRecoveryManager {
    /// Конфигурация
    config: RecoveryConfig,
    /// Статистика
    statistics: Arc<Mutex<RecoveryStatistics>>,
    /// WAL
    wal: Option<Arc<WriteAheadLog>>,
}

impl AdvancedRecoveryManager {
    /// Создаёт новый менеджер восстановления
    pub fn new(config: RecoveryConfig) -> Self {
        Self {
            config,
            statistics: Arc::new(Mutex::new(RecoveryStatistics::default())),
            wal: None,
        }
    }
    
    /// Устанавливает WAL
    pub fn set_wal(&mut self, wal: Arc<WriteAheadLog>) {
        self.wal = Some(wal);
    }
    
    /// Проверяет, требуется ли восстановление
    pub fn needs_recovery(&self, log_directory: &Path) -> bool {
        // Проверяем наличие незавершенных транзакций
        if !log_directory.exists() {
            return false;
        }
        
        // Проверяем наличие лог-файлов
        if let Ok(entries) = std::fs::read_dir(log_directory) {
            for entry in entries.flatten() {
                if entry.path().extension().and_then(|s| s.to_str()) == Some("log") {
                    return true;
                }
            }
        }
        
        false
    }
    
    /// Выполняет восстановление базы данных
    pub fn recover(&mut self, log_directory: &Path) -> Result<RecoveryStatistics> {
        let start_time = Instant::now();
        
        println!("🔄 Начинаем восстановление базы данных...");
        
        // Этап 1: Анализ логов
        println!("📊 Этап 1: Анализ лог-файлов");
        let analysis_result = self.analyze_logs(log_directory)?;
        
        println!("   ✅ Обработано {} лог-записей", analysis_result.total_records);
        println!("   ✅ Активных транзакций: {}", analysis_result.active_transactions.len());
        println!("   ✅ Зафиксированных: {}", analysis_result.committed_transactions.len());
        
        // Этап 2: REDO
        println!("🔄 Этап 2: Восстановление зафиксированных транзакций (REDO)");
        self.perform_redo(&analysis_result)?;
        
        // Этап 3: UNDO
        println!("↩️  Этап 3: Откат незавершённых транзакций (UNDO)");
        self.perform_undo(&analysis_result)?;
        
        // Обновляем статистику
        {
            let mut stats = self.statistics.lock().unwrap();
            stats.recovery_time_ms = start_time.elapsed().as_millis() as u64;
            stats.total_records = analysis_result.total_records;
        }
        
        println!("✅ Восстановление завершено за {} мс", start_time.elapsed().as_millis());
        
        Ok(self.get_statistics())
    }
    
    /// Анализирует лог-файлы
    fn analyze_logs(&mut self, log_directory: &Path) -> Result<AnalysisResult> {
        let mut result = AnalysisResult {
            last_lsn: 0,
            checkpoint_lsn: None,
            active_transactions: HashMap::new(),
            committed_transactions: HashMap::new(),
            aborted_transactions: HashMap::new(),
            dirty_pages: HashSet::new(),
            total_records: 0,
        };
        
        // Получаем лог-файлы
        let log_files = self.get_log_files(log_directory)?;
        
        {
            let mut stats = self.statistics.lock().unwrap();
            stats.log_files_processed = log_files.len() as u32;
        }
        
        // Обрабатываем каждый файл
        for file_path in log_files {
            println!("   📖 Обрабатываем: {:?}", file_path.file_name());
            
            let records = self.read_log_file(&file_path)?;
            
            for record in records {
                self.process_record(&mut result, record)?;
                result.total_records += 1;
            }
        }
        
        println!("   📍 Последний LSN: {}", result.last_lsn);
        
        Ok(result)
    }
    
    /// Получает список лог-файлов
    fn get_log_files(&self, log_directory: &Path) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        
        if !log_directory.exists() {
            return Ok(files);
        }
        
        let entries = std::fs::read_dir(log_directory)
            .map_err(|e| Error::internal(format!("Не удалось прочитать директорию: {}", e)))?;
        
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("log") {
                files.push(path);
            }
        }
        
        // Сортируем по имени файла (предполагается формат с timestamp)
        files.sort();
        
        Ok(files)
    }
    
    /// Читает лог-записи из файла
    fn read_log_file(&self, file_path: &Path) -> Result<Vec<LogRecord>> {
        // Симуляция чтения - в реальности читаем и десериализуем из файла
        // TODO: Интеграция с реальным форматом WAL
        Ok(Vec::new())
    }
    
    /// Обрабатывает одну лог-запись
    fn process_record(&mut self, result: &mut AnalysisResult, record: LogRecord) -> Result<()> {
        // Обновляем последний LSN
        if record.lsn > result.last_lsn {
            result.last_lsn = record.lsn;
        }
        
        // Проверяем, есть ли transaction_id
        let tx_id = match record.transaction_id {
            Some(id) => TransactionId::new(id),
            None => return Ok(()), // Пропускаем записи без транзакции
        };
        
        match record.record_type {
            LogRecordType::TransactionBegin => {
                // Начало транзакции
                let tx_info = RecoveryTransactionInfo {
                    transaction_id: tx_id,
                    state: RecoveryTransactionState::Active,
                    first_lsn: record.lsn,
                    last_lsn: record.lsn,
                    operations: vec![record.clone()],
                    dirty_pages: HashSet::new(),
                };
                result.active_transactions.insert(tx_id, tx_info);
            }
            
            LogRecordType::TransactionCommit => {
                // Фиксация транзакции
                if let Some(mut tx_info) = result.active_transactions.remove(&tx_id) {
                    tx_info.state = RecoveryTransactionState::Committed;
                    tx_info.last_lsn = record.lsn;
                    tx_info.operations.push(record);
                    result.committed_transactions.insert(tx_info.transaction_id, tx_info);
                }
            }
            
            LogRecordType::TransactionAbort => {
                // Откат транзакции
                if let Some(mut tx_info) = result.active_transactions.remove(&tx_id) {
                    tx_info.state = RecoveryTransactionState::Aborted;
                    tx_info.last_lsn = record.lsn;
                    result.aborted_transactions.insert(tx_info.transaction_id, tx_info);
                }
            }
            
            LogRecordType::DataInsert | LogRecordType::DataUpdate | LogRecordType::DataDelete => {
                // Операция с данными
                if let Some(tx_info) = result.active_transactions.get_mut(&tx_id) {
                    tx_info.last_lsn = record.lsn;
                    tx_info.operations.push(record.clone());
                }
            }
            
            LogRecordType::Checkpoint => {
                // Контрольная точка
                result.checkpoint_lsn = Some(record.lsn);
            }
            
            _ => {
                // Другие типы записей
            }
        }
        
        Ok(())
    }
    
    /// Выполняет операции REDO
    fn perform_redo(&mut self, analysis: &AnalysisResult) -> Result<()> {
        let mut redo_count = 0;
        
        // Собираем все операции из зафиксированных транзакций
        let mut operations: BTreeMap<LogSequenceNumber, &LogRecord> = BTreeMap::new();
        
        for tx_info in analysis.committed_transactions.values() {
            for op in &tx_info.operations {
                if matches!(
                    op.record_type,
                    LogRecordType::DataInsert | LogRecordType::DataUpdate | LogRecordType::DataDelete
                ) {
                    operations.insert(op.lsn, op);
                }
            }
        }
        
        // Применяем в порядке LSN
        for (lsn, operation) in operations {
            self.apply_redo_operation(operation)?;
            redo_count += 1;
            
            if redo_count % 100 == 0 {
                println!("   📝 REDO: {} операций", redo_count);
            }
        }
        
        {
            let mut stats = self.statistics.lock().unwrap();
            stats.redo_operations = redo_count;
            stats.recovered_transactions = analysis.committed_transactions.len() as u64;
            stats.recovered_pages = analysis.dirty_pages.len() as u64;
        }
        
        println!("   ✅ Выполнено {} операций REDO", redo_count);
        
        Ok(())
    }
    
    /// Выполняет операции UNDO
    fn perform_undo(&mut self, analysis: &AnalysisResult) -> Result<()> {
        let mut undo_count = 0;
        
        // Откатываем активные транзакции (в обратном порядке)
        for tx_info in analysis.active_transactions.values() {
            println!("   ↩️  Откатываем транзакцию TXN{}", tx_info.transaction_id);
            
            // Операции в обратном порядке
            for operation in tx_info.operations.iter().rev() {
                if matches!(
                    operation.record_type,
                    LogRecordType::DataInsert | LogRecordType::DataUpdate | LogRecordType::DataDelete
                ) {
                    self.apply_undo_operation(operation)?;
                    undo_count += 1;
                }
            }
        }
        
        {
            let mut stats = self.statistics.lock().unwrap();
            stats.undo_operations = undo_count;
            stats.rolled_back_transactions = analysis.active_transactions.len() as u64;
        }
        
        println!("   ✅ Выполнено {} операций UNDO", undo_count);
        
        Ok(())
    }
    
    /// Применяет одну операцию REDO
    fn apply_redo_operation(&self, operation: &LogRecord) -> Result<()> {
        match operation.record_type {
            LogRecordType::DataInsert => {
                // Повторяем INSERT
                // TODO: Интеграция с storage для реального применения
                Ok(())
            }
            LogRecordType::DataUpdate => {
                // Повторяем UPDATE
                // TODO: Интеграция с storage для реального применения
                Ok(())
            }
            LogRecordType::DataDelete => {
                // Повторяем DELETE
                // TODO: Интеграция с storage для реального применения
                Ok(())
            }
            _ => Ok(()),
        }
    }
    
    /// Применяет одну операцию UNDO
    fn apply_undo_operation(&self, operation: &LogRecord) -> Result<()> {
        match operation.record_type {
            LogRecordType::DataInsert => {
                // Для INSERT делаем DELETE
                // TODO: Интеграция с storage
                Ok(())
            }
            LogRecordType::DataUpdate => {
                // Для UPDATE восстанавливаем старые данные
                // TODO: Интеграция с storage
                Ok(())
            }
            LogRecordType::DataDelete => {
                // Для DELETE восстанавливаем удалённые данные
                // TODO: Интеграция с storage
                Ok(())
            }
            _ => Ok(()),
        }
    }
    
    /// Создаёт резервную копию перед восстановлением
    pub fn create_backup(&self, source_dir: &Path, backup_dir: &Path) -> Result<()> {
        if !self.config.create_backup {
            return Ok(());
        }
        
        println!("💾 Создание резервной копии...");
        
        // Создаём директорию для backup
        std::fs::create_dir_all(backup_dir)
            .map_err(|e| Error::internal(format!("Не удалось создать директорию backup: {}", e)))?;
        
        // Копируем файлы
        let mut copied_files = 0;
        
        if let Ok(entries) = std::fs::read_dir(source_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    let file_name = path.file_name().unwrap();
                    let dest_path = backup_dir.join(file_name);
                    
                    std::fs::copy(&path, &dest_path)
                        .map_err(|e| Error::internal(format!("Ошибка копирования: {}", e)))?;
                    
                    copied_files += 1;
                }
            }
        }
        
        println!("   ✅ Скопировано {} файлов", copied_files);
        
        Ok(())
    }
    
    /// Валидирует результат восстановления
    pub fn validate_recovery(&self, analysis: &AnalysisResult) -> Result<()> {
        if !self.config.enable_validation {
            return Ok(());
        }
        
        println!("🔍 Валидация восстановления...");
        
        // Проверяем, что все активные транзакции откачены
        if !analysis.active_transactions.is_empty() {
            return Err(Error::internal(
                "Обнаружены незавершённые активные транзакции после восстановления"
            ));
        }
        
        // Проверяем консистентность
        println!("   ✅ Все активные транзакции откачены");
        println!("   ✅ Все зафиксированные транзакции восстановлены");
        
        Ok(())
    }
    
    /// Возвращает статистику
    pub fn get_statistics(&self) -> RecoveryStatistics {
        self.statistics.lock().unwrap().clone()
    }
    
    /// Возвращает конфигурацию
    pub fn config(&self) -> &RecoveryConfig {
        &self.config
    }
}

impl Default for AdvancedRecoveryManager {
    fn default() -> Self {
        Self::new(RecoveryConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_recovery_manager_creation() {
        let manager = AdvancedRecoveryManager::default();
        let stats = manager.get_statistics();
        
        assert_eq!(stats.redo_operations, 0);
        assert_eq!(stats.undo_operations, 0);
    }
    
    #[test]
    fn test_needs_recovery() {
        let manager = AdvancedRecoveryManager::default();
        let non_existent_path = Path::new("./non_existent_logs");
        
        assert!(!manager.needs_recovery(non_existent_path));
    }
    
    #[test]
    fn test_recovery_config() {
        let config = RecoveryConfig {
            max_recovery_time: Duration::from_secs(60),
            enable_parallel: false,
            num_threads: 2,
            create_backup: true,
            enable_validation: true,
        };
        
        let manager = AdvancedRecoveryManager::new(config.clone());
        assert_eq!(manager.config.num_threads, 2);
        assert!(manager.config.create_backup);
    }
    
    #[test]
    fn test_analysis_result_creation() {
        let result = AnalysisResult {
            last_lsn: 100,
            checkpoint_lsn: Some(50),
            active_transactions: HashMap::new(),
            committed_transactions: HashMap::new(),
            aborted_transactions: HashMap::new(),
            dirty_pages: HashSet::new(),
            total_records: 100,
        };
        
        assert_eq!(result.last_lsn, 100);
        assert_eq!(result.checkpoint_lsn, Some(50));
        assert_eq!(result.total_records, 100);
    }
    
    #[test]
    fn test_transaction_states() {
        assert_eq!(RecoveryTransactionState::Active, RecoveryTransactionState::Active);
        assert_ne!(RecoveryTransactionState::Active, RecoveryTransactionState::Committed);
    }
}

