//! Структуры лог-записей для системы логирования RustBD
//!
//! Этот модуль определяет различные типы лог-записей для отслеживания
//! всех изменений в базе данных:
//! - Операции с данными (INSERT, UPDATE, DELETE)
//! - Транзакционные операции (BEGIN, COMMIT, ABORT)
//! - Системные операции (CHECKPOINT, COMPACTION)
//! - Метаданные и восстановление

use crate::common::{Error, Result};
use crate::storage::database_file::PageId;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Уникальный идентификатор лог-записи (Log Sequence Number)
pub type LogSequenceNumber = u64;

/// Идентификатор транзакции
pub type TransactionId = u64;

/// Тип лог-записи
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogRecordType {
    /// Начало транзакции
    TransactionBegin,
    /// Подтверждение транзакции
    TransactionCommit,
    /// Отмена транзакции
    TransactionAbort,
    /// Вставка данных
    DataInsert,
    /// Обновление данных
    DataUpdate,
    /// Удаление данных
    DataDelete,
    /// Создание контрольной точки
    Checkpoint,
    /// Завершение контрольной точки
    CheckpointEnd,
    /// Операция сжатия логов
    Compaction,
    /// Создание файла
    FileCreate,
    /// Удаление файла
    FileDelete,
    /// Расширение файла
    FileExtend,
    /// Изменение метаданных
    MetadataUpdate,
}

/// Приоритет лог-записи
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LogPriority {
    /// Низкий приоритет (фоновые операции)
    Low = 0,
    /// Нормальный приоритет (обычные операции)
    Normal = 1,
    /// Высокий приоритет (пользовательские транзакции)
    High = 2,
    /// Критический приоритет (системные операции)
    Critical = 3,
}

/// Данные операции с записью
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordOperation {
    /// ID файла
    pub file_id: u32,
    /// ID страницы
    pub page_id: PageId,
    /// Смещение записи на странице
    pub record_offset: u16,
    /// Размер записи
    pub record_size: u16,
    /// Старые данные (для UNDO)
    pub old_data: Option<Vec<u8>>,
    /// Новые данные (для REDO)
    pub new_data: Option<Vec<u8>>,
}

/// Данные транзакционной операции
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionOperation {
    /// Список измененных страниц
    pub dirty_pages: Vec<(u32, PageId)>,
    /// Список заблокированных ресурсов
    pub locked_resources: Vec<String>,
    /// Время начала транзакции
    pub start_time: u64,
    /// Уровень изоляции
    pub isolation_level: IsolationLevel,
}

/// Уровень изоляции транзакции
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsolationLevel {
    /// Чтение незафиксированных данных
    ReadUncommitted,
    /// Чтение зафиксированных данных
    ReadCommitted,
    /// Повторяемое чтение
    RepeatableRead,
    /// Сериализуемость
    Serializable,
}

/// Данные операции с файлом
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileOperation {
    /// ID файла
    pub file_id: u32,
    /// Имя файла
    pub filename: String,
    /// Тип файла
    pub file_type: crate::storage::database_file::DatabaseFileType,
    /// Размер файла (в страницах)
    pub file_size: u64,
    /// Дополнительные параметры
    pub parameters: std::collections::HashMap<String, String>,
}

/// Данные операции контрольной точки
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointOperation {
    /// ID контрольной точки
    pub checkpoint_id: u64,
    /// Список активных транзакций
    pub active_transactions: Vec<TransactionId>,
    /// Список измененных страниц
    pub dirty_pages: Vec<(u32, PageId)>,
    /// LSN последней записи
    pub last_lsn: LogSequenceNumber,
    /// Время создания
    pub timestamp: u64,
}

/// Основная структура лог-записи
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogRecord {
    /// Уникальный номер лог-записи (LSN)
    pub lsn: LogSequenceNumber,
    /// ID транзакции (если применимо)
    pub transaction_id: Option<TransactionId>,
    /// Тип операции
    pub record_type: LogRecordType,
    /// Приоритет записи
    pub priority: LogPriority,
    /// Время создания записи
    pub timestamp: u64,
    /// Размер записи в байтах
    pub record_size: u32,
    /// Контрольная сумма записи
    pub checksum: u32,
    /// Данные операции (в зависимости от типа)
    pub operation_data: LogOperationData,
    /// LSN предыдущей записи той же транзакции
    pub prev_lsn: Option<LogSequenceNumber>,
    /// Дополнительные метаданные
    pub metadata: std::collections::HashMap<String, String>,
}

/// Данные операции в лог-записи
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogOperationData {
    /// Операция с записью
    Record(RecordOperation),
    /// Транзакционная операция
    Transaction(TransactionOperation),
    /// Операция с файлом
    File(FileOperation),
    /// Операция контрольной точки
    Checkpoint(CheckpointOperation),
    /// Пустые данные (для простых операций)
    Empty,
    /// Произвольные данные
    Raw(Vec<u8>),
}

impl LogRecord {
    /// Создает новую лог-запись
    pub fn new(
        lsn: LogSequenceNumber,
        record_type: LogRecordType,
        operation_data: LogOperationData,
    ) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut record = Self {
            lsn,
            transaction_id: None,
            record_type,
            priority: LogPriority::Normal,
            timestamp,
            record_size: 0,
            checksum: 0,
            operation_data,
            prev_lsn: None,
            metadata: std::collections::HashMap::new(),
        };

        // Вычисляем размер и контрольную сумму
        record.update_size_and_checksum();
        record
    }

    /// Создает лог-запись для начала транзакции
    pub fn new_transaction_begin(
        lsn: LogSequenceNumber,
        transaction_id: TransactionId,
        isolation_level: IsolationLevel,
    ) -> Self {
        let transaction_op = TransactionOperation {
            dirty_pages: Vec::new(),
            locked_resources: Vec::new(),
            start_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            isolation_level,
        };

        let mut record = Self::new(
            lsn,
            LogRecordType::TransactionBegin,
            LogOperationData::Transaction(transaction_op),
        );
        record.transaction_id = Some(transaction_id);
        record.priority = LogPriority::High;
        record
    }

    /// Создает лог-запись для подтверждения транзакции
    pub fn new_transaction_commit(
        lsn: LogSequenceNumber,
        transaction_id: TransactionId,
        dirty_pages: Vec<(u32, PageId)>,
        prev_lsn: Option<LogSequenceNumber>,
    ) -> Self {
        let transaction_op = TransactionOperation {
            dirty_pages,
            locked_resources: Vec::new(),
            start_time: 0,
            isolation_level: IsolationLevel::ReadCommitted,
        };

        let mut record = Self::new(
            lsn,
            LogRecordType::TransactionCommit,
            LogOperationData::Transaction(transaction_op),
        );
        record.transaction_id = Some(transaction_id);
        record.prev_lsn = prev_lsn;
        record.priority = LogPriority::Critical;
        record
    }

    /// Создает лог-запись для отмены транзакции
    pub fn new_transaction_abort(
        lsn: LogSequenceNumber,
        transaction_id: TransactionId,
        prev_lsn: Option<LogSequenceNumber>,
    ) -> Self {
        let mut record = Self::new(
            lsn,
            LogRecordType::TransactionAbort,
            LogOperationData::Empty,
        );
        record.transaction_id = Some(transaction_id);
        record.prev_lsn = prev_lsn;
        record.priority = LogPriority::Critical;
        record
    }

    /// Создает лог-запись для вставки данных
    pub fn new_data_insert(
        lsn: LogSequenceNumber,
        transaction_id: TransactionId,
        file_id: u32,
        page_id: PageId,
        record_offset: u16,
        new_data: Vec<u8>,
        prev_lsn: Option<LogSequenceNumber>,
    ) -> Self {
        let record_op = RecordOperation {
            file_id,
            page_id,
            record_offset,
            record_size: new_data.len() as u16,
            old_data: None,
            new_data: Some(new_data),
        };

        let mut record = Self::new(
            lsn,
            LogRecordType::DataInsert,
            LogOperationData::Record(record_op),
        );
        record.transaction_id = Some(transaction_id);
        record.prev_lsn = prev_lsn;
        record.priority = LogPriority::High;
        record
    }

    /// Создает лог-запись для обновления данных
    pub fn new_data_update(
        lsn: LogSequenceNumber,
        transaction_id: TransactionId,
        file_id: u32,
        page_id: PageId,
        record_offset: u16,
        old_data: Vec<u8>,
        new_data: Vec<u8>,
        prev_lsn: Option<LogSequenceNumber>,
    ) -> Self {
        let record_op = RecordOperation {
            file_id,
            page_id,
            record_offset,
            record_size: new_data.len() as u16,
            old_data: Some(old_data),
            new_data: Some(new_data),
        };

        let mut record = Self::new(
            lsn,
            LogRecordType::DataUpdate,
            LogOperationData::Record(record_op),
        );
        record.transaction_id = Some(transaction_id);
        record.prev_lsn = prev_lsn;
        record.priority = LogPriority::High;
        record
    }

    /// Создает лог-запись для удаления данных
    pub fn new_data_delete(
        lsn: LogSequenceNumber,
        transaction_id: TransactionId,
        file_id: u32,
        page_id: PageId,
        record_offset: u16,
        old_data: Vec<u8>,
        prev_lsn: Option<LogSequenceNumber>,
    ) -> Self {
        let record_op = RecordOperation {
            file_id,
            page_id,
            record_offset,
            record_size: old_data.len() as u16,
            old_data: Some(old_data),
            new_data: None,
        };

        let mut record = Self::new(
            lsn,
            LogRecordType::DataDelete,
            LogOperationData::Record(record_op),
        );
        record.transaction_id = Some(transaction_id);
        record.prev_lsn = prev_lsn;
        record.priority = LogPriority::High;
        record
    }

    /// Создает лог-запись для контрольной точки
    pub fn new_checkpoint(
        lsn: LogSequenceNumber,
        checkpoint_id: u64,
        active_transactions: Vec<TransactionId>,
        dirty_pages: Vec<(u32, PageId)>,
        last_lsn: LogSequenceNumber,
    ) -> Self {
        let checkpoint_op = CheckpointOperation {
            checkpoint_id,
            active_transactions,
            dirty_pages,
            last_lsn,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        let mut record = Self::new(
            lsn,
            LogRecordType::Checkpoint,
            LogOperationData::Checkpoint(checkpoint_op),
        );
        record.priority = LogPriority::Critical;
        record
    }

    /// Обновляет размер записи и контрольную сумму
    fn update_size_and_checksum(&mut self) {
        // Сериализуем запись для вычисления размера
        if let Ok(serialized) = bincode::serialize(self) {
            self.record_size = serialized.len() as u32;
            self.checksum = Self::calculate_checksum(&serialized);
        }
    }

    /// Вычисляет контрольную сумму данных
    fn calculate_checksum(data: &[u8]) -> u32 {
        // Простая контрольная сумма CRC32
        let mut checksum = 0u32;
        for &byte in data {
            checksum = checksum.wrapping_mul(31).wrapping_add(byte as u32);
        }
        checksum
    }

    /// Проверяет целостность лог-записи
    pub fn verify_checksum(&self) -> bool {
        // Создаем копию записи с нулевой контрольной суммой
        let mut temp_record = self.clone();
        temp_record.checksum = 0;
        
        if let Ok(serialized) = bincode::serialize(&temp_record) {
            let calculated_checksum = Self::calculate_checksum(&serialized);
            calculated_checksum == self.checksum
        } else {
            false
        }
    }

    /// Возвращает размер записи в байтах
    pub fn size(&self) -> u32 {
        self.record_size
    }

    /// Проверяет, является ли запись транзакционной
    pub fn is_transactional(&self) -> bool {
        matches!(
            self.record_type,
            LogRecordType::TransactionBegin
                | LogRecordType::TransactionCommit
                | LogRecordType::TransactionAbort
                | LogRecordType::DataInsert
                | LogRecordType::DataUpdate
                | LogRecordType::DataDelete
        )
    }

    /// Проверяет, требует ли запись немедленной записи на диск
    pub fn requires_immediate_flush(&self) -> bool {
        matches!(
            self.record_type,
            LogRecordType::TransactionCommit
                | LogRecordType::TransactionAbort
                | LogRecordType::Checkpoint
                | LogRecordType::CheckpointEnd
        ) || self.priority >= LogPriority::Critical
    }

    /// Добавляет метаданные к записи
    pub fn add_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
        self.update_size_and_checksum();
    }

    /// Получает метаданные записи
    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }

    /// Сериализует запись в байты
    pub fn serialize(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| Error::internal(&format!("Ошибка сериализации лог-записи: {}", e)))
    }

    /// Десериализует запись из байтов
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        bincode::deserialize(data).map_err(|e| Error::internal(&format!("Ошибка десериализации лог-записи: {}", e)))
    }

    /// Возвращает читаемое описание записи
    pub fn description(&self) -> String {
        match &self.record_type {
            LogRecordType::TransactionBegin => {
                format!("BEGIN TRANSACTION {}", self.transaction_id.unwrap_or(0))
            }
            LogRecordType::TransactionCommit => {
                format!("COMMIT TRANSACTION {}", self.transaction_id.unwrap_or(0))
            }
            LogRecordType::TransactionAbort => {
                format!("ABORT TRANSACTION {}", self.transaction_id.unwrap_or(0))
            }
            LogRecordType::DataInsert => {
                if let LogOperationData::Record(op) = &self.operation_data {
                    format!("INSERT INTO file:{} page:{} offset:{}", op.file_id, op.page_id, op.record_offset)
                } else {
                    "INSERT".to_string()
                }
            }
            LogRecordType::DataUpdate => {
                if let LogOperationData::Record(op) = &self.operation_data {
                    format!("UPDATE file:{} page:{} offset:{}", op.file_id, op.page_id, op.record_offset)
                } else {
                    "UPDATE".to_string()
                }
            }
            LogRecordType::DataDelete => {
                if let LogOperationData::Record(op) = &self.operation_data {
                    format!("DELETE FROM file:{} page:{} offset:{}", op.file_id, op.page_id, op.record_offset)
                } else {
                    "DELETE".to_string()
                }
            }
            LogRecordType::Checkpoint => {
                if let LogOperationData::Checkpoint(op) = &self.operation_data {
                    format!("CHECKPOINT {} (активных транзакций: {})", op.checkpoint_id, op.active_transactions.len())
                } else {
                    "CHECKPOINT".to_string()
                }
            }
            LogRecordType::CheckpointEnd => "CHECKPOINT END".to_string(),
            LogRecordType::Compaction => "LOG COMPACTION".to_string(),
            LogRecordType::FileCreate => "CREATE FILE".to_string(),
            LogRecordType::FileDelete => "DELETE FILE".to_string(),
            LogRecordType::FileExtend => "EXTEND FILE".to_string(),
            LogRecordType::MetadataUpdate => "UPDATE METADATA".to_string(),
        }
    }
}

impl Default for LogRecord {
    fn default() -> Self {
        Self {
            lsn: 0,
            transaction_id: None,
            record_type: LogRecordType::TransactionBegin,
            priority: LogPriority::Normal,
            timestamp: 0,
            record_size: 0,
            checksum: 0,
            operation_data: LogOperationData::Empty,
            prev_lsn: None,
            metadata: std::collections::HashMap::new(),
        }
    }
}

/// Итератор по лог-записям
pub struct LogRecordIterator {
    records: Vec<LogRecord>,
    position: usize,
}

impl LogRecordIterator {
    /// Создает новый итератор
    pub fn new(records: Vec<LogRecord>) -> Self {
        Self {
            records,
            position: 0,
        }
    }

    /// Фильтрует записи по типу
    pub fn filter_by_type(mut self, record_type: LogRecordType) -> Self {
        self.records.retain(|record| record.record_type == record_type);
        self
    }

    /// Фильтрует записи по транзакции
    pub fn filter_by_transaction(mut self, transaction_id: TransactionId) -> Self {
        self.records.retain(|record| record.transaction_id == Some(transaction_id));
        self
    }

    /// Фильтрует записи по временному диапазону
    pub fn filter_by_time_range(mut self, start_time: u64, end_time: u64) -> Self {
        self.records.retain(|record| record.timestamp >= start_time && record.timestamp <= end_time);
        self
    }

    /// Сортирует записи по LSN
    pub fn sort_by_lsn(mut self) -> Self {
        self.records.sort_by_key(|record| record.lsn);
        self
    }

    /// Возвращает количество записей
    pub fn count(&self) -> usize {
        self.records.len()
    }
}

impl Iterator for LogRecordIterator {
    type Item = LogRecord;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position < self.records.len() {
            let record = self.records[self.position].clone();
            self.position += 1;
            Some(record)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_record_creation() {
        let record = LogRecord::new_transaction_begin(1, 100, IsolationLevel::ReadCommitted);
        
        assert_eq!(record.lsn, 1);
        assert_eq!(record.transaction_id, Some(100));
        assert_eq!(record.record_type, LogRecordType::TransactionBegin);
        assert_eq!(record.priority, LogPriority::High);
        assert!(record.is_transactional());
    }

    #[test]
    fn test_data_operations() {
        let insert_record = LogRecord::new_data_insert(
            2, 100, 1, 10, 0, vec![1, 2, 3, 4], None
        );
        
        assert_eq!(insert_record.record_type, LogRecordType::DataInsert);
        assert!(insert_record.is_transactional());
        
        let update_record = LogRecord::new_data_update(
            3, 100, 1, 10, 0, vec![1, 2], vec![5, 6, 7], Some(2)
        );
        
        assert_eq!(update_record.prev_lsn, Some(2));
        
        let delete_record = LogRecord::new_data_delete(
            4, 100, 1, 10, 0, vec![5, 6, 7], Some(3)
        );
        
        assert_eq!(delete_record.record_type, LogRecordType::DataDelete);
    }

    #[test]
    fn test_checkpoint_record() {
        let checkpoint = LogRecord::new_checkpoint(
            5, 1, vec![100, 101], vec![(1, 10), (1, 11)], 4
        );
        
        assert_eq!(checkpoint.record_type, LogRecordType::Checkpoint);
        assert_eq!(checkpoint.priority, LogPriority::Critical);
        assert!(checkpoint.requires_immediate_flush());
    }

    #[test]
    fn test_serialization() {
        let record = LogRecord::new_transaction_commit(2, 100, vec![(1, 10)], Some(1));
        
        let serialized = record.serialize().unwrap();
        let deserialized = LogRecord::deserialize(&serialized).unwrap();
        
        assert_eq!(record.lsn, deserialized.lsn);
        assert_eq!(record.transaction_id, deserialized.transaction_id);
        assert_eq!(record.record_type, deserialized.record_type);
    }

    #[test]
    fn test_checksum_verification() {
        let mut record = LogRecord::new_transaction_begin(1, 100, IsolationLevel::Serializable);
        
        // Проверяем, что контрольная сумма корректна
        assert!(record.verify_checksum());
        
        // Изменяем данные и проверяем, что контрольная сумма становится некорректной
        record.timestamp += 1;
        assert!(!record.verify_checksum());
    }

    #[test]
    fn test_metadata() {
        let mut record = LogRecord::new_transaction_begin(1, 100, IsolationLevel::ReadCommitted);
        
        record.add_metadata("user".to_string(), "admin".to_string());
        record.add_metadata("client_ip".to_string(), "127.0.0.1".to_string());
        
        assert_eq!(record.get_metadata("user"), Some(&"admin".to_string()));
        assert_eq!(record.get_metadata("client_ip"), Some(&"127.0.0.1".to_string()));
        assert_eq!(record.get_metadata("nonexistent"), None);
    }

    #[test]
    fn test_record_iterator() {
        let records = vec![
            LogRecord::new_transaction_begin(1, 100, IsolationLevel::ReadCommitted),
            LogRecord::new_data_insert(2, 100, 1, 10, 0, vec![1, 2, 3], Some(1)),
            LogRecord::new_transaction_commit(3, 100, vec![(1, 10)], Some(2)),
            LogRecord::new_transaction_begin(4, 101, IsolationLevel::Serializable),
        ];
        
        let iterator = LogRecordIterator::new(records);
        
        // Фильтруем по транзакции 100
        let tx100_records: Vec<_> = iterator
            .filter_by_transaction(100)
            .collect();
        
        assert_eq!(tx100_records.len(), 3);
        assert_eq!(tx100_records[0].lsn, 1);
        assert_eq!(tx100_records[1].lsn, 2);
        assert_eq!(tx100_records[2].lsn, 3);
    }

    #[test]
    fn test_record_descriptions() {
        let begin_record = LogRecord::new_transaction_begin(1, 100, IsolationLevel::ReadCommitted);
        assert_eq!(begin_record.description(), "BEGIN TRANSACTION 100");
        
        let insert_record = LogRecord::new_data_insert(2, 100, 1, 10, 5, vec![1, 2, 3], Some(1));
        assert_eq!(insert_record.description(), "INSERT INTO file:1 page:10 offset:5");
        
        let checkpoint = LogRecord::new_checkpoint(3, 1, vec![100], vec![], 2);
        assert_eq!(checkpoint.description(), "CHECKPOINT 1 (активных транзакций: 1)");
    }
}
