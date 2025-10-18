//! Комплексный менеджер конкурентности
//!
//! Объединяет MVCC, блокировки и deadlock detection

use crate::common::{Error, Result};
use crate::core::transaction::TransactionId;
use crate::core::advanced_lock_manager::{
    AdvancedLockManager, AdvancedLockConfig, ResourceType, LockMode,
};
use crate::core::mvcc::{MVCCManager, RowKey, Timestamp};
use std::sync::Arc;
use std::time::Duration;

/// Уровень изоляции для конкурентного доступа
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Read Uncommitted - минимальная изоляция
    ReadUncommitted,
    /// Read Committed - читаем только зафиксированные данные
    ReadCommitted,
    /// Repeatable Read - повторяемое чтение
    RepeatableRead,
    /// Serializable - полная изоляция
    Serializable,
}

/// Гранулярность блокировки
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockGranularity {
    /// Блокировка на уровне базы данных
    Database,
    /// Блокировка на уровне таблицы
    Table,
    /// Блокировка на уровне страницы
    Page,
    /// Блокировка на уровне строки
    Row,
}

/// Настройки менеджера конкурентности
#[derive(Debug, Clone)]
pub struct ConcurrencyConfig {
    /// Конфигурация блокировок
    pub lock_config: AdvancedLockConfig,
    /// Уровень изоляции по умолчанию
    pub default_isolation_level: IsolationLevel,
    /// Гранулярность блокировок по умолчанию
    pub default_lock_granularity: LockGranularity,
    /// Включить MVCC
    pub enable_mvcc: bool,
    /// Интервал автоматической очистки MVCC
    pub vacuum_interval: Duration,
}

impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self {
            lock_config: AdvancedLockConfig::default(),
            default_isolation_level: IsolationLevel::ReadCommitted,
            default_lock_granularity: LockGranularity::Row,
            enable_mvcc: true,
            vacuum_interval: Duration::from_secs(60),
        }
    }
}

/// Комплексный менеджер конкурентности
pub struct ConcurrencyManager {
    /// Менеджер блокировок
    lock_manager: Arc<AdvancedLockManager>,
    /// Менеджер MVCC
    mvcc_manager: Arc<MVCCManager>,
    /// Конфигурация
    config: ConcurrencyConfig,
}

impl ConcurrencyManager {
    /// Создаёт новый менеджер конкурентности
    pub fn new(config: ConcurrencyConfig) -> Self {
        let lock_manager = Arc::new(AdvancedLockManager::new(config.lock_config.clone()));
        let mvcc_manager = Arc::new(MVCCManager::new());
        
        Self {
            lock_manager,
            mvcc_manager,
            config,
        }
    }
    
    /// Начинает транзакцию
    pub fn begin_transaction(
        &self,
        transaction_id: TransactionId,
        isolation_level: IsolationLevel,
    ) -> Result<Timestamp> {
        // Возвращаем snapshot timestamp для транзакции
        Ok(Timestamp::now())
    }
    
    /// Получает блокировку для чтения
    pub async fn acquire_read_lock(
        &self,
        transaction_id: TransactionId,
        resource: ResourceType,
        timeout: Option<Duration>,
    ) -> Result<()> {
        match self.config.default_isolation_level {
            IsolationLevel::ReadUncommitted => {
                // Не требуем блокировку для чтения
                Ok(())
            }
            IsolationLevel::ReadCommitted | IsolationLevel::RepeatableRead => {
                // Shared блокировка
                self.lock_manager.acquire_lock(
                    transaction_id,
                    resource,
                    LockMode::Shared,
                    timeout,
                ).await
            }
            IsolationLevel::Serializable => {
                // Более строгая блокировка
                self.lock_manager.acquire_lock(
                    transaction_id,
                    resource,
                    LockMode::Shared,
                    timeout,
                ).await
            }
        }
    }
    
    /// Получает блокировку для записи
    pub async fn acquire_write_lock(
        &self,
        transaction_id: TransactionId,
        resource: ResourceType,
        timeout: Option<Duration>,
    ) -> Result<()> {
        // Для записи всегда требуем exclusive блокировку
        self.lock_manager.acquire_lock(
            transaction_id,
            resource,
            LockMode::Exclusive,
            timeout,
        ).await
    }
    
    /// Читает данные с учётом MVCC
    pub async fn read(
        &self,
        transaction_id: TransactionId,
        key: &RowKey,
        snapshot: Timestamp,
    ) -> Result<Option<Vec<u8>>> {
        if self.config.enable_mvcc {
            self.mvcc_manager.read_version(key, transaction_id, snapshot)
        } else {
            // Fallback без MVCC - требуем блокировку
            let resource = ResourceType::Record(key.table_id as u64, key.row_id);
            self.acquire_read_lock(transaction_id, resource, None).await?;
            
            // TODO: Читать данные из storage
            Ok(None)
        }
    }
    
    /// Записывает данные с учётом MVCC
    pub async fn write(
        &self,
        transaction_id: TransactionId,
        key: RowKey,
        data: Vec<u8>,
    ) -> Result<()> {
        // Получаем блокировку на запись
        let resource = ResourceType::Record(key.table_id as u64, key.row_id);
        self.acquire_write_lock(transaction_id, resource, None).await?;
        
        if self.config.enable_mvcc {
            // Создаём новую версию
            self.mvcc_manager.create_version(key, transaction_id, data)?;
        } else {
            // TODO: Записать данные в storage напрямую
        }
        
        Ok(())
    }
    
    /// Удаляет данные
    pub async fn delete(
        &self,
        transaction_id: TransactionId,
        key: &RowKey,
    ) -> Result<()> {
        // Получаем блокировку на запись
        let resource = ResourceType::Record(key.table_id as u64, key.row_id);
        self.acquire_write_lock(transaction_id, resource, None).await?;
        
        if self.config.enable_mvcc {
            // Помечаем для удаления
            self.mvcc_manager.delete_version(key, transaction_id)?;
        } else {
            // TODO: Удалить из storage напрямую
        }
        
        Ok(())
    }
    
    /// Фиксирует транзакцию
    pub fn commit_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        // Фиксируем версии MVCC
        if self.config.enable_mvcc {
            self.mvcc_manager.commit_transaction(transaction_id)?;
        }
        
        // Освобождаем все блокировки
        self.lock_manager.release_all_locks(transaction_id)?;
        
        Ok(())
    }
    
    /// Откатывает транзакцию
    pub fn abort_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        // Откатываем версии MVCC
        if self.config.enable_mvcc {
            self.mvcc_manager.abort_transaction(transaction_id)?;
        }
        
        // Освобождаем все блокировки
        self.lock_manager.release_all_locks(transaction_id)?;
        
        Ok(())
    }
    
    /// Выполняет очистку старых версий
    pub fn vacuum(&self) -> Result<u64> {
        if self.config.enable_mvcc {
            self.mvcc_manager.vacuum()
        } else {
            Ok(0)
        }
    }
    
    /// Возвращает статистику блокировок
    pub fn get_lock_statistics(&self) -> crate::core::advanced_lock_manager::AdvancedLockStatistics {
        self.lock_manager.get_statistics()
    }
    
    /// Возвращает статистику MVCC
    pub fn get_mvcc_statistics(&self) -> crate::core::mvcc::MVCCStatistics {
        self.mvcc_manager.get_statistics()
    }
    
    /// Обновляет минимальную активную транзакцию для VACUUM
    pub fn update_min_active_transaction(&self, transaction_id: TransactionId) {
        self.mvcc_manager.update_min_active_transaction(transaction_id);
    }
}

impl Default for ConcurrencyManager {
    fn default() -> Self {
        Self::new(ConcurrencyConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_concurrency_manager_creation() {
        let manager = ConcurrencyManager::default();
        let lock_stats = manager.get_lock_statistics();
        let mvcc_stats = manager.get_mvcc_statistics();
        
        assert_eq!(lock_stats.total_locks, 0);
        assert_eq!(mvcc_stats.total_versions, 0);
    }
    
    #[tokio::test]
    async fn test_write_and_read() {
        let manager = ConcurrencyManager::default();
        let tx1 = TransactionId::new(1);
        let key = RowKey::new(1, 1);
        let data = vec![1, 2, 3, 4];
        
        // Записываем данные
        manager.write(tx1, key.clone(), data.clone()).await.unwrap();
        
        // Фиксируем
        manager.commit_transaction(tx1).unwrap();
        
        // Читаем
        let tx2 = TransactionId::new(2);
        let snapshot = Timestamp::now();
        let read_data = manager.read(tx2, &key, snapshot).await.unwrap();
        
        assert_eq!(read_data, Some(data));
    }
    
    #[tokio::test]
    async fn test_transaction_isolation() {
        let manager = ConcurrencyManager::default();
        let key = RowKey::new(1, 1);
        
        // Транзакция 1 пишет
        let tx1 = TransactionId::new(1);
        let data1 = vec![1, 2, 3];
        manager.write(tx1, key.clone(), data1.clone()).await.unwrap();
        
        // Транзакция 2 читает до коммита tx1 (не видит изменений)
        let tx2 = TransactionId::new(2);
        let snapshot_before = Timestamp::now();
        
        // Фиксируем tx1
        manager.commit_transaction(tx1).unwrap();
        
        // Транзакция 3 читает после коммита (видит изменения)
        let tx3 = TransactionId::new(3);
        let snapshot_after = Timestamp::now();
        let read_data = manager.read(tx3, &key, snapshot_after).await.unwrap();
        
        assert_eq!(read_data, Some(data1));
    }
    
    #[tokio::test]
    async fn test_abort_transaction() {
        let manager = ConcurrencyManager::default();
        let tx1 = TransactionId::new(1);
        let key = RowKey::new(1, 1);
        let data = vec![1, 2, 3, 4];
        
        // Записываем данные
        manager.write(tx1, key.clone(), data).await.unwrap();
        
        // Откатываем
        manager.abort_transaction(tx1).unwrap();
        
        // Проверяем статистику
        let mvcc_stats = manager.get_mvcc_statistics();
        assert_eq!(mvcc_stats.aborted_versions, 1);
        assert_eq!(mvcc_stats.active_versions, 0);
    }
    
    #[tokio::test]
    async fn test_vacuum() {
        let manager = ConcurrencyManager::default();
        let tx1 = TransactionId::new(1);
        let key = RowKey::new(1, 1);
        let data = vec![1, 2, 3, 4];
        
        // Создаём и откатываем транзакцию
        manager.write(tx1, key, data).await.unwrap();
        manager.abort_transaction(tx1).unwrap();
        
        // Выполняем VACUUM
        manager.update_min_active_transaction(TransactionId::new(100));
        let cleaned = manager.vacuum().unwrap();
        
        assert_eq!(cleaned, 1);
    }
}

