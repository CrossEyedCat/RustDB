//! Менеджер транзакций для rustdb
//!
//! Этот модуль реализует полноценную систему управления транзакциями
//! с поддержкой ACID свойств, двухфазного блокирования (2PL) и
//! обнаружения дедлоков.

use crate::common::{Error, Result};
use crate::core::lock::{LockManager, LockMode, LockType};
use crate::logging::log_record::{LogRecord, LogRecordType};
use crate::logging::wal::WriteAheadLog;
use std::collections::{HashMap, HashSet};
use std::sync::{atomic::AtomicU64, Arc, Mutex, RwLock};
use std::time::SystemTime;

/// Уникальный идентификатор транзакции
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TransactionId(pub u64);

impl TransactionId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn value(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for TransactionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TXN{}", self.0)
    }
}

/// Состояния транзакции в соответствии с моделью состояний СУБД
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionState {
    /// Транзакция активна и выполняет операции
    Active,
    /// Транзакция завершила все операции, но еще не зафиксирована
    PartiallyCommitted,
    /// Транзакция успешно зафиксирована
    Committed,
    /// Транзакция была отменена
    Aborted,
    /// Транзакция находится в процессе отката
    Aborting,
}

/// Режим изоляции транзакции
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// Информация о транзакции
#[derive(Debug, Clone)]
pub struct TransactionInfo {
    /// Идентификатор транзакции
    pub id: TransactionId,
    /// Текущее состояние
    pub state: TransactionState,
    /// Время начала транзакции
    pub start_time: SystemTime,
    /// Время последней активности
    pub last_activity: SystemTime,
    /// Уровень изоляции
    pub isolation_level: IsolationLevel,
    /// Список заблокированных ресурсов
    pub locked_resources: HashSet<String>,
    /// Список ожидаемых блокировок
    pub waiting_for: Option<String>,
    /// Флаг только для чтения
    pub read_only: bool,
}

impl TransactionInfo {
    pub fn new(id: TransactionId, isolation_level: IsolationLevel, read_only: bool) -> Self {
        let now = SystemTime::now();
        Self {
            id,
            state: TransactionState::Active,
            start_time: now,
            last_activity: now,
            isolation_level,
            locked_resources: HashSet::new(),
            waiting_for: None,
            read_only,
        }
    }

    pub fn update_activity(&mut self) {
        self.last_activity = SystemTime::now();
    }

    pub fn duration(&self) -> Result<std::time::Duration> {
        SystemTime::now()
            .duration_since(self.start_time)
            .map_err(|e| Error::internal(format!("Time calculation error: {}", e)))
    }
}

/// Статистика менеджера транзакций
#[derive(Debug, Clone, Default)]
pub struct TransactionManagerStats {
    /// Общее количество запущенных транзакций
    pub total_transactions: u64,
    /// Количество активных транзакций
    pub active_transactions: u64,
    /// Количество зафиксированных транзакций
    pub committed_transactions: u64,
    /// Количество отмененных транзакций
    pub aborted_transactions: u64,
    /// Количество обнаруженных дедлоков
    pub deadlocks_detected: u64,
    /// Среднее время выполнения транзакции (в миллисекундах)
    pub average_transaction_time: f64,
    /// Количество операций блокирования
    pub lock_operations: u64,
    /// Количество операций разблокирования
    pub unlock_operations: u64,
}

/// Конфигурация менеджера транзакций
#[derive(Debug, Clone)]
pub struct TransactionManagerConfig {
    /// Максимальное количество одновременных транзакций
    pub max_concurrent_transactions: usize,
    /// Таймаут ожидания блокировки (в миллисекундах)
    pub lock_timeout_ms: u64,
    /// Интервал проверки дедлоков (в миллисекундах)
    pub deadlock_detection_interval_ms: u64,
    /// Максимальное время жизни неактивной транзакции (в секундах)
    pub max_idle_time_seconds: u64,
    /// Включить автоматическое обнаружение дедлоков
    pub enable_deadlock_detection: bool,
}

impl Default for TransactionManagerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_transactions: 1000,
            lock_timeout_ms: 30000,               // 30 секунд
            deadlock_detection_interval_ms: 1000, // 1 секунда
            max_idle_time_seconds: 3600,          // 1 час
            enable_deadlock_detection: true,
        }
    }
}

/// Менеджер транзакций
///
/// Отвечает за управление жизненным циклом транзакций, координацию
/// с менеджером блокировок и обеспечение ACID свойств.
pub struct TransactionManager {
    /// Конфигурация менеджера
    config: TransactionManagerConfig,
    /// Счетчик для генерации уникальных ID транзакций
    next_transaction_id: AtomicU64,
    /// Активные транзакции
    active_transactions: Arc<RwLock<HashMap<TransactionId, TransactionInfo>>>,
    /// Менеджер блокировок
    lock_manager: Arc<LockManager>,
    /// Write-Ahead Log для логирования операций
    wal: Option<Arc<Mutex<WriteAheadLog>>>,
    /// Статистика
    stats: Arc<Mutex<TransactionManagerStats>>,
}

impl TransactionManager {
    /// Создает новый менеджер транзакций с конфигурацией по умолчанию
    pub fn new() -> Result<Self> {
        Self::with_config(TransactionManagerConfig::default())
    }

    /// Создает новый менеджер транзакций с заданной конфигурацией
    pub fn with_config(config: TransactionManagerConfig) -> Result<Self> {
        let lock_manager = Arc::new(LockManager::new()?);

        Ok(Self {
            config,
            next_transaction_id: AtomicU64::new(1),
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
            lock_manager,
            wal: None,
            stats: Arc::new(Mutex::new(TransactionManagerStats::default())),
        })
    }

    /// Устанавливает Write-Ahead Log
    pub fn set_wal(&mut self, wal: Arc<Mutex<WriteAheadLog>>) {
        self.wal = Some(wal);
    }

    /// Получает конфигурацию менеджера
    pub fn get_config(&self) -> &TransactionManagerConfig {
        &self.config
    }

    /// Получает статистику менеджера транзакций
    pub fn get_statistics(&self) -> Result<TransactionManagerStats> {
        let stats = self
            .stats
            .lock()
            .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
        Ok(stats.clone())
    }

    /// Начинает новую транзакцию
    pub fn begin_transaction(
        &self,
        isolation_level: IsolationLevel,
        read_only: bool,
    ) -> Result<TransactionId> {
        // Проверяем лимит одновременных транзакций
        {
            let active = self.active_transactions.read().map_err(|_| {
                Error::internal("Failed to acquire read lock on active transactions".to_string())
            })?;

            if active.len() >= self.config.max_concurrent_transactions {
                return Err(Error::TransactionError(
                    "Maximum number of concurrent transactions reached".to_string(),
                ));
            }
        }

        // Генерируем новый ID
        let transaction_id = TransactionId(
            self.next_transaction_id
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        );

        // Создаем информацию о транзакции
        let transaction_info = TransactionInfo::new(transaction_id, isolation_level, read_only);

        // Добавляем в активные транзакции
        {
            let mut active = self.active_transactions.write().map_err(|_| {
                Error::internal("Failed to acquire write lock on active transactions".to_string())
            })?;
            active.insert(transaction_id, transaction_info);
        }

        // Обновляем статистику
        {
            let mut stats = self
                .stats
                .lock()
                .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
            stats.total_transactions += 1;
            stats.active_transactions += 1;
        }

        Ok(transaction_id)
    }

    /// Фиксирует транзакцию
    pub fn commit_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        // Получаем информацию о транзакции
        let transaction_info = {
            let active = self.active_transactions.read().map_err(|_| {
                Error::internal("Failed to acquire read lock on active transactions".to_string())
            })?;

            active.get(&transaction_id).cloned().ok_or_else(|| {
                Error::TransactionError(format!("Transaction {} not found", transaction_id))
            })?
        };

        // Проверяем состояние транзакции
        if transaction_info.state != TransactionState::Active {
            return Err(Error::TransactionError(format!(
                "Cannot commit transaction {} in state {:?}",
                transaction_id, transaction_info.state
            )));
        }

        // Переводим в состояние PartiallyCommitted
        self.update_transaction_state(transaction_id, TransactionState::PartiallyCommitted)?;

        // Освобождаем все блокировки (фаза сокращения в 2PL)
        self.release_all_locks(transaction_id)?;

        // Переводим в состояние Committed и удаляем из активных
        {
            let mut active = self.active_transactions.write().map_err(|_| {
                Error::internal("Failed to acquire write lock on active transactions".to_string())
            })?;

            if let Some(mut info) = active.remove(&transaction_id) {
                info.state = TransactionState::Committed;
                info.update_activity();
            }
        }

        // Обновляем статистику
        {
            let mut stats = self
                .stats
                .lock()
                .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
            stats.active_transactions -= 1;
            stats.committed_transactions += 1;
        }

        Ok(())
    }

    /// Отменяет транзакцию
    pub fn abort_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        // Получаем информацию о транзакции
        let _transaction_info = {
            let active = self.active_transactions.read().map_err(|_| {
                Error::internal("Failed to acquire read lock on active transactions".to_string())
            })?;

            active.get(&transaction_id).cloned().ok_or_else(|| {
                Error::TransactionError(format!("Transaction {} not found", transaction_id))
            })?
        };

        // Переводим в состояние Aborting
        self.update_transaction_state(transaction_id, TransactionState::Aborting)?;

        // Освобождаем все блокировки
        self.release_all_locks(transaction_id)?;

        // Переводим в состояние Aborted и удаляем из активных
        {
            let mut active = self.active_transactions.write().map_err(|_| {
                Error::internal("Failed to acquire write lock on active transactions".to_string())
            })?;

            if let Some(mut info) = active.remove(&transaction_id) {
                info.state = TransactionState::Aborted;
                info.update_activity();
            }
        }

        // Обновляем статистику
        {
            let mut stats = self
                .stats
                .lock()
                .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
            stats.active_transactions -= 1;
            stats.aborted_transactions += 1;
        }

        Ok(())
    }

    /// Получает информацию о транзакции
    pub fn get_transaction_info(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<TransactionInfo>> {
        let active = self.active_transactions.read().map_err(|_| {
            Error::internal("Failed to acquire read lock on active transactions".to_string())
        })?;

        Ok(active.get(&transaction_id).cloned())
    }

    /// Получает список всех активных транзакций
    pub fn get_active_transactions(&self) -> Result<Vec<TransactionInfo>> {
        let active = self.active_transactions.read().map_err(|_| {
            Error::internal("Failed to acquire read lock on active transactions".to_string())
        })?;

        Ok(active.values().cloned().collect())
    }

    /// Запрашивает блокировку для транзакции
    pub fn acquire_lock(
        &self,
        transaction_id: TransactionId,
        resource: String,
        lock_type: LockType,
        lock_mode: LockMode,
    ) -> Result<()> {
        // Проверяем, что транзакция активна
        self.ensure_transaction_active(transaction_id)?;

        // Пытаемся получить блокировку
        let acquired = self.lock_manager.acquire_lock(
            transaction_id,
            resource.clone(),
            lock_type,
            lock_mode,
        )?;

        if acquired {
            // Добавляем ресурс в список заблокированных
            let mut active = self.active_transactions.write().map_err(|_| {
                Error::internal("Failed to acquire write lock on active transactions".to_string())
            })?;

            if let Some(info) = active.get_mut(&transaction_id) {
                info.locked_resources.insert(resource);
                info.update_activity();
            }

            // Обновляем статистику
            let mut stats = self
                .stats
                .lock()
                .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
            stats.lock_operations += 1;
        } else {
            // Блокировка не получена - возможно дедлок или таймаут
            return Err(Error::TransactionError(format!(
                "Failed to acquire lock on resource {} for transaction {}",
                resource, transaction_id
            )));
        }

        Ok(())
    }

    /// Освобождает блокировку
    pub fn release_lock(&self, transaction_id: TransactionId, resource: String) -> Result<()> {
        // Освобождаем блокировку
        self.lock_manager
            .release_lock(transaction_id, resource.clone())?;

        // Удаляем ресурс из списка заблокированных
        let mut active = self.active_transactions.write().map_err(|_| {
            Error::internal("Failed to acquire write lock on active transactions".to_string())
        })?;

        if let Some(info) = active.get_mut(&transaction_id) {
            info.locked_resources.remove(&resource);
            info.update_activity();
        }

        // Обновляем статистику
        let mut stats = self
            .stats
            .lock()
            .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
        stats.unlock_operations += 1;

        Ok(())
    }

    /// Освобождает все блокировки транзакции
    fn release_all_locks(&self, transaction_id: TransactionId) -> Result<()> {
        // Получаем список заблокированных ресурсов
        let locked_resources = {
            let active = self.active_transactions.read().map_err(|_| {
                Error::internal("Failed to acquire read lock on active transactions".to_string())
            })?;

            active
                .get(&transaction_id)
                .map(|info| info.locked_resources.clone())
                .unwrap_or_default()
        };

        // Освобождаем все блокировки
        for resource in locked_resources {
            self.lock_manager.release_lock(transaction_id, resource)?;
        }

        // Очищаем список заблокированных ресурсов
        let mut active = self.active_transactions.write().map_err(|_| {
            Error::internal("Failed to acquire write lock on active transactions".to_string())
        })?;

        if let Some(info) = active.get_mut(&transaction_id) {
            let unlock_count = info.locked_resources.len();
            info.locked_resources.clear();
            info.update_activity();

            // Обновляем статистику
            let mut stats = self
                .stats
                .lock()
                .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
            stats.unlock_operations += unlock_count as u64;
        }

        Ok(())
    }

    /// Обновляет состояние транзакции
    fn update_transaction_state(
        &self,
        transaction_id: TransactionId,
        new_state: TransactionState,
    ) -> Result<()> {
        let mut active = self.active_transactions.write().map_err(|_| {
            Error::internal("Failed to acquire write lock on active transactions".to_string())
        })?;

        if let Some(info) = active.get_mut(&transaction_id) {
            info.state = new_state;
            info.update_activity();
        }

        Ok(())
    }

    /// Проверяет, что транзакция активна
    fn ensure_transaction_active(&self, transaction_id: TransactionId) -> Result<()> {
        let active = self.active_transactions.read().map_err(|_| {
            Error::internal("Failed to acquire read lock on active transactions".to_string())
        })?;

        match active.get(&transaction_id) {
            Some(info) if info.state == TransactionState::Active => Ok(()),
            Some(info) => Err(Error::TransactionError(format!(
                "Transaction {} is not active (state: {:?})",
                transaction_id, info.state
            ))),
            None => Err(Error::TransactionError(format!(
                "Transaction {} not found",
                transaction_id
            ))),
        }
    }
}
