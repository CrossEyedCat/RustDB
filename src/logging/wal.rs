//! Write-Ahead Logging (WAL) система для rustdb
//!
//! Этот модуль реализует WAL - ключевой компонент для обеспечения ACID свойств:
//! - Гарантирует, что изменения сначала записываются в лог, затем в данные
//! - Обеспечивает атомарность и долговечность транзакций
//! - Поддерживает восстановление после сбоев
//! - Интегрируется с системой транзакций и блокировок

use crate::common::{Error, Result};
use crate::logging::log_record::{IsolationLevel, LogRecord, LogSequenceNumber, TransactionId};
use crate::logging::log_writer::{LogWriter, LogWriterConfig};
use crate::storage::database_file::PageId;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;

/// Конфигурация WAL системы
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    /// Конфигурация записи логов
    pub log_writer_config: LogWriterConfig,
    /// Включить строгий режим WAL (все изменения логируются)
    pub strict_mode: bool,
    /// Максимальное время ожидания блокировки (мс)
    pub lock_timeout_ms: u64,
    /// Размер пула транзакций
    pub transaction_pool_size: usize,
    /// Автоматическое создание контрольных точек
    pub auto_checkpoint: bool,
    /// Интервал создания контрольных точек
    pub checkpoint_interval: Duration,
    /// Максимальное количество активных транзакций
    pub max_active_transactions: usize,
    /// Включить валидацию целостности
    pub enable_integrity_validation: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            log_writer_config: LogWriterConfig::default(),
            strict_mode: true,
            lock_timeout_ms: 5000,
            transaction_pool_size: 100,
            auto_checkpoint: true,
            checkpoint_interval: Duration::from_secs(60),
            max_active_transactions: 1000,
            enable_integrity_validation: true,
        }
    }
}

/// Состояние транзакции
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionState {
    /// Активна
    Active,
    /// Подготовка к коммиту
    Preparing,
    /// Зафиксирована
    Committed,
    /// Отменена
    Aborted,
    /// Завершена (можно удалить)
    Finished,
}

/// Информация о транзакции
#[derive(Debug, Clone)]
pub struct TransactionInfo {
    /// ID транзакции
    pub id: TransactionId,
    /// Состояние
    pub state: TransactionState,
    /// Уровень изоляции
    pub isolation_level: IsolationLevel,
    /// Время начала
    pub start_time: u64,
    /// Время последней активности
    pub last_activity: u64,
    /// LSN первой записи транзакции
    pub first_lsn: Option<LogSequenceNumber>,
    /// LSN последней записи транзакции
    pub last_lsn: Option<LogSequenceNumber>,
    /// Список измененных страниц
    pub dirty_pages: HashSet<(u32, PageId)>,
    /// Список заблокированных ресурсов
    pub locks: HashSet<String>,
    /// Количество операций в транзакции
    pub operation_count: u64,
}

impl TransactionInfo {
    /// Создает новую информацию о транзакции
    pub fn new(id: TransactionId, isolation_level: IsolationLevel) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            id,
            state: TransactionState::Active,
            isolation_level,
            start_time: now,
            last_activity: now,
            first_lsn: None,
            last_lsn: None,
            dirty_pages: HashSet::new(),
            locks: HashSet::new(),
            operation_count: 0,
        }
    }

    /// Обновляет время последней активности
    pub fn update_activity(&mut self) {
        self.last_activity = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    /// Добавляет измененную страницу
    pub fn add_dirty_page(&mut self, file_id: u32, page_id: PageId) {
        self.dirty_pages.insert((file_id, page_id));
        self.update_activity();
    }

    /// Добавляет блокировку ресурса
    pub fn add_lock(&mut self, resource: String) {
        self.locks.insert(resource);
        self.update_activity();
    }

    /// Устанавливает LSN записи
    pub fn set_lsn(&mut self, lsn: LogSequenceNumber) {
        if self.first_lsn.is_none() {
            self.first_lsn = Some(lsn);
        }
        self.last_lsn = Some(lsn);
        self.operation_count += 1;
        self.update_activity();
    }

    /// Возвращает продолжительность транзакции
    pub fn duration(&self) -> Duration {
        Duration::from_secs(self.last_activity.saturating_sub(self.start_time))
    }

    /// Проверяет, истек ли таймаут транзакции
    pub fn is_timed_out(&self, timeout: Duration) -> bool {
        self.duration() > timeout
    }
}

/// Статистика WAL системы
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WalStatistics {
    /// Общее количество транзакций
    pub total_transactions: u64,
    /// Активных транзакций
    pub active_transactions: u64,
    /// Зафиксированных транзакций
    pub committed_transactions: u64,
    /// Отмененных транзакций
    pub aborted_transactions: u64,
    /// Общее количество лог-записей
    pub total_log_records: u64,
    /// Средняя продолжительность транзакции (мс)
    pub average_transaction_duration_ms: u64,
    /// Количество дедлоков
    pub deadlock_count: u64,
    /// Количество таймаутов
    pub timeout_count: u64,
    /// Текущий LSN
    pub current_lsn: LogSequenceNumber,
    /// Последняя контрольная точка LSN
    pub last_checkpoint_lsn: LogSequenceNumber,
    /// Количество принудительных синхронизаций
    pub forced_syncs: u64,
}

/// Write-Ahead Logging система
pub struct WriteAheadLog {
    /// Конфигурация
    config: WalConfig,
    /// Система записи логов
    log_writer: Arc<LogWriter>,
    /// Активные транзакции
    transactions: Arc<RwLock<HashMap<TransactionId, TransactionInfo>>>,
    /// Генератор ID транзакций
    transaction_id_generator: Arc<Mutex<TransactionId>>,
    /// Статистика
    statistics: Arc<RwLock<WalStatistics>>,
    /// Уведомления о завершении транзакций
    commit_notify: Arc<Notify>,
    /// Фоновые задачи
    background_handle: Option<JoinHandle<()>>,
    /// Канал для команд управления
    command_tx: mpsc::UnboundedSender<WalCommand>,
}

/// Команды управления WAL
#[derive(Debug)]
enum WalCommand {
    /// Создать контрольную точку
    CreateCheckpoint,
    /// Очистить завершенные транзакции
    CleanupTransactions,
    /// Проверить таймауты
    CheckTimeouts,
}

impl WriteAheadLog {
    /// Создает новую WAL систему
    pub async fn new(config: WalConfig) -> Result<Self> {
        let log_writer = Arc::new(LogWriter::new(config.log_writer_config.clone())?);
        let (command_tx, command_rx) = mpsc::unbounded_channel();

        let mut wal = Self {
            config: config.clone(),
            log_writer,
            transactions: Arc::new(RwLock::new(HashMap::new())),
            transaction_id_generator: Arc::new(Mutex::new(1)),
            statistics: Arc::new(RwLock::new(WalStatistics::default())),
            commit_notify: Arc::new(Notify::new()),
            background_handle: None,
            command_tx,
        };

        // Запускаем фоновые задачи
        wal.start_background_tasks(command_rx).await;

        Ok(wal)
    }

    /// Запускает фоновые задачи
    async fn start_background_tasks(
        &mut self,
        mut command_rx: mpsc::UnboundedReceiver<WalCommand>,
    ) {
        let transactions = self.transactions.clone();
        let statistics = self.statistics.clone();
        let config = self.config.clone();
        let log_writer = self.log_writer.clone();
        let command_sender = self.command_tx.clone();

        self.background_handle = Some(tokio::spawn(async move {
            let mut checkpoint_interval = tokio::time::interval(config.checkpoint_interval);
            let mut cleanup_interval = tokio::time::interval(Duration::from_secs(30));

            loop {
                tokio::select! {
                    // Обработка команд
                    Some(command) = command_rx.recv() => {
                        Self::handle_command(command, &transactions, &statistics, &log_writer).await;
                    }

                    // Автоматические контрольные точки
                    _ = checkpoint_interval.tick() => {
                        if config.auto_checkpoint {
                            let _ = command_sender.send(WalCommand::CreateCheckpoint);
                        }
                    }

                    // Периодическая очистка
                    _ = cleanup_interval.tick() => {
                        let _ = command_sender.send(WalCommand::CleanupTransactions);
                        let _ = command_sender.send(WalCommand::CheckTimeouts);
                    }
                }
            }
        }));
    }

    /// Обрабатывает команду управления
    async fn handle_command(
        command: WalCommand,
        transactions: &Arc<RwLock<HashMap<TransactionId, TransactionInfo>>>,
        statistics: &Arc<RwLock<WalStatistics>>,
        log_writer: &Arc<LogWriter>,
    ) {
        match command {
            WalCommand::CreateCheckpoint => {
                Self::create_checkpoint_internal(transactions, statistics, log_writer).await;
            }
            WalCommand::CleanupTransactions => {
                Self::cleanup_finished_transactions(transactions, statistics).await;
            }
            WalCommand::CheckTimeouts => {
                Self::check_transaction_timeouts(transactions, statistics).await;
            }
        }
    }

    /// Начинает новую транзакцию
    pub async fn begin_transaction(
        &self,
        isolation_level: IsolationLevel,
    ) -> Result<TransactionId> {
        // Проверяем лимит активных транзакций
        {
            let transactions = self.transactions.read().unwrap();
            if transactions.len() >= self.config.max_active_transactions {
                return Err(Error::database("Превышен лимит активных транзакций"));
            }
        }

        // Генерируем ID транзакции
        let transaction_id = {
            let mut generator = self.transaction_id_generator.lock().unwrap();
            let id = *generator;
            *generator += 1;
            id
        };

        // Создаем информацию о транзакции
        let transaction_info = TransactionInfo::new(transaction_id, isolation_level);

        // Записываем лог-запись BEGIN
        let begin_record = LogRecord::new_transaction_begin(0, transaction_id, isolation_level);
        let lsn = self.log_writer.write_log(begin_record).await?;

        // Обновляем информацию о транзакции
        {
            let mut transactions = self.transactions.write().unwrap();
            let mut tx_info = transaction_info;
            tx_info.set_lsn(lsn);
            transactions.insert(transaction_id, tx_info);
        }

        // Обновляем статистику
        {
            let mut stats = self.statistics.write().unwrap();
            stats.total_transactions += 1;
            stats.active_transactions += 1;
            stats.total_log_records += 1; // Считаем запись BEGIN
            stats.current_lsn = lsn;
        }

        Ok(transaction_id)
    }

    /// Фиксирует транзакцию
    pub async fn commit_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        // Получаем информацию о транзакции
        let (dirty_pages, last_lsn) = {
            let mut transactions = self.transactions.write().unwrap();
            if let Some(tx_info) = transactions.get_mut(&transaction_id) {
                if tx_info.state != TransactionState::Active {
                    return Err(Error::database("Транзакция не активна"));
                }

                tx_info.state = TransactionState::Preparing;
                let dirty_pages: Vec<_> = tx_info.dirty_pages.iter().copied().collect();
                let last_lsn = tx_info.last_lsn;

                (dirty_pages, last_lsn)
            } else {
                return Err(Error::database("Транзакция не найдена"));
            }
        };

        // Записываем лог-запись COMMIT с принудительной синхронизацией
        let commit_record =
            LogRecord::new_transaction_commit(0, transaction_id, dirty_pages, last_lsn);
        let commit_lsn = self.log_writer.write_log_sync(commit_record).await?;

        // Обновляем состояние транзакции
        {
            let mut transactions = self.transactions.write().unwrap();
            if let Some(tx_info) = transactions.get_mut(&transaction_id) {
                tx_info.state = TransactionState::Committed;
                tx_info.set_lsn(commit_lsn);
            }
        }

        // Обновляем статистику
        {
            let mut stats = self.statistics.write().unwrap();
            stats.committed_transactions += 1;
            stats.active_transactions = stats.active_transactions.saturating_sub(1);
            stats.total_log_records += 1; // Считаем запись COMMIT
            stats.current_lsn = commit_lsn;
            stats.forced_syncs += 1;
        }

        // Уведомляем о завершении транзакции
        self.commit_notify.notify_waiters();

        Ok(())
    }

    /// Отменяет транзакцию
    pub async fn abort_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        // Получаем информацию о транзакции
        let last_lsn = {
            let mut transactions = self.transactions.write().unwrap();
            if let Some(tx_info) = transactions.get_mut(&transaction_id) {
                if tx_info.state == TransactionState::Committed {
                    return Err(Error::database(
                        "Нельзя отменить зафиксированную транзакцию",
                    ));
                }

                tx_info.state = TransactionState::Aborted;
                tx_info.last_lsn
            } else {
                return Err(Error::database("Транзакция не найдена"));
            }
        };

        // Записываем лог-запись ABORT с принудительной синхронизацией
        let abort_record = LogRecord::new_transaction_abort(0, transaction_id, last_lsn);
        let abort_lsn = self.log_writer.write_log_sync(abort_record).await?;

        // Обновляем информацию о транзакции
        {
            let mut transactions = self.transactions.write().unwrap();
            if let Some(tx_info) = transactions.get_mut(&transaction_id) {
                tx_info.set_lsn(abort_lsn);
            }
        }

        // Обновляем статистику
        {
            let mut stats = self.statistics.write().unwrap();
            stats.aborted_transactions += 1;
            stats.active_transactions = stats.active_transactions.saturating_sub(1);
            stats.total_log_records += 1; // Считаем запись ABORT
            stats.current_lsn = abort_lsn;
            stats.forced_syncs += 1;
        }

        Ok(())
    }

    /// Логирует операцию вставки данных
    pub async fn log_insert(
        &self,
        transaction_id: TransactionId,
        file_id: u32,
        page_id: PageId,
        record_offset: u16,
        data: Vec<u8>,
    ) -> Result<LogSequenceNumber> {
        self.validate_transaction(transaction_id)?;

        // Получаем предыдущий LSN транзакции
        let prev_lsn = {
            let transactions = self.transactions.read().unwrap();
            transactions.get(&transaction_id).and_then(|tx| tx.last_lsn)
        };

        // Записываем лог-запись
        let insert_record = LogRecord::new_data_insert(
            0,
            transaction_id,
            file_id,
            page_id,
            record_offset,
            data,
            prev_lsn,
        );
        let lsn = self.log_writer.write_log(insert_record).await?;

        // Обновляем информацию о транзакции
        {
            let mut transactions = self.transactions.write().unwrap();
            if let Some(tx_info) = transactions.get_mut(&transaction_id) {
                tx_info.set_lsn(lsn);
                tx_info.add_dirty_page(file_id, page_id);
            }
        }

        // Обновляем статистику
        {
            let mut stats = self.statistics.write().unwrap();
            stats.total_log_records += 1;
            stats.current_lsn = lsn;
        }

        Ok(lsn)
    }

    /// Логирует операцию обновления данных
    pub async fn log_update(
        &self,
        transaction_id: TransactionId,
        file_id: u32,
        page_id: PageId,
        record_offset: u16,
        old_data: Vec<u8>,
        new_data: Vec<u8>,
    ) -> Result<LogSequenceNumber> {
        self.validate_transaction(transaction_id)?;

        let prev_lsn = {
            let transactions = self.transactions.read().unwrap();
            transactions.get(&transaction_id).and_then(|tx| tx.last_lsn)
        };

        let update_record = LogRecord::new_data_update(
            0,
            transaction_id,
            file_id,
            page_id,
            record_offset,
            old_data,
            new_data,
            prev_lsn,
        );
        let lsn = self.log_writer.write_log(update_record).await?;

        {
            let mut transactions = self.transactions.write().unwrap();
            if let Some(tx_info) = transactions.get_mut(&transaction_id) {
                tx_info.set_lsn(lsn);
                tx_info.add_dirty_page(file_id, page_id);
            }
        }

        {
            let mut stats = self.statistics.write().unwrap();
            stats.total_log_records += 1;
            stats.current_lsn = lsn;
        }

        Ok(lsn)
    }

    /// Логирует операцию удаления данных
    pub async fn log_delete(
        &self,
        transaction_id: TransactionId,
        file_id: u32,
        page_id: PageId,
        record_offset: u16,
        old_data: Vec<u8>,
    ) -> Result<LogSequenceNumber> {
        self.validate_transaction(transaction_id)?;

        let prev_lsn = {
            let transactions = self.transactions.read().unwrap();
            transactions.get(&transaction_id).and_then(|tx| tx.last_lsn)
        };

        let delete_record = LogRecord::new_data_delete(
            0,
            transaction_id,
            file_id,
            page_id,
            record_offset,
            old_data,
            prev_lsn,
        );
        let lsn = self.log_writer.write_log(delete_record).await?;

        {
            let mut transactions = self.transactions.write().unwrap();
            if let Some(tx_info) = transactions.get_mut(&transaction_id) {
                tx_info.set_lsn(lsn);
                tx_info.add_dirty_page(file_id, page_id);
            }
        }

        {
            let mut stats = self.statistics.write().unwrap();
            stats.total_log_records += 1;
            stats.current_lsn = lsn;
        }

        Ok(lsn)
    }

    /// Создает контрольную точку
    pub async fn create_checkpoint(&self) -> Result<LogSequenceNumber> {
        let _ = self.command_tx.send(WalCommand::CreateCheckpoint);

        // Ждем завершения создания контрольной точки
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(self.get_current_lsn())
    }

    /// Внутренняя реализация создания контрольной точки
    async fn create_checkpoint_internal(
        transactions: &Arc<RwLock<HashMap<TransactionId, TransactionInfo>>>,
        statistics: &Arc<RwLock<WalStatistics>>,
        log_writer: &Arc<LogWriter>,
    ) {
        let (active_txs, dirty_pages, checkpoint_id) = {
            let txs = transactions.read().unwrap();
            let stats = statistics.read().unwrap();

            let active_txs: Vec<_> = txs
                .values()
                .filter(|tx| tx.state == TransactionState::Active)
                .map(|tx| tx.id)
                .collect();

            let dirty_pages: Vec<_> = txs
                .values()
                .flat_map(|tx| tx.dirty_pages.iter())
                .copied()
                .collect();

            let checkpoint_id = stats.last_checkpoint_lsn + 1;

            (active_txs, dirty_pages, checkpoint_id)
        };

        // Записываем запись контрольной точки
        let current_lsn = log_writer.current_lsn();
        let checkpoint_record =
            LogRecord::new_checkpoint(0, checkpoint_id, active_txs, dirty_pages, current_lsn);

        if let Ok(lsn) = log_writer.write_log_sync(checkpoint_record).await {
            let mut stats = statistics.write().unwrap();
            stats.last_checkpoint_lsn = lsn;
            stats.current_lsn = lsn;
        }
    }

    /// Очищает завершенные транзакции
    async fn cleanup_finished_transactions(
        transactions: &Arc<RwLock<HashMap<TransactionId, TransactionInfo>>>,
        _statistics: &Arc<RwLock<WalStatistics>>,
    ) {
        let mut txs = transactions.write().unwrap();
        txs.retain(|_, tx| {
            tx.state == TransactionState::Active || tx.state == TransactionState::Preparing
        });
    }

    /// Проверяет таймауты транзакций
    async fn check_transaction_timeouts(
        transactions: &Arc<RwLock<HashMap<TransactionId, TransactionInfo>>>,
        statistics: &Arc<RwLock<WalStatistics>>,
    ) {
        let timeout = Duration::from_secs(300); // 5 минут
        let mut timed_out_txs = Vec::new();

        {
            let txs = transactions.read().unwrap();
            for (id, tx) in txs.iter() {
                if tx.state == TransactionState::Active && tx.is_timed_out(timeout) {
                    timed_out_txs.push(*id);
                }
            }
        }

        if !timed_out_txs.is_empty() {
            let mut stats = statistics.write().unwrap();
            stats.timeout_count += timed_out_txs.len() as u64;
        }
    }

    /// Проверяет валидность транзакции
    fn validate_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        let transactions = self.transactions.read().unwrap();
        if let Some(tx_info) = transactions.get(&transaction_id) {
            if tx_info.state != TransactionState::Active {
                return Err(Error::database("Транзакция не активна"));
            }
            Ok(())
        } else {
            Err(Error::database("Транзакция не найдена"))
        }
    }

    /// Возвращает информацию о транзакции
    pub fn get_transaction_info(&self, transaction_id: TransactionId) -> Option<TransactionInfo> {
        let transactions = self.transactions.read().unwrap();
        transactions.get(&transaction_id).cloned()
    }

    /// Возвращает список активных транзакций
    pub fn get_active_transactions(&self) -> Vec<TransactionInfo> {
        let transactions = self.transactions.read().unwrap();
        transactions
            .values()
            .filter(|tx| tx.state == TransactionState::Active)
            .cloned()
            .collect()
    }

    /// Возвращает текущий LSN
    pub fn get_current_lsn(&self) -> LogSequenceNumber {
        self.log_writer.current_lsn()
    }

    /// Возвращает статистику WAL
    pub fn get_statistics(&self) -> WalStatistics {
        let mut stats = self.statistics.read().unwrap().clone();

        // Обновляем текущие значения
        let transactions = self.transactions.read().unwrap();
        stats.active_transactions = transactions
            .values()
            .filter(|tx| tx.state == TransactionState::Active)
            .count() as u64;

        // Вычисляем среднюю продолжительность транзакций
        let total_duration: u64 = transactions
            .values()
            .filter(|tx| tx.state != TransactionState::Active)
            .map(|tx| tx.duration().as_millis() as u64)
            .sum();

        let completed_count = stats.committed_transactions + stats.aborted_transactions;
        if completed_count > 0 {
            stats.average_transaction_duration_ms = total_duration / completed_count;
        }

        stats.current_lsn = self.get_current_lsn();

        stats
    }

    /// Принудительно синхронизирует логи
    pub async fn force_sync(&self) -> Result<()> {
        self.log_writer.flush().await?;

        {
            let mut stats = self.statistics.write().unwrap();
            stats.forced_syncs += 1;
        }

        Ok(())
    }

    /// Ожидает завершения всех активных транзакций
    pub async fn wait_for_transactions(&self, timeout: Duration) -> Result<()> {
        let start = tokio::time::Instant::now();

        while start.elapsed() < timeout {
            {
                let transactions = self.transactions.read().unwrap();
                if transactions
                    .values()
                    .all(|tx| tx.state != TransactionState::Active)
                {
                    return Ok(());
                }
            }

            // Ждем уведомления о завершении транзакции или таймаут
            tokio::select! {
                _ = self.commit_notify.notified() => {
                    // Проверяем еще раз
                    continue;
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    // Периодическая проверка
                    continue;
                }
            }
        }

        Err(Error::database("Таймаут ожидания завершения транзакций"))
    }
}

impl Drop for WriteAheadLog {
    fn drop(&mut self) {
        if let Some(handle) = self.background_handle.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_test_wal() -> Result<WriteAheadLog> {
        let temp_dir = TempDir::new().unwrap();
        let mut config = WalConfig::default();
        config.log_writer_config.log_directory = temp_dir.path().to_path_buf();
        config.auto_checkpoint = false; // Отключаем для тестов

        WriteAheadLog::new(config).await
    }

    #[tokio::test]
    async fn test_transaction_lifecycle() -> Result<()> {
        let wal = create_test_wal().await?;

        // Начинаем транзакцию
        let tx_id = wal.begin_transaction(IsolationLevel::ReadCommitted).await?;
        assert!(tx_id > 0);

        // Проверяем, что транзакция активна
        let tx_info = wal.get_transaction_info(tx_id).unwrap();
        assert_eq!(tx_info.state, TransactionState::Active);

        // Выполняем операции
        wal.log_insert(tx_id, 1, 10, 0, vec![1, 2, 3]).await?;
        wal.log_update(tx_id, 1, 10, 0, vec![1, 2, 3], vec![4, 5, 6])
            .await?;

        // Фиксируем транзакцию
        wal.commit_transaction(tx_id).await?;

        // Проверяем статистику
        let stats = wal.get_statistics();
        assert_eq!(stats.total_transactions, 1);
        assert_eq!(stats.committed_transactions, 1);
        assert!(stats.total_log_records >= 3); // BEGIN, INSERT, UPDATE, COMMIT

        Ok(())
    }

    #[tokio::test]
    async fn test_transaction_abort() -> Result<()> {
        let wal = create_test_wal().await?;

        let tx_id = wal.begin_transaction(IsolationLevel::Serializable).await?;

        // Выполняем операции
        wal.log_insert(tx_id, 1, 20, 0, vec![7, 8, 9]).await?;
        wal.log_delete(tx_id, 1, 20, 0, vec![7, 8, 9]).await?;

        // Отменяем транзакцию
        wal.abort_transaction(tx_id).await?;

        let stats = wal.get_statistics();
        assert_eq!(stats.aborted_transactions, 1);
        assert_eq!(stats.committed_transactions, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_transactions() -> Result<()> {
        let wal = create_test_wal().await?;

        // Начинаем несколько транзакций
        let tx1 = wal.begin_transaction(IsolationLevel::ReadCommitted).await?;
        let tx2 = wal
            .begin_transaction(IsolationLevel::RepeatableRead)
            .await?;
        let tx3 = wal.begin_transaction(IsolationLevel::Serializable).await?;

        // Проверяем активные транзакции
        let active_txs = wal.get_active_transactions();
        assert_eq!(active_txs.len(), 3);

        // Выполняем операции в разных транзакциях
        wal.log_insert(tx1, 1, 10, 0, vec![1]).await?;
        wal.log_insert(tx2, 2, 20, 0, vec![2]).await?;
        wal.log_insert(tx3, 3, 30, 0, vec![3]).await?;

        // Фиксируем две транзакции
        wal.commit_transaction(tx1).await?;
        wal.commit_transaction(tx2).await?;

        // Отменяем третью
        wal.abort_transaction(tx3).await?;

        let stats = wal.get_statistics();
        assert_eq!(stats.total_transactions, 3);
        assert_eq!(stats.committed_transactions, 2);
        assert_eq!(stats.aborted_transactions, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_checkpoint() -> Result<()> {
        let wal = create_test_wal().await?;

        // Начинаем транзакцию
        let tx_id = wal.begin_transaction(IsolationLevel::ReadCommitted).await?;
        wal.log_insert(tx_id, 1, 10, 0, vec![1, 2, 3]).await?;

        // Создаем контрольную точку
        let checkpoint_lsn = wal.create_checkpoint().await?;
        assert!(checkpoint_lsn > 0);

        // Фиксируем транзакцию
        wal.commit_transaction(tx_id).await?;

        let stats = wal.get_statistics();
        assert!(stats.last_checkpoint_lsn > 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_lsn_ordering() -> Result<()> {
        let wal = create_test_wal().await?;

        let tx_id = wal.begin_transaction(IsolationLevel::ReadCommitted).await?;

        let lsn1 = wal.log_insert(tx_id, 1, 10, 0, vec![1]).await?;
        let lsn2 = wal.log_update(tx_id, 1, 10, 0, vec![1], vec![2]).await?;
        let lsn3 = wal.log_delete(tx_id, 1, 10, 0, vec![2]).await?;

        // LSN должны увеличиваться
        assert!(lsn1 < lsn2);
        assert!(lsn2 < lsn3);

        wal.commit_transaction(tx_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_transaction_validation() -> Result<()> {
        let wal = create_test_wal().await?;

        // Пытаемся выполнить операцию с несуществующей транзакцией
        let result = wal.log_insert(999, 1, 10, 0, vec![1]).await;
        assert!(result.is_err());

        // Начинаем транзакцию и отменяем её
        let tx_id = wal.begin_transaction(IsolationLevel::ReadCommitted).await?;
        wal.abort_transaction(tx_id).await?;

        // Пытаемся выполнить операцию с отмененной транзакцией
        let result = wal.log_insert(tx_id, 1, 10, 0, vec![1]).await;
        assert!(result.is_err());

        Ok(())
    }
}
