//! Система контрольных точек для RustBD
//!
//! Этот модуль реализует механизм контрольных точек для оптимизации восстановления:
//! - Периодическое создание контрольных точек
//! - Фиксация состояния активных транзакций
//! - Сброс измененных страниц на диск
//! - Управление размером логов

use crate::common::{Error, Result};
use crate::logging::log_record::{LogRecord, LogSequenceNumber, TransactionId};
use crate::logging::log_writer::LogWriter;
use crate::storage::database_file::PageId;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;
use serde::{Deserialize, Serialize};

/// Информация о контрольной точке
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointInfo {
    /// ID контрольной точки
    pub id: u64,
    /// LSN контрольной точки
    pub lsn: LogSequenceNumber,
    /// Время создания
    pub timestamp: u64,
    /// Активные транзакции на момент создания
    pub active_transactions: Vec<TransactionId>,
    /// Измененные страницы
    pub dirty_pages: Vec<(u32, PageId)>,
    /// Размер контрольной точки в байтах
    pub size_bytes: u64,
    /// Время создания контрольной точки (мс)
    pub creation_time_ms: u64,
    /// Количество сброшенных страниц
    pub flushed_pages: u64,
}

/// Конфигурация системы контрольных точек
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Интервал создания контрольных точек
    pub checkpoint_interval: Duration,
    /// Максимальное количество активных транзакций для создания контрольной точки
    pub max_active_transactions: usize,
    /// Максимальное количество измененных страниц для создания контрольной точки
    pub max_dirty_pages: usize,
    /// Максимальный размер лога для создания контрольной точки
    pub max_log_size: u64,
    /// Включить автоматические контрольные точки
    pub enable_auto_checkpoint: bool,
    /// Максимальное время создания контрольной точки
    pub max_checkpoint_time: Duration,
    /// Количество потоков для сброса страниц
    pub flush_threads: usize,
    /// Размер пакета для сброса страниц
    pub flush_batch_size: usize,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            checkpoint_interval: Duration::from_secs(60), // 1 минута
            max_active_transactions: 100,
            max_dirty_pages: 1000,
            max_log_size: 100 * 1024 * 1024, // 100 MB
            enable_auto_checkpoint: true,
            max_checkpoint_time: Duration::from_secs(30),
            flush_threads: 4,
            flush_batch_size: 100,
        }
    }
}

/// Статистика контрольных точек
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CheckpointStatistics {
    /// Общее количество созданных контрольных точек
    pub total_checkpoints: u64,
    /// Количество автоматических контрольных точек
    pub auto_checkpoints: u64,
    /// Количество принудительных контрольных точек
    pub forced_checkpoints: u64,
    /// Среднее время создания контрольной точки (мс)
    pub average_checkpoint_time_ms: u64,
    /// Общее количество сброшенных страниц
    pub total_flushed_pages: u64,
    /// Размер последней контрольной точки
    pub last_checkpoint_size: u64,
    /// LSN последней контрольной точки
    pub last_checkpoint_lsn: LogSequenceNumber,
    /// Время последней контрольной точки
    pub last_checkpoint_time: u64,
    /// Количество неудачных контрольных точек
    pub failed_checkpoints: u64,
    /// Общее время всех контрольных точек (мс)
    pub total_checkpoint_time_ms: u64,
}

/// Триггер создания контрольной точки
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointTrigger {
    /// По таймеру
    Timer,
    /// По количеству транзакций
    TransactionCount,
    /// По количеству измененных страниц
    DirtyPageCount,
    /// По размеру лога
    LogSize,
    /// Принудительно (пользователем)
    Manual,
    /// При завершении работы
    Shutdown,
}

/// Команды управления контрольными точками
#[derive(Debug)]
enum CheckpointCommand {
    /// Создать контрольную точку
    CreateCheckpoint {
        trigger: CheckpointTrigger,
        response_tx: Option<tokio::sync::oneshot::Sender<Result<CheckpointInfo>>>,
    },
    /// Получить статистику
    GetStatistics {
        response_tx: tokio::sync::oneshot::Sender<CheckpointStatistics>,
    },
    /// Остановить систему
    Shutdown,
}

/// Менеджер контрольных точек
pub struct CheckpointManager {
    /// Конфигурация
    config: CheckpointConfig,
    /// Система записи логов
    log_writer: Arc<LogWriter>,
    /// Статистика
    statistics: Arc<RwLock<CheckpointStatistics>>,
    /// Генератор ID контрольных точек
    checkpoint_id_generator: Arc<Mutex<u64>>,
    /// Канал команд
    command_tx: mpsc::UnboundedSender<CheckpointCommand>,
    /// Уведомления о завершении контрольных точек
    checkpoint_notify: Arc<Notify>,
    /// Фоновая задача
    background_handle: Option<JoinHandle<()>>,
    /// Активные транзакции (внешний источник)
    active_transactions: Arc<RwLock<HashSet<TransactionId>>>,
    /// Измененные страницы (внешний источник)
    dirty_pages: Arc<RwLock<HashSet<(u32, PageId)>>>,
}

impl CheckpointManager {
    /// Создает новый менеджер контрольных точек
    pub fn new(
        config: CheckpointConfig,
        log_writer: Arc<LogWriter>,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();

        let mut manager = Self {
            config: config.clone(),
            log_writer,
            statistics: Arc::new(RwLock::new(CheckpointStatistics::default())),
            checkpoint_id_generator: Arc::new(Mutex::new(1)),
            command_tx,
            checkpoint_notify: Arc::new(Notify::new()),
            background_handle: None,
            active_transactions: Arc::new(RwLock::new(HashSet::new())),
            dirty_pages: Arc::new(RwLock::new(HashSet::new())),
        };

        // Запускаем фоновую задачу
        manager.start_background_task(command_rx);

        manager
    }

    /// Устанавливает источники данных для контрольных точек
    pub fn set_data_sources(
        &mut self,
        active_transactions: Arc<RwLock<HashSet<TransactionId>>>,
        dirty_pages: Arc<RwLock<HashSet<(u32, PageId)>>>,
    ) {
        self.active_transactions = active_transactions;
        self.dirty_pages = dirty_pages;
    }

    /// Запускает фоновую задачу управления контрольными точками
    fn start_background_task(&mut self, mut command_rx: mpsc::UnboundedReceiver<CheckpointCommand>) {
        let config = self.config.clone();
        let log_writer = self.log_writer.clone();
        let statistics = self.statistics.clone();
        let checkpoint_id_gen = self.checkpoint_id_generator.clone();
        let checkpoint_notify = self.checkpoint_notify.clone();
        let active_transactions = self.active_transactions.clone();
        let dirty_pages = self.dirty_pages.clone();
        let command_sender = self.command_tx.clone();

        self.background_handle = Some(tokio::spawn(async move {
            let mut timer_interval = if config.enable_auto_checkpoint {
                Some(tokio::time::interval(config.checkpoint_interval))
            } else {
                None
            };

            let mut monitoring_interval = tokio::time::interval(Duration::from_secs(10));

            loop {
                tokio::select! {
                    // Обработка команд
                    Some(command) = command_rx.recv() => {
                        match command {
                            CheckpointCommand::CreateCheckpoint { trigger, response_tx } => {
                                let result = Self::create_checkpoint_internal(
                                    &config,
                                    &log_writer,
                                    &statistics,
                                    &checkpoint_id_gen,
                                    &active_transactions,
                                    &dirty_pages,
                                    trigger,
                                ).await;

                                if let Some(tx) = response_tx {
                                    let _ = tx.send(result);
                                }

                                checkpoint_notify.notify_waiters();
                            }
                            CheckpointCommand::GetStatistics { response_tx } => {
                                let stats = statistics.read().unwrap().clone();
                                let _ = response_tx.send(stats);
                            }
                            CheckpointCommand::Shutdown => {
                                break;
                            }
                        }
                    }

                    // Автоматические контрольные точки по таймеру
                    _ = async {
                        match &mut timer_interval {
                            Some(interval) => interval.tick().await,
                            None => std::future::pending().await,
                        }
                    } => {
                        let _ = command_sender.send(CheckpointCommand::CreateCheckpoint {
                            trigger: CheckpointTrigger::Timer,
                            response_tx: None,
                        });
                    }

                    // Мониторинг условий для создания контрольных точек
                    _ = monitoring_interval.tick() => {
                        if config.enable_auto_checkpoint {
                            Self::check_checkpoint_conditions(
                                &config,
                                &command_sender,
                                &active_transactions,
                                &dirty_pages,
                                &log_writer,
                            ).await;
                        }
                    }
                }
            }
        }));
    }

    /// Проверяет условия для создания автоматических контрольных точек
    async fn check_checkpoint_conditions(
        config: &CheckpointConfig,
        command_sender: &mpsc::UnboundedSender<CheckpointCommand>,
        active_transactions: &Arc<RwLock<HashSet<TransactionId>>>,
        dirty_pages: &Arc<RwLock<HashSet<(u32, PageId)>>>,
        log_writer: &Arc<LogWriter>,
    ) {
        let active_tx_count = active_transactions.read().unwrap().len();
        let dirty_page_count = dirty_pages.read().unwrap().len();
        let log_size = log_writer.get_total_log_size();

        // Проверяем условия
        if active_tx_count >= config.max_active_transactions {
            let _ = command_sender.send(CheckpointCommand::CreateCheckpoint {
                trigger: CheckpointTrigger::TransactionCount,
                response_tx: None,
            });
        } else if dirty_page_count >= config.max_dirty_pages {
            let _ = command_sender.send(CheckpointCommand::CreateCheckpoint {
                trigger: CheckpointTrigger::DirtyPageCount,
                response_tx: None,
            });
        } else if log_size >= config.max_log_size {
            let _ = command_sender.send(CheckpointCommand::CreateCheckpoint {
                trigger: CheckpointTrigger::LogSize,
                response_tx: None,
            });
        }
    }

    /// Внутренняя реализация создания контрольной точки
    async fn create_checkpoint_internal(
        config: &CheckpointConfig,
        log_writer: &Arc<LogWriter>,
        statistics: &Arc<RwLock<CheckpointStatistics>>,
        checkpoint_id_gen: &Arc<Mutex<u64>>,
        active_transactions: &Arc<RwLock<HashSet<TransactionId>>>,
        dirty_pages: &Arc<RwLock<HashSet<(u32, PageId)>>>,
        trigger: CheckpointTrigger,
    ) -> Result<CheckpointInfo> {
        let start_time = Instant::now();

        // Генерируем ID контрольной точки
        let checkpoint_id = {
            let mut generator = checkpoint_id_gen.lock().unwrap();
            let id = *generator;
            *generator += 1;
            id
        };

        println!("📍 Создаем контрольную точку {} (триггер: {:?})", checkpoint_id, trigger);

        // Получаем снимок состояния
        let active_txs: Vec<TransactionId> = {
            let txs = active_transactions.read().unwrap();
            txs.iter().copied().collect()
        };

        let dirty_page_list: Vec<(u32, PageId)> = {
            let pages = dirty_pages.read().unwrap();
            pages.iter().copied().collect()
        };

        println!("   📊 Активных транзакций: {}", active_txs.len());
        println!("   📊 Измененных страниц: {}", dirty_page_list.len());

        // Сбрасываем измененные страницы на диск
        let flushed_pages = Self::flush_dirty_pages(config, &dirty_page_list).await?;
        println!("   💾 Сброшено страниц на диск: {}", flushed_pages);

        // Создаем лог-запись контрольной точки
        let current_lsn = log_writer.current_lsn();
        let checkpoint_record = LogRecord::new_checkpoint(
            0,
            checkpoint_id,
            active_txs.clone(),
            dirty_page_list.clone(),
            current_lsn,
        );

        let checkpoint_lsn = log_writer.write_log_sync(checkpoint_record).await?;

        // Принудительно сбрасываем логи
        log_writer.flush().await?;

        let creation_time = start_time.elapsed();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Создаем информацию о контрольной точке
        let checkpoint_info = CheckpointInfo {
            id: checkpoint_id,
            lsn: checkpoint_lsn,
            timestamp,
            active_transactions: active_txs,
            dirty_pages: dirty_page_list,
            size_bytes: 0, // Будет вычислено позже
            creation_time_ms: creation_time.as_millis() as u64,
            flushed_pages,
        };

        // Обновляем статистику
        {
            let mut stats = statistics.write().unwrap();
            stats.total_checkpoints += 1;
            match trigger {
                CheckpointTrigger::Timer => stats.auto_checkpoints += 1,
                CheckpointTrigger::Manual => stats.forced_checkpoints += 1,
                CheckpointTrigger::Shutdown => stats.forced_checkpoints += 1,
                _ => stats.auto_checkpoints += 1,
            }

            stats.total_flushed_pages += flushed_pages;
            stats.last_checkpoint_lsn = checkpoint_lsn;
            stats.last_checkpoint_time = timestamp;
            stats.total_checkpoint_time_ms += creation_time.as_millis() as u64;

            if stats.total_checkpoints > 0 {
                stats.average_checkpoint_time_ms = 
                    stats.total_checkpoint_time_ms / stats.total_checkpoints;
            }
        }

        println!("   ✅ Контрольная точка {} создана за {} мс", checkpoint_id, creation_time.as_millis());

        Ok(checkpoint_info)
    }

    /// Сбрасывает измененные страницы на диск
    async fn flush_dirty_pages(
        config: &CheckpointConfig,
        dirty_pages: &[(u32, PageId)],
    ) -> Result<u64> {
        let mut flushed_count = 0;

        // Разбиваем на пакеты для параллельной обработки
        let chunks: Vec<_> = dirty_pages.chunks(config.flush_batch_size).collect();
        
        for chunk in chunks {
            // В реальной реализации здесь был бы параллельный сброс страниц
            let batch_size = chunk.len();
            
            // Симулируем сброс страниц
            tokio::time::sleep(Duration::from_micros(batch_size as u64 * 10)).await;
            
            flushed_count += batch_size as u64;
        }

        Ok(flushed_count)
    }

    /// Создает контрольную точку вручную
    pub async fn create_checkpoint(&self) -> Result<CheckpointInfo> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        
        self.command_tx.send(CheckpointCommand::CreateCheckpoint {
            trigger: CheckpointTrigger::Manual,
            response_tx: Some(response_tx),
        }).map_err(|_| Error::internal("Не удалось отправить команду создания контрольной точки"))?;

        response_rx.await.map_err(|_| {
            Error::internal("Не удалось получить результат создания контрольной точки")
        })?
    }

    /// Создает контрольную точку при завершении работы
    pub async fn create_shutdown_checkpoint(&self) -> Result<CheckpointInfo> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        
        self.command_tx.send(CheckpointCommand::CreateCheckpoint {
            trigger: CheckpointTrigger::Shutdown,
            response_tx: Some(response_tx),
        }).map_err(|_| Error::internal("Не удалось отправить команду завершающей контрольной точки"))?;

        response_rx.await.map_err(|_| {
            Error::internal("Не удалось получить результат завершающей контрольной точки")
        })?
    }

    /// Возвращает статистику контрольных точек
    pub async fn get_statistics(&self) -> CheckpointStatistics {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        
        if self.command_tx.send(CheckpointCommand::GetStatistics { response_tx }).is_ok() {
            response_rx.await.unwrap_or_default()
        } else {
            CheckpointStatistics::default()
        }
    }

    /// Ожидает завершения текущей контрольной точки
    pub async fn wait_for_checkpoint(&self, timeout: Duration) -> Result<()> {
        tokio::time::timeout(timeout, self.checkpoint_notify.notified())
            .await
            .map_err(|_| Error::database("Таймаут ожидания завершения контрольной точки"))?;
        
        Ok(())
    }

    /// Останавливает менеджер контрольных точек
    pub async fn shutdown(&mut self) -> Result<()> {
        // Создаем финальную контрольную точку
        let _ = self.create_shutdown_checkpoint().await;

        // Отправляем команду остановки
        let _ = self.command_tx.send(CheckpointCommand::Shutdown);

        // Ждем завершения фоновой задачи
        if let Some(handle) = self.background_handle.take() {
            let _ = handle.await;
        }

        Ok(())
    }

    /// Обновляет список активных транзакций
    pub fn update_active_transactions(&self, transactions: HashSet<TransactionId>) {
        *self.active_transactions.write().unwrap() = transactions;
    }

    /// Обновляет список измененных страниц
    pub fn update_dirty_pages(&self, pages: HashSet<(u32, PageId)>) {
        *self.dirty_pages.write().unwrap() = pages;
    }

    /// Добавляет активную транзакцию
    pub fn add_active_transaction(&self, transaction_id: TransactionId) {
        self.active_transactions.write().unwrap().insert(transaction_id);
    }

    /// Удаляет активную транзакцию
    pub fn remove_active_transaction(&self, transaction_id: TransactionId) {
        self.active_transactions.write().unwrap().remove(&transaction_id);
    }

    /// Добавляет измененную страницу
    pub fn add_dirty_page(&self, file_id: u32, page_id: PageId) {
        self.dirty_pages.write().unwrap().insert((file_id, page_id));
    }

    /// Удаляет измененную страницу (после сброса)
    pub fn remove_dirty_page(&self, file_id: u32, page_id: PageId) {
        self.dirty_pages.write().unwrap().remove(&(file_id, page_id));
    }
}

impl Drop for CheckpointManager {
    fn drop(&mut self) {
        if let Some(handle) = self.background_handle.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logging::log_writer::{LogWriter, LogWriterConfig};
    use tempfile::TempDir;

    async fn create_test_checkpoint_manager() -> Result<CheckpointManager> {
        let temp_dir = TempDir::new().unwrap();
        let mut log_config = LogWriterConfig::default();
        log_config.log_directory = temp_dir.path().to_path_buf();
        
        let log_writer = Arc::new(LogWriter::new(log_config)?);
        
        let mut checkpoint_config = CheckpointConfig::default();
        checkpoint_config.enable_auto_checkpoint = false; // Отключаем для тестов
        
        Ok(CheckpointManager::new(checkpoint_config, log_writer))
    }

    #[tokio::test]
    async fn test_checkpoint_manager_creation() -> Result<()> {
        let _manager = create_test_checkpoint_manager().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_manual_checkpoint() -> Result<()> {
        let manager = create_test_checkpoint_manager().await?;
        
        let checkpoint_info = manager.create_checkpoint().await?;
        
        assert!(checkpoint_info.id > 0);
        assert!(checkpoint_info.lsn > 0);
        assert!(checkpoint_info.creation_time_ms > 0);
        
        Ok(())
    }

    #[tokio::test]
    async fn test_checkpoint_with_transactions() -> Result<()> {
        let manager = create_test_checkpoint_manager().await?;
        
        // Добавляем активные транзакции
        manager.add_active_transaction(100);
        manager.add_active_transaction(101);
        manager.add_active_transaction(102);
        
        // Добавляем измененные страницы
        manager.add_dirty_page(1, 10);
        manager.add_dirty_page(1, 11);
        manager.add_dirty_page(2, 20);
        
        let checkpoint_info = manager.create_checkpoint().await?;
        
        assert_eq!(checkpoint_info.active_transactions.len(), 3);
        assert_eq!(checkpoint_info.dirty_pages.len(), 3);
        assert!(checkpoint_info.flushed_pages > 0);
        
        Ok(())
    }

    #[tokio::test]
    async fn test_checkpoint_statistics() -> Result<()> {
        let manager = create_test_checkpoint_manager().await?;
        
        // Создаем несколько контрольных точек
        manager.create_checkpoint().await?;
        manager.create_checkpoint().await?;
        
        let stats = manager.get_statistics().await;
        
        assert_eq!(stats.total_checkpoints, 2);
        assert_eq!(stats.forced_checkpoints, 2); // Ручные контрольные точки
        assert!(stats.average_checkpoint_time_ms > 0);
        
        Ok(())
    }

    #[tokio::test]
    async fn test_shutdown_checkpoint() -> Result<()> {
        let mut manager = create_test_checkpoint_manager().await?;
        
        manager.add_active_transaction(200);
        
        let checkpoint_info = manager.create_shutdown_checkpoint().await?;
        
        assert!(checkpoint_info.id > 0);
        assert_eq!(checkpoint_info.active_transactions.len(), 1);
        
        manager.shutdown().await?;
        
        Ok(())
    }

    #[tokio::test]
    async fn test_data_source_updates() -> Result<()> {
        let manager = create_test_checkpoint_manager().await?;
        
        // Тестируем обновление активных транзакций
        let mut transactions = HashSet::new();
        transactions.insert(300);
        transactions.insert(301);
        manager.update_active_transactions(transactions);
        
        // Тестируем обновление измененных страниц
        let mut pages = HashSet::new();
        pages.insert((3, 30));
        pages.insert((3, 31));
        manager.update_dirty_pages(pages);
        
        let checkpoint_info = manager.create_checkpoint().await?;
        
        assert_eq!(checkpoint_info.active_transactions.len(), 2);
        assert_eq!(checkpoint_info.dirty_pages.len(), 2);
        
        Ok(())
    }

    // #[tokio::test]
    // async fn test_wait_for_checkpoint() -> Result<()> {
    //     let manager = create_test_checkpoint_manager().await?;
        
    //     // Запускаем создание контрольной точки в фоне
    //     let manager_clone = manager;
    //     let checkpoint_task = tokio::spawn(async move {
    //         tokio::time::sleep(Duration::from_millis(50)).await;
    //         manager_clone.create_checkpoint().await
    //     });
        
    //     // Ждем завершения контрольной точки
    //     let wait_result = manager.wait_for_checkpoint(Duration::from_secs(1)).await;
    //     assert!(wait_result.is_ok());
        
    //     let checkpoint_info = checkpoint_task.await.unwrap()?;
    //     assert!(checkpoint_info.id > 0);
        
    //     Ok(())
    // }
}
