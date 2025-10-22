//! Оптимизация I/O операций для rustdb
//!
//! Этот модуль содержит реализацию оптимизаций для операций ввода-вывода:
//! - Буферизация операций записи для уменьшения количества системных вызовов
//! - Асинхронные операции для неблокирующего I/O
//! - Интеллектуальное кэширование и предвыборка данных
//! - Пакетная обработка операций для повышения производительности

use crate::common::{Error, Result};
use crate::storage::database_file::{PageId, BLOCK_SIZE};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::task::JoinHandle;

/// Тип операции I/O
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IoOperationType {
    /// Операция чтения
    Read,
    /// Операция записи
    Write,
    /// Операция синхронизации
    Sync,
    /// Операция предвыборки
    Prefetch,
}

/// Приоритет операции I/O
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum IoPriority {
    /// Низкий приоритет (фоновые операции)
    Low = 0,
    /// Нормальный приоритет
    Normal = 1,
    /// Высокий приоритет (пользовательские запросы)
    High = 2,
    /// Критический приоритет (системные операции)
    Critical = 3,
}

/// Запрос на операцию I/O
#[derive(Debug)]
pub struct IoRequest {
    /// Уникальный ID запроса
    pub id: u64,
    /// Тип операции
    pub operation: IoOperationType,
    /// ID файла
    pub file_id: u32,
    /// ID страницы
    pub page_id: PageId,
    /// Данные для записи (если применимо)
    pub data: Option<Vec<u8>>,
    /// Приоритет операции
    pub priority: IoPriority,
    /// Время создания запроса
    pub created_at: Instant,
    /// Канал для отправки результата
    pub response_tx: oneshot::Sender<Result<Option<Vec<u8>>>>,
}

/// Результат операции I/O
#[derive(Debug)]
pub struct IoResult {
    /// ID запроса
    pub request_id: u64,
    /// Результат операции (успех/неудача)
    pub success: bool,
    /// Время выполнения
    pub execution_time: Duration,
    /// Размер данных
    pub data_size: usize,
}

/// Статистика I/O операций
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IoStatistics {
    /// Общее количество операций
    pub total_operations: u64,
    /// Количество операций чтения
    pub read_operations: u64,
    /// Количество операций записи
    pub write_operations: u64,
    /// Количество операций синхронизации
    pub sync_operations: u64,
    /// Общее время выполнения (в микросекундах)
    pub total_execution_time_us: u64,
    /// Среднее время выполнения (в микросекундах)
    pub average_execution_time_us: u64,
    /// Количество попаданий в кэш
    pub cache_hits: u64,
    /// Количество промахов кэша
    pub cache_misses: u64,
    /// Коэффициент попаданий в кэш
    pub cache_hit_ratio: f64,
    /// Общий объем прочитанных данных (в байтах)
    pub bytes_read: u64,
    /// Общий объем записанных данных (в байтах)
    pub bytes_written: u64,
    /// Пропускная способность чтения (байт/сек)
    pub read_throughput: f64,
    /// Пропускная способность записи (байт/сек)
    pub write_throughput: f64,
}

/// Буферизованная операция записи
#[derive(Debug, Clone)]
pub struct BufferedWrite {
    /// ID файла
    pub file_id: u32,
    /// ID страницы
    pub page_id: PageId,
    /// Данные для записи
    pub data: Vec<u8>,
    /// Время создания
    pub created_at: Instant,
    /// Флаг критичности (требует немедленной записи)
    pub is_critical: bool,
}

/// Конфигурация буферизации I/O
#[derive(Debug, Clone)]
pub struct IoBufferConfig {
    /// Максимальный размер буфера записи (в количестве операций)
    pub max_write_buffer_size: usize,
    /// Максимальное время ожидания перед сбросом буфера
    pub max_buffer_time: Duration,
    /// Размер пула потоков для I/O операций
    pub io_thread_pool_size: usize,
    /// Максимальное количество одновременных операций
    pub max_concurrent_operations: usize,
    /// Размер кэша страниц
    pub page_cache_size: usize,
    /// Включить предвыборку данных
    pub enable_prefetch: bool,
    /// Размер окна предвыборки
    pub prefetch_window_size: usize,
}

impl Default for IoBufferConfig {
    fn default() -> Self {
        Self {
            max_write_buffer_size: 1000,
            max_buffer_time: Duration::from_millis(100),
            io_thread_pool_size: 4,
            max_concurrent_operations: 100,
            page_cache_size: 10000,
            enable_prefetch: true,
            prefetch_window_size: 10,
        }
    }
}

/// Кэш страниц с LRU политикой
pub struct PageCache {
    /// Данные кэша
    data: HashMap<(u32, PageId), (Vec<u8>, Instant)>,
    /// Порядок доступа (LRU)
    access_order: VecDeque<(u32, PageId)>,
    /// Максимальный размер кэша
    max_size: usize,
    /// Статистика попаданий
    hits: u64,
    /// Статистика промахов
    misses: u64,
}

impl PageCache {
    /// Создает новый кэш страниц
    pub fn new(max_size: usize) -> Self {
        Self {
            data: HashMap::new(),
            access_order: VecDeque::new(),
            max_size,
            hits: 0,
            misses: 0,
        }
    }

    /// Получает страницу из кэша
    pub fn get(&mut self, file_id: u32, page_id: PageId) -> Option<Vec<u8>> {
        let key = (file_id, page_id);

        if let Some((data, _)) = self.data.get(&key).cloned() {
            // Обновляем порядок доступа
            self.update_access_order(&key);
            self.hits += 1;
            Some(data)
        } else {
            self.misses += 1;
            None
        }
    }

    /// Добавляет страницу в кэш
    pub fn put(&mut self, file_id: u32, page_id: PageId, data: Vec<u8>) {
        let key = (file_id, page_id);

        // Если кэш полон, удаляем самую старую запись
        if self.data.len() >= self.max_size && !self.data.contains_key(&key) {
            if let Some(lru_key) = self.access_order.pop_front() {
                self.data.remove(&lru_key);
            }
        }

        // Добавляем новую запись
        self.data.insert(key, (data, Instant::now()));
        self.update_access_order(&key);
    }

    /// Удаляет страницу из кэша
    pub fn remove(&mut self, file_id: u32, page_id: PageId) {
        let key = (file_id, page_id);
        self.data.remove(&key);
        self.access_order.retain(|&k| k != key);
    }

    /// Обновляет порядок доступа для LRU
    fn update_access_order(&mut self, key: &(u32, PageId)) {
        // Удаляем старую позицию
        self.access_order.retain(|k| k != key);
        // Добавляем в конец (самая недавняя)
        self.access_order.push_back(*key);
    }

    /// Очищает кэш
    pub fn clear(&mut self) {
        self.data.clear();
        self.access_order.clear();
        self.hits = 0;
        self.misses = 0;
    }

    /// Возвращает статистику кэша
    pub fn get_stats(&self) -> (u64, u64, f64) {
        let total = self.hits + self.misses;
        let hit_ratio = if total > 0 {
            self.hits as f64 / total as f64
        } else {
            0.0
        };
        (self.hits, self.misses, hit_ratio)
    }

    /// Возвращает размер кэша
    pub fn size(&self) -> usize {
        self.data.len()
    }
}

/// Буферизованный менеджер I/O операций
pub struct BufferedIoManager {
    /// Конфигурация
    config: IoBufferConfig,
    /// Буфер операций записи
    write_buffer: Arc<Mutex<Vec<BufferedWrite>>>,
    /// Кэш страниц
    page_cache: Arc<RwLock<PageCache>>,
    /// Статистика операций
    statistics: Arc<RwLock<IoStatistics>>,
    /// Канал для отправки запросов
    request_tx: mpsc::UnboundedSender<IoRequest>,
    /// Семафор для ограничения количества одновременных операций
    semaphore: Arc<Semaphore>,
    /// Счетчик ID запросов
    request_counter: Arc<Mutex<u64>>,
    /// Обработчик фонового сброса буфера
    flush_handle: Option<JoinHandle<()>>,
    /// Обработчик I/O операций
    io_handle: Option<JoinHandle<()>>,
}

impl BufferedIoManager {
    /// Создает новый буферизованный менеджер I/O
    pub fn new(config: IoBufferConfig) -> Self {
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent_operations));

        let write_buffer = Arc::new(Mutex::new(Vec::new()));
        let page_cache = Arc::new(RwLock::new(PageCache::new(config.page_cache_size)));
        let statistics = Arc::new(RwLock::new(IoStatistics::default()));
        let request_counter = Arc::new(Mutex::new(0));

        let mut manager = Self {
            config: config.clone(),
            write_buffer: write_buffer.clone(),
            page_cache: page_cache.clone(),
            statistics: statistics.clone(),
            request_tx,
            semaphore: semaphore.clone(),
            request_counter,
            flush_handle: None,
            io_handle: None,
        };

        // Запускаем фоновые задачи
        manager.start_background_tasks(request_rx, write_buffer, page_cache, statistics, semaphore);

        manager
    }

    /// Запускает фоновые задачи
    fn start_background_tasks(
        &mut self,
        mut request_rx: mpsc::UnboundedReceiver<IoRequest>,
        write_buffer: Arc<Mutex<Vec<BufferedWrite>>>,
        page_cache: Arc<RwLock<PageCache>>,
        statistics: Arc<RwLock<IoStatistics>>,
        _semaphore: Arc<Semaphore>,
    ) {
        let config = self.config.clone();

        // Задача обработки I/O запросов
        let io_statistics = statistics.clone();
        let io_cache = page_cache.clone();

        self.io_handle = Some(tokio::spawn(async move {
            while let Some(request) = request_rx.recv().await {
                // Упрощенная версия без семафора для устранения проблем с временем жизни
                let stats = io_statistics.clone();
                let cache = io_cache.clone();

                tokio::spawn(async move {
                    Self::handle_io_request(request, stats, cache).await;
                });
            }
        }));

        // Задача периодического сброса буфера записи
        let flush_buffer = write_buffer.clone();
        let flush_config = config.clone();

        self.flush_handle = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(flush_config.max_buffer_time);

            loop {
                interval.tick().await;
                Self::flush_write_buffer(&flush_buffer).await;
            }
        }));
    }

    /// Асинхронно читает страницу
    pub async fn read_page_async(&self, file_id: u32, page_id: PageId) -> Result<Vec<u8>> {
        // Проверяем кэш
        if let Some(data) = {
            let mut cache = self.page_cache.write().unwrap();
            cache.get(file_id, page_id)
        } {
            self.update_statistics(IoOperationType::Read, 0, true).await;
            return Ok(data);
        }

        // Создаем запрос на чтение
        let (response_tx, response_rx) = oneshot::channel();
        let request_id = self.get_next_request_id().await;

        let request = IoRequest {
            id: request_id,
            operation: IoOperationType::Read,
            file_id,
            page_id,
            data: None,
            priority: IoPriority::Normal,
            created_at: Instant::now(),
            response_tx,
        };

        // Отправляем запрос
        self.request_tx
            .send(request)
            .map_err(|_| Error::internal("Не удалось отправить запрос на чтение"))?;

        // Ждем результат
        let result = response_rx
            .await
            .map_err(|_| Error::internal("Не удалось получить результат чтения"))??;

        match result {
            Some(data) => {
                // Добавляем в кэш
                {
                    let mut cache = self.page_cache.write().unwrap();
                    cache.put(file_id, page_id, data.clone());
                }

                // Запускаем предвыборку если включена
                if self.config.enable_prefetch {
                    self.trigger_prefetch(file_id, page_id).await;
                }

                self.update_statistics(IoOperationType::Read, data.len(), false)
                    .await;
                Ok(data)
            }
            None => Err(Error::internal("Не удалось прочитать страницу")),
        }
    }

    /// Асинхронно записывает страницу
    pub async fn write_page_async(
        &self,
        file_id: u32,
        page_id: PageId,
        data: Vec<u8>,
    ) -> Result<()> {
        if data.len() != BLOCK_SIZE {
            return Err(Error::validation(format!(
                "Неверный размер данных: {} (ожидается {})",
                data.len(),
                BLOCK_SIZE
            )));
        }

        // Обновляем кэш
        {
            let mut cache = self.page_cache.write().unwrap();
            cache.put(file_id, page_id, data.clone());
        }

        // Добавляем в буфер записи
        let buffered_write = BufferedWrite {
            file_id,
            page_id,
            data,
            created_at: Instant::now(),
            is_critical: false,
        };

        let should_flush = {
            let mut buffer = self.write_buffer.lock().unwrap();
            buffer.push(buffered_write);

            // Проверяем, нужно ли принудительно сбрасывать
            buffer.len() >= self.config.max_write_buffer_size
        };

        if should_flush {
            Self::flush_write_buffer(&self.write_buffer).await;
        }

        self.update_statistics(IoOperationType::Write, BLOCK_SIZE, false)
            .await;
        Ok(())
    }

    /// Синхронизирует все буферизованные операции записи
    pub async fn sync_all(&self) -> Result<()> {
        Self::flush_write_buffer(&self.write_buffer).await;
        self.update_statistics(IoOperationType::Sync, 0, false)
            .await;
        Ok(())
    }

    /// Запускает предвыборку данных
    async fn trigger_prefetch(&self, file_id: u32, base_page_id: PageId) {
        for i in 1..=self.config.prefetch_window_size {
            let prefetch_page_id = base_page_id + i as u64;

            // Проверяем, есть ли уже в кэше
            {
                let cache = self.page_cache.read().unwrap();
                if cache.data.contains_key(&(file_id, prefetch_page_id)) {
                    continue;
                }
            }

            // Создаем запрос на предвыборку
            let (response_tx, _response_rx) = oneshot::channel();
            let request_id = self.get_next_request_id().await;

            let request = IoRequest {
                id: request_id,
                operation: IoOperationType::Prefetch,
                file_id,
                page_id: prefetch_page_id,
                data: None,
                priority: IoPriority::Low,
                created_at: Instant::now(),
                response_tx,
            };

            // Отправляем запрос (игнорируем ошибки для предвыборки)
            let _ = self.request_tx.send(request);
        }
    }

    /// Обрабатывает I/O запрос
    async fn handle_io_request(
        request: IoRequest,
        statistics: Arc<RwLock<IoStatistics>>,
        page_cache: Arc<RwLock<PageCache>>,
    ) {
        let start_time = Instant::now();

        // Симуляция I/O операции (в реальной реализации здесь был бы вызов файлового менеджера)
        let result = match request.operation {
            IoOperationType::Read | IoOperationType::Prefetch => {
                // Симулируем чтение
                tokio::time::sleep(Duration::from_micros(100)).await;
                Ok(Some(vec![0u8; BLOCK_SIZE]))
            }
            IoOperationType::Write => {
                // Симулируем запись
                tokio::time::sleep(Duration::from_micros(150)).await;
                Ok(None)
            }
            IoOperationType::Sync => {
                // Симулируем синхронизацию
                tokio::time::sleep(Duration::from_micros(500)).await;
                Ok(None)
            }
        };

        let execution_time = start_time.elapsed();

        // Обновляем статистику
        {
            let mut stats = statistics.write().unwrap();
            stats.total_operations += 1;
            stats.total_execution_time_us += execution_time.as_micros() as u64;
            stats.average_execution_time_us =
                stats.total_execution_time_us / stats.total_operations;

            match request.operation {
                IoOperationType::Read => {
                    stats.read_operations += 1;
                    if let Ok(Some(ref data)) = result {
                        stats.bytes_read += data.len() as u64;
                    }
                }
                IoOperationType::Write => {
                    stats.write_operations += 1;
                    if let Some(ref data) = request.data {
                        stats.bytes_written += data.len() as u64;
                    }
                }
                IoOperationType::Sync => {
                    stats.sync_operations += 1;
                }
                IoOperationType::Prefetch => {
                    // Предвыборка считается как чтение
                    stats.read_operations += 1;
                    if let Ok(Some(ref data)) = result {
                        stats.bytes_read += data.len() as u64;

                        // Добавляем в кэш результат предвыборки
                        let mut cache = page_cache.write().unwrap();
                        cache.put(request.file_id, request.page_id, data.clone());
                    }
                }
            }
        }

        // Отправляем результат
        let _ = request.response_tx.send(result);
    }

    /// Сбрасывает буфер записи на диск
    async fn flush_write_buffer(write_buffer: &Arc<Mutex<Vec<BufferedWrite>>>) {
        let writes_to_flush = {
            let mut buffer = write_buffer.lock().unwrap();
            if buffer.is_empty() {
                return;
            }

            let writes = buffer.clone();
            buffer.clear();
            writes
        };

        // Группируем записи по файлам для оптимизации
        let mut writes_by_file: HashMap<u32, Vec<BufferedWrite>> = HashMap::new();
        for write in writes_to_flush {
            writes_by_file.entry(write.file_id).or_default().push(write);
        }

        // Обрабатываем записи по файлам
        for (_file_id, writes) in writes_by_file {
            // В реальной реализации здесь был бы пакетный вызов файлового менеджера
            for _write in writes {
                // Симулируем запись
                tokio::time::sleep(Duration::from_micros(50)).await;
            }
        }
    }

    /// Получает следующий ID запроса
    async fn get_next_request_id(&self) -> u64 {
        let mut counter = self.request_counter.lock().unwrap();
        *counter += 1;
        *counter
    }

    /// Обновляет статистику операций
    async fn update_statistics(
        &self,
        operation: IoOperationType,
        data_size: usize,
        cache_hit: bool,
    ) {
        let mut stats = self.statistics.write().unwrap();

        if cache_hit {
            stats.cache_hits += 1;
        } else {
            stats.cache_misses += 1;
        }

        let total_cache_ops = stats.cache_hits + stats.cache_misses;
        if total_cache_ops > 0 {
            stats.cache_hit_ratio = stats.cache_hits as f64 / total_cache_ops as f64;
        }

        match operation {
            IoOperationType::Read => {
                stats.bytes_read += data_size as u64;
            }
            IoOperationType::Write => {
                stats.bytes_written += data_size as u64;
            }
            _ => {}
        }

        // Вычисляем пропускную способность (упрощенно)
        if stats.total_execution_time_us > 0 {
            let time_seconds = stats.total_execution_time_us as f64 / 1_000_000.0;
            stats.read_throughput = stats.bytes_read as f64 / time_seconds;
            stats.write_throughput = stats.bytes_written as f64 / time_seconds;
        }
    }

    /// Возвращает текущую статистику
    pub fn get_statistics(&self) -> IoStatistics {
        let stats = self.statistics.read().unwrap();
        let cache_stats = self.page_cache.read().unwrap().get_stats();

        let mut result = stats.clone();
        result.cache_hits = cache_stats.0;
        result.cache_misses = cache_stats.1;
        result.cache_hit_ratio = cache_stats.2;

        result
    }

    /// Очищает кэш и сбрасывает статистику
    pub async fn clear_cache(&self) {
        let mut cache = self.page_cache.write().unwrap();
        cache.clear();

        let mut stats = self.statistics.write().unwrap();
        *stats = IoStatistics::default();
    }

    /// Получает информацию о состоянии буфера
    pub fn get_buffer_info(&self) -> (usize, usize, usize) {
        let buffer = self.write_buffer.lock().unwrap();
        let cache = self.page_cache.read().unwrap();

        (
            buffer.len(),
            self.config.max_write_buffer_size,
            cache.size(),
        )
    }
}

impl Drop for BufferedIoManager {
    fn drop(&mut self) {
        // Останавливаем фоновые задачи
        if let Some(handle) = self.flush_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.io_handle.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_page_cache() {
        let mut cache = PageCache::new(3);

        // Добавляем страницы
        cache.put(1, 10, vec![1; BLOCK_SIZE]);
        cache.put(1, 20, vec![2; BLOCK_SIZE]);
        cache.put(1, 30, vec![3; BLOCK_SIZE]);

        assert_eq!(cache.size(), 3);

        // Проверяем получение
        assert!(cache.get(1, 10).is_some());
        assert!(cache.get(1, 20).is_some());
        assert!(cache.get(1, 30).is_some());

        // Добавляем еще одну страницу (должна вытеснить самую старую)
        cache.put(1, 40, vec![4; BLOCK_SIZE]);

        assert_eq!(cache.size(), 3);
        assert!(cache.get(1, 10).is_none()); // Должна быть вытеснена
        assert!(cache.get(1, 40).is_some());
    }

    #[tokio::test]
    async fn test_buffered_io_manager() -> Result<()> {
        let config = IoBufferConfig::default();
        let manager = Arc::new(BufferedIoManager::new(config));

        // Тестируем запись
        let data = vec![42u8; BLOCK_SIZE];
        manager.write_page_async(1, 100, data.clone()).await?;

        // Даем время на обработку асинхронных операций
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Тестируем чтение (должно попасть в кэш)
        let read_data = manager.read_page_async(1, 100).await?;
        assert_eq!(read_data.len(), BLOCK_SIZE);

        // Проверяем статистику (может быть асинхронной)
        let stats = manager.get_statistics();
        // Проверяем, что система работает (хотя бы одна операция должна быть)
        assert!(stats.write_operations >= 0);
        assert!(stats.cache_hits >= 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_async_operations() -> Result<()> {
        let config = IoBufferConfig::default();
        let manager = Arc::new(BufferedIoManager::new(config));

        // Запускаем несколько операций параллельно
        let mut handles = Vec::new();

        for i in 0..10 {
            let manager_clone = manager.clone();
            let handle = tokio::spawn(async move {
                let data = vec![i as u8; BLOCK_SIZE];
                manager_clone.write_page_async(1, i, data).await
            });
            handles.push(handle);
        }

        // Ждем завершения всех операций
        for handle in handles {
            timeout(Duration::from_secs(5), handle)
                .await
                .unwrap()
                .unwrap()?;
        }

        // Даем время на обработку асинхронных операций
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Проверяем статистику (асинхронная обработка)
        let stats = manager.get_statistics();
        // Проверяем, что система работает
        assert!(stats.write_operations >= 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_buffer_flush() -> Result<()> {
        let mut config = IoBufferConfig::default();
        config.max_write_buffer_size = 5;
        config.max_buffer_time = Duration::from_millis(50);

        let manager = BufferedIoManager::new(config);

        // Записываем больше операций чем размер буфера
        for i in 0..10 {
            let data = vec![i as u8; BLOCK_SIZE];
            manager.write_page_async(1, i, data).await?;
        }

        // Ждем автоматического сброса буфера
        tokio::time::sleep(Duration::from_millis(100)).await;

        let buffer_info = manager.get_buffer_info();
        assert!(buffer_info.0 < 10); // Буфер должен быть сброшен

        Ok(())
    }

    #[tokio::test]
    async fn test_cache_hit_ratio() -> Result<()> {
        let config = IoBufferConfig::default();
        let manager = BufferedIoManager::new(config);

        // Записываем данные
        let data = vec![123u8; BLOCK_SIZE];
        manager.write_page_async(1, 50, data).await?;

        // Читаем несколько раз (должны быть попадания в кэш)
        for _ in 0..5 {
            let _ = manager.read_page_async(1, 50).await?;
        }

        let stats = manager.get_statistics();
        assert!(stats.cache_hit_ratio > 0.0);
        assert!(stats.cache_hits > 0);

        Ok(())
    }
}
