//! Менеджер блокировок для rustdb
//! 
//! Реализует систему блокировок с поддержкой Shared/Exclusive блокировок,
//! обнаружения дедлоков и двухфазного блокирования (2PL).

use crate::common::{Error, Result};
use crate::core::transaction::TransactionId;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

/// Тип блокировки
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LockType {
    /// Блокировка страницы
    Page(u64),
    /// Блокировка таблицы
    Table(String),
    /// Блокировка записи
    Record(u64, u64), // (page_id, record_id)
    /// Блокировка индекса
    Index(String),
    /// Произвольный ресурс
    Resource(String),
}

impl std::fmt::Display for LockType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockType::Page(id) => write!(f, "Page({})", id),
            LockType::Table(name) => write!(f, "Table({})", name),
            LockType::Record(page_id, record_id) => write!(f, "Record({}, {})", page_id, record_id),
            LockType::Index(name) => write!(f, "Index({})", name),
            LockType::Resource(name) => write!(f, "Resource({})", name),
        }
    }
}

/// Режим блокировки
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum LockMode {
    /// Разделяемая блокировка (Shared) - для чтения
    Shared,
    /// Исключительная блокировка (Exclusive) - для записи
    Exclusive,
}

impl LockMode {
    /// Проверяет совместимость режимов блокировки
    pub fn is_compatible(&self, other: &LockMode) -> bool {
        match (self, other) {
            // Shared блокировки совместимы между собой
            (LockMode::Shared, LockMode::Shared) => true,
            // Exclusive блокировки не совместимы ни с чем
            (LockMode::Exclusive, _) | (_, LockMode::Exclusive) => false,
        }
    }
}

/// Информация о блокировке
#[derive(Debug, Clone)]
pub struct LockInfo {
    /// Транзакция, владеющая блокировкой
    pub transaction_id: TransactionId,
    /// Тип ресурса
    pub lock_type: LockType,
    /// Режим блокировки
    pub lock_mode: LockMode,
    /// Время получения блокировки
    pub acquired_at: Instant,
}

/// Запрос на блокировку в очереди ожидания
#[derive(Debug, Clone)]
pub struct LockRequest {
    /// Транзакция, запрашивающая блокировку
    pub transaction_id: TransactionId,
    /// Тип ресурса
    pub lock_type: LockType,
    /// Режим блокировки
    pub lock_mode: LockMode,
    /// Время создания запроса
    pub requested_at: Instant,
}

/// Граф ожидания для обнаружения дедлоков
#[derive(Debug, Default)]
pub struct WaitForGraph {
    /// Рёбра графа: транзакция -> множество транзакций, которых она ждет
    edges: HashMap<TransactionId, HashSet<TransactionId>>,
}

impl WaitForGraph {
    /// Добавляет ребро в граф (transaction ждет waiting_for)
    pub fn add_edge(&mut self, transaction: TransactionId, waiting_for: TransactionId) {
        self.edges.entry(transaction).or_insert_with(HashSet::new).insert(waiting_for);
    }
    
    /// Удаляет все рёбра, связанные с транзакцией
    pub fn remove_transaction(&mut self, transaction: TransactionId) {
        self.edges.remove(&transaction);
        for edges in self.edges.values_mut() {
            edges.remove(&transaction);
        }
    }
    
    /// Обнаруживает циклы в графе (дедлоки)
    pub fn detect_deadlock(&self) -> Option<Vec<TransactionId>> {
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();
        let mut path = Vec::new();
        
        for &transaction in self.edges.keys() {
            if !visited.contains(&transaction) {
                if let Some(cycle) = self.dfs_detect_cycle(transaction, &mut visited, &mut rec_stack, &mut path) {
                    return Some(cycle);
                }
            }
        }
        
        None
    }
    
    /// Поиск в глубину для обнаружения циклов
    fn dfs_detect_cycle(
        &self,
        transaction: TransactionId,
        visited: &mut HashSet<TransactionId>,
        rec_stack: &mut HashSet<TransactionId>,
        path: &mut Vec<TransactionId>,
    ) -> Option<Vec<TransactionId>> {
        visited.insert(transaction);
        rec_stack.insert(transaction);
        path.push(transaction);
        
        if let Some(neighbors) = self.edges.get(&transaction) {
            for &neighbor in neighbors {
                if !visited.contains(&neighbor) {
                    if let Some(cycle) = self.dfs_detect_cycle(neighbor, visited, rec_stack, path) {
                        return Some(cycle);
                    }
                } else if rec_stack.contains(&neighbor) {
                    // Найден цикл
                    let cycle_start = path.iter().position(|&t| t == neighbor).unwrap();
                    return Some(path[cycle_start..].to_vec());
                }
            }
        }
        
        path.pop();
        rec_stack.remove(&transaction);
        None
    }
}

/// Статистика менеджера блокировок
#[derive(Debug, Clone, Default)]
pub struct LockManagerStats {
    /// Общее количество запросов блокировок
    pub total_lock_requests: u64,
    /// Количество успешно полученных блокировок
    pub locks_acquired: u64,
    /// Количество освобожденных блокировок
    pub locks_released: u64,
    /// Количество заблокированных запросов
    pub blocked_requests: u64,
    /// Количество обнаруженных дедлоков
    pub deadlocks_detected: u64,
    /// Среднее время ожидания блокировки (в миллисекундах)
    pub average_wait_time: f64,
    /// Количество активных блокировок
    pub active_locks: u64,
    /// Количество запросов в очереди ожидания
    pub waiting_requests: u64,
}

/// Менеджер блокировок
pub struct LockManager {
    /// Активные блокировки: ресурс -> список блокировок
    active_locks: Arc<RwLock<HashMap<String, Vec<LockInfo>>>>,
    /// Очереди ожидания: ресурс -> очередь запросов
    wait_queues: Arc<Mutex<HashMap<String, VecDeque<LockRequest>>>>,
    /// Граф ожидания для обнаружения дедлоков
    wait_for_graph: Arc<Mutex<WaitForGraph>>,
    /// Статистика
    stats: Arc<Mutex<LockManagerStats>>,
}

impl LockManager {
    /// Создает новый менеджер блокировок
    pub fn new() -> Result<Self> {
        Ok(Self {
            active_locks: Arc::new(RwLock::new(HashMap::new())),
            wait_queues: Arc::new(Mutex::new(HashMap::new())),
            wait_for_graph: Arc::new(Mutex::new(WaitForGraph::default())),
            stats: Arc::new(Mutex::new(LockManagerStats::default())),
        })
    }
    
    /// Пытается получить блокировку
    pub fn acquire_lock(
        &self,
        transaction_id: TransactionId,
        resource: String,
        lock_type: LockType,
        lock_mode: LockMode,
    ) -> Result<bool> {
        // Обновляем статистику
        {
            let mut stats = self.stats.lock()
                .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
            stats.total_lock_requests += 1;
        }
        
        // Проверяем совместимость с существующими блокировками
        let can_acquire = {
            let active_locks = self.active_locks.read()
                .map_err(|_| Error::internal("Failed to acquire read lock on active locks".to_string()))?;
            
            if let Some(existing_locks) = active_locks.get(&resource) {
                // Проверяем, может ли эта транзакция уже владеть блокировкой
                if let Some(existing) = existing_locks.iter().find(|l| l.transaction_id == transaction_id) {
                    // Транзакция уже владеет блокировкой - проверяем upgrade
                    if existing.lock_mode == lock_mode {
                        return Ok(true); // Уже есть нужная блокировка
                    } else if existing.lock_mode == LockMode::Shared && lock_mode == LockMode::Exclusive {
                        // Пытаемся upgrade с Shared на Exclusive
                        // Это возможно только если нет других Shared блокировок
                        existing_locks.len() == 1
                    } else {
                        false // Downgrade не поддерживается
                    }
                } else {
                    // Проверяем совместимость с блокировками других транзакций
                    existing_locks.iter().all(|existing| lock_mode.is_compatible(&existing.lock_mode))
                }
            } else {
                true // Нет существующих блокировок
            }
        };
        
        if can_acquire {
            // Можем получить блокировку немедленно
            self.grant_lock(transaction_id, resource, lock_type, lock_mode)?;
            Ok(true)
        } else {
            // Добавляем в очередь ожидания
            self.add_to_wait_queue(transaction_id, resource, lock_type, lock_mode)?;
            Ok(false)
        }
    }
    
    /// Освобождает блокировку
    pub fn release_lock(&self, transaction_id: TransactionId, resource: String) -> Result<()> {
        // Удаляем блокировку
        let removed = {
            let mut active_locks = self.active_locks.write()
                .map_err(|_| Error::internal("Failed to acquire write lock on active locks".to_string()))?;
            
            let mut should_remove_resource = false;
            let removed = if let Some(locks) = active_locks.get_mut(&resource) {
                let original_len = locks.len();
                locks.retain(|lock| lock.transaction_id != transaction_id);
                
                if locks.is_empty() {
                    should_remove_resource = true;
                }
                
                original_len != locks.len()
            } else {
                false
            };
            
            if should_remove_resource {
                active_locks.remove(&resource);
            }
            
            removed
        };
        
        if removed {
            // Обновляем граф ожидания
            {
                let mut graph = self.wait_for_graph.lock()
                    .map_err(|_| Error::internal("Failed to acquire wait-for graph lock".to_string()))?;
                graph.remove_transaction(transaction_id);
            }
            
            // Обновляем статистику
            {
                let mut stats = self.stats.lock()
                    .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
                stats.locks_released += 1;
                stats.active_locks = stats.active_locks.saturating_sub(1);
            }
            
            // Проверяем очередь ожидания
            self.process_wait_queue(&resource)?;
        }
        
        Ok(())
    }
    
    /// Предоставляет блокировку
    fn grant_lock(
        &self,
        transaction_id: TransactionId,
        resource: String,
        lock_type: LockType,
        lock_mode: LockMode,
    ) -> Result<()> {
        let lock_info = LockInfo {
            transaction_id,
            lock_type,
            lock_mode,
            acquired_at: Instant::now(),
        };
        
        {
            let mut active_locks = self.active_locks.write()
                .map_err(|_| Error::internal("Failed to acquire write lock on active locks".to_string()))?;
            
            active_locks.entry(resource).or_insert_with(Vec::new).push(lock_info);
        }
        
        // Обновляем статистику
        {
            let mut stats = self.stats.lock()
                .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
            stats.locks_acquired += 1;
            stats.active_locks += 1;
        }
        
        Ok(())
    }
    
    /// Добавляет запрос в очередь ожидания
    fn add_to_wait_queue(
        &self,
        transaction_id: TransactionId,
        resource: String,
        lock_type: LockType,
        lock_mode: LockMode,
    ) -> Result<()> {
        let request = LockRequest {
            transaction_id,
            lock_type,
            lock_mode,
            requested_at: Instant::now(),
        };
        
        {
            let mut wait_queues = self.wait_queues.lock()
                .map_err(|_| Error::internal("Failed to acquire wait queues lock".to_string()))?;
            
            wait_queues.entry(resource.clone()).or_insert_with(VecDeque::new).push_back(request);
        }
        
        // Обновляем граф ожидания
        self.update_wait_for_graph(transaction_id, &resource)?;
        
        // Обновляем статистику
        {
            let mut stats = self.stats.lock()
                .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
            stats.blocked_requests += 1;
            stats.waiting_requests += 1;
        }
        
        // Проверяем на дедлок
        self.check_for_deadlock()?;
        
        Ok(())
    }
    
    /// Обрабатывает очередь ожидания для ресурса
    fn process_wait_queue(&self, resource: &str) -> Result<()> {
        let mut requests_to_grant = Vec::new();
        
        {
            let mut wait_queues = self.wait_queues.lock()
                .map_err(|_| Error::internal("Failed to acquire wait queues lock".to_string()))?;
            
            if let Some(queue) = wait_queues.get_mut(resource) {
                while let Some(request) = queue.front() {
                    // Проверяем, можно ли предоставить блокировку
                    let can_grant = {
                        let active_locks = self.active_locks.read()
                            .map_err(|_| Error::internal("Failed to acquire read lock on active locks".to_string()))?;
                        
                        if let Some(existing_locks) = active_locks.get(resource) {
                            existing_locks.iter().all(|existing| {
                                request.lock_mode.is_compatible(&existing.lock_mode)
                            })
                        } else {
                            true
                        }
                    };
                    
                    if can_grant {
                        let request = queue.pop_front().unwrap();
                        requests_to_grant.push(request);
                    } else {
                        break; // Не можем предоставить - останавливаемся
                    }
                }
                
                if queue.is_empty() {
                    wait_queues.remove(resource);
                }
            }
        }
        
        // Предоставляем блокировки вне блокировки очереди
        for request in requests_to_grant {
            self.grant_lock(request.transaction_id, resource.to_string(), request.lock_type, request.lock_mode)?;
            
            // Обновляем статистику
            let mut stats = self.stats.lock()
                .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
            stats.waiting_requests = stats.waiting_requests.saturating_sub(1);
        }
        
        Ok(())
    }
    
    /// Обновляет граф ожидания
    fn update_wait_for_graph(&self, waiting_transaction: TransactionId, resource: &str) -> Result<()> {
        let mut graph = self.wait_for_graph.lock()
            .map_err(|_| Error::internal("Failed to acquire wait-for graph lock".to_string()))?;
        
        // Находим транзакции, владеющие блокировками на этот ресурс
        let active_locks = self.active_locks.read()
            .map_err(|_| Error::internal("Failed to acquire read lock on active locks".to_string()))?;
        
        if let Some(locks) = active_locks.get(resource) {
            for lock in locks {
                if lock.transaction_id != waiting_transaction {
                    graph.add_edge(waiting_transaction, lock.transaction_id);
                }
            }
        }
        
        Ok(())
    }
    
    /// Проверяет наличие дедлоков
    fn check_for_deadlock(&self) -> Result<()> {
        let graph = self.wait_for_graph.lock()
            .map_err(|_| Error::internal("Failed to acquire wait-for graph lock".to_string()))?;
        
        if let Some(cycle) = graph.detect_deadlock() {
            // Обновляем статистику
            {
                let mut stats = self.stats.lock()
                    .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
                stats.deadlocks_detected += 1;
            }
            
            // Возвращаем ошибку дедлока
            return Err(Error::DeadlockDetected(format!(
                "Deadlock detected involving transactions: {:?}", cycle
            )));
        }
        
        Ok(())
    }
    
    /// Получает статистику менеджера блокировок
    pub fn get_statistics(&self) -> Result<LockManagerStats> {
        let stats = self.stats.lock()
            .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
        Ok(stats.clone())
    }
    
    /// Получает информацию о всех активных блокировках
    pub fn get_active_locks(&self) -> Result<HashMap<String, Vec<LockInfo>>> {
        let active_locks = self.active_locks.read()
            .map_err(|_| Error::internal("Failed to acquire read lock on active locks".to_string()))?;
        Ok(active_locks.clone())
    }
    
    /// Получает информацию о всех ожидающих запросах
    pub fn get_waiting_requests(&self) -> Result<HashMap<String, VecDeque<LockRequest>>> {
        let wait_queues = self.wait_queues.lock()
            .map_err(|_| Error::internal("Failed to acquire wait queues lock".to_string()))?;
        Ok(wait_queues.clone())
    }
}