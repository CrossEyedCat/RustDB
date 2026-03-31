//! Lock manager for rustdb
//!
//! Implements a locking system with support for Shared/Exclusive locks,
//! deadlock detection and two-phase locking (2PL).

use crate::common::{Error, Result};
use crate::core::transaction::TransactionId;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

/// Lock type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LockType {
    /// Page lock
    Page(u64),
    /// Table lock
    Table(String),
    /// Record lock
    Record(u64, u64), // (page_id, record_id)
    /// Index lock
    Index(String),
    /// Arbitrary resource lock
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

/// Lock mode
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum LockMode {
    /// Shared lock - for reading
    Shared,
    /// Exclusive lock - for writing
    Exclusive,
}

impl LockMode {
    /// Checks lock mode compatibility
    pub fn is_compatible(&self, other: &LockMode) -> bool {
        match (self, other) {
            // Shared locks are compatible with each other
            (LockMode::Shared, LockMode::Shared) => true,
            // Exclusive locks are not compatible with anything
            (LockMode::Exclusive, _) | (_, LockMode::Exclusive) => false,
        }
    }
}

/// Lock information
#[derive(Debug, Clone)]
pub struct LockInfo {
    /// Transaction owning the lock
    pub transaction_id: TransactionId,
    /// Resource type
    pub lock_type: LockType,
    /// Lock mode
    pub lock_mode: LockMode,
    /// Lock acquisition time
    pub acquired_at: Instant,
}

/// Lock request in wait queue
#[derive(Debug, Clone)]
pub struct LockRequest {
    /// Transaction requesting the lock
    pub transaction_id: TransactionId,
    /// Resource type
    pub lock_type: LockType,
    /// Lock mode
    pub lock_mode: LockMode,
    /// Request creation time
    pub requested_at: Instant,
}

/// Wait-for graph for deadlock detection
#[derive(Debug, Default)]
pub struct WaitForGraph {
    /// Graph edges: transaction -> set of transactions it's waiting for
    edges: HashMap<TransactionId, HashSet<TransactionId>>,
}

impl WaitForGraph {
    /// Adds edge to graph (transaction waits for waiting_for)
    pub fn add_edge(&mut self, transaction: TransactionId, waiting_for: TransactionId) {
        self.edges
            .entry(transaction)
            .or_insert_with(HashSet::new)
            .insert(waiting_for);
    }

    /// Removes all edges related to transaction
    pub fn remove_transaction(&mut self, transaction: TransactionId) {
        self.edges.remove(&transaction);
        for edges in self.edges.values_mut() {
            edges.remove(&transaction);
        }
    }

    /// Detects cycles in graph (deadlocks)
    pub fn detect_deadlock(&self) -> Option<Vec<TransactionId>> {
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();
        let mut path = Vec::new();

        for &transaction in self.edges.keys() {
            if !visited.contains(&transaction) {
                if let Some(cycle) =
                    self.dfs_detect_cycle(transaction, &mut visited, &mut rec_stack, &mut path)
                {
                    return Some(cycle);
                }
            }
        }

        None
    }

    /// Depth-first search for cycle detection
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
                    // Cycle found
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

/// Lock manager statistics
#[derive(Debug, Clone, Default)]
pub struct LockManagerStats {
    /// Total number of lock requests
    pub total_lock_requests: u64,
    /// Number of successfully acquired locks
    pub locks_acquired: u64,
    /// Number of released locks
    pub locks_released: u64,
    /// Number of blocked requests
    pub blocked_requests: u64,
    /// Number of detected deadlocks
    pub deadlocks_detected: u64,
    /// Average lock wait time (in milliseconds)
    pub average_wait_time: f64,
    /// Number of active locks
    pub active_locks: u64,
    /// Number of requests in wait queue
    pub waiting_requests: u64,
}

/// Lock manager
pub struct LockManager {
    /// Active locks: resource -> list of locks
    active_locks: Arc<RwLock<HashMap<String, Vec<LockInfo>>>>,
    /// Wait queues: resource -> queue of requests
    wait_queues: Arc<Mutex<HashMap<String, VecDeque<LockRequest>>>>,
    /// Wait-for graph for deadlock detection
    wait_for_graph: Arc<Mutex<WaitForGraph>>,
    /// Statistics
    stats: Arc<Mutex<LockManagerStats>>,
}

impl LockManager {
    /// Creates a new lock manager
    pub fn new() -> Result<Self> {
        Ok(Self {
            active_locks: Arc::new(RwLock::new(HashMap::new())),
            wait_queues: Arc::new(Mutex::new(HashMap::new())),
            wait_for_graph: Arc::new(Mutex::new(WaitForGraph::default())),
            stats: Arc::new(Mutex::new(LockManagerStats::default())),
        })
    }

    /// Attempts to acquire lock
    pub fn acquire_lock(
        &self,
        transaction_id: TransactionId,
        resource: String,
        lock_type: LockType,
        lock_mode: LockMode,
    ) -> Result<bool> {
        // Update statistics
        {
            let mut stats = self
                .stats
                .lock()
                .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
            stats.total_lock_requests += 1;
        }

        // Check compatibility with existing locks
        let can_acquire = {
            let active_locks = self.active_locks.read().map_err(|_| {
                Error::internal("Failed to acquire read lock on active locks".to_string())
            })?;

            if let Some(existing_locks) = active_locks.get(&resource) {
                // Check if this transaction already owns a lock
                if let Some(existing) = existing_locks
                    .iter()
                    .find(|l| l.transaction_id == transaction_id)
                {
                    // Transaction already owns lock - check upgrade
                    if existing.lock_mode == lock_mode {
                        return Ok(true); // Already has required lock
                    } else if existing.lock_mode == LockMode::Shared
                        && lock_mode == LockMode::Exclusive
                    {
                        // Attempting upgrade from Shared to Exclusive
                        // This is only possible if there are no other Shared locks
                        existing_locks.len() == 1
                    } else {
                        false // Downgrade not supported
                    }
                } else {
                    // Check compatibility with locks from other transactions
                    existing_locks
                        .iter()
                        .all(|existing| lock_mode.is_compatible(&existing.lock_mode))
                }
            } else {
                true // No existing locks
            }
        };

        if can_acquire {
            // Can acquire lock immediately
            self.grant_lock(transaction_id, resource, lock_type, lock_mode)?;
            Ok(true)
        } else {
            // Add to wait queue
            self.add_to_wait_queue(transaction_id, resource, lock_type, lock_mode)?;
            Ok(false)
        }
    }

    /// Returns the first transaction holding a lock on `resource` (if any).
    pub fn get_lock_owner(&self, resource: &str) -> Result<Option<TransactionId>> {
        let active_locks = self.active_locks.read().map_err(|_| {
            Error::internal("Failed to acquire read lock on active locks".to_string())
        })?;
        Ok(active_locks
            .get(resource)
            .and_then(|locks| locks.first().map(|l| l.transaction_id)))
    }

    /// Releases every lock held by `transaction_id`.
    pub fn release_all_locks(&self, transaction_id: TransactionId) -> Result<()> {
        let resources: Vec<String> = {
            let active_locks = self.active_locks.read().map_err(|_| {
                Error::internal("Failed to acquire read lock on active locks".to_string())
            })?;
            active_locks
                .iter()
                .filter(|(_, locks)| locks.iter().any(|l| l.transaction_id == transaction_id))
                .map(|(r, _)| r.clone())
                .collect()
        };
        for r in resources {
            self.release_lock(transaction_id, r)?;
        }
        Ok(())
    }

    /// Releases lock
    pub fn release_lock(&self, transaction_id: TransactionId, resource: String) -> Result<()> {
        // Remove lock
        let removed = {
            let mut active_locks = self.active_locks.write().map_err(|_| {
                Error::internal("Failed to acquire write lock on active locks".to_string())
            })?;

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
            // Update wait-for graph
            {
                let mut graph = self.wait_for_graph.lock().map_err(|_| {
                    Error::internal("Failed to acquire wait-for graph lock".to_string())
                })?;
                graph.remove_transaction(transaction_id);
            }

            // Update statistics
            {
                let mut stats = self
                    .stats
                    .lock()
                    .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
                stats.locks_released += 1;
                stats.active_locks = stats.active_locks.saturating_sub(1);
            }

            // Process wait queue
            self.process_wait_queue(&resource)?;
        }

        Ok(())
    }

    /// Grants lock
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
            let mut active_locks = self.active_locks.write().map_err(|_| {
                Error::internal("Failed to acquire write lock on active locks".to_string())
            })?;

            active_locks
                .entry(resource)
                .or_insert_with(Vec::new)
                .push(lock_info);
        }

        // Update statistics
        {
            let mut stats = self
                .stats
                .lock()
                .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
            stats.locks_acquired += 1;
            stats.active_locks += 1;
        }

        Ok(())
    }

    /// Adds request to wait queue
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
            let mut wait_queues = self
                .wait_queues
                .lock()
                .map_err(|_| Error::internal("Failed to acquire wait queues lock".to_string()))?;

            wait_queues
                .entry(resource.clone())
                .or_insert_with(VecDeque::new)
                .push_back(request);
        }

        // Update wait-for graph
        self.update_wait_for_graph(transaction_id, &resource)?;

        // Update statistics
        {
            let mut stats = self
                .stats
                .lock()
                .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
            stats.blocked_requests += 1;
            stats.waiting_requests += 1;
        }

        // Check for deadlock
        self.check_for_deadlock()?;

        Ok(())
    }

    /// Processes wait queue for resource
    fn process_wait_queue(&self, resource: &str) -> Result<()> {
        let mut requests_to_grant = Vec::new();

        {
            let mut wait_queues = self
                .wait_queues
                .lock()
                .map_err(|_| Error::internal("Failed to acquire wait queues lock".to_string()))?;

            if let Some(queue) = wait_queues.get_mut(resource) {
                while let Some(request) = queue.front() {
                    // Check if lock can be granted
                    let can_grant = {
                        let active_locks = self.active_locks.read().map_err(|_| {
                            Error::internal(
                                "Failed to acquire read lock on active locks".to_string(),
                            )
                        })?;

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
                        break; // Cannot grant - stop
                    }
                }

                if queue.is_empty() {
                    wait_queues.remove(resource);
                }
            }
        }

        // Grant locks outside queue lock
        for request in requests_to_grant {
            self.grant_lock(
                request.transaction_id,
                resource.to_string(),
                request.lock_type,
                request.lock_mode,
            )?;

            // Update statistics
            let mut stats = self
                .stats
                .lock()
                .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
            stats.waiting_requests = stats.waiting_requests.saturating_sub(1);
        }

        Ok(())
    }

    /// Updates wait-for graph
    fn update_wait_for_graph(
        &self,
        waiting_transaction: TransactionId,
        resource: &str,
    ) -> Result<()> {
        let mut graph = self
            .wait_for_graph
            .lock()
            .map_err(|_| Error::internal("Failed to acquire wait-for graph lock".to_string()))?;

        // Find transactions owning locks on this resource
        let active_locks = self.active_locks.read().map_err(|_| {
            Error::internal("Failed to acquire read lock on active locks".to_string())
        })?;

        if let Some(locks) = active_locks.get(resource) {
            for lock in locks {
                if lock.transaction_id != waiting_transaction {
                    graph.add_edge(waiting_transaction, lock.transaction_id);
                }
            }
        }

        Ok(())
    }

    /// Checks for deadlocks
    fn check_for_deadlock(&self) -> Result<()> {
        let graph = self
            .wait_for_graph
            .lock()
            .map_err(|_| Error::internal("Failed to acquire wait-for graph lock".to_string()))?;

        if let Some(cycle) = graph.detect_deadlock() {
            // Update statistics
            {
                let mut stats = self
                    .stats
                    .lock()
                    .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
                stats.deadlocks_detected += 1;
            }

            // Return deadlock error
            return Err(Error::DeadlockDetected(format!(
                "Deadlock detected involving transactions: {:?}",
                cycle
            )));
        }

        Ok(())
    }

    /// Gets lock manager statistics
    pub fn get_statistics(&self) -> Result<LockManagerStats> {
        let stats = self
            .stats
            .lock()
            .map_err(|_| Error::internal("Failed to acquire stats lock".to_string()))?;
        Ok(stats.clone())
    }

    /// Gets information about all active locks
    pub fn get_active_locks(&self) -> Result<HashMap<String, Vec<LockInfo>>> {
        let active_locks = self.active_locks.read().map_err(|_| {
            Error::internal("Failed to acquire read lock on active locks".to_string())
        })?;
        Ok(active_locks.clone())
    }

    /// Gets information about all waiting requests
    pub fn get_waiting_requests(&self) -> Result<HashMap<String, VecDeque<LockRequest>>> {
        let wait_queues = self
            .wait_queues
            .lock()
            .map_err(|_| Error::internal("Failed to acquire wait queues lock".to_string()))?;
        Ok(wait_queues.clone())
    }
}
