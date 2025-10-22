//! Multi-Version Concurrency Control (MVCC) система
//!
//! Обеспечивает изоляцию транзакций через версионирование данных

use crate::common::{Error, Result};
use crate::core::transaction::TransactionId;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Идентификатор версии
pub type VersionId = u64;

/// Метка времени для версии
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp(u64);

impl Timestamp {
    /// Создаёт новую метку времени
    pub fn now() -> Self {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        Self(duration.as_secs() * 1_000_000 + duration.subsec_micros() as u64)
    }

    /// Возвращает значение timestamp
    pub fn value(&self) -> u64 {
        self.0
    }
}

/// Состояние версии
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionState {
    /// Версия активна
    Active,
    /// Версия зафиксирована
    Committed,
    /// Версия откачена
    Aborted,
    /// Версия помечена для удаления
    MarkedForDeletion,
}

/// Версия записи
#[derive(Debug, Clone)]
pub struct RowVersion {
    /// Идентификатор версии
    pub version_id: VersionId,
    /// Транзакция, создавшая версию
    pub created_by: TransactionId,
    /// Транзакция, удалившая версию (если есть)
    pub deleted_by: Option<TransactionId>,
    /// Метка времени создания
    pub created_at: Timestamp,
    /// Метка времени удаления (если есть)
    pub deleted_at: Option<Timestamp>,
    /// Состояние версии
    pub state: VersionState,
    /// Данные версии
    pub data: Vec<u8>,
    /// Ссылка на предыдущую версию
    pub prev_version: Option<VersionId>,
}

impl RowVersion {
    /// Создаёт новую версию
    pub fn new(
        version_id: VersionId,
        transaction_id: TransactionId,
        data: Vec<u8>,
        prev_version: Option<VersionId>,
    ) -> Self {
        Self {
            version_id,
            created_by: transaction_id,
            deleted_by: None,
            created_at: Timestamp::now(),
            deleted_at: None,
            state: VersionState::Active,
            data,
            prev_version,
        }
    }

    /// Проверяет видимость версии для транзакции
    pub fn is_visible(&self, transaction_id: TransactionId, snapshot_timestamp: Timestamp) -> bool {
        // Версия видна, если:
        // 1. Она создана до snapshot_timestamp
        if self.created_at > snapshot_timestamp {
            return false;
        }

        // 2. Она не удалена или удалена после snapshot_timestamp
        if let Some(deleted_at) = self.deleted_at {
            if deleted_at <= snapshot_timestamp {
                return false;
            }
        }

        // 3. Версия зафиксирована или создана текущей транзакцией
        match self.state {
            VersionState::Committed => true,
            VersionState::Active => self.created_by == transaction_id,
            _ => false,
        }
    }

    /// Помечает версию как зафиксированную
    pub fn commit(&mut self) {
        self.state = VersionState::Committed;
    }

    /// Помечает версию как откаченную
    pub fn abort(&mut self) {
        self.state = VersionState::Aborted;
    }

    /// Помечает версию как удалённую
    pub fn mark_deleted(&mut self, transaction_id: TransactionId) {
        self.deleted_by = Some(transaction_id);
        self.deleted_at = Some(Timestamp::now());
    }
}

/// Ключ записи (таблица + ID записи)
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct RowKey {
    pub table_id: u32,
    pub row_id: u64,
}

impl RowKey {
    pub fn new(table_id: u32, row_id: u64) -> Self {
        Self { table_id, row_id }
    }
}

/// Менеджер версий
pub struct MVCCManager {
    /// Хранилище версий (ключ записи -> список версий)
    versions: Arc<RwLock<HashMap<RowKey, Vec<RowVersion>>>>,
    /// Счётчик версий
    version_counter: Arc<Mutex<VersionId>>,
    /// Минимальная активная транзакция (для очистки старых версий)
    min_active_transaction: Arc<RwLock<TransactionId>>,
    /// Статистика MVCC
    statistics: Arc<Mutex<MVCCStatistics>>,
}

/// Статистика MVCC
#[derive(Debug, Clone)]
pub struct MVCCStatistics {
    /// Всего версий
    pub total_versions: u64,
    /// Активных версий
    pub active_versions: u64,
    /// Зафиксированных версий
    pub committed_versions: u64,
    /// Откаченных версий
    pub aborted_versions: u64,
    /// Версий, помеченных для удаления
    pub marked_for_deletion: u64,
    /// Операций VACUUM
    pub vacuum_operations: u64,
    /// Удалено версий при VACUUM
    pub versions_cleaned: u64,
    /// Последнее обновление
    pub last_updated: Instant,
}

impl MVCCStatistics {
    fn new() -> Self {
        Self {
            total_versions: 0,
            active_versions: 0,
            committed_versions: 0,
            aborted_versions: 0,
            marked_for_deletion: 0,
            vacuum_operations: 0,
            versions_cleaned: 0,
            last_updated: Instant::now(),
        }
    }
}

impl MVCCManager {
    /// Создаёт новый менеджер MVCC
    pub fn new() -> Self {
        Self {
            versions: Arc::new(RwLock::new(HashMap::new())),
            version_counter: Arc::new(Mutex::new(1)),
            min_active_transaction: Arc::new(RwLock::new(TransactionId::new(0))),
            statistics: Arc::new(Mutex::new(MVCCStatistics::new())),
        }
    }

    /// Создаёт новую версию записи
    pub fn create_version(
        &self,
        key: RowKey,
        transaction_id: TransactionId,
        data: Vec<u8>,
    ) -> Result<VersionId> {
        // Генерируем ID версии
        let version_id = {
            let mut counter = self.version_counter.lock().unwrap();
            let id = *counter;
            *counter += 1;
            id
        };

        let mut versions = self.versions.write().unwrap();
        let row_versions = versions.entry(key.clone()).or_insert_with(Vec::new);

        // Находим предыдущую версию
        let prev_version = row_versions.last().map(|v| v.version_id);

        // Создаём новую версию
        let version = RowVersion::new(version_id, transaction_id, data, prev_version);
        row_versions.push(version);

        // Обновляем статистику
        {
            let mut stats = self.statistics.lock().unwrap();
            stats.total_versions += 1;
            stats.active_versions += 1;
            stats.last_updated = Instant::now();
        }

        Ok(version_id)
    }

    /// Читает версию записи, видимую для транзакции
    pub fn read_version(
        &self,
        key: &RowKey,
        transaction_id: TransactionId,
        snapshot_timestamp: Timestamp,
    ) -> Result<Option<Vec<u8>>> {
        let versions = self.versions.read().unwrap();

        if let Some(row_versions) = versions.get(key) {
            // Ищем последнюю видимую версию
            for version in row_versions.iter().rev() {
                if version.is_visible(transaction_id, snapshot_timestamp) {
                    return Ok(Some(version.data.clone()));
                }
            }
        }

        Ok(None)
    }

    /// Удаляет запись (создаёт новую версию с пометкой удаления)
    pub fn delete_version(&self, key: &RowKey, transaction_id: TransactionId) -> Result<()> {
        let mut versions = self.versions.write().unwrap();

        if let Some(row_versions) = versions.get_mut(key) {
            if let Some(last_version) = row_versions.last_mut() {
                last_version.mark_deleted(transaction_id);

                // Обновляем статистику
                {
                    let mut stats = self.statistics.lock().unwrap();
                    stats.marked_for_deletion += 1;
                    stats.last_updated = Instant::now();
                }

                return Ok(());
            }
        }

        Err(Error::database("Версия записи не найдена"))
    }

    /// Фиксирует версии транзакции
    pub fn commit_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        let mut versions = self.versions.write().unwrap();
        let mut committed_count = 0;

        for row_versions in versions.values_mut() {
            for version in row_versions.iter_mut() {
                if version.created_by == transaction_id && version.state == VersionState::Active {
                    version.commit();
                    committed_count += 1;
                }
            }
        }

        // Обновляем статистику
        {
            let mut stats = self.statistics.lock().unwrap();
            stats.active_versions = stats.active_versions.saturating_sub(committed_count);
            stats.committed_versions += committed_count;
            stats.last_updated = Instant::now();
        }

        Ok(())
    }

    /// Откатывает версии транзакции
    pub fn abort_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        let mut versions = self.versions.write().unwrap();
        let mut aborted_count = 0;

        for row_versions in versions.values_mut() {
            for version in row_versions.iter_mut() {
                if version.created_by == transaction_id && version.state == VersionState::Active {
                    version.abort();
                    aborted_count += 1;
                }
            }
        }

        // Обновляем статистику
        {
            let mut stats = self.statistics.lock().unwrap();
            stats.active_versions = stats.active_versions.saturating_sub(aborted_count);
            stats.aborted_versions += aborted_count;
            stats.last_updated = Instant::now();
        }

        Ok(())
    }

    /// Очищает старые версии (VACUUM)
    pub fn vacuum(&self) -> Result<u64> {
        let min_active = *self.min_active_transaction.read().unwrap();
        let mut versions = self.versions.write().unwrap();
        let mut cleaned_count = 0;

        // Очищаем каждую цепочку версий
        for row_versions in versions.values_mut() {
            row_versions.retain(|version| {
                // Сохраняем версии, которые:
                // 1. Активны
                // 2. Созданы активными транзакциями
                // 3. Зафиксированы и могут быть видимы
                let should_keep = match version.state {
                    VersionState::Active => version.created_by >= min_active,
                    VersionState::Committed => {
                        // Оставляем последнюю зафиксированную версию
                        true
                    }
                    VersionState::Aborted | VersionState::MarkedForDeletion => {
                        // Удаляем откаченные и помеченные для удаления
                        false
                    }
                };

                if !should_keep {
                    cleaned_count += 1;
                }

                should_keep
            });
        }

        // Удаляем пустые цепочки
        versions.retain(|_, row_versions| !row_versions.is_empty());

        // Обновляем статистику
        {
            let mut stats = self.statistics.lock().unwrap();
            stats.vacuum_operations += 1;
            stats.versions_cleaned += cleaned_count;
            stats.total_versions = stats.total_versions.saturating_sub(cleaned_count);
            stats.last_updated = Instant::now();
        }

        Ok(cleaned_count)
    }

    /// Обновляет минимальную активную транзакцию
    pub fn update_min_active_transaction(&self, transaction_id: TransactionId) {
        let mut min_active = self.min_active_transaction.write().unwrap();
        *min_active = transaction_id;
    }

    /// Возвращает статистику
    pub fn get_statistics(&self) -> MVCCStatistics {
        self.statistics.lock().unwrap().clone()
    }

    /// Возвращает количество версий для записи
    pub fn get_version_count(&self, key: &RowKey) -> usize {
        let versions = self.versions.read().unwrap();
        versions.get(key).map(|v| v.len()).unwrap_or(0)
    }
}

impl Default for MVCCManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mvcc_manager_creation() {
        let manager = MVCCManager::new();
        let stats = manager.get_statistics();
        assert_eq!(stats.total_versions, 0);
        assert_eq!(stats.active_versions, 0);
    }

    #[test]
    fn test_create_version() {
        let manager = MVCCManager::new();
        let key = RowKey::new(1, 1);
        let transaction_id = TransactionId::new(1);
        let data = vec![1, 2, 3, 4];

        let version_id = manager
            .create_version(key.clone(), transaction_id, data.clone())
            .unwrap();
        assert_eq!(version_id, 1);

        let stats = manager.get_statistics();
        assert_eq!(stats.total_versions, 1);
        assert_eq!(stats.active_versions, 1);
    }

    #[test]
    fn test_read_version() {
        let manager = MVCCManager::new();
        let key = RowKey::new(1, 1);
        let transaction_id = TransactionId::new(1);
        let data = vec![1, 2, 3, 4];

        // Создаём версию
        manager
            .create_version(key.clone(), transaction_id, data.clone())
            .unwrap();

        // Фиксируем транзакцию
        manager.commit_transaction(transaction_id).unwrap();

        // Читаем версию (snapshot после создания)
        let snapshot = Timestamp::now();
        let read_data = manager
            .read_version(&key, transaction_id, snapshot)
            .unwrap();
        assert_eq!(read_data, Some(data));
    }

    #[test]
    fn test_commit_transaction() {
        let manager = MVCCManager::new();
        let key = RowKey::new(1, 1);
        let transaction_id = TransactionId::new(1);
        let data = vec![1, 2, 3, 4];

        manager.create_version(key, transaction_id, data).unwrap();
        manager.commit_transaction(transaction_id).unwrap();

        let stats = manager.get_statistics();
        assert_eq!(stats.active_versions, 0);
        assert_eq!(stats.committed_versions, 1);
    }

    #[test]
    fn test_abort_transaction() {
        let manager = MVCCManager::new();
        let key = RowKey::new(1, 1);
        let transaction_id = TransactionId::new(1);
        let data = vec![1, 2, 3, 4];

        manager.create_version(key, transaction_id, data).unwrap();
        manager.abort_transaction(transaction_id).unwrap();

        let stats = manager.get_statistics();
        assert_eq!(stats.active_versions, 0);
        assert_eq!(stats.aborted_versions, 1);
    }

    #[test]
    fn test_vacuum() {
        let manager = MVCCManager::new();
        let key = RowKey::new(1, 1);
        let transaction_id = TransactionId::new(1);
        let data = vec![1, 2, 3, 4];

        // Создаём и откатываем версию
        manager
            .create_version(key.clone(), transaction_id, data)
            .unwrap();
        manager.abort_transaction(transaction_id).unwrap();

        // Очищаем
        let cleaned = manager.vacuum().unwrap();
        assert_eq!(cleaned, 1);

        let stats = manager.get_statistics();
        assert_eq!(stats.total_versions, 0);
        assert_eq!(stats.versions_cleaned, 1);
    }

    #[test]
    fn test_multiple_versions() {
        let manager = MVCCManager::new();
        let key = RowKey::new(1, 1);
        let data1 = vec![1, 2, 3];
        let data2 = vec![4, 5, 6];

        // Создаём первую версию
        let tx1 = TransactionId::new(1);
        manager.create_version(key.clone(), tx1, data1).unwrap();
        manager.commit_transaction(tx1).unwrap();

        // Создаём вторую версию
        let tx2 = TransactionId::new(2);
        manager
            .create_version(key.clone(), tx2, data2.clone())
            .unwrap();
        manager.commit_transaction(tx2).unwrap();

        // Проверяем количество версий
        assert_eq!(manager.get_version_count(&key), 2);

        // Читаем последнюю версию
        let snapshot = Timestamp::now();
        let read_data = manager.read_version(&key, tx2, snapshot).unwrap();
        assert_eq!(read_data, Some(data2));
    }
}
