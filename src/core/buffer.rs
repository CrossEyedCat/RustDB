//! Менеджер буферов для RustBD

use crate::common::{Error, Result, types::PageId};
use crate::storage::page::{Page, PageHeader};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Статистика буфера
#[derive(Debug, Clone)]
pub struct BufferStats {
    /// Количество обращений к кэшу
    pub total_accesses: u64,
    /// Количество попаданий в кэш
    pub cache_hits: u64,
    /// Количество промахов кэша
    pub cache_misses: u64,
    /// Количество операций записи
    pub write_operations: u64,
    /// Количество операций чтения
    pub read_operations: u64,
    /// Время последнего сброса статистики
    pub last_reset: Instant,
}

impl BufferStats {
    /// Создает новую статистику
    pub fn new() -> Self {
        Self {
            total_accesses: 0,
            cache_hits: 0,
            cache_misses: 0,
            write_operations: 0,
            read_operations: 0,
            last_reset: Instant::now(),
        }
    }

    /// Возвращает hit ratio (отношение попаданий к общему количеству обращений)
    pub fn hit_ratio(&self) -> f64 {
        if self.total_accesses == 0 {
            0.0
        } else {
            self.cache_hits as f64 / self.total_accesses as f64
        }
    }

    /// Возвращает miss ratio (отношение промахов к общему количеству обращений)
    pub fn miss_ratio(&self) -> f64 {
        if self.total_accesses == 0 {
            0.0
        } else {
            self.cache_misses as f64 / self.total_accesses as f64
        }
    }

    /// Сбрасывает статистику
    pub fn reset(&mut self) {
        self.total_accesses = 0;
        self.cache_hits = 0;
        self.cache_misses = 0;
        self.write_operations = 0;
        self.read_operations = 0;
        self.last_reset = Instant::now();
    }

    /// Регистрирует обращение к кэшу
    pub fn record_access(&mut self, is_hit: bool) {
        self.total_accesses += 1;
        if is_hit {
            self.cache_hits += 1;
        } else {
            self.cache_misses += 1;
        }
    }

    /// Регистрирует операцию записи
    pub fn record_write(&mut self) {
        self.write_operations += 1;
    }

    /// Регистрирует операцию чтения
    pub fn record_read(&mut self) {
        self.read_operations += 1;
    }
}

/// Элемент LRU кэша
#[derive(Debug, Clone)]
struct LRUEntry {
    /// Страница
    page: Page,
    /// Время последнего доступа
    last_access: Instant,
    /// Количество обращений
    access_count: u32,
    /// Флаг "грязной" страницы
    is_dirty: bool,
}

impl LRUEntry {
    /// Создает новый элемент LRU
    fn new(page: Page) -> Self {
        Self {
            page,
            last_access: Instant::now(),
            access_count: 1,
            is_dirty: false,
        }
    }

    /// Обновляет время доступа
    fn touch(&mut self) {
        self.last_access = Instant::now();
        self.access_count += 1;
    }

    /// Помечает страницу как измененную
    fn mark_dirty(&mut self) {
        self.is_dirty = true;
    }

    /// Помечает страницу как чистую
    fn mark_clean(&mut self) {
        self.is_dirty = false;
    }
}

/// Стратегия вытеснения
#[derive(Debug, Clone, PartialEq)]
pub enum EvictionStrategy {
    /// LRU (Least Recently Used)
    LRU,
    /// Clock алгоритм
    Clock,
    /// Адаптивная стратегия
    Adaptive,
}

/// Менеджер буферов с LRU кэшем
pub struct BufferManager {
    /// LRU кэш страниц
    cache: HashMap<PageId, LRUEntry>,
    /// Очередь LRU для отслеживания порядка доступа
    lru_queue: VecDeque<PageId>,
    /// Максимальное количество страниц в кэше
    max_pages: usize,
    /// Стратегия вытеснения
    strategy: EvictionStrategy,
    /// Статистика буфера
    stats: BufferStats,
    /// Указатель для Clock алгоритма
    clock_pointer: usize,
    /// Счетчик обращений для адаптивной стратегии
    access_counter: u64,
}

impl BufferManager {
    /// Создает новый менеджер буферов
    pub fn new(max_pages: usize, strategy: EvictionStrategy) -> Self {
        Self {
            cache: HashMap::new(),
            lru_queue: VecDeque::new(),
            max_pages,
            strategy,
            stats: BufferStats::new(),
            clock_pointer: 0,
            access_counter: 0,
        }
    }

    /// Получает страницу по ID
    pub fn get_page(&mut self, page_id: PageId) -> Option<&Page> {
        self.stats.record_access(true);
        self.stats.record_read();

        if !self.cache.contains_key(&page_id) {
            self.stats.record_access(false);
            return None;
        }

        // Сначала обновляем LRU порядок
        self.update_lru_order(page_id);
        
        // Затем получаем ссылку на страницу
        if let Some(entry) = self.cache.get_mut(&page_id) {
            entry.touch();
            Some(&entry.page)
        } else {
            None
        }
    }

    /// Получает изменяемую ссылку на страницу
    pub fn get_page_mut(&mut self, page_id: PageId) -> Option<&mut Page> {
        self.stats.record_access(true);
        self.stats.record_write();

        if !self.cache.contains_key(&page_id) {
            self.stats.record_access(false);
            return None;
        }

        // Сначала обновляем LRU порядок
        self.update_lru_order(page_id);
        
        // Затем получаем изменяемую ссылку на страницу
        if let Some(entry) = self.cache.get_mut(&page_id) {
            entry.touch();
            entry.mark_dirty();
            Some(&mut entry.page)
        } else {
            None
        }
    }

    /// Добавляет страницу в кэш
    pub fn add_page(&mut self, page: Page) -> Result<()> {
        let page_id = page.header.page_id;

        // Если страница уже существует, обновляем её
        if self.cache.contains_key(&page_id) {
            self.update_page(page)?;
            return Ok(());
        }

        // Если превышен лимит, вытесняем страницу
        if self.cache.len() >= self.max_pages {
            self.evict_page()?;
        }

        // Добавляем страницу в кэш
        let entry = LRUEntry::new(page);
        self.cache.insert(page_id, entry);
        self.lru_queue.push_back(page_id);

        Ok(())
    }

    /// Обновляет существующую страницу
    pub fn update_page(&mut self, page: Page) -> Result<()> {
        let page_id = page.header.page_id;
        
        if !self.cache.contains_key(&page_id) {
            return Ok(());
        }

        // Сначала обновляем LRU порядок
        self.update_lru_order(page_id);
        
        // Затем обновляем страницу
        if let Some(entry) = self.cache.get_mut(&page_id) {
            entry.page = page;
            entry.touch();
            entry.mark_dirty();
        }

        Ok(())
    }

    /// Удаляет страницу из кэша
    pub fn remove_page(&mut self, page_id: PageId) -> Option<Page> {
        if let Some(entry) = self.cache.remove(&page_id) {
            // Удаляем из LRU очереди
            if let Some(pos) = self.lru_queue.iter().position(|&id| id == page_id) {
                self.lru_queue.remove(pos);
            }
            Some(entry.page)
        } else {
            None
        }
    }

    /// Вытесняет страницу из кэша
    fn evict_page(&mut self) -> Result<()> {
        let page_id = match self.strategy {
            EvictionStrategy::LRU => self.evict_lru()?,
            EvictionStrategy::Clock => self.evict_clock()?,
            EvictionStrategy::Adaptive => self.evict_adaptive()?,
        };

        if let Some(page) = self.remove_page(page_id) {
            // Если страница "грязная", нужно записать её на диск
            if page.header.is_dirty {
                // TODO: Реализовать запись на диск
                log::warn!("Вытеснение грязной страницы {} без записи на диск", page_id);
            }
        }

        Ok(())
    }

    /// Вытесняет страницу по LRU алгоритму
    fn evict_lru(&mut self) -> Result<PageId> {
        if let Some(page_id) = self.lru_queue.front().copied() {
            // Проверяем, не зафиксирована ли страница
            if let Some(entry) = self.cache.get(&page_id) {
                if entry.page.header.is_pinned {
                    // Ищем следующую незафиксированную страницу
                    for &id in self.lru_queue.iter().skip(1) {
                        if let Some(entry) = self.cache.get(&id) {
                            if !entry.page.header.is_pinned {
                                return Ok(id);
                            }
                        }
                    }
                    return Err(Error::validation("Все страницы зафиксированы"));
                }
            }
            Ok(page_id)
        } else {
            Err(Error::validation("Кэш пуст"))
        }
    }

    /// Вытесняет страницу по Clock алгоритму
    fn evict_clock(&mut self) -> Result<PageId> {
        let mut attempts = 0;
        let max_attempts = self.cache.len() * 2;

        while attempts < max_attempts {
            let page_ids: Vec<PageId> = self.cache.keys().copied().collect();
            if page_ids.is_empty() {
                return Err(Error::validation("Кэш пуст"));
            }

            let page_id = page_ids[self.clock_pointer % page_ids.len()];
            self.clock_pointer = (self.clock_pointer + 1) % page_ids.len();

            if let Some(entry) = self.cache.get(&page_id) {
                if !entry.page.header.is_pinned {
                    if entry.access_count == 0 {
                        return Ok(page_id);
                    } else {
                        // Уменьшаем счетчик обращений
                        if let Some(entry_mut) = self.cache.get_mut(&page_id) {
                            entry_mut.access_count = entry_mut.access_count.saturating_sub(1);
                        }
                    }
                }
            }

            attempts += 1;
        }

        Err(Error::validation("Не удалось найти страницу для вытеснения"))
    }

    /// Вытесняет страницу по адаптивной стратегии
    fn evict_adaptive(&mut self) -> Result<PageId> {
        // Адаптивная стратегия: комбинация LRU и Clock
        let hit_ratio = self.stats.hit_ratio();
        
        if hit_ratio > 0.8 {
            // Высокий hit ratio - используем LRU
            self.evict_lru()
        } else if hit_ratio < 0.2 {
            // Низкий hit ratio - используем Clock
            self.evict_clock()
        } else {
            // Средний hit ratio - используем LRU
            self.evict_lru()
        }
    }

    /// Обновляет порядок LRU
    fn update_lru_order(&mut self, page_id: PageId) {
        // Удаляем страницу из текущей позиции
        if let Some(pos) = self.lru_queue.iter().position(|&id| id == page_id) {
            self.lru_queue.remove(pos);
        }
        // Добавляем в конец (самая недавно использованная)
        self.lru_queue.push_back(page_id);
    }

    /// Фиксирует страницу в памяти
    pub fn pin_page(&mut self, page_id: PageId) -> Result<()> {
        if let Some(entry) = self.cache.get_mut(&page_id) {
            entry.page.header.pin();
        } else {
            return Err(Error::validation("Страница не найдена в кэше"));
        }
        Ok(())
    }

    /// Освобождает страницу из памяти
    pub fn unpin_page(&mut self, page_id: PageId) -> Result<()> {
        if let Some(entry) = self.cache.get_mut(&page_id) {
            entry.page.header.unpin();
        } else {
            return Err(Error::validation("Страница не найдена в кэше"));
        }
        Ok(())
    }

    /// Возвращает статистику буфера
    pub fn get_stats(&self) -> BufferStats {
        self.stats.clone()
    }

    /// Сбрасывает статистику
    pub fn reset_stats(&mut self) {
        self.stats.reset();
    }

    /// Возвращает количество страниц в кэше
    pub fn page_count(&self) -> usize {
        self.cache.len()
    }

    /// Проверяет, содержит ли кэш страницу
    pub fn contains_page(&self, page_id: PageId) -> bool {
        self.cache.contains_key(&page_id)
    }

    /// Возвращает количество "грязных" страниц
    pub fn dirty_page_count(&self) -> usize {
        self.cache.values().filter(|entry| entry.is_dirty).count()
    }

    /// Принудительно записывает все "грязные" страницы
    pub fn flush_dirty_pages(&mut self) -> Result<usize> {
        let mut flushed_count = 0;
        let dirty_pages: Vec<PageId> = self.cache
            .iter()
            .filter(|(_, entry)| entry.is_dirty)
            .map(|(&id, _)| id)
            .collect();

        for page_id in dirty_pages {
            if let Some(entry) = self.cache.get_mut(&page_id) {
                // TODO: Реализовать запись на диск
                entry.mark_clean();
                flushed_count += 1;
            }
        }

        Ok(flushed_count)
    }

    /// Изменяет стратегию вытеснения
    pub fn set_eviction_strategy(&mut self, strategy: EvictionStrategy) {
        self.strategy = strategy;
        // Сбрасываем указатель для Clock алгоритма
        self.clock_pointer = 0;
    }

    /// Возвращает текущую стратегию вытеснения
    pub fn get_eviction_strategy(&self) -> EvictionStrategy {
        self.strategy.clone()
    }
}

/// Потокобезопасный менеджер буферов
pub type SharedBufferManager = Arc<Mutex<BufferManager>>;

/// Создает потокобезопасный менеджер буферов
pub fn create_shared_buffer_manager(max_pages: usize, strategy: EvictionStrategy) -> SharedBufferManager {
    Arc::new(Mutex::new(BufferManager::new(max_pages, strategy)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::{Page, PageType};

    #[test]
    fn test_buffer_manager_creation() {
        let manager = BufferManager::new(100, EvictionStrategy::LRU);
        assert_eq!(manager.max_pages, 100);
        assert_eq!(manager.strategy, EvictionStrategy::LRU);
        assert_eq!(manager.page_count(), 0);
    }

    #[test]
    fn test_add_and_get_page() {
        let mut manager = BufferManager::new(10, EvictionStrategy::LRU);
        let page = Page::new(1, PageType::Data);
        
        manager.add_page(page).unwrap();
        assert_eq!(manager.page_count(), 1);
        assert!(manager.contains_page(1));
        
        let retrieved = manager.get_page(1);
        assert!(retrieved.is_some());
    }

    #[test]
    fn test_lru_eviction() {
        let mut manager = BufferManager::new(2, EvictionStrategy::LRU);
        
        // Добавляем 3 страницы
        manager.add_page(Page::new(1, PageType::Data)).unwrap();
        manager.add_page(Page::new(2, PageType::Data)).unwrap();
        manager.add_page(Page::new(3, PageType::Data)).unwrap();
        
        // Должна быть вытеснена первая страница
        assert_eq!(manager.page_count(), 2);
        assert!(!manager.contains_page(1));
        assert!(manager.contains_page(2));
        assert!(manager.contains_page(3));
    }

    #[test]
    fn test_page_pinning() {
        let mut manager = BufferManager::new(2, EvictionStrategy::LRU);
        
        manager.add_page(Page::new(1, PageType::Data)).unwrap();
        manager.pin_page(1).unwrap();
        
        // Добавляем еще 2 страницы
        manager.add_page(Page::new(2, PageType::Data)).unwrap();
        manager.add_page(Page::new(3, PageType::Data)).unwrap();
        
        // Зафиксированная страница не должна быть вытеснена
        assert!(manager.contains_page(1));
        assert_eq!(manager.page_count(), 3);
    }

    #[test]
    fn test_buffer_stats() {
        let mut manager = BufferManager::new(10, EvictionStrategy::LRU);
        let page = Page::new(1, PageType::Data);
        
        manager.add_page(page).unwrap();
        manager.get_page(1);
        manager.get_page(1);
        manager.get_page(999); // Промах
        
        let stats = manager.get_stats();
        assert_eq!(stats.total_accesses, 3);
        assert_eq!(stats.cache_hits, 2);
        assert_eq!(stats.cache_misses, 1);
        assert_eq!(stats.hit_ratio(), 2.0 / 3.0);
    }

    #[test]
    fn test_eviction_strategies() {
        let mut manager = BufferManager::new(2, EvictionStrategy::LRU);
        
        // Тестируем смену стратегии
        manager.set_eviction_strategy(EvictionStrategy::Clock);
        assert_eq!(manager.get_eviction_strategy(), EvictionStrategy::Clock);
        
        manager.set_eviction_strategy(EvictionStrategy::Adaptive);
        assert_eq!(manager.get_eviction_strategy(), EvictionStrategy::Adaptive);
    }
}
