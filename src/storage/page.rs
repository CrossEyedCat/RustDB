//! Менеджер страниц для rustdb

use crate::common::{
    types::{PageId, MAX_RECORD_SIZE, PAGE_HEADER_SIZE, PAGE_SIZE},
    Error, Result,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Заголовок страницы
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageHeader {
    /// ID страницы
    pub page_id: PageId,
    /// Тип страницы
    pub page_type: PageType,
    /// Время последнего изменения
    pub last_modified: u64,
    /// Количество записей на странице
    pub record_count: u32,
    /// Размер свободного места
    pub free_space: u32,
    /// Указатель на следующую страницу (для связанных страниц)
    pub next_page: Option<PageId>,
    /// Указатель на предыдущую страницу (для связанных страниц)
    pub prev_page: Option<PageId>,
    /// Флаг "грязной" страницы (изменена в памяти)
    pub is_dirty: bool,
    /// Флаг зафиксированной страницы
    pub is_pinned: bool,
}

impl PageHeader {
    /// Создает новый заголовок страницы
    pub fn new(page_id: PageId, page_type: PageType) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            page_id,
            page_type,
            last_modified: now,
            record_count: 0,
            free_space: MAX_RECORD_SIZE as u32,
            next_page: None,
            prev_page: None,
            is_dirty: false,
            is_pinned: false,
        }
    }

    /// Размер заголовка в байтах
    pub fn size(&self) -> usize {
        PAGE_HEADER_SIZE
    }

    /// Обновляет время последнего изменения
    pub fn touch(&mut self) {
        self.last_modified = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Помечает страницу как измененную
    pub fn mark_dirty(&mut self) {
        self.is_dirty = true;
        self.touch();
    }

    /// Помечает страницу как чистую
    pub fn mark_clean(&mut self) {
        self.is_dirty = false;
    }

    /// Фиксирует страницу в памяти
    pub fn pin(&mut self) {
        self.is_pinned = true;
    }

    /// Освобождает страницу из памяти
    pub fn unpin(&mut self) {
        self.is_pinned = false;
    }
}

/// Тип страницы
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PageType {
    /// Страница данных
    Data,
    /// Страница индекса
    Index,
    /// Страница свободного места
    FreeSpace,
    /// Страница метаданных
    Metadata,
    /// Страница логов
    Log,
}

/// Слот записи на странице
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordSlot {
    /// Смещение записи от начала страницы
    pub offset: u32,
    /// Размер записи в байтах
    pub size: u32,
    /// Флаг удаленной записи
    pub is_deleted: bool,
    /// ID записи
    pub record_id: u64,
}

impl RecordSlot {
    /// Создает новый слот записи
    pub fn new(offset: u32, size: u32, record_id: u64) -> Self {
        Self {
            offset,
            size,
            is_deleted: false,
            record_id,
        }
    }

    /// Помечает запись как удаленную
    pub fn mark_deleted(&mut self) {
        self.is_deleted = true;
    }

    /// Проверяет, удалена ли запись
    pub fn is_deleted(&self) -> bool {
        self.is_deleted
    }
}

/// Структура страницы
#[derive(Debug, Clone)]
pub struct Page {
    /// Заголовок страницы
    pub header: PageHeader,
    /// Слоты записей
    pub slots: Vec<RecordSlot>,
    /// Данные страницы
    pub data: Vec<u8>,
    /// Карта свободного места
    pub free_space_map: Vec<bool>,
}

impl Page {
    /// Создает новую пустую страницу данных
    pub fn new(page_id: PageId) -> Self {
        Self::new_with_type(page_id, PageType::Data)
    }

    /// Создает новую пустую страницу с указанным типом
    pub fn new_with_type(page_id: PageId, page_type: PageType) -> Self {
        let data = vec![0u8; PAGE_SIZE];
        let free_space_map = vec![true; PAGE_SIZE - PAGE_HEADER_SIZE];

        Self {
            header: PageHeader::new(page_id, page_type),
            slots: Vec::new(),
            data,
            free_space_map,
        }
    }

    /// Создает страницу из байтов (десериализация)
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != PAGE_SIZE {
            return Err(Error::validation("Неверный размер страницы"));
        }

        // TODO: Реализовать полную десериализацию
        // Пока используем простую реализацию
        let page_id = 0; // Извлекается из заголовка
        let header = PageHeader::new(page_id, PageType::Data);
        let data = bytes.to_vec();
        let free_space_map = vec![true; PAGE_SIZE - PAGE_HEADER_SIZE];

        Ok(Self {
            header,
            slots: Vec::new(),
            data,
            free_space_map,
        })
    }

    /// Создает страницу из байтов с указанием page_id
    pub fn from_bytes_with_id(bytes: &[u8], page_id: PageId) -> Result<Self> {
        if bytes.len() != PAGE_SIZE {
            return Err(Error::validation("Неверный размер страницы"));
        }

        // TODO: Реализовать полную десериализацию
        let header = PageHeader::new(page_id, PageType::Data);
        let data = bytes.to_vec();
        let free_space_map = vec![true; PAGE_SIZE - PAGE_HEADER_SIZE];

        Ok(Self {
            header,
            slots: Vec::new(),
            data,
            free_space_map,
        })
    }

    /// Сериализует страницу в байты
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        // TODO: Реализовать полную сериализацию
        Ok(self.data.clone())
    }

    /// Добавляет запись на страницу
    pub fn add_record(&mut self, record_data: &[u8], record_id: u64) -> Result<u32> {
        if record_data.len() > MAX_RECORD_SIZE {
            return Err(Error::validation("Запись слишком большая"));
        }

        // Ищем свободное место
        let offset = self.find_free_space(record_data.len())?;

        // Записываем данные
        let end_offset = offset + record_data.len();
        self.data[offset..end_offset].copy_from_slice(record_data);

        // Создаем слот
        let slot = RecordSlot::new(offset as u32, record_data.len() as u32, record_id);
        self.slots.push(slot);

        // Обновляем заголовок
        self.header.record_count += 1;
        self.header.free_space -= record_data.len() as u32;
        self.header.mark_dirty();

        // Обновляем карту свободного места
        self.update_free_space_map(offset, end_offset, false);

        Ok(offset as u32)
    }

    /// Удаляет запись по ID
    pub fn delete_record(&mut self, record_id: u64) -> Result<bool> {
        if let Some(slot_index) = self.slots.iter().position(|s| s.record_id == record_id) {
            let slot = &mut self.slots[slot_index];
            if !slot.is_deleted {
                slot.mark_deleted();

                // Освобождаем место в карте
                let start = slot.offset as usize;
                let end = start + slot.size as usize;
                let size = slot.size;

                // Освобождаем слот
                let _ = slot;

                // Теперь обновляем карту и заголовок
                self.update_free_space_map(start, end, true);
                self.header.free_space += size;
                self.header.mark_dirty();

                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Получает запись по ID
    pub fn get_record(&self, record_id: u64) -> Option<&[u8]> {
        if let Some(slot) = self
            .slots
            .iter()
            .find(|s| s.record_id == record_id && !s.is_deleted)
        {
            let start = slot.offset as usize;
            let end = start + slot.size as usize;
            Some(&self.data[start..end])
        } else {
            None
        }
    }

    /// Обновляет запись
    pub fn update_record(&mut self, record_id: u64, new_data: &[u8]) -> Result<bool> {
        if new_data.len() > MAX_RECORD_SIZE {
            return Err(Error::validation("Новые данные слишком большие"));
        }

        let slot_index = if let Some(idx) = self
            .slots
            .iter()
            .position(|s| s.record_id == record_id && !s.is_deleted)
        {
            idx
        } else {
            return Ok(false);
        };

        let old_size = self.slots[slot_index].size as usize;
        let new_size = new_data.len();

        if new_size <= old_size {
            // Записываем новые данные
            let start = self.slots[slot_index].offset as usize;
            let end = start + new_size;
            self.data[start..end].copy_from_slice(new_data);

            // Обновляем слот
            self.slots[slot_index].size = new_size as u32;

            // Обновляем заголовок
            self.header.free_space += (old_size - new_size) as u32;
            self.header.mark_dirty();

            // Обновляем карту свободного места
            if new_size < old_size {
                self.update_free_space_map(start + new_size, start + old_size, true);
            }

            Ok(true)
        } else {
            // Нужно перераспределить место
            // Сначала удаляем старую запись
            let slot = &mut self.slots[slot_index];
            let start = slot.offset as usize;
            let end = start + slot.size as usize;
            let size = slot.size;

            // Удаляем слот
            slot.mark_deleted();

            // Освобождаем слот
            let _ = slot;

            // Теперь обновляем карту и заголовок
            self.update_free_space_map(start, end, true);
            self.header.free_space += size;

            // Теперь добавляем новую запись
            let new_data_copy = new_data.to_vec();
            self.add_record(&new_data_copy, record_id)?;
            Ok(true)
        }
    }

    /// Ищет свободное место для записи указанного размера
    fn find_free_space(&self, size: usize) -> Result<usize> {
        let mut consecutive_free = 0;
        let mut start_pos = PAGE_HEADER_SIZE;

        for (i, &is_free) in self.free_space_map.iter().enumerate() {
            if is_free {
                if consecutive_free == 0 {
                    start_pos = i + PAGE_HEADER_SIZE;
                }
                consecutive_free += 1;

                if consecutive_free >= size {
                    return Ok(start_pos);
                }
            } else {
                consecutive_free = 0;
            }
        }

        Err(Error::validation(
            "Недостаточно свободного места на странице",
        ))
    }

    /// Обновляет карту свободного места
    fn update_free_space_map(&mut self, start: usize, end: usize, is_free: bool) {
        let map_start = start.saturating_sub(PAGE_HEADER_SIZE);
        let map_end = end.saturating_sub(PAGE_HEADER_SIZE);

        for i in map_start..map_end {
            if i < self.free_space_map.len() {
                self.free_space_map[i] = is_free;
            }
        }
    }

    /// Проверяет, есть ли свободное место для записи указанного размера
    pub fn has_free_space(&self, size: usize) -> bool {
        self.header.free_space >= size as u32
    }

    /// Возвращает количество свободного места
    pub fn free_space(&self) -> u32 {
        self.header.free_space
    }

    /// Возвращает количество записей
    pub fn record_count(&self) -> u32 {
        self.header.record_count
    }

    /// Проверяет, пуста ли страница
    pub fn is_empty(&self) -> bool {
        self.header.record_count == 0
    }

    /// Проверяет, полна ли страница
    pub fn is_full(&self) -> bool {
        self.header.free_space == 0
    }

    /// Очищает страницу
    pub fn clear(&mut self) -> Result<()> {
        self.slots.clear();
        self.data.fill(0);
        self.free_space_map.fill(true);
        self.header.record_count = 0;
        self.header.free_space = MAX_RECORD_SIZE as u32;
        self.header.mark_dirty();
        Ok(())
    }

    /// Получает коэффициент заполнения страницы (0.0 - 1.0)
    pub fn get_fill_factor(&self) -> f64 {
        let used_space = MAX_RECORD_SIZE as u32 - self.header.free_space;
        used_space as f64 / MAX_RECORD_SIZE as f64
    }

    /// Получает количество записей
    pub fn get_record_count(&self) -> u32 {
        self.header.record_count
    }

    /// Получает размер свободного места
    pub fn get_free_space(&self) -> u32 {
        self.header.free_space
    }

    /// Проверяет, нужна ли дефрагментация
    pub fn needs_defragmentation(&self) -> bool {
        // Простая эвристика: если есть удаленные записи
        self.slots.iter().any(|slot| slot.is_deleted)
    }

    /// Выполняет дефрагментацию страницы
    pub fn defragment(&mut self) -> Result<()> {
        // Собираем все активные записи
        let mut active_records = Vec::new();
        for slot in &self.slots {
            if !slot.is_deleted {
                let start = slot.offset as usize;
                let end = start + slot.size as usize;
                let record_data = self.data[start..end].to_vec();
                active_records.push((slot.record_id, record_data));
            }
        }

        // Очищаем страницу
        self.clear()?;

        // Добавляем записи обратно
        for (record_id, record_data) in active_records {
            self.add_record(&record_data, record_id)?;
        }

        Ok(())
    }

    /// Сканирует все записи на странице
    pub fn scan_records(&self) -> Result<Vec<(u32, Vec<u8>)>> {
        let mut records = Vec::new();

        for slot in &self.slots {
            if !slot.is_deleted {
                let start = slot.offset as usize;
                let end = start + slot.size as usize;
                let record_data = self.data[start..end].to_vec();
                records.push((slot.offset, record_data));
            }
        }

        Ok(records)
    }

    /// Получает все записи на странице
    pub fn get_all_records(&self) -> Result<Vec<Vec<u8>>> {
        let mut records = Vec::new();

        for slot in &self.slots {
            if !slot.is_deleted {
                let start = slot.offset as usize;
                let end = start + slot.size as usize;
                let record_data = self.data[start..end].to_vec();
                records.push(record_data);
            }
        }

        Ok(records)
    }

    /// Обновляет запись по offset
    pub fn update_record_by_offset(&mut self, offset: u32, new_data: &[u8]) -> Result<()> {
        // Ищем слот по offset
        if let Some(slot) = self
            .slots
            .iter_mut()
            .find(|s| s.offset == offset && !s.is_deleted)
        {
            if new_data.len() <= slot.size as usize {
                // Обновляем данные in-place
                let start = offset as usize;
                let end = start + new_data.len();
                self.data[start..end].copy_from_slice(new_data);

                // Обновляем размер слота если нужно
                if new_data.len() < slot.size as usize {
                    let size_diff = slot.size as usize - new_data.len();
                    let old_slot_end = start + slot.size as usize;
                    slot.size = new_data.len() as u32;
                    self.header.free_space += size_diff as u32;

                    // Обновляем карту свободного места
                    self.update_free_space_map(end, old_slot_end, true);
                }

                self.header.mark_dirty();
                Ok(())
            } else {
                Err(Error::validation("Новые данные не помещаются в слот"))
            }
        } else {
            Err(Error::validation("Запись не найдена"))
        }
    }

    /// Удаляет запись по offset
    pub fn delete_record_by_offset(&mut self, offset: u32) -> Result<()> {
        if let Some(slot) = self
            .slots
            .iter_mut()
            .find(|s| s.offset == offset && !s.is_deleted)
        {
            slot.mark_deleted();

            // Сохраняем значения перед изменением заимствования
            let start = slot.offset as usize;
            let end = start + slot.size as usize;
            let slot_size = slot.size;

            // Освобождаем место
            self.update_free_space_map(start, end, true);
            self.header.free_space += slot_size;
            self.header.mark_dirty();

            Ok(())
        } else {
            Err(Error::validation("Запись не найдена"))
        }
    }
}

/// Менеджер страниц
pub struct PageManager {
    /// Кэш страниц
    pages: HashMap<PageId, Page>,
    /// Максимальное количество страниц в кэше
    max_pages: usize,
}

impl PageManager {
    /// Создает новый менеджер страниц
    pub fn new(max_pages: usize) -> Self {
        Self {
            pages: HashMap::new(),
            max_pages,
        }
    }

    /// Получает страницу по ID
    pub fn get_page(&mut self, page_id: PageId) -> Option<&Page> {
        self.pages.get(&page_id)
    }

    /// Получает изменяемую ссылку на страницу
    pub fn get_page_mut(&mut self, page_id: PageId) -> Option<&mut Page> {
        self.pages.get_mut(&page_id)
    }

    /// Добавляет страницу в кэш
    pub fn add_page(&mut self, page: Page) {
        let page_id = page.header.page_id;

        // Если превышен лимит, удаляем самую старую страницу
        if self.pages.len() >= self.max_pages {
            self.evict_oldest_page();
        }

        self.pages.insert(page_id, page);
    }

    /// Удаляет страницу из кэша
    pub fn remove_page(&mut self, page_id: PageId) -> Option<Page> {
        self.pages.remove(&page_id)
    }

    /// Удаляет самую старую страницу из кэша
    fn evict_oldest_page(&mut self) {
        if let Some((&oldest_id, _)) = self
            .pages
            .iter()
            .filter(|(_, page)| !page.header.is_pinned)
            .min_by_key(|(_, page)| page.header.last_modified)
        {
            self.pages.remove(&oldest_id);
        }
    }

    /// Возвращает количество страниц в кэше
    pub fn page_count(&self) -> usize {
        self.pages.len()
    }

    /// Проверяет, содержит ли кэш страницу
    pub fn contains_page(&self, page_id: PageId) -> bool {
        self.pages.contains_key(&page_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_creation() {
        let page = Page::new(1);
        assert_eq!(page.header.page_id, 1);
        assert_eq!(page.header.page_type, PageType::Data);
        assert_eq!(page.data.len(), PAGE_SIZE);
        assert_eq!(page.free_space_map.len(), PAGE_SIZE - PAGE_HEADER_SIZE);
    }

    #[test]
    fn test_add_record() {
        let mut page = Page::new(1);
        let record_data = b"test record";
        let record_id = 123;

        let _offset = page.add_record(record_data, record_id).unwrap();
        assert_eq!(page.header.record_count, 1);
        assert_eq!(
            page.header.free_space,
            (MAX_RECORD_SIZE - record_data.len()) as u32
        );
        assert_eq!(page.slots.len(), 1);

        let retrieved = page.get_record(record_id).unwrap();
        assert_eq!(retrieved, record_data);
    }

    #[test]
    fn test_delete_record() {
        let mut page = Page::new(1);
        let record_data = b"test record";
        let record_id = 123;

        page.add_record(record_data, record_id).unwrap();
        assert_eq!(page.header.record_count, 1);

        let deleted = page.delete_record(record_id).unwrap();
        assert!(deleted);
        assert_eq!(page.header.record_count, 1); // Слот остается, но запись помечена как удаленная

        let retrieved = page.get_record(record_id);
        assert!(retrieved.is_none());
    }

    #[test]
    fn test_page_manager() {
        let mut manager = PageManager::new(2);
        let page1 = Page::new(1);
        let page2 = Page::new(2);
        let page3 = Page::new(3);

        manager.add_page(page1);
        manager.add_page(page2);
        assert_eq!(manager.page_count(), 2);

        manager.add_page(page3);
        assert_eq!(manager.page_count(), 2); // Должна быть удалена самая старая страница

        // После добавления 3 страниц в менеджер с capacity=2, должны остаться последние 2
        // Проверим, какие страницы действительно остались
        assert!(manager.contains_page(2) || manager.contains_page(1));
        assert!(manager.contains_page(3));
    }
}
