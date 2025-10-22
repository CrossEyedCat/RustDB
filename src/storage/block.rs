//! Структуры блоков для rustdb

use crate::common::{types::PageId, Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Идентификатор блока
pub type BlockId = u64;

/// Тип блока
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BlockType {
    /// Блок данных
    Data,
    /// Блок индекса
    Index,
    /// Блок метаданных
    Metadata,
    /// Блок логов
    Log,
    /// Блок свободного места
    FreeSpace,
}

/// Заголовок блока
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockHeader {
    /// ID блока
    pub block_id: BlockId,
    /// Тип блока
    pub block_type: BlockType,
    /// Размер блока в байтах
    pub size: u32,
    /// Количество страниц в блоке
    pub page_count: u32,
    /// Время создания блока
    pub created_at: u64,
    /// Время последнего изменения
    pub last_modified: u64,
    /// Флаг "грязного" блока
    pub is_dirty: bool,
    /// Флаг зафиксированного блока
    pub is_pinned: bool,
}

impl BlockHeader {
    /// Создает новый заголовок блока
    pub fn new(block_id: BlockId, block_type: BlockType, size: u32) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            block_id,
            block_type,
            size,
            page_count: 0,
            created_at: now,
            last_modified: now,
            is_dirty: false,
            is_pinned: false,
        }
    }

    /// Обновляет время последнего изменения
    pub fn touch(&mut self) {
        self.last_modified = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Помечает блок как измененный
    pub fn mark_dirty(&mut self) {
        self.is_dirty = true;
        self.touch();
    }

    /// Помечает блок как чистый
    pub fn mark_clean(&mut self) {
        self.is_dirty = false;
    }

    /// Фиксирует блок в памяти
    pub fn pin(&mut self) {
        self.is_pinned = true;
    }

    /// Освобождает блок из памяти
    pub fn unpin(&mut self) {
        self.is_pinned = false;
    }
}

/// Структура блока
#[derive(Debug, Clone)]
pub struct Block {
    /// Заголовок блока
    pub header: BlockHeader,
    /// Страницы в блоке
    pub pages: HashMap<PageId, Vec<u8>>,
    /// Связи с другими блоками
    pub links: BlockLinks,
    /// Метаданные блока
    pub metadata: HashMap<String, String>,
}

impl Block {
    /// Создает новый пустой блок
    pub fn new(block_id: BlockId, block_type: BlockType, size: u32) -> Self {
        Self {
            header: BlockHeader::new(block_id, block_type, size),
            pages: HashMap::new(),
            links: BlockLinks::new(),
            metadata: HashMap::new(),
        }
    }

    /// Добавляет страницу в блок
    pub fn add_page(&mut self, page_id: PageId, page_data: Vec<u8>) -> Result<()> {
        // Проверяем, не превышает ли размер данных размер блока
        let total_size: usize =
            self.pages.values().map(|p| p.len()).sum::<usize>() + page_data.len();
        if total_size > (self.header.size as usize - 256) {
            // оставляем место для метаданных
            return Err(Error::validation("Блок переполнен"));
        }

        self.pages.insert(page_id, page_data);
        self.header.page_count = self.pages.len() as u32;
        self.header.mark_dirty();
        Ok(())
    }

    /// Удаляет страницу из блока
    pub fn remove_page(&mut self, page_id: PageId) -> Option<Vec<u8>> {
        let page_data = self.pages.remove(&page_id);
        if page_data.is_some() {
            self.header.page_count = self.pages.len() as u32;
            self.header.mark_dirty();
        }
        page_data
    }

    /// Получает страницу по ID
    pub fn get_page(&self, page_id: PageId) -> Option<&Vec<u8>> {
        self.pages.get(&page_id)
    }

    /// Получает изменяемую ссылку на страницу
    pub fn get_page_mut(&mut self, page_id: PageId) -> Option<&mut Vec<u8>> {
        self.pages.get_mut(&page_id)
    }

    /// Проверяет, содержит ли блок страницу
    pub fn contains_page(&self, page_id: PageId) -> bool {
        self.pages.contains_key(&page_id)
    }

    /// Возвращает количество страниц в блоке
    pub fn page_count(&self) -> u32 {
        self.header.page_count
    }

    /// Проверяет, пуст ли блок
    pub fn is_empty(&self) -> bool {
        self.header.page_count == 0
    }

    /// Проверяет, полон ли блок
    pub fn is_full(&self) -> bool {
        self.pages.len() >= self.header.page_count as usize
    }

    /// Очищает блок
    pub fn clear(&mut self) {
        self.pages.clear();
        self.header.page_count = 0;
        self.header.mark_dirty();
    }

    /// Сериализует блок в байты
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        // TODO: Реализовать полную сериализацию
        let mut bytes = Vec::new();

        // Добавляем заголовок
        let header_bytes =
            bincode::serialize(&self.header).map_err(|e| Error::BincodeSerialization(e))?;
        bytes.extend_from_slice(&header_bytes);

        // Добавляем количество страниц
        bytes.extend_from_slice(&(self.pages.len() as u32).to_le_bytes());

        // Добавляем страницы
        for (page_id, page_data) in &self.pages {
            bytes.extend_from_slice(&page_id.to_le_bytes());
            bytes.extend_from_slice(&(page_data.len() as u32).to_le_bytes());
            bytes.extend_from_slice(page_data);
        }

        Ok(bytes)
    }

    /// Создает блок из байтов (десериализация)
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        // TODO: Реализовать полную десериализацию
        if bytes.len() < 64 {
            return Err(Error::validation("Неверный размер блока"));
        }

        // Временная реализация
        let block_id = 0;
        let block_type = BlockType::Data;
        let size = bytes.len() as u32;

        Ok(Self::new(block_id, block_type, size))
    }
}

/// Связи блока с другими блоками
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockLinks {
    /// Следующий блок в цепочке
    pub next_block: Option<BlockId>,
    /// Предыдущий блок в цепочке
    pub prev_block: Option<BlockId>,
    /// Родительский блок (для иерархических структур)
    pub parent_block: Option<BlockId>,
    /// Дочерние блоки
    pub child_blocks: Vec<BlockId>,
}

impl BlockLinks {
    /// Создает новые связи блока
    pub fn new() -> Self {
        Self {
            next_block: None,
            prev_block: None,
            parent_block: None,
            child_blocks: Vec::new(),
        }
    }

    /// Устанавливает следующий блок
    pub fn set_next(&mut self, block_id: BlockId) {
        self.next_block = Some(block_id);
    }

    /// Устанавливает предыдущий блок
    pub fn set_prev(&mut self, block_id: BlockId) {
        self.prev_block = Some(block_id);
    }

    /// Устанавливает родительский блок
    pub fn set_parent(&mut self, block_id: BlockId) {
        self.parent_block = Some(block_id);
    }

    /// Добавляет дочерний блок
    pub fn add_child(&mut self, block_id: BlockId) {
        if !self.child_blocks.contains(&block_id) {
            self.child_blocks.push(block_id);
        }
    }

    /// Удаляет дочерний блок
    pub fn remove_child(&mut self, block_id: BlockId) -> bool {
        if let Some(pos) = self.child_blocks.iter().position(|&id| id == block_id) {
            self.child_blocks.remove(pos);
            true
        } else {
            false
        }
    }

    /// Проверяет, является ли блок листовым
    pub fn is_leaf(&self) -> bool {
        self.child_blocks.is_empty()
    }

    /// Возвращает количество дочерних блоков
    pub fn child_count(&self) -> usize {
        self.child_blocks.len()
    }
}

/// Менеджер блоков
pub struct BlockManager {
    /// Кэш блоков
    blocks: HashMap<BlockId, Block>,
    /// Максимальное количество блоков в кэше
    max_blocks: usize,
}

impl BlockManager {
    /// Создает новый менеджер блоков
    pub fn new(max_blocks: usize) -> Self {
        Self {
            blocks: HashMap::new(),
            max_blocks,
        }
    }

    /// Получает блок по ID
    pub fn get_block(&self, block_id: BlockId) -> Option<&Block> {
        self.blocks.get(&block_id)
    }

    /// Получает изменяемую ссылку на блок
    pub fn get_block_mut(&mut self, block_id: BlockId) -> Option<&mut Block> {
        self.blocks.get_mut(&block_id)
    }

    /// Добавляет блок в кэш
    pub fn add_block(&mut self, block: Block) {
        let block_id = block.header.block_id;

        // Если превышен лимит, удаляем самый старый блок
        if self.blocks.len() >= self.max_blocks {
            self.evict_oldest_block();
        }

        self.blocks.insert(block_id, block);
    }

    /// Удаляет блок из кэша
    pub fn remove_block(&mut self, block_id: BlockId) -> Option<Block> {
        self.blocks.remove(&block_id)
    }

    /// Удаляет самый старый блок из кэша
    fn evict_oldest_block(&mut self) {
        if let Some((&oldest_id, _)) = self
            .blocks
            .iter()
            .filter(|(_, block)| !block.header.is_pinned)
            .min_by_key(|(id, block)| (block.header.last_modified, *id))
        {
            self.blocks.remove(&oldest_id);
        }
    }

    /// Возвращает количество блоков в кэше
    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    /// Проверяет, содержит ли кэш блок
    pub fn contains_block(&self, block_id: BlockId) -> bool {
        self.blocks.contains_key(&block_id)
    }

    /// Создает новый блок
    pub fn create_block(&mut self, block_type: BlockType, size: u32) -> BlockId {
        let block_id = self.generate_block_id();
        let block = Block::new(block_id, block_type, size);
        self.add_block(block);
        block_id
    }

    /// Генерирует уникальный ID блока
    fn generate_block_id(&self) -> BlockId {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        now as BlockId
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_creation() {
        let block = Block::new(1, BlockType::Data, 1024);
        assert_eq!(block.header.block_id, 1);
        assert_eq!(block.header.block_type, BlockType::Data);
        assert_eq!(block.header.size, 1024);
        assert_eq!(block.pages.len(), 0);
    }

    #[test]
    fn test_add_page() {
        let mut block = Block::new(1, BlockType::Data, 1024);
        let page_data = vec![1, 2, 3, 4];

        block.add_page(1, page_data.clone()).unwrap();
        assert_eq!(block.page_count(), 1);
        assert!(block.contains_page(1));

        let retrieved = block.get_page(1).unwrap();
        assert_eq!(retrieved, &page_data);
    }

    #[test]
    fn test_block_links() {
        let mut links = BlockLinks::new();
        links.set_next(2);
        links.set_prev(0);
        links.add_child(3);
        links.add_child(4);

        assert_eq!(links.next_block, Some(2));
        assert_eq!(links.prev_block, Some(0));
        assert_eq!(links.child_count(), 2);
        assert!(!links.is_leaf());
    }

    #[test]
    fn test_block_manager() {
        let mut manager = BlockManager::new(2);
        let block1 = Block::new(1, BlockType::Data, 1024);
        let block2 = Block::new(2, BlockType::Data, 1024);
        let block3 = Block::new(3, BlockType::Data, 1024);

        manager.add_block(block1);
        manager.add_block(block2);
        assert_eq!(manager.block_count(), 2);

        manager.add_block(block3);
        assert_eq!(manager.block_count(), 2); // Должен быть удален самый старый блок

        assert!(manager.contains_block(2));
        assert!(manager.contains_block(3));
    }
}
