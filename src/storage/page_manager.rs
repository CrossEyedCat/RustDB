//! Менеджер страниц для RustBD
//! 
//! Этот модуль предоставляет высокоуровневый интерфейс для управления страницами данных,
//! включая CRUD операции, разделение/объединение страниц и оптимизации.

use crate::common::{Result, types::{PageId, RecordId}};
use crate::storage::{
    page::Page,
    advanced_file_manager::{AdvancedFileManager, AdvancedFileId},
    database_file::{DatabaseFileType, ExtensionStrategy},
};
use std::collections::HashMap;
use std::path::PathBuf;

/// Конфигурация менеджера страниц
#[derive(Debug, Clone)]
pub struct PageManagerConfig {
    /// Максимальный коэффициент заполнения страницы (0.0 - 1.0)
    pub max_fill_factor: f64,
    /// Минимальный коэффициент заполнения для объединения страниц
    pub min_fill_factor: f64,
    /// Размер буфера для предвыделения страниц
    pub preallocation_buffer_size: u32,
    /// Включить ли компрессию данных
    pub enable_compression: bool,
    /// Размер batch для операций
    pub batch_size: u32,
}

impl Default for PageManagerConfig {
    fn default() -> Self {
        Self {
            max_fill_factor: 0.9,
            min_fill_factor: 0.4,
            preallocation_buffer_size: 10,
            enable_compression: false,
            batch_size: 100,
        }
    }
}

/// Результат операции вставки
#[derive(Debug, Clone)]
pub struct InsertResult {
    /// ID записи
    pub record_id: RecordId,
    /// ID страницы, куда была вставлена запись
    pub page_id: PageId,
    /// Было ли выполнено разделение страницы
    pub page_split: bool,
}

/// Результат операции обновления
#[derive(Debug, Clone)]
pub struct UpdateResult {
    /// Было ли обновление выполнено in-place
    pub in_place: bool,
    /// ID новой страницы (если запись была перемещена)
    pub new_page_id: Option<PageId>,
    /// Было ли выполнено разделение страницы
    pub page_split: bool,
}

/// Результат операции удаления
#[derive(Debug, Clone)]
pub struct DeleteResult {
    /// Было ли удаление физическим (true) или логическим (false)
    pub physical_delete: bool,
    /// Было ли выполнено объединение страниц
    pub page_merge: bool,
}

/// Статистика операций менеджера страниц
#[derive(Debug, Default, Clone)]
pub struct PageManagerStatistics {
    /// Количество операций вставки
    pub insert_operations: u64,
    /// Количество операций выборки
    pub select_operations: u64,
    /// Количество операций обновления
    pub update_operations: u64,
    /// Количество операций удаления
    pub delete_operations: u64,
    /// Количество разделений страниц
    pub page_splits: u64,
    /// Количество объединений страниц
    pub page_merges: u64,
    /// Количество операций дефрагментации
    pub defragmentation_operations: u64,
}

/// Информация о странице для менеджера
#[derive(Debug, Clone)]
pub struct PageInfo {
    /// ID страницы
    pub page_id: PageId,
    /// Коэффициент заполнения (0.0 - 1.0)
    pub fill_factor: f64,
    /// Количество записей на странице
    pub record_count: u32,
    /// Размер свободного пространства в байтах
    pub free_space: u32,
    /// Требует ли страница дефрагментации
    pub needs_defragmentation: bool,
}

/// Менеджер страниц
pub struct PageManager {
    /// Файловый менеджер
    file_manager: AdvancedFileManager,
    /// ID файла данных
    file_id: AdvancedFileId,
    /// Конфигурация
    config: PageManagerConfig,
    /// Кеш информации о страницах
    page_cache: HashMap<PageId, PageInfo>,
    /// Пул предвыделенных страниц
    preallocated_pages: Vec<PageId>,
    /// Статистика операций
    statistics: PageManagerStatistics,
}

impl PageManager {
    /// Создает новый менеджер страниц
    pub fn new(data_dir: PathBuf, table_name: &str, config: PageManagerConfig) -> Result<Self> {
        let mut file_manager = AdvancedFileManager::new(data_dir)?;
        
        let filename = format!("{}.tbl", table_name);
        let file_id = file_manager.create_database_file(
            &filename,
            DatabaseFileType::Data,
            1, // database_id
            ExtensionStrategy::Linear,
        )?;
        
        let mut manager = Self {
            file_manager,
            file_id,
            config,
            page_cache: HashMap::new(),
            preallocated_pages: Vec::new(),
            statistics: PageManagerStatistics::default(),
        };
        
        // Предвыделяем начальные страницы
        manager.preallocate_pages()?;
        
        Ok(manager)
    }
    
    /// Открывает существующий менеджер страниц
    pub fn open(data_dir: PathBuf, table_name: &str, config: PageManagerConfig) -> Result<Self> {
        let mut file_manager = AdvancedFileManager::new(data_dir)?;
        
        let filename = format!("{}.tbl", table_name);
        let file_id = file_manager.open_database_file(&filename)?;
        
        let mut manager = Self {
            file_manager,
            file_id,
            config,
            page_cache: HashMap::new(),
            preallocated_pages: Vec::new(),
            statistics: PageManagerStatistics::default(),
        };
        
        // Загружаем информацию о существующих страницах
        manager.load_existing_pages()?;
        
        Ok(manager)
    }
    
    /// Вставляет новую запись
    pub fn insert(&mut self, data: &[u8]) -> Result<InsertResult> {
        self.statistics.insert_operations += 1;
        
        // Ищем страницу с достаточным свободным местом
        let page_id = self.find_page_with_space(data.len())?;
        
        // Загружаем страницу
        let page_data = self.file_manager.read_page(self.file_id, page_id)?;
        let mut page = Page::from_bytes(&page_data)?;
        
        // Пытаемся вставить запись
        match page.add_record(data, 0u64) { // Используем временный record_id
            Ok(offset) => {
                // Запись успешно вставлена
                let record_id = self.generate_record_id(page_id, offset);
                
                // Сохраняем страницу
                let serialized = page.to_bytes()?;
                self.file_manager.write_page(self.file_id, page_id, &serialized)?;
                
                // Обновляем кеш
                self.update_page_cache(page_id, &page);
                
                Ok(InsertResult {
                    record_id,
                    page_id,
                    page_split: false,
                })
            },
            Err(_) => {
                // Страница переполнена, нужно разделение
                self.split_page_and_insert(page_id, data)
            }
        }
    }
    
    /// Выбирает записи по условию
    pub fn select(&mut self, condition: Option<Box<dyn Fn(&[u8]) -> bool>>) -> Result<Vec<(RecordId, Vec<u8>)>> {
        self.statistics.select_operations += 1;
        
        let mut results = Vec::new();
        
        // Сканируем все страницы
        for page_id in self.get_all_page_ids()? {
            let page_data = self.file_manager.read_page(self.file_id, page_id)?;
            let page = Page::from_bytes(&page_data)?;
            
            // Сканируем записи на странице
            for (offset, record_data) in page.scan_records()? {
                let record_id = self.generate_record_id(page_id, offset);
                
                // Применяем условие фильтрации
                if let Some(ref cond) = condition {
                    if cond(&record_data) {
                        results.push((record_id, record_data));
                    }
                } else {
                    results.push((record_id, record_data));
                }
            }
        }
        
        Ok(results)
    }
    
    /// Обновляет запись
    pub fn update(&mut self, record_id: RecordId, new_data: &[u8]) -> Result<UpdateResult> {
        self.statistics.update_operations += 1;
        
        let (page_id, offset) = self.parse_record_id(record_id);
        
        // Загружаем страницу
        let page_data = self.file_manager.read_page(self.file_id, page_id)?;
        let mut page = Page::from_bytes(&page_data)?;
        
        // Пытаемся обновить запись in-place
        match page.update_record_by_offset(offset, new_data) {
            Ok(_) => {
                // Обновление in-place успешно
                let serialized = page.to_bytes()?;
                self.file_manager.write_page(self.file_id, page_id, &serialized)?;
                
                self.update_page_cache(page_id, &page);
                
                Ok(UpdateResult {
                    in_place: true,
                    new_page_id: None,
                    page_split: false,
                })
            },
            Err(_) => {
                // Нужно удалить старую запись и вставить новую
                self.delete_record_internal(page_id, offset)?;
                let insert_result = self.insert(new_data)?;
                
                Ok(UpdateResult {
                    in_place: false,
                    new_page_id: Some(insert_result.page_id),
                    page_split: insert_result.page_split,
                })
            }
        }
    }
    
    /// Удаляет запись
    pub fn delete(&mut self, record_id: RecordId) -> Result<DeleteResult> {
        self.statistics.delete_operations += 1;
        
        let (page_id, offset) = self.parse_record_id(record_id);
        self.delete_record_internal(page_id, offset)
    }
    
    /// Получает статистику операций
    pub fn get_statistics(&self) -> &PageManagerStatistics {
        &self.statistics
    }
    
    /// Выполняет дефрагментацию страниц
    pub fn defragment(&mut self) -> Result<u32> {
        self.statistics.defragmentation_operations += 1;
        
        let mut defragmented_count = 0;
        
        // Собираем ID страниц, которые нуждаются в дефрагментации
        let pages_to_defrag: Vec<PageId> = self.page_cache
            .iter()
            .filter_map(|(&page_id, page_info)| {
                if page_info.needs_defragmentation {
                    Some(page_id)
                } else {
                    None
                }
            })
            .collect();
        
        // Дефрагментируем собранные страницы
        for page_id in pages_to_defrag {
            self.defragment_page(page_id)?;
            defragmented_count += 1;
        }
        
        Ok(defragmented_count)
    }
    
    /// Выполняет batch операции вставки
    pub fn batch_insert(&mut self, records: Vec<Vec<u8>>) -> Result<Vec<InsertResult>> {
        let mut results = Vec::with_capacity(records.len());
        
        for chunk in records.chunks(self.config.batch_size as usize) {
            for record in chunk {
                results.push(self.insert(record)?);
            }
        }
        
        Ok(results)
    }
    
    // Приватные методы
    
    /// Предвыделяет страницы
    fn preallocate_pages(&mut self) -> Result<()> {
        for _ in 0..self.config.preallocation_buffer_size {
            let page_id = self.file_manager.allocate_pages(self.file_id, 1)?;
            
            // Инициализируем пустую страницу
            let page = Page::new(page_id);
            let serialized = page.to_bytes()?;
            self.file_manager.write_page(self.file_id, page_id, &serialized)?;
            
            self.preallocated_pages.push(page_id);
        }
        
        Ok(())
    }
    
    /// Загружает информацию о существующих страницах
    fn load_existing_pages(&mut self) -> Result<()> {
        // TODO: Реализовать сканирование существующих страниц
        Ok(())
    }
    
    /// Ищет страницу с достаточным свободным местом
    fn find_page_with_space(&mut self, required_size: usize) -> Result<PageId> {
        // Сначала проверяем кеш
        for (&page_id, page_info) in &self.page_cache {
            if page_info.free_space as usize >= required_size {
                return Ok(page_id);
            }
        }
        
        // Используем предвыделенную страницу
        if let Some(page_id) = self.preallocated_pages.pop() {
            return Ok(page_id);
        }
        
        // Выделяем новую страницу
        let page_id = self.file_manager.allocate_pages(self.file_id, 1)?;
        let page = Page::new(page_id);
        let serialized = page.to_bytes()?;
        self.file_manager.write_page(self.file_id, page_id, &serialized)?;
        
        Ok(page_id)
    }
    
    /// Разделяет страницу и вставляет запись
    fn split_page_and_insert(&mut self, page_id: PageId, data: &[u8]) -> Result<InsertResult> {
        self.statistics.page_splits += 1;
        
        // Загружаем переполненную страницу
        let page_data = self.file_manager.read_page(self.file_id, page_id)?;
        let mut old_page = Page::from_bytes(&page_data)?;
        
        // Создаем новую страницу
        let new_page_id = self.file_manager.allocate_pages(self.file_id, 1)?;
        let mut new_page = Page::new(new_page_id);
        
        // Перераспределяем записи между страницами
        let records = old_page.get_all_records()?;
        old_page.clear()?;
        
        let mid_point = records.len() / 2;
        
        // Записи для старой страницы
        for (i, record_data) in records.iter().enumerate().take(mid_point) {
            old_page.add_record(record_data, i as u64)?;
        }
        
        // Записи для новой страницы
        for (i, record_data) in records.iter().enumerate().skip(mid_point) {
            new_page.add_record(record_data, i as u64)?;
        }
        
        // Пытаемся вставить новую запись
        let insert_page_id = if old_page.get_free_space() >= data.len() as u32 {
            old_page.add_record(data, 0u64)?;
            page_id
        } else {
            new_page.add_record(data, 0u64)?;
            new_page_id
        };
        
        // Сохраняем страницы
        let old_serialized = old_page.to_bytes()?;
        self.file_manager.write_page(self.file_id, page_id, &old_serialized)?;
        
        let new_serialized = new_page.to_bytes()?;
        self.file_manager.write_page(self.file_id, new_page_id, &new_serialized)?;
        
        // Обновляем кеш
        self.update_page_cache(page_id, &old_page);
        self.update_page_cache(new_page_id, &new_page);
        
        let record_id = self.generate_record_id(insert_page_id, 0);
        
        Ok(InsertResult {
            record_id,
            page_id: insert_page_id,
            page_split: true,
        })
    }
    
    /// Удаляет запись (внутренний метод)
    fn delete_record_internal(&mut self, page_id: PageId, offset: u32) -> Result<DeleteResult> {
        let page_data = self.file_manager.read_page(self.file_id, page_id)?;
        let mut page = Page::from_bytes(&page_data)?;
        
        // Удаляем запись
        page.delete_record_by_offset(offset)?;
        
        // Сохраняем страницу
        let serialized = page.to_bytes()?;
        self.file_manager.write_page(self.file_id, page_id, &serialized)?;
        
        // Обновляем кеш
        self.update_page_cache(page_id, &page);
        
        // Проверяем, нужно ли объединение страниц
        let needs_merge = if let Some(page_info) = self.page_cache.get(&page_id) {
            page_info.fill_factor < self.config.min_fill_factor
        } else {
            false
        };
        
        if needs_merge {
            // TODO: Реализовать объединение страниц
            self.statistics.page_merges += 1;
        }
        
        Ok(DeleteResult {
            physical_delete: true,
            page_merge: needs_merge,
        })
    }
    
    /// Дефрагментирует страницу
    fn defragment_page(&mut self, page_id: PageId) -> Result<()> {
        let page_data = self.file_manager.read_page(self.file_id, page_id)?;
        let mut page = Page::from_bytes(&page_data)?;
        
        // Выполняем дефрагментацию
        page.defragment()?;
        
        // Сохраняем страницу
        let serialized = page.to_bytes()?;
        self.file_manager.write_page(self.file_id, page_id, &serialized)?;
        
        // Обновляем кеш
        self.update_page_cache(page_id, &page);
        
        Ok(())
    }
    
    /// Обновляет кеш информации о странице
    fn update_page_cache(&mut self, page_id: PageId, page: &Page) {
        let page_info = PageInfo {
            page_id,
            fill_factor: page.get_fill_factor(),
            record_count: page.get_record_count(),
            free_space: page.get_free_space(),
            needs_defragmentation: page.needs_defragmentation(),
        };
        
        self.page_cache.insert(page_id, page_info);
    }
    
    /// Генерирует ID записи
    fn generate_record_id(&self, page_id: PageId, offset: u32) -> RecordId {
        // Комбинируем page_id и offset в один ID
        ((page_id as u64) << 32) | (offset as u64)
    }
    
    /// Парсит ID записи
    fn parse_record_id(&self, record_id: RecordId) -> (PageId, u32) {
        let page_id = (record_id >> 32) as PageId;
        let offset = (record_id & 0xFFFFFFFF) as u32;
        (page_id, offset)
    }
    
    /// Получает все ID страниц
    fn get_all_page_ids(&self) -> Result<Vec<PageId>> {
        // TODO: Реализовать получение всех страниц из файла
        Ok(self.page_cache.keys().cloned().collect())
    }
}
