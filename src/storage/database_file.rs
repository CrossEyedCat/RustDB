//! Структуры файлов базы данных rustdb
//!
//! Этот модуль содержит расширенные структуры для организации файлов базы данных:
//! - Расширенный заголовок файла БД с метаданными
//! - Карта свободных страниц для эффективного управления пространством
//! - Управление расширением файлов с оптимизацией
//! - Структуры для организации данных в файле

use crate::common::{Error, Result};
use serde::{Deserialize, Serialize};

/// Размер блока в байтах (4KB)
pub const BLOCK_SIZE: usize = 4096;

/// Размер страницы (равен размеру блока)
pub const PAGE_SIZE: usize = BLOCK_SIZE;

/// ID файла базы данных
pub type DatabaseFileId = u32;

/// ID страницы в файле
pub type PageId = u64;

/// Тип файла базы данных
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatabaseFileType {
    /// Основной файл данных
    Data,
    /// Файл индексов
    Index,
    /// Файл журнала транзакций
    Log,
    /// Временный файл
    Temporary,
    /// Системный файл метаданных
    System,
}

/// Состояние файла базы данных
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatabaseFileState {
    /// Файл активен и готов к работе
    Active,
    /// Файл в процессе создания
    Creating,
    /// Файл помечен для удаления
    MarkedForDeletion,
    /// Файл поврежден
    Corrupted,
    /// Файл в режиме только для чтения
    ReadOnly,
}

/// Расширенный заголовок файла базы данных
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseFileHeader {
    /// Магическое число для идентификации файлов rustdb
    pub magic: u32,
    /// Версия формата файла
    pub version: u16,
    /// Подверсия формата (для обратной совместимости)
    pub subversion: u16,

    /// Размер блока/страницы
    pub page_size: u32,
    /// Общее количество страниц в файле
    pub total_pages: u64,
    /// Количество используемых страниц
    pub used_pages: u64,
    /// Количество свободных страниц
    pub free_pages: u64,

    /// Тип файла базы данных
    pub file_type: DatabaseFileType,
    /// Состояние файла
    pub file_state: DatabaseFileState,

    /// ID базы данных (для связи файлов одной БД)
    pub database_id: u32,
    /// Порядковый номер файла в БД
    pub file_sequence: u32,

    /// Указатель на корневую страницу каталога
    pub catalog_root_page: Option<PageId>,
    /// Указатель на первую страницу карты свободных страниц
    pub free_page_map_start: Option<PageId>,
    /// Количество страниц, занимаемых картой свободных страниц
    pub free_page_map_pages: u32,

    /// Максимальный размер файла в страницах (0 = без ограничения)
    pub max_pages: u64,
    /// Размер расширения файла в страницах
    pub extension_size: u32,

    /// Время создания файла (Unix timestamp)
    pub created_at: u64,
    /// Время последнего изменения
    pub modified_at: u64,
    /// Время последней проверки целостности
    pub last_check_at: u64,

    /// Счетчик операций записи
    pub write_count: u64,
    /// Счетчик операций чтения
    pub read_count: u64,

    /// Флаги файла (битовая маска)
    pub flags: u32,

    /// Контрольная сумма заголовка (должна вычисляться последней)
    pub checksum: u32,

    /// Резерв для будущих расширений (должен быть заполнен нулями)
    pub reserved: Vec<u8>,
}

/// Флаги файла базы данных
impl DatabaseFileHeader {
    /// Магическое число для файлов rustdb
    pub const MAGIC: u32 = 0x52555354; // "RUST"

    /// Текущая версия формата файла
    pub const VERSION: u16 = 2;
    /// Текущая подверсия формата файла
    pub const SUBVERSION: u16 = 0;

    /// Флаг: файл сжат
    pub const FLAG_COMPRESSED: u32 = 0x0001;
    /// Флаг: файл зашифрован
    pub const FLAG_ENCRYPTED: u32 = 0x0002;
    /// Флаг: включена проверка целостности
    pub const FLAG_CHECKSUM_ENABLED: u32 = 0x0004;
    /// Флаг: файл в режиме отладки
    pub const FLAG_DEBUG_MODE: u32 = 0x0008;

    /// Создает новый заголовок файла БД
    pub fn new(file_type: DatabaseFileType, database_id: u32) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            magic: Self::MAGIC,
            version: Self::VERSION,
            subversion: Self::SUBVERSION,
            page_size: PAGE_SIZE as u32,
            total_pages: 0,
            used_pages: 0,
            free_pages: 0,
            file_type,
            file_state: DatabaseFileState::Creating,
            database_id,
            file_sequence: 0,
            catalog_root_page: None,
            free_page_map_start: None,
            free_page_map_pages: 0,
            max_pages: 0,         // Без ограничения
            extension_size: 1024, // Расширяем по 1024 страницы (4MB)
            created_at: now,
            modified_at: now,
            last_check_at: now,
            write_count: 0,
            read_count: 0,
            flags: Self::FLAG_CHECKSUM_ENABLED,
            checksum: 0,
            reserved: vec![0; 64],
        }
    }

    /// Обновляет время модификации
    pub fn touch(&mut self) {
        self.modified_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Увеличивает счетчик записей
    pub fn increment_write_count(&mut self) {
        self.write_count = self.write_count.wrapping_add(1);
        self.touch();
    }

    /// Увеличивает счетчик чтений
    pub fn increment_read_count(&mut self) {
        self.read_count = self.read_count.wrapping_add(1);
    }

    /// Проверяет флаг
    pub fn has_flag(&self, flag: u32) -> bool {
        (self.flags & flag) != 0
    }

    /// Устанавливает флаг
    pub fn set_flag(&mut self, flag: u32) {
        self.flags |= flag;
    }

    /// Снимает флаг
    pub fn clear_flag(&mut self, flag: u32) {
        self.flags &= !flag;
    }

    /// Вычисляет контрольную сумму заголовка
    pub fn calculate_checksum(&self) -> u32 {
        let mut sum = 0u32;
        sum = sum.wrapping_add(self.magic);
        sum = sum.wrapping_add(self.version as u32);
        sum = sum.wrapping_add(self.subversion as u32);
        sum = sum.wrapping_add(self.page_size);
        sum = sum.wrapping_add(self.total_pages as u32);
        sum = sum.wrapping_add(self.used_pages as u32);
        sum = sum.wrapping_add(self.free_pages as u32);
        sum = sum.wrapping_add(self.database_id);
        sum = sum.wrapping_add(self.file_sequence);
        sum = sum.wrapping_add(self.free_page_map_pages);
        sum = sum.wrapping_add(self.max_pages as u32);
        sum = sum.wrapping_add(self.extension_size);
        sum = sum.wrapping_add(self.created_at as u32);
        sum = sum.wrapping_add(self.modified_at as u32);
        sum = sum.wrapping_add(self.write_count as u32);
        sum = sum.wrapping_add(self.read_count as u32);
        sum = sum.wrapping_add(self.flags);

        // Добавляем байты из reserved
        for &byte in &self.reserved {
            sum = sum.wrapping_add(byte as u32);
        }

        sum
    }

    /// Обновляет контрольную сумму
    pub fn update_checksum(&mut self) {
        self.checksum = self.calculate_checksum();
    }

    /// Проверяет корректность заголовка
    pub fn is_valid(&self) -> bool {
        self.magic == Self::MAGIC
            && self.version == Self::VERSION
            && self.page_size == PAGE_SIZE as u32
            && self.checksum == self.calculate_checksum()
            && self.used_pages <= self.total_pages
            && self.free_pages <= self.total_pages
            && (self.used_pages + self.free_pages) <= self.total_pages
    }

    /// Возвращает описание состояния файла
    pub fn state_description(&self) -> &'static str {
        match self.file_state {
            DatabaseFileState::Active => "Активен",
            DatabaseFileState::Creating => "Создается",
            DatabaseFileState::MarkedForDeletion => "Помечен для удаления",
            DatabaseFileState::Corrupted => "Поврежден",
            DatabaseFileState::ReadOnly => "Только для чтения",
        }
    }

    /// Возвращает описание типа файла
    pub fn type_description(&self) -> &'static str {
        match self.file_type {
            DatabaseFileType::Data => "Данные",
            DatabaseFileType::Index => "Индексы",
            DatabaseFileType::Log => "Журнал",
            DatabaseFileType::Temporary => "Временный",
            DatabaseFileType::System => "Системный",
        }
    }
}

impl Default for DatabaseFileHeader {
    fn default() -> Self {
        Self::new(DatabaseFileType::Data, 0)
    }
}

/// Запись в карте свободных страниц
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreePageMapEntry {
    /// Начальная страница свободного блока
    pub start_page: PageId,
    /// Количество свободных страниц подряд
    pub page_count: u32,
    /// Приоритет использования (0 = высший)
    pub priority: u8,
    /// Флаги записи
    pub flags: u8,
}

/// Карта свободных страниц
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreePageMap {
    /// Заголовок карты
    pub header: FreePageMapHeader,
    /// Записи о свободных блоках страниц
    pub entries: Vec<FreePageMapEntry>,
    /// Битовая карта для быстрого поиска (опционально)
    pub bitmap: Option<Vec<u8>>,
}

/// Заголовок карты свободных страниц
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreePageMapHeader {
    /// Магическое число карты
    pub magic: u32,
    /// Версия формата карты
    pub version: u16,
    /// Общее количество записей в карте
    pub total_entries: u32,
    /// Количество активных записей
    pub active_entries: u32,
    /// Общее количество свободных страниц
    pub total_free_pages: u64,
    /// Наибольший непрерывный блок свободных страниц
    pub largest_free_block: u32,
    /// Время последнего обновления карты
    pub last_updated: u64,
    /// Контрольная сумма карты
    pub checksum: u32,
}

impl FreePageMap {
    /// Магическое число для карты свободных страниц
    pub const MAGIC: u32 = 0x46524545; // "FREE"

    /// Версия формата карты
    pub const VERSION: u16 = 1;

    /// Создает новую карту свободных страниц
    pub fn new() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            header: FreePageMapHeader {
                magic: Self::MAGIC,
                version: Self::VERSION,
                total_entries: 0,
                active_entries: 0,
                total_free_pages: 0,
                largest_free_block: 0,
                last_updated: now,
                checksum: 0,
            },
            entries: Vec::new(),
            bitmap: None,
        }
    }

    /// Добавляет свободный блок страниц
    pub fn add_free_block(&mut self, start_page: PageId, page_count: u32) -> Result<()> {
        if page_count == 0 {
            return Err(Error::validation(
                "Количество страниц не может быть нулевым",
            ));
        }

        // Проверяем пересечения с существующими блоками
        for entry in &self.entries {
            let entry_end = entry.start_page + entry.page_count as u64;
            let new_end = start_page + page_count as u64;

            if start_page < entry_end && new_end > entry.start_page {
                return Err(Error::validation(
                    "Свободный блок пересекается с существующим",
                ));
            }
        }

        // Пытаемся объединить с соседними блоками
        let mut merged = false;
        for entry in &mut self.entries {
            // Объединение с предыдущим блоком
            if entry.start_page + entry.page_count as u64 == start_page {
                entry.page_count += page_count;
                merged = true;
                break;
            }
            // Объединение со следующим блоком
            else if start_page + page_count as u64 == entry.start_page {
                entry.start_page = start_page;
                entry.page_count += page_count;
                merged = true;
                break;
            }
        }

        // Если не удалось объединить, добавляем новую запись
        if !merged {
            self.entries.push(FreePageMapEntry {
                start_page,
                page_count,
                priority: 0,
                flags: 0,
            });
        }

        // Сортируем записи по начальной странице
        self.entries.sort_by_key(|entry| entry.start_page);

        // Обновляем статистику
        self.update_statistics();

        Ok(())
    }

    /// Выделяет блок свободных страниц
    pub fn allocate_pages(&mut self, page_count: u32) -> Option<PageId> {
        if page_count == 0 {
            return None;
        }

        // Ищем подходящий блок (first-fit алгоритм)
        for i in 0..self.entries.len() {
            let entry = &self.entries[i];

            if entry.page_count >= page_count {
                let allocated_start = entry.start_page;

                if entry.page_count == page_count {
                    // Удаляем запись полностью
                    self.entries.remove(i);
                } else {
                    // Уменьшаем размер блока
                    self.entries[i].start_page += page_count as u64;
                    self.entries[i].page_count -= page_count;
                }

                self.update_statistics();
                // Убеждаемся, что не возвращаем page_id = 0
                return Some(if allocated_start == 0 {
                    1
                } else {
                    allocated_start
                });
            }
        }

        None
    }

    /// Освобождает блок страниц
    pub fn free_pages(&mut self, start_page: PageId, page_count: u32) -> Result<()> {
        self.add_free_block(start_page, page_count)
    }

    /// Ищет наибольший непрерывный блок свободных страниц
    pub fn find_largest_free_block(&self) -> u32 {
        self.entries
            .iter()
            .map(|entry| entry.page_count)
            .max()
            .unwrap_or(0)
    }

    /// Возвращает общее количество свободных страниц
    pub fn total_free_pages(&self) -> u64 {
        self.entries
            .iter()
            .map(|entry| entry.page_count as u64)
            .sum()
    }

    /// Обновляет статистику карты
    fn update_statistics(&mut self) {
        self.header.total_entries = self.entries.len() as u32;
        self.header.active_entries = self.entries.len() as u32;
        self.header.total_free_pages = self.total_free_pages();
        self.header.largest_free_block = self.find_largest_free_block();
        self.header.last_updated = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Проверяет целостность карты
    pub fn validate(&self) -> Result<()> {
        if self.header.magic != Self::MAGIC {
            return Err(Error::validation(
                "Неверное магическое число карты свободных страниц",
            ));
        }

        if self.header.version != Self::VERSION {
            return Err(Error::validation(
                "Неподдерживаемая версия карты свободных страниц",
            ));
        }

        // Проверяем пересечения блоков
        for (i, entry1) in self.entries.iter().enumerate() {
            for (j, entry2) in self.entries.iter().enumerate() {
                if i != j {
                    let end1 = entry1.start_page + entry1.page_count as u64;
                    let end2 = entry2.start_page + entry2.page_count as u64;

                    if entry1.start_page < end2 && end1 > entry2.start_page {
                        return Err(Error::validation("Обнаружены пересекающиеся блоки в карте"));
                    }
                }
            }
        }

        Ok(())
    }

    /// Дефрагментирует карту (объединяет соседние блоки)
    pub fn defragment(&mut self) {
        // Сортируем по начальной странице
        self.entries.sort_by_key(|entry| entry.start_page);

        // Объединяем соседние блоки
        let mut i = 0;
        while i < self.entries.len().saturating_sub(1) {
            let current_end = self.entries[i].start_page + self.entries[i].page_count as u64;
            let next_start = self.entries[i + 1].start_page;

            if current_end == next_start {
                // Объединяем блоки
                self.entries[i].page_count += self.entries[i + 1].page_count;
                self.entries.remove(i + 1);
            } else {
                i += 1;
            }
        }

        self.update_statistics();
    }

    /// Создает битовую карту для быстрого поиска
    pub fn create_bitmap(&mut self, total_pages: u64) {
        let bitmap_size = ((total_pages + 7) / 8) as usize;
        let mut bitmap = vec![0u8; bitmap_size];

        // Отмечаем свободные страницы в битовой карте
        for entry in &self.entries {
            for page in entry.start_page..entry.start_page + entry.page_count as u64 {
                let byte_index = (page / 8) as usize;
                let bit_index = (page % 8) as u8;

                if byte_index < bitmap.len() {
                    bitmap[byte_index] |= 1 << bit_index;
                }
            }
        }

        self.bitmap = Some(bitmap);
    }
}

impl Default for FreePageMap {
    fn default() -> Self {
        Self::new()
    }
}

/// Менеджер расширения файлов
#[derive(Debug, Clone)]
pub struct FileExtensionManager {
    /// Стратегия расширения
    pub strategy: ExtensionStrategy,
    /// Минимальный размер расширения (в страницах)
    pub min_extension_size: u32,
    /// Максимальный размер расширения (в страницах)
    pub max_extension_size: u32,
    /// Коэффициент роста для экспоненциальной стратегии
    pub growth_factor: f64,
    /// История расширений для анализа
    pub extension_history: Vec<ExtensionRecord>,
}

/// Стратегия расширения файла
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionStrategy {
    /// Фиксированный размер расширения
    Fixed,
    /// Линейное увеличение
    Linear,
    /// Экспоненциальное увеличение
    Exponential,
    /// Адаптивное расширение на основе паттернов использования
    Adaptive,
}

/// Запись о расширении файла
#[derive(Debug, Clone)]
pub struct ExtensionRecord {
    /// Время расширения
    pub timestamp: u64,
    /// Размер файла до расширения
    pub old_size: u64,
    /// Размер файла после расширения
    pub new_size: u64,
    /// Причина расширения
    pub reason: ExtensionReason,
}

/// Причина расширения файла
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionReason {
    /// Недостаток свободного места
    OutOfSpace,
    /// Предварительное расширение
    Preallocation,
    /// Оптимизация производительности
    Performance,
    /// Дефрагментация
    Defragmentation,
}

impl FileExtensionManager {
    /// Создает новый менеджер расширения файлов
    pub fn new(strategy: ExtensionStrategy) -> Self {
        Self {
            strategy,
            min_extension_size: 64,   // 256KB
            max_extension_size: 4096, // 16MB
            growth_factor: 1.5,
            extension_history: Vec::new(),
        }
    }

    /// Вычисляет размер следующего расширения
    pub fn calculate_extension_size(&self, current_size: u64, required_size: u32) -> u32 {
        let base_size = match self.strategy {
            ExtensionStrategy::Fixed => self.min_extension_size,
            ExtensionStrategy::Linear => {
                // Увеличиваем на 10% от текущего размера
                let linear_size = (current_size as f64 * 0.1) as u32;
                linear_size.max(self.min_extension_size)
            }
            ExtensionStrategy::Exponential => {
                // Экспоненциальное увеличение
                let exp_size = (current_size as f64 * (self.growth_factor - 1.0)) as u32;
                exp_size.max(self.min_extension_size)
            }
            ExtensionStrategy::Adaptive => self.calculate_adaptive_size(current_size),
        };

        // Убеждаемся, что размер достаточен для требуемого количества страниц
        let final_size = base_size.max(required_size);

        // Ограничиваем максимальным размером
        final_size.min(self.max_extension_size)
    }

    /// Вычисляет адаптивный размер расширения
    fn calculate_adaptive_size(&self, _current_size: u64) -> u32 {
        if self.extension_history.is_empty() {
            return self.min_extension_size;
        }

        // Анализируем историю расширений за последний час
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let recent_extensions: Vec<_> = self
            .extension_history
            .iter()
            .filter(|record| now - record.timestamp < 3600) // Последний час
            .collect();

        if recent_extensions.is_empty() {
            return self.min_extension_size;
        }

        // Если было много расширений недавно, увеличиваем размер
        let extension_count = recent_extensions.len() as u32;
        let adaptive_multiplier = if extension_count > 5 {
            3.0
        } else if extension_count > 2 {
            2.0
        } else {
            1.0
        };

        let adaptive_size = (self.min_extension_size as f64 * adaptive_multiplier) as u32;
        adaptive_size.min(self.max_extension_size)
    }

    /// Записывает расширение в историю
    pub fn record_extension(&mut self, old_size: u64, new_size: u64, reason: ExtensionReason) {
        let record = ExtensionRecord {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            old_size,
            new_size,
            reason,
        };

        self.extension_history.push(record);

        // Ограничиваем размер истории
        if self.extension_history.len() > 1000 {
            self.extension_history.drain(0..500); // Удаляем старые записи
        }
    }

    /// Рекомендует предварительное расширение файла
    pub fn should_preextend(&self, _current_size: u64, free_pages: u64, total_pages: u64) -> bool {
        if total_pages == 0 {
            return false;
        }

        let usage_ratio = (total_pages - free_pages) as f64 / total_pages as f64;

        // Предварительно расширяем, если использовано более 80% места
        usage_ratio > 0.8
    }

    /// Возвращает статистику расширений
    pub fn get_statistics(&self) -> ExtensionStatistics {
        ExtensionStatistics {
            total_extensions: self.extension_history.len(),
            average_extension_size: if self.extension_history.is_empty() {
                0.0
            } else {
                let total_growth: u64 = self
                    .extension_history
                    .iter()
                    .map(|record| record.new_size - record.old_size)
                    .sum();
                total_growth as f64 / self.extension_history.len() as f64
            },
            last_extension: self.extension_history.last().cloned(),
        }
    }
}

/// Статистика расширений файла
#[derive(Debug, Clone)]
pub struct ExtensionStatistics {
    /// Общее количество расширений
    pub total_extensions: usize,
    /// Средний размер расширения
    pub average_extension_size: f64,
    /// Последнее расширение
    pub last_extension: Option<ExtensionRecord>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_file_header_creation() {
        let header = DatabaseFileHeader::new(DatabaseFileType::Data, 123);

        assert_eq!(header.magic, DatabaseFileHeader::MAGIC);
        assert_eq!(header.version, DatabaseFileHeader::VERSION);
        assert_eq!(header.database_id, 123);
        assert_eq!(header.file_type, DatabaseFileType::Data);
        assert_eq!(header.file_state, DatabaseFileState::Creating);
        assert!(header.has_flag(DatabaseFileHeader::FLAG_CHECKSUM_ENABLED));
    }

    #[test]
    fn test_database_file_header_checksum() {
        let mut header = DatabaseFileHeader::new(DatabaseFileType::Index, 456);
        header.update_checksum();
        assert!(header.is_valid());

        // Изменяем данные и проверяем, что валидация не пройдет
        header.database_id = 999;
        assert!(!header.is_valid());

        // Обновляем контрольную сумму и проверяем снова
        header.update_checksum();
        assert!(header.is_valid());
    }

    #[test]
    fn test_free_page_map_basic_operations() -> Result<()> {
        let mut map = FreePageMap::new();

        // Добавляем свободные блоки
        map.add_free_block(10, 5)?;
        map.add_free_block(20, 3)?;

        assert_eq!(map.total_free_pages(), 8);
        assert_eq!(map.find_largest_free_block(), 5);

        // Выделяем страницы
        let allocated = map.allocate_pages(3);
        assert_eq!(allocated, Some(10));
        assert_eq!(map.total_free_pages(), 5);

        Ok(())
    }

    #[test]
    fn test_free_page_map_merge_blocks() -> Result<()> {
        let mut map = FreePageMap::new();

        // Добавляем соседние блоки
        map.add_free_block(10, 5)?;
        map.add_free_block(15, 3)?; // Должен объединиться с предыдущим

        assert_eq!(map.entries.len(), 1);
        assert_eq!(map.entries[0].start_page, 10);
        assert_eq!(map.entries[0].page_count, 8);

        Ok(())
    }

    #[test]
    fn test_file_extension_manager() {
        let mut manager = FileExtensionManager::new(ExtensionStrategy::Fixed);

        let extension_size = manager.calculate_extension_size(1000, 50);
        assert_eq!(extension_size, manager.min_extension_size.max(50));

        // Записываем расширение
        manager.record_extension(1000, 1064, ExtensionReason::OutOfSpace);

        let stats = manager.get_statistics();
        assert_eq!(stats.total_extensions, 1);
        assert_eq!(stats.average_extension_size, 64.0);
    }

    #[test]
    fn test_extension_strategies() {
        let manager_fixed = FileExtensionManager::new(ExtensionStrategy::Fixed);
        let manager_linear = FileExtensionManager::new(ExtensionStrategy::Linear);
        let manager_exp = FileExtensionManager::new(ExtensionStrategy::Exponential);

        let current_size = 1000;
        let required = 10;

        let fixed_size = manager_fixed.calculate_extension_size(current_size, required);
        let linear_size = manager_linear.calculate_extension_size(current_size, required);
        let exp_size = manager_exp.calculate_extension_size(current_size, required);

        // Фиксированный должен быть минимальным
        assert_eq!(fixed_size, manager_fixed.min_extension_size);

        // Линейный должен быть больше фиксированного
        assert!(linear_size >= fixed_size);

        // Экспоненциальный должен быть больше линейного для больших файлов
        assert!(exp_size >= linear_size);
    }

    #[test]
    fn test_free_page_map_validation() -> Result<()> {
        let mut map = FreePageMap::new();

        // Добавляем корректные блоки
        map.add_free_block(10, 5)?;
        map.add_free_block(20, 3)?;

        assert!(map.validate().is_ok());

        // Добавляем пересекающийся блок
        let result = map.add_free_block(12, 3);
        assert!(result.is_err());

        Ok(())
    }
}
