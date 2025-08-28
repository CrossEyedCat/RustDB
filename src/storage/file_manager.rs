//! Менеджер файлов для RustBD
//!
//! Этот модуль отвечает за базовые операции с файлами базы данных:
//! - Создание, открытие и закрытие файлов
//! - Чтение и запись блоков данных
//! - Управление размерами файлов
//! - Синхронизация данных на диск

use crate::common::{Error, Result};
use crate::storage::block::BlockId;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};

/// Размер блока в байтах (4KB)
pub const BLOCK_SIZE: usize = 4096;

/// ID файла
pub type FileId = u32;

/// Заголовок файла базы данных
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileHeader {
    /// Магическое число для идентификации файлов RustBD
    pub magic: u32,
    /// Версия формата файла
    pub version: u16,
    /// Размер блока
    pub block_size: u32,
    /// Общее количество блоков в файле
    pub total_blocks: u32,
    /// Количество используемых блоков
    pub used_blocks: u32,
    /// Указатель на первый свободный блок
    pub first_free_block: Option<BlockId>,
    /// Время создания файла
    pub created_at: u64,
    /// Время последнего изменения
    pub modified_at: u64,
    /// Контрольная сумма заголовка
    pub checksum: u32,
}

impl FileHeader {
    /// Магическое число для файлов RustBD
    pub const MAGIC: u32 = 0x52555354; // "RUST"
    
    /// Текущая версия формата файла
    pub const VERSION: u16 = 1;

    /// Создает новый заголовок файла
    pub fn new() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            magic: Self::MAGIC,
            version: Self::VERSION,
            block_size: BLOCK_SIZE as u32,
            total_blocks: 0,
            used_blocks: 0,
            first_free_block: None,
            created_at: now,
            modified_at: now,
            checksum: 0,
        }
    }

    /// Обновляет время модификации
    pub fn touch(&mut self) {
        self.modified_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Вычисляет контрольную сумму заголовка
    pub fn calculate_checksum(&self) -> u32 {
        // Простая контрольная сумма (в реальной БД используется CRC32)
        let mut sum = 0u32;
        sum = sum.wrapping_add(self.magic);
        sum = sum.wrapping_add(self.version as u32);
        sum = sum.wrapping_add(self.block_size);
        sum = sum.wrapping_add(self.total_blocks);
        sum = sum.wrapping_add(self.used_blocks);
        sum = sum.wrapping_add(self.created_at as u32);
        sum = sum.wrapping_add(self.modified_at as u32);
        sum
    }

    /// Проверяет корректность заголовка
    pub fn is_valid(&self) -> bool {
        self.magic == Self::MAGIC 
            && self.version == Self::VERSION
            && self.block_size == BLOCK_SIZE as u32
            && self.checksum == self.calculate_checksum()
    }
}

impl Default for FileHeader {
    fn default() -> Self {
        Self::new()
    }
}

/// Дескриптор открытого файла базы данных
pub struct DatabaseFile {
    /// ID файла
    pub file_id: FileId,
    /// Путь к файлу
    pub path: PathBuf,
    /// Файловый дескриптор
    pub file: File,
    /// Заголовок файла
    pub header: FileHeader,
    /// Флаг изменения заголовка
    pub header_dirty: bool,
    /// Флаг только для чтения
    pub read_only: bool,
}

impl DatabaseFile {
    /// Создает новый файл базы данных
    pub fn create<P: AsRef<Path>>(file_id: FileId, path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        
        // Проверяем, что файл не существует
        if path.exists() {
            return Err(Error::database(format!("Файл {} уже существует", path.display())));
        }

        // Создаем файл
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&path)?;

        // Создаем заголовок
        let mut header = FileHeader::new();
        header.checksum = header.calculate_checksum();

        // Записываем заголовок в файл
        let header_bytes = bincode::serialize(&header)
            .map_err(|e| Error::database(format!("Ошибка сериализации заголовка: {}", e)))?;
        
        file.write_all(&header_bytes)?;
        
        // Дополняем до границы блока, чтобы данные начинались с четкой позиции
        let header_size = header_bytes.len();
        let padding_size = BLOCK_SIZE - (header_size % BLOCK_SIZE);
        if padding_size < BLOCK_SIZE {
            let padding = vec![0u8; padding_size];
            file.write_all(&padding)?;
        }
        
        file.sync_all()?;

        Ok(Self {
            file_id,
            path,
            file,
            header,
            header_dirty: false,
            read_only: false,
        })
    }

    /// Открывает существующий файл базы данных
    pub fn open<P: AsRef<Path>>(file_id: FileId, path: P, read_only: bool) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        
        // Проверяем, что файл существует
        if !path.exists() {
            return Err(Error::database(format!("Файл {} не найден", path.display())));
        }

        // Открываем файл
        let mut file = if read_only {
            OpenOptions::new().read(true).open(&path)?
        } else {
            OpenOptions::new().read(true).write(true).open(&path)?
        };

        // Читаем заголовок
        let mut header_bytes = Vec::new();
        file.read_to_end(&mut header_bytes)?;
        
        if header_bytes.is_empty() {
            return Err(Error::database("Файл поврежден: пустой файл".to_string()));
        }

        let header: FileHeader = bincode::deserialize(&header_bytes)
            .map_err(|e| Error::database(format!("Ошибка десериализации заголовка: {}", e)))?;

        // Проверяем корректность заголовка
        if !header.is_valid() {
            return Err(Error::database("Файл поврежден: некорректный заголовок".to_string()));
        }

        Ok(Self {
            file_id,
            path,
            file,
            header,
            header_dirty: false,
            read_only,
        })
    }

    /// Читает блок данных из файла
    pub fn read_block(&mut self, block_id: BlockId) -> Result<Vec<u8>> {
        if block_id >= self.header.total_blocks as u64 {
            return Err(Error::database(format!("Блок {} не существует", block_id)));
        }

        // Вычисляем позицию блока в файле (данные начинаются с первого блока после заголовка)
        let offset = BLOCK_SIZE as u64 + (block_id as u64 * BLOCK_SIZE as u64);
        
        // Переходим к позиции блока
        self.file.seek(SeekFrom::Start(offset))?;
        
        // Читаем данные блока
        let mut buffer = vec![0u8; BLOCK_SIZE];
        self.file.read_exact(&mut buffer)?;
        
        Ok(buffer)
    }

    /// Записывает блок данных в файл
    pub fn write_block(&mut self, block_id: BlockId, data: &[u8]) -> Result<()> {
        if self.read_only {
            return Err(Error::database("Файл открыт только для чтения".to_string()));
        }

        if data.len() != BLOCK_SIZE {
            return Err(Error::database(format!("Неверный размер блока: {} (ожидается {})", data.len(), BLOCK_SIZE)));
        }

        // Расширяем файл если необходимо
        if block_id >= self.header.total_blocks as u64 {
            self.extend_file((block_id + 1) as u32)?;
        }

        // Вычисляем позицию блока в файле (данные начинаются с первого блока после заголовка)
        let offset = BLOCK_SIZE as u64 + (block_id as u64 * BLOCK_SIZE as u64);
        
        // Переходим к позиции блока
        self.file.seek(SeekFrom::Start(offset))?;
        
        // Записываем данные блока
        self.file.write_all(data)?;
        
        // Обновляем счетчик используемых блоков
        if block_id >= self.header.used_blocks as u64 {
            self.header.used_blocks = (block_id + 1) as u32;
            self.header.touch();
            self.header_dirty = true;
        }
        
        Ok(())
    }

    /// Расширяет файл до указанного количества блоков
    pub fn extend_file(&mut self, new_block_count: u32) -> Result<()> {
        if self.read_only {
            return Err(Error::database("Файл открыт только для чтения".to_string()));
        }

        if new_block_count <= self.header.total_blocks {
            return Ok(()); // Файл уже достаточного размера
        }

        // Вычисляем новый размер файла (заголовок + блоки данных)
        let new_size = BLOCK_SIZE as u64 + (new_block_count as u64 * BLOCK_SIZE as u64);
        
        // Расширяем файл
        self.file.seek(SeekFrom::Start(new_size - 1))?;
        self.file.write_all(&[0])?;
        
        // Обновляем заголовок
        self.header.total_blocks = new_block_count;
        self.header.touch();
        self.header_dirty = true;
        
        Ok(())
    }

    /// Синхронизирует данные на диск
    pub fn sync(&mut self) -> Result<()> {
        if self.read_only {
            return Ok(());
        }

        // Записываем заголовок если он изменился
        if self.header_dirty {
            self.write_header()?;
            self.header_dirty = false;
        }
        
        // Синхронизируем все данные
        self.file.sync_all()?;
        
        Ok(())
    }

    /// Записывает заголовок в файл
    fn write_header(&mut self) -> Result<()> {
        // Обновляем контрольную сумму
        self.header.checksum = self.header.calculate_checksum();
        
        // Сериализуем заголовок
        let header_bytes = bincode::serialize(&self.header)
            .map_err(|e| Error::database(format!("Ошибка сериализации заголовка: {}", e)))?;
        
        // Записываем в начало файла
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header_bytes)?;
        
        Ok(())
    }

    /// Возвращает размер файла в блоках
    pub fn size_in_blocks(&self) -> u32 {
        self.header.total_blocks
    }

    /// Возвращает количество используемых блоков
    pub fn used_blocks(&self) -> u32 {
        self.header.used_blocks
    }

    /// Возвращает количество свободных блоков
    pub fn free_blocks(&self) -> u32 {
        self.header.total_blocks.saturating_sub(self.header.used_blocks)
    }
}

/// Менеджер файлов базы данных
pub struct FileManager {
    /// Открытые файлы
    files: HashMap<FileId, DatabaseFile>,
    /// Корневая директория для файлов БД
    root_dir: PathBuf,
    /// Счетчик для генерации ID файлов
    next_file_id: FileId,
}

impl FileManager {
    /// Создает новый менеджер файлов
    pub fn new<P: AsRef<Path>>(root_dir: P) -> Result<Self> {
        let root_dir = root_dir.as_ref().to_path_buf();
        
        // Создаем директорию если она не существует
        if !root_dir.exists() {
            std::fs::create_dir_all(&root_dir)?;
        }
        
        Ok(Self {
            files: HashMap::new(),
            root_dir,
            next_file_id: 1,
        })
    }

    /// Создает новый файл базы данных
    pub fn create_file(&mut self, filename: &str) -> Result<FileId> {
        let file_id = self.next_file_id;
        self.next_file_id += 1;
        
        let file_path = self.root_dir.join(filename);
        let db_file = DatabaseFile::create(file_id, file_path)?;
        
        self.files.insert(file_id, db_file);
        
        Ok(file_id)
    }

    /// Открывает существующий файл базы данных
    pub fn open_file(&mut self, filename: &str, read_only: bool) -> Result<FileId> {
        let file_path = self.root_dir.join(filename);
        
        // Проверяем, не открыт ли файл уже
        for (id, file) in &self.files {
            if file.path == file_path {
                return Ok(*id);
            }
        }
        
        let file_id = self.next_file_id;
        self.next_file_id += 1;
        
        let db_file = DatabaseFile::open(file_id, file_path, read_only)?;
        self.files.insert(file_id, db_file);
        
        Ok(file_id)
    }

    /// Закрывает файл базы данных
    pub fn close_file(&mut self, file_id: FileId) -> Result<()> {
        if let Some(mut file) = self.files.remove(&file_id) {
            file.sync()?;
        }
        Ok(())
    }

    /// Читает блок из файла
    pub fn read_block(&mut self, file_id: FileId, block_id: BlockId) -> Result<Vec<u8>> {
        let file = self.files.get_mut(&file_id)
            .ok_or_else(|| Error::database(format!("Файл {} не открыт", file_id)))?;
        
        file.read_block(block_id)
    }

    /// Записывает блок в файл
    pub fn write_block(&mut self, file_id: FileId, block_id: BlockId, data: &[u8]) -> Result<()> {
        let file = self.files.get_mut(&file_id)
            .ok_or_else(|| Error::database(format!("Файл {} не открыт", file_id)))?;
        
        file.write_block(block_id, data)
    }

    /// Синхронизирует файл на диск
    pub fn sync_file(&mut self, file_id: FileId) -> Result<()> {
        let file = self.files.get_mut(&file_id)
            .ok_or_else(|| Error::database(format!("Файл {} не открыт", file_id)))?;
        
        file.sync()
    }

    /// Синхронизирует все открытые файлы
    pub fn sync_all(&mut self) -> Result<()> {
        for file in self.files.values_mut() {
            file.sync()?;
        }
        Ok(())
    }

    /// Возвращает информацию о файле
    pub fn get_file_info(&self, file_id: FileId) -> Option<&DatabaseFile> {
        self.files.get(&file_id)
    }

    /// Возвращает список открытых файлов
    pub fn list_open_files(&self) -> Vec<FileId> {
        self.files.keys().cloned().collect()
    }

    /// Закрывает все открытые файлы
    pub fn close_all(&mut self) -> Result<()> {
        let file_ids: Vec<FileId> = self.files.keys().cloned().collect();
        for file_id in file_ids {
            self.close_file(file_id)?;
        }
        Ok(())
    }

    /// Удаляет файл с диска
    pub fn delete_file(&mut self, filename: &str) -> Result<()> {
        let file_path = self.root_dir.join(filename);
        
        // Закрываем файл если он открыт
        let file_id_to_close = self.files.iter()
            .find(|(_, file)| file.path == file_path)
            .map(|(id, _)| *id);
        
        if let Some(file_id) = file_id_to_close {
            self.close_file(file_id)?;
        }
        
        // Удаляем файл
        if file_path.exists() {
            std::fs::remove_file(file_path)?;
        }
        
        Ok(())
    }
}

impl Drop for FileManager {
    fn drop(&mut self) {
        let _ = self.close_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_file_header_creation() {
        let header = FileHeader::new();
        assert_eq!(header.magic, FileHeader::MAGIC);
        assert_eq!(header.version, FileHeader::VERSION);
        assert_eq!(header.block_size, BLOCK_SIZE as u32);
        assert_eq!(header.total_blocks, 0);
        assert_eq!(header.used_blocks, 0);
    }

    #[test]
    fn test_file_header_checksum() {
        let mut header = FileHeader::new();
        let checksum = header.calculate_checksum();
        header.checksum = checksum;
        assert!(header.is_valid());
    }

    #[test]
    fn test_create_database_file() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.db");
        
        let db_file = DatabaseFile::create(1, &file_path)?;
        assert_eq!(db_file.file_id, 1);
        assert_eq!(db_file.path, file_path);
        assert!(!db_file.read_only);
        assert!(file_path.exists());
        
        Ok(())
    }

    #[test]
    fn test_open_database_file() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.db");
        
        // Создаем файл
        let _db_file = DatabaseFile::create(1, &file_path)?;
        
        // Открываем файл
        let db_file = DatabaseFile::open(2, &file_path, false)?;
        assert_eq!(db_file.file_id, 2);
        assert_eq!(db_file.path, file_path);
        assert!(!db_file.read_only);
        
        Ok(())
    }

    #[test]
    fn test_write_read_block() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.db");
        
        let mut db_file = DatabaseFile::create(1, &file_path)?;
        
        // Создаем тестовые данные
        let test_data = vec![42u8; BLOCK_SIZE];
        
        // Записываем блок
        db_file.write_block(0, &test_data)?;
        
        // Читаем блок
        let read_data = db_file.read_block(0)?;
        assert_eq!(read_data, test_data);
        
        Ok(())
    }

    #[test]
    fn test_file_manager() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut manager = FileManager::new(temp_dir.path())?;
        
        // Создаем файл
        let file_id = manager.create_file("test.db")?;
        
        // Записываем данные
        let test_data = vec![123u8; BLOCK_SIZE];
        manager.write_block(file_id, 0, &test_data)?;
        
        // Читаем данные
        let read_data = manager.read_block(file_id, 0)?;
        assert_eq!(read_data, test_data);
        
        // Синхронизируем
        manager.sync_file(file_id)?;
        
        // Закрываем файл
        manager.close_file(file_id)?;
        
        Ok(())
    }

    #[test]
    fn test_file_extension() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.db");
        
        let mut db_file = DatabaseFile::create(1, &file_path)?;
        
        // Изначально файл пустой
        assert_eq!(db_file.size_in_blocks(), 0);
        
        // Записываем блок с большим ID
        let test_data = vec![1u8; BLOCK_SIZE];
        db_file.write_block(10, &test_data)?;
        
        // Файл должен расшириться
        assert_eq!(db_file.size_in_blocks(), 11);
        assert_eq!(db_file.used_blocks(), 11);
        
        Ok(())
    }
}
