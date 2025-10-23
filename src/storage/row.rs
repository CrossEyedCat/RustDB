//! Структуры строк и таблиц для rustdb

use crate::common::{
    types::{ColumnValue, DataType, PageId},
    Error, Result,
};
use crate::storage::tuple::{Schema, Tuple};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Строка таблицы с версионированием
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    /// ID строки
    pub id: u64,
    /// Текущая версия строки
    pub current_tuple: Tuple,
    /// Все версии строки
    pub versions: HashMap<u64, Tuple>,
    /// Указатель на следующую строку в таблице
    pub next_row: Option<u64>,
    /// Указатель на предыдущую строку в таблице
    pub prev_row: Option<u64>,
    /// Статистика строки
    pub stats: RowStats,
}

impl Row {
    /// Создает новую строку
    pub fn new(id: u64, tuple: Tuple) -> Self {
        let mut versions = HashMap::new();
        versions.insert(tuple.version, tuple.clone());

        Self {
            id,
            current_tuple: tuple,
            versions,
            next_row: None,
            prev_row: None,
            stats: RowStats::new(),
        }
    }

    /// Обновляет строку
    pub fn update(&mut self, new_values: HashMap<String, ColumnValue>) -> Result<()> {
        // Создаем новую версию
        let mut new_tuple = self.current_tuple.create_new_version();

        // Обновляем значения
        for (column, value) in new_values {
            new_tuple.set_value(&column, value);
        }

        // Добавляем версию в историю
        self.versions.insert(new_tuple.version, new_tuple.clone());

        // Обновляем текущую версию
        self.current_tuple = new_tuple;

        // Обновляем статистику
        self.stats.update_count += 1;
        self.stats.last_updated = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Ok(())
    }

    /// Удаляет строку
    pub fn delete(&mut self) -> Result<()> {
        self.current_tuple.mark_deleted();
        self.stats.delete_count += 1;
        self.stats.last_updated = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        Ok(())
    }

    /// Восстанавливает удаленную строку
    pub fn restore(&mut self) -> Result<()> {
        if self.current_tuple.is_deleted() {
            self.current_tuple.is_deleted = false;
            self.stats.restore_count += 1;
            self.stats.last_updated = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
        }
        Ok(())
    }

    /// Получает значение колонки
    pub fn get_value(&self, column: &str) -> Option<&ColumnValue> {
        self.current_tuple.get_value(column)
    }

    /// Устанавливает значение колонки
    pub fn set_value(&mut self, column: &str, value: ColumnValue) -> Result<()> {
        let mut new_values = HashMap::new();
        new_values.insert(column.to_string(), value);
        self.update(new_values)
    }

    /// Проверяет, удалена ли строка
    pub fn is_deleted(&self) -> bool {
        self.current_tuple.is_deleted()
    }

    /// Возвращает версию строки
    pub fn get_version(&self, version: u64) -> Option<&Tuple> {
        self.versions.get(&version)
    }

    /// Возвращает все версии строки
    pub fn get_all_versions(&self) -> &HashMap<u64, Tuple> {
        &self.versions
    }

    /// Возвращает количество версий
    pub fn version_count(&self) -> usize {
        self.versions.len()
    }

    /// Устанавливает связь со следующей строкой
    pub fn set_next_row(&mut self, next_id: u64) {
        self.next_row = Some(next_id);
    }

    /// Устанавливает связь с предыдущей строкой
    pub fn set_prev_row(&mut self, prev_id: u64) {
        self.prev_row = Some(prev_id);
    }

    /// Сериализует строку в байты
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(Error::BincodeSerialization)
    }

    /// Создает строку из байтов (десериализация)
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).map_err(Error::BincodeSerialization)
    }

    /// Возвращает размер строки в байтах
    pub fn size(&self) -> usize {
        self.to_bytes().unwrap_or_default().len()
    }
}

/// Статистика строки
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowStats {
    /// Количество обновлений
    pub update_count: u64,
    /// Количество удалений
    pub delete_count: u64,
    /// Количество восстановлений
    pub restore_count: u64,
    /// Время последнего обновления
    pub last_updated: u64,
    /// Время создания
    pub created_at: u64,
}

impl Default for RowStats {
    fn default() -> Self {
        Self::new()
    }
}

impl RowStats {
    /// Создает новую статистику
    pub fn new() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            update_count: 0,
            delete_count: 0,
            restore_count: 0,
            last_updated: now,
            created_at: now,
        }
    }
}

/// Метаданные таблицы
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    /// Имя таблицы
    pub name: String,
    /// Схема таблицы
    pub schema: Schema,
    /// Количество строк
    pub row_count: u64,
    /// Размер таблицы в байтах
    pub size_bytes: u64,
    /// Время создания
    pub created_at: u64,
    /// Время последнего изменения
    pub last_modified: u64,
    /// Статистика таблицы
    pub stats: TableStats,
    /// Настройки таблицы
    pub options: TableOptions,
}

impl TableMetadata {
    /// Создает новые метаданные таблицы
    pub fn new(name: String, schema: Schema) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            name,
            schema,
            row_count: 0,
            size_bytes: 0,
            created_at: now,
            last_modified: now,
            stats: TableStats::new(),
            options: TableOptions::default(),
        }
    }

    /// Обновляет количество строк
    pub fn update_row_count(&mut self, count: u64) {
        self.row_count = count;
        self.last_modified = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Обновляет размер таблицы
    pub fn update_size(&mut self, size: u64) {
        self.size_bytes = size;
        self.last_modified = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
}

/// Статистика таблицы
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStats {
    /// Количество операций INSERT
    pub insert_count: u64,
    /// Количество операций UPDATE
    pub update_count: u64,
    /// Количество операций DELETE
    pub delete_count: u64,
    /// Количество операций SELECT
    pub select_count: u64,
    /// Время последнего сброса статистики
    pub last_reset: u64,
}

impl Default for TableStats {
    fn default() -> Self {
        Self::new()
    }
}

impl TableStats {
    /// Создает новую статистику таблицы
    pub fn new() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            insert_count: 0,
            update_count: 0,
            delete_count: 0,
            select_count: 0,
            last_reset: now,
        }
    }

    /// Сбрасывает статистику
    pub fn reset(&mut self) {
        self.insert_count = 0;
        self.update_count = 0;
        self.delete_count = 0;
        self.select_count = 0;
        self.last_reset = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Регистрирует операцию INSERT
    pub fn record_insert(&mut self) {
        self.insert_count += 1;
    }

    /// Регистрирует операцию UPDATE
    pub fn record_update(&mut self) {
        self.update_count += 1;
    }

    /// Регистрирует операцию DELETE
    pub fn record_delete(&mut self) {
        self.delete_count += 1;
    }

    /// Регистрирует операцию SELECT
    pub fn record_select(&mut self) {
        self.select_count += 1;
    }
}

/// Опции таблицы
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TableOptions {
    /// Максимальное количество строк
    pub max_rows: Option<u64>,
    /// Минимальное количество строк
    pub min_rows: Option<u64>,
    /// Автоинкремент
    pub auto_increment: Option<u64>,
    /// Комментарий
    pub comment: Option<String>,
    /// Флаг временной таблицы
    pub is_temporary: bool,
    /// Флаг системной таблицы
    pub is_system: bool,
}

/// Таблица с данными
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    /// Метаданные таблицы
    pub metadata: TableMetadata,
    /// Строки таблицы
    pub rows: HashMap<u64, Row>,
    /// Связанные индексы
    pub indexes: HashMap<String, PageId>,
    /// Связанные страницы
    pub pages: Vec<PageId>,
}

impl Table {
    /// Создает новую таблицу
    pub fn new(name: String, schema: Schema) -> Self {
        Self {
            metadata: TableMetadata::new(name, schema),
            rows: HashMap::new(),
            indexes: HashMap::new(),
            pages: Vec::new(),
        }
    }

    /// Добавляет строку в таблицу
    pub fn insert_row(&mut self, row: Row) -> Result<()> {
        let row_id = row.id;

        // Проверяем соответствие схеме
        self.metadata.schema.validate_tuple(&row.current_tuple)?;

        // Добавляем строку
        self.rows.insert(row_id, row);

        // Обновляем метаданные
        self.metadata.update_row_count(self.rows.len() as u64);
        self.metadata.stats.record_insert();

        Ok(())
    }

    /// Обновляет строку в таблице
    pub fn update_row(
        &mut self,
        row_id: u64,
        new_values: HashMap<String, ColumnValue>,
    ) -> Result<()> {
        if let Some(row) = self.rows.get_mut(&row_id) {
            row.update(new_values)?;
            self.metadata.stats.record_update();
            Ok(())
        } else {
            Err(Error::validation("Строка не найдена"))
        }
    }

    /// Удаляет строку из таблицы
    pub fn delete_row(&mut self, row_id: u64) -> Result<()> {
        if let Some(row) = self.rows.get_mut(&row_id) {
            row.delete()?;
            self.metadata.stats.record_delete();
            Ok(())
        } else {
            Err(Error::validation("Строка не найдена"))
        }
    }

    /// Получает строку по ID
    pub fn get_row(&self, row_id: u64) -> Option<&Row> {
        self.rows.get(&row_id)
    }

    /// Получает изменяемую ссылку на строку
    pub fn get_row_mut(&mut self, row_id: u64) -> Option<&mut Row> {
        self.rows.get_mut(&row_id)
    }

    /// Проверяет, содержит ли таблица строку
    pub fn contains_row(&self, row_id: u64) -> bool {
        self.rows.contains_key(&row_id)
    }

    /// Возвращает количество строк
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Добавляет индекс
    pub fn add_index(&mut self, name: String, page_id: PageId) {
        self.indexes.insert(name, page_id);
    }

    /// Удаляет индекс
    pub fn remove_index(&mut self, name: &str) -> Option<PageId> {
        self.indexes.remove(name)
    }

    /// Добавляет страницу
    pub fn add_page(&mut self, page_id: PageId) {
        if !self.pages.contains(&page_id) {
            self.pages.push(page_id);
        }
    }

    /// Удаляет страницу
    pub fn remove_page(&mut self, page_id: PageId) -> bool {
        if let Some(pos) = self.pages.iter().position(|&id| id == page_id) {
            self.pages.remove(pos);
            true
        } else {
            false
        }
    }

    /// Очищает таблицу
    pub fn clear(&mut self) {
        self.rows.clear();
        self.metadata.update_row_count(0);
        self.metadata.stats.reset();
    }

    /// Сериализует таблицу в байты
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(Error::BincodeSerialization)
    }

    /// Создает таблицу из байтов (десериализация)
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).map_err(Error::BincodeSerialization)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::{ColumnValue, DataType};

    #[test]
    fn test_row_creation() {
        let tuple = Tuple::new(1);
        let row = Row::new(1, tuple);

        assert_eq!(row.id, 1);
        assert_eq!(row.version_count(), 1);
        assert!(!row.is_deleted());
    }

    #[test]
    fn test_row_update() {
        let tuple = Tuple::new(1);
        let mut row = Row::new(1, tuple);

        let mut new_values = HashMap::new();
        new_values.insert("age".to_string(), ColumnValue::new(DataType::Integer(25)));

        row.update(new_values).unwrap();
        assert_eq!(row.version_count(), 2);
        assert_eq!(
            row.get_value("age").unwrap().data_type,
            DataType::Integer(25)
        );
    }

    #[test]
    fn test_row_delete() {
        let tuple = Tuple::new(1);
        let mut row = Row::new(1, tuple);

        row.delete().unwrap();
        assert!(row.is_deleted());
    }

    #[test]
    fn test_table_creation() {
        let schema = Schema::new("users".to_string());
        let table = Table::new("users".to_string(), schema);

        assert_eq!(table.metadata.name, "users");
        assert_eq!(table.row_count(), 0);
    }

    #[test]
    fn test_table_insert() {
        let schema = Schema::new("users".to_string());
        let mut table = Table::new("users".to_string(), schema);

        let tuple = Tuple::new(1);
        let row = Row::new(1, tuple);

        table.insert_row(row).unwrap();
        assert_eq!(table.row_count(), 1);
        assert!(table.contains_row(1));
    }

    #[test]
    fn test_table_update() {
        let schema = Schema::new("users".to_string());
        let mut table = Table::new("users".to_string(), schema);

        let tuple = Tuple::new(1);
        let row = Row::new(1, tuple);
        table.insert_row(row).unwrap();

        let mut new_values = HashMap::new();
        new_values.insert(
            "name".to_string(),
            ColumnValue::new(DataType::Varchar("John".to_string())),
        );

        table.update_row(1, new_values).unwrap();
        let updated_row = table.get_row(1).unwrap();
        assert_eq!(
            updated_row.get_value("name").unwrap().data_type,
            DataType::Varchar("John".to_string())
        );
    }

    #[test]
    fn test_table_delete() {
        let schema = Schema::new("users".to_string());
        let mut table = Table::new("users".to_string(), schema);

        let tuple = Tuple::new(1);
        let row = Row::new(1, tuple);
        table.insert_row(row).unwrap();

        table.delete_row(1).unwrap();
        let deleted_row = table.get_row(1).unwrap();
        assert!(deleted_row.is_deleted());
    }
}
