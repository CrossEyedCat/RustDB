//! Базовые типы данных для RustBD

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Идентификатор страницы
pub type PageId = u64;

/// Идентификатор транзакции
pub type TransactionId = u64;

/// Идентификатор сессии
pub type SessionId = u64;

/// Идентификатор пользователя
pub type UserId = u32;

/// Размер страницы в байтах
pub const PAGE_SIZE: usize = 4096;

/// Размер заголовка страницы в байтах
pub const PAGE_HEADER_SIZE: usize = 64;

/// Максимальный размер записи в странице
pub const MAX_RECORD_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE;

/// Поддерживаемые типы данных
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    /// NULL значение
    Null,
    /// Булево значение
    Boolean(bool),
    /// 8-битное целое число
    TinyInt(i8),
    /// 16-битное целое число
    SmallInt(i16),
    /// 32-битное целое число
    Integer(i32),
    /// 64-битное целое число
    BigInt(i64),
    /// 32-битное число с плавающей точкой
    Float(f32),
    /// 64-битное число с плавающей точкой
    Double(f64),
    /// Строка фиксированной длины
    Char(String),
    /// Строка переменной длины
    Varchar(String),
    /// Текст
    Text(String),
    /// Дата
    Date(String),
    /// Время
    Time(String),
    /// Дата и время
    Timestamp(String),
    /// Двоичные данные
    Blob(Vec<u8>),
}

impl DataType {
    /// Возвращает размер типа данных в байтах
    pub fn size(&self) -> usize {
        match self {
            DataType::Null => 0,
            DataType::Boolean(_) => 1,
            DataType::TinyInt(_) => 1,
            DataType::SmallInt(_) => 2,
            DataType::Integer(_) => 4,
            DataType::BigInt(_) => 8,
            DataType::Float(_) => 4,
            DataType::Double(_) => 8,
            DataType::Char(s) => s.len(),
            DataType::Varchar(s) => s.len() + 4, // +4 для длины
            DataType::Text(s) => s.len() + 8,    // +8 для длины
            DataType::Date(_) => 10,
            DataType::Time(_) => 8,
            DataType::Timestamp(_) => 19,
            DataType::Blob(b) => b.len() + 8,    // +8 для длины
        }
    }
    
    /// Проверяет, является ли тип NULL
    pub fn is_null(&self) -> bool {
        matches!(self, DataType::Null)
    }
    
    /// Проверяет, является ли тип числовым
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataType::TinyInt(_) | DataType::SmallInt(_) | DataType::Integer(_) | 
            DataType::BigInt(_) | DataType::Float(_) | DataType::Double(_)
        )
    }
    
    /// Проверяет, является ли тип строковым
    pub fn is_string(&self) -> bool {
        matches!(
            self,
            DataType::Char(_) | DataType::Varchar(_) | DataType::Text(_)
        )
    }
}

/// Значение колонки
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnValue {
    /// Тип данных
    pub data_type: DataType,
    /// Флаг NULL
    pub is_null: bool,
}

impl ColumnValue {
    /// Создает новое значение колонки
    pub fn new(data_type: DataType) -> Self {
        let is_null = data_type.is_null();
        Self { data_type, is_null }
    }
    
    /// Создает NULL значение
    pub fn null() -> Self {
        Self {
            data_type: DataType::Null,
            is_null: true,
        }
    }
    
    /// Проверяет, является ли значение NULL
    pub fn is_null(&self) -> bool {
        self.is_null
    }
}

/// Определение колонки
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    /// Имя колонки
    pub name: String,
    /// Тип данных
    pub data_type: DataType,
    /// Флаг NOT NULL
    pub not_null: bool,
    /// Значение по умолчанию
    pub default_value: Option<ColumnValue>,
    /// Комментарий
    pub comment: Option<String>,
}

impl Column {
    /// Создает новую колонку
    pub fn new(name: String, data_type: DataType) -> Self {
        Self {
            name,
            data_type,
            not_null: false,
            default_value: None,
            comment: None,
        }
    }
    
    /// Устанавливает флаг NOT NULL
    pub fn not_null(mut self) -> Self {
        self.not_null = true;
        self
    }
    
    /// Устанавливает значение по умолчанию
    pub fn default_value(mut self, value: ColumnValue) -> Self {
        self.default_value = Some(value);
        self
    }
    
    /// Устанавливает комментарий
    pub fn comment(mut self, comment: String) -> Self {
        self.comment = Some(comment);
        self
    }
}

/// Схема таблицы
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Имя таблицы
    pub table_name: String,
    /// Колонки таблицы
    pub columns: Vec<Column>,
    /// Первичный ключ
    pub primary_key: Option<Vec<String>>,
    /// Уникальные ограничения
    pub unique_constraints: Vec<Vec<String>>,
    /// Внешние ключи
    pub foreign_keys: Vec<ForeignKey>,
    /// Индексы
    pub indexes: Vec<Index>,
}

impl Schema {
    /// Создает новую схему таблицы
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            columns: Vec::new(),
            primary_key: None,
            unique_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
        }
    }
    
    /// Добавляет колонку в схему
    pub fn add_column(mut self, column: Column) -> Self {
        self.columns.push(column);
        self
    }
    
    /// Устанавливает первичный ключ
    pub fn primary_key(mut self, columns: Vec<String>) -> Self {
        self.primary_key = Some(columns);
        self
    }
    
    /// Добавляет уникальное ограничение
    pub fn unique(mut self, columns: Vec<String>) -> Self {
        self.unique_constraints.push(columns);
        self
    }
    
    /// Добавляет внешний ключ
    pub fn foreign_key(mut self, fk: ForeignKey) -> Self {
        self.foreign_keys.push(fk);
        self
    }
    
    /// Добавляет индекс
    pub fn index(mut self, index: Index) -> Self {
        self.indexes.push(index);
        self
    }
}

/// Внешний ключ
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKey {
    /// Имя ограничения
    pub name: String,
    /// Колонки в текущей таблице
    pub columns: Vec<String>,
    /// Ссылающаяся таблица
    pub referenced_table: String,
    /// Ссылающиеся колонки
    pub referenced_columns: Vec<String>,
    /// Действие при удалении
    pub on_delete: Option<ForeignKeyAction>,
    /// Действие при обновлении
    pub on_update: Option<ForeignKeyAction>,
}

/// Действие внешнего ключа
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ForeignKeyAction {
    /// Каскадное удаление/обновление
    Cascade,
    /// Установка NULL
    SetNull,
    /// Установка значения по умолчанию
    SetDefault,
    /// Ограничение
    Restrict,
    /// Ничего не делать
    NoAction,
}

/// Индекс
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Index {
    /// Имя индекса
    pub name: String,
    /// Колонки индекса
    pub columns: Vec<String>,
    /// Тип индекса
    pub index_type: IndexType,
    /// Уникальность индекса
    pub unique: bool,
}

/// Тип индекса
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexType {
    /// B+ дерево
    BTree,
    /// Хеш-индекс
    Hash,
    /// Полнотекстовый индекс
    FullText,
}

/// Запись в таблице
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    /// Значения колонок
    pub values: HashMap<String, ColumnValue>,
    /// Версия записи (для MVCC)
    pub version: u64,
    /// Время создания
    pub created_at: u64,
    /// Время последнего обновления
    pub updated_at: u64,
}

impl Row {
    /// Создает новую запись
    pub fn new() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        Self {
            values: HashMap::new(),
            version: 1,
            created_at: now,
            updated_at: now,
        }
    }
    
    /// Устанавливает значение колонки
    pub fn set_value(&mut self, column: &str, value: ColumnValue) {
        self.values.insert(column.to_string(), value);
        self.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
    
    /// Получает значение колонки
    pub fn get_value(&self, column: &str) -> Option<&ColumnValue> {
        self.values.get(column)
    }
    
    /// Проверяет, содержит ли запись колонку
    pub fn has_column(&self, column: &str) -> bool {
        self.values.contains_key(column)
    }
}
