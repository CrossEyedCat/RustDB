//! Структуры данных для таблиц RustBD

use crate::common::{Error, Result, types::{DataType, ColumnValue, Column, Schema as BaseSchema}};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Кортеж (строка) таблицы
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tuple {
    /// ID кортежа
    pub id: u64,
    /// Значения колонок
    pub values: HashMap<String, ColumnValue>,
    /// Версия кортежа (для MVCC)
    pub version: u64,
    /// Время создания
    pub created_at: u64,
    /// Время последнего обновления
    pub updated_at: u64,
    /// Флаг удаления
    pub is_deleted: bool,
    /// Указатель на следующую версию
    pub next_version: Option<u64>,
    /// Указатель на предыдущую версию
    pub prev_version: Option<u64>,
}

impl Tuple {
    /// Создает новый кортеж
    pub fn new(id: u64) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            id,
            values: HashMap::new(),
            version: 1,
            created_at: now,
            updated_at: now,
            is_deleted: false,
            next_version: None,
            prev_version: None,
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

    /// Проверяет, содержит ли кортеж колонку
    pub fn has_column(&self, column: &str) -> bool {
        self.values.contains_key(column)
    }

    /// Помечает кортеж как удаленный
    pub fn mark_deleted(&mut self) {
        self.is_deleted = true;
        self.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Проверяет, удален ли кортеж
    pub fn is_deleted(&self) -> bool {
        self.is_deleted
    }

    /// Создает новую версию кортежа
    pub fn create_new_version(&mut self) -> Tuple {
        let mut new_tuple = self.clone();
        new_tuple.version += 1;
        new_tuple.created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        new_tuple.updated_at = new_tuple.created_at;
        new_tuple.prev_version = Some(self.version);
        new_tuple.next_version = None;
        
        // Обновляем указатель на следующую версию
        self.next_version = Some(new_tuple.version);
        
        new_tuple
    }

    /// Сериализует кортеж в байты
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self)
            .map_err(|e| Error::BincodeSerialization(e))
    }

    /// Создает кортеж из байтов (десериализация)
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes)
            .map_err(|e| Error::BincodeSerialization(e))
    }

    /// Возвращает размер кортежа в байтах
    pub fn size(&self) -> usize {
        self.to_bytes().unwrap_or_default().len()
    }

    /// Проверяет, соответствует ли кортеж схеме
    pub fn validate_against_schema(&self, schema: &Schema) -> Result<()> {
        for column in &schema.base.columns {
            if column.not_null {
                if let Some(value) = self.values.get(&column.name) {
                    if value.is_null() {
                        return Err(Error::validation(
                            format!("Колонка {} не может быть NULL", column.name)
                        ));
                    }
                } else {
                    return Err(Error::validation(
                        format!("Отсутствует обязательная колонка {}", column.name)
                    ));
                }
            }
        }
        Ok(())
    }

    /// Проверяет, соответствует ли кортеж базовой схеме
    pub fn validate_against_base_schema(&self, schema: &BaseSchema) -> Result<()> {
        for column in &schema.columns {
            if column.not_null {
                if let Some(value) = self.values.get(&column.name) {
                    if value.is_null() {
                        return Err(Error::validation(
                            format!("Колонка {} не может быть NULL", column.name)
                        ));
                    }
                } else {
                    return Err(Error::validation(
                        format!("Отсутствует обязательная колонка {}", column.name)
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Расширенная схема таблицы с дополнительными возможностями
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Базовая схема
    pub base: BaseSchema,
    /// Дополнительные ограничения
    pub constraints: Vec<Constraint>,
    /// Триггеры
    pub triggers: Vec<Trigger>,
    /// Настройки таблицы
    pub table_options: TableOptions,
}

impl Schema {
    /// Создает новую схему
    pub fn new(table_name: String) -> Self {
        Self {
            base: BaseSchema::new(table_name),
            constraints: Vec::new(),
            triggers: Vec::new(),
            table_options: TableOptions::default(),
        }
    }

    /// Добавляет колонку в схему
    pub fn add_column(mut self, column: Column) -> Self {
        self.base = self.base.add_column(column);
        self
    }

    /// Устанавливает первичный ключ
    pub fn primary_key(mut self, columns: Vec<String>) -> Self {
        self.base = self.base.primary_key(columns);
        self
    }

    /// Добавляет уникальное ограничение
    pub fn unique(mut self, columns: Vec<String>) -> Self {
        self.base = self.base.unique(columns);
        self
    }

    /// Добавляет внешний ключ
    pub fn foreign_key(mut self, fk: crate::common::types::ForeignKey) -> Self {
        self.base = self.base.foreign_key(fk);
        self
    }

    /// Добавляет индекс
    pub fn index(mut self, index: crate::common::types::Index) -> Self {
        self.base = self.base.index(index);
        self
    }

    /// Добавляет ограничение
    pub fn add_constraint(mut self, constraint: Constraint) -> Self {
        self.constraints.push(constraint);
        self
    }

    /// Добавляет триггер
    pub fn add_trigger(mut self, trigger: Trigger) -> Self {
        self.triggers.push(trigger);
        self
    }

    /// Устанавливает опции таблицы
    pub fn with_options(mut self, options: TableOptions) -> Self {
        self.table_options = options;
        self
    }

    /// Проверяет, соответствует ли кортеж схеме
    pub fn validate_tuple(&self, tuple: &Tuple) -> Result<()> {
        // Проверяем базовую схему
        tuple.validate_against_base_schema(&self.base)?;
        
        // Проверяем дополнительные ограничения
        for constraint in &self.constraints {
            constraint.validate(tuple)?;
        }
        
        Ok(())
    }

    /// Возвращает все колонки схемы
    pub fn get_columns(&self) -> &[Column] {
        &self.base.columns
    }

    /// Возвращает колонку по имени
    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.base.columns.iter().find(|c| c.name == name)
    }

    /// Проверяет, содержит ли схема колонку
    pub fn has_column(&self, name: &str) -> bool {
        self.base.columns.iter().any(|c| c.name == name)
    }
}

/// Ограничение таблицы
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Constraint {
    /// Имя ограничения
    pub name: String,
    /// Тип ограничения
    pub constraint_type: ConstraintType,
    /// Выражение ограничения
    pub expression: String,
    /// Колонки, к которым применяется ограничение
    pub columns: Vec<String>,
}

impl Constraint {
    /// Создает новое ограничение
    pub fn new(name: String, constraint_type: ConstraintType, expression: String, columns: Vec<String>) -> Self {
        Self {
            name,
            constraint_type,
            expression,
            columns,
        }
    }

    /// Проверяет ограничение для кортежа
    pub fn validate(&self, _tuple: &Tuple) -> Result<()> {
        // TODO: Реализовать проверку ограничений
        Ok(())
    }
}

/// Тип ограничения
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ConstraintType {
    /// Проверочное ограничение
    Check,
    /// Ограничение по умолчанию
    Default,
    /// Ограничение NOT NULL
    NotNull,
    /// Пользовательское ограничение
    Custom,
}

/// Триггер таблицы
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trigger {
    /// Имя триггера
    pub name: String,
    /// Событие триггера
    pub event: TriggerEvent,
    /// Время выполнения триггера
    pub timing: TriggerTiming,
    /// SQL код триггера
    pub sql_code: String,
    /// Условие выполнения триггера
    pub condition: Option<String>,
}

impl Trigger {
    /// Создает новый триггер
    pub fn new(name: String, event: TriggerEvent, timing: TriggerTiming, sql_code: String) -> Self {
        Self {
            name,
            event,
            timing,
            sql_code,
            condition: None,
        }
    }

    /// Устанавливает условие выполнения
    pub fn with_condition(mut self, condition: String) -> Self {
        self.condition = Some(condition);
        self
    }
}

/// Событие триггера
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TriggerEvent {
    /// Вставка
    Insert,
    /// Обновление
    Update,
    /// Удаление
    Delete,
}

/// Время выполнения триггера
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TriggerTiming {
    /// До выполнения операции
    Before,
    /// После выполнения операции
    After,
    /// Вместо выполнения операции
    InsteadOf,
}

/// Опции таблицы
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableOptions {
    /// Движок таблицы
    pub engine: String,
    /// Кодировка
    pub charset: String,
    /// Коллация
    pub collation: String,
    /// Комментарий к таблице
    pub comment: Option<String>,
    /// Автоинкремент
    pub auto_increment: Option<u64>,
    /// Максимальное количество строк
    pub max_rows: Option<u64>,
    /// Минимальное количество строк
    pub min_rows: Option<u64>,
}

impl Default for TableOptions {
    fn default() -> Self {
        Self {
            engine: "InnoDB".to_string(),
            charset: "utf8mb4".to_string(),
            collation: "utf8mb4_unicode_ci".to_string(),
            comment: None,
            auto_increment: None,
            max_rows: None,
            min_rows: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::{DataType, ColumnValue};

    #[test]
    fn test_tuple_creation() {
        let mut tuple = Tuple::new(1);
        assert_eq!(tuple.id, 1);
        assert_eq!(tuple.version, 1);
        assert!(!tuple.is_deleted);
        assert_eq!(tuple.values.len(), 0);
    }

    #[test]
    fn test_tuple_values() {
        let mut tuple = Tuple::new(1);
        let value = ColumnValue::new(DataType::Integer(42));
        
        tuple.set_value("age", value);
        assert!(tuple.has_column("age"));
        assert_eq!(tuple.get_value("age").unwrap().data_type, DataType::Integer(42));
    }

    #[test]
    fn test_tuple_versioning() {
        let mut tuple = Tuple::new(1);
        let new_tuple = tuple.create_new_version();
        
        assert_eq!(new_tuple.version, 2);
        assert_eq!(new_tuple.prev_version, Some(1));
        assert_eq!(tuple.next_version, Some(2));
    }

    #[test]
    fn test_schema_creation() {
        let schema = Schema::new("users".to_string());
        assert_eq!(schema.base.table_name, "users");
        assert_eq!(schema.constraints.len(), 0);
        assert_eq!(schema.triggers.len(), 0);
    }

    #[test]
    fn test_schema_validation() {
        let mut schema = Schema::new("users".to_string());
        schema = schema.add_column(Column::new("id".to_string(), DataType::Integer(0)).not_null());
        
        let mut tuple = Tuple::new(1);
        tuple.set_value("id", ColumnValue::new(DataType::Integer(42)));
        
        assert!(schema.validate_tuple(&tuple).is_ok());
    }

    #[test]
    fn test_constraint_creation() {
        let constraint = Constraint::new(
            "age_check".to_string(),
            ConstraintType::Check,
            "age >= 0".to_string(),
            vec!["age".to_string()],
        );
        
        assert_eq!(constraint.name, "age_check");
        assert_eq!(constraint.constraint_type, ConstraintType::Check);
    }

    #[test]
    fn test_trigger_creation() {
        let trigger = Trigger::new(
            "before_insert".to_string(),
            TriggerEvent::Insert,
            TriggerTiming::Before,
            "SET created_at = NOW()".to_string(),
        );
        
        assert_eq!(trigger.name, "before_insert");
        assert_eq!(trigger.event, TriggerEvent::Insert);
        assert_eq!(trigger.timing, TriggerTiming::Before);
    }
}
