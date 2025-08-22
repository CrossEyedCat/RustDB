//! Управление схемой таблиц для RustBD

use crate::common::{Error, Result, types::{Column, DataType, ColumnValue, Schema as BaseSchema}};
use crate::storage::tuple::{Schema, Constraint, Trigger, TableOptions};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Операция изменения схемы
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchemaOperation {
    /// Добавление колонки
    AddColumn {
        column: Column,
        after: Option<String>, // После какой колонки добавить
    },
    /// Удаление колонки
    DropColumn {
        column_name: String,
        cascade: bool, // Каскадное удаление
    },
    /// Изменение колонки
    ModifyColumn {
        column_name: String,
        new_column: Column,
    },
    /// Переименование колонки
    RenameColumn {
        old_name: String,
        new_name: String,
    },
    /// Добавление ограничения
    AddConstraint {
        constraint: Constraint,
    },
    /// Удаление ограничения
    DropConstraint {
        constraint_name: String,
    },
    /// Добавление индекса
    AddIndex {
        index_name: String,
        columns: Vec<String>,
        unique: bool,
    },
    /// Удаление индекса
    DropIndex {
        index_name: String,
    },
    /// Изменение первичного ключа
    ModifyPrimaryKey {
        new_columns: Vec<String>,
    },
    /// Изменение опций таблицы
    ModifyTableOptions {
        options: TableOptions,
    },
}

/// Менеджер схем таблиц
pub struct SchemaManager {
    /// Схемы таблиц
    schemas: HashMap<String, Schema>,
    /// История изменений схем
    change_history: Vec<SchemaChange>,
    /// Валидаторы схем
    validators: Vec<Box<dyn SchemaValidator>>,
}

impl SchemaManager {
    /// Создает новый менеджер схем
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
            change_history: Vec::new(),
            validators: Vec::new(),
        }
    }

    /// Регистрирует валидатор схемы
    pub fn register_validator(&mut self, validator: Box<dyn SchemaValidator>) {
        self.validators.push(validator);
    }

    /// Создает новую схему таблицы
    pub fn create_schema(&mut self, table_name: String, schema: Schema) -> Result<()> {
        // Валидируем схему
        self.validate_schema(&schema)?;
        
        // Проверяем, что таблица не существует
        if self.schemas.contains_key(&table_name) {
            return Err(Error::validation(format!("Таблица {} уже существует", table_name)));
        }
        
        // Добавляем схему
        self.schemas.insert(table_name.clone(), schema);
        
        // Записываем изменение
        let change = SchemaChange::new(
            table_name,
            SchemaOperationType::Create,
            "Создание таблицы".to_string(),
        );
        self.change_history.push(change);
        
        Ok(())
    }

    /// Получает схему таблицы
    pub fn get_schema(&self, table_name: &str) -> Option<&Schema> {
        self.schemas.get(table_name)
    }

    /// Получает изменяемую ссылку на схему таблицы
    pub fn get_schema_mut(&mut self, table_name: &str) -> Option<&mut Schema> {
        self.schemas.get_mut(table_name)
    }

    /// Выполняет ALTER TABLE операцию
    pub fn alter_table(&mut self, table_name: &str, operation: SchemaOperation) -> Result<()> {
        // Сначала валидируем операцию
        {
            let schema = self.get_schema(table_name)
                .ok_or_else(|| Error::validation(format!("Таблица {} не найдена", table_name)))?;
            self.validate_operation(schema, &operation)?;
        }
        
        // Теперь выполняем операцию
        {
            let schema = self.get_schema_mut(table_name)
                .ok_or_else(|| Error::validation(format!("Таблица {} не найдена", table_name)))?;
            Self::execute_operation_static(schema, &operation)?;
        }
        
        // Записываем изменение
        let change = SchemaChange::new(
            table_name.to_string(),
            SchemaOperationType::Alter,
            format!("{:?}", operation),
        );
        self.change_history.push(change);
        
        Ok(())
    }

    /// Валидирует схему
    fn validate_schema(&self, schema: &Schema) -> Result<()> {
        for validator in &self.validators {
            validator.validate_schema(schema)?;
        }
        Ok(())
    }

    /// Валидирует операцию изменения схемы
    fn validate_operation(&self, schema: &Schema, operation: &SchemaOperation) -> Result<()> {
        match operation {
            SchemaOperation::AddColumn { column, .. } => {
                self.validate_add_column(schema, column)?;
            }
            SchemaOperation::DropColumn { column_name, .. } => {
                self.validate_drop_column(schema, column_name)?;
            }
            SchemaOperation::ModifyColumn { column_name, new_column } => {
                self.validate_modify_column(schema, column_name, new_column)?;
            }
            SchemaOperation::RenameColumn { old_name, new_name } => {
                self.validate_rename_column(schema, old_name, new_name)?;
            }
            SchemaOperation::AddConstraint { constraint } => {
                self.validate_add_constraint(schema, constraint)?;
            }
            SchemaOperation::DropConstraint { constraint_name } => {
                self.validate_drop_constraint(schema, constraint_name)?;
            }
            SchemaOperation::AddIndex { index_name, columns, .. } => {
                self.validate_add_index(schema, index_name, columns)?;
            }
            SchemaOperation::DropIndex { index_name } => {
                self.validate_drop_index(schema, index_name)?;
            }
            SchemaOperation::ModifyPrimaryKey { new_columns } => {
                self.validate_modify_primary_key(schema, new_columns)?;
            }
            SchemaOperation::ModifyTableOptions { .. } => {
                // Опции таблицы не требуют специальной валидации
            }
        }
        Ok(())
    }

    /// Валидирует добавление колонки
    fn validate_add_column(&self, schema: &Schema, column: &Column) -> Result<()> {
        // Проверяем, что колонка с таким именем не существует
        if schema.has_column(&column.name) {
            return Err(Error::validation(format!("Колонка {} уже существует", column.name)));
        }
        
        // Проверяем ограничения колонки
        if column.not_null && column.default_value.is_none() {
            return Err(Error::validation(
                format!("Колонка {} с NOT NULL должна иметь значение по умолчанию", column.name)
            ));
        }
        
        Ok(())
    }

    /// Валидирует удаление колонки
    fn validate_drop_column(&self, schema: &Schema, column_name: &str) -> Result<()> {
        // Проверяем, что колонка существует
        if !schema.has_column(column_name) {
            return Err(Error::validation(format!("Колонка {} не найдена", column_name)));
        }
        
        // Проверяем, что колонка не является частью первичного ключа
        if let Some(pk) = &schema.base.primary_key {
            if pk.contains(&column_name.to_string()) {
                return Err(Error::validation(
                    format!("Нельзя удалить колонку {}, которая является частью первичного ключа", column_name)
                ));
            }
        }
        
        // Проверяем, что колонка не используется в индексах
        for index in &schema.base.indexes {
            if index.columns.contains(&column_name.to_string()) {
                return Err(Error::validation(
                    format!("Нельзя удалить колонку {}, которая используется в индексе {}", column_name, index.name)
                ));
            }
        }
        
        Ok(())
    }

    /// Валидирует изменение колонки
    fn validate_modify_column(&self, schema: &Schema, column_name: &str, new_column: &Column) -> Result<()> {
        // Проверяем, что колонка существует
        if !schema.has_column(column_name) {
            return Err(Error::validation(format!("Колонка {} не найдена", column_name)));
        }
        
        // Проверяем совместимость типов данных
        let old_column = schema.get_column(column_name).unwrap();
        if !self.is_type_compatible(&old_column.data_type, &new_column.data_type) {
            return Err(Error::validation(
                format!("Тип данных {} несовместим с {}", 
                    format!("{:?}", old_column.data_type), 
                    format!("{:?}", new_column.data_type))
            ));
        }
        
        Ok(())
    }

    /// Валидирует переименование колонки
    fn validate_rename_column(&self, schema: &Schema, old_name: &str, new_name: &str) -> Result<()> {
        // Проверяем, что старая колонка существует
        if !schema.has_column(old_name) {
            return Err(Error::validation(format!("Колонка {} не найдена", old_name)));
        }
        
        // Проверяем, что новое имя не занято
        if schema.has_column(new_name) {
            return Err(Error::validation(format!("Колонка {} уже существует", new_name)));
        }
        
        Ok(())
    }

    /// Валидирует добавление ограничения
    fn validate_add_constraint(&self, schema: &Schema, constraint: &Constraint) -> Result<()> {
        // Проверяем, что все колонки существуют
        for column_name in &constraint.columns {
            if !schema.has_column(column_name) {
                return Err(Error::validation(
                    format!("Колонка {} не найдена для ограничения {}", column_name, constraint.name)
                ));
            }
        }
        
        Ok(())
    }

    /// Валидирует удаление ограничения
    fn validate_drop_constraint(&self, schema: &Schema, constraint_name: &str) -> Result<()> {
        // Проверяем, что ограничение существует
        let exists = schema.constraints.iter().any(|c| c.name == *constraint_name);
        if !exists {
            return Err(Error::validation(format!("Ограничение {} не найдено", constraint_name)));
        }
        
        Ok(())
    }

    /// Валидирует добавление индекса
    fn validate_add_index(&self, schema: &Schema, index_name: &str, columns: &[String]) -> Result<()> {
        // Проверяем, что индекс с таким именем не существует
        if schema.base.indexes.iter().any(|i| i.name == *index_name) {
            return Err(Error::validation(format!("Индекс {} уже существует", index_name)));
        }
        
        // Проверяем, что все колонки существуют
        for column_name in columns {
            if !schema.has_column(column_name) {
                return Err(Error::validation(
                    format!("Колонка {} не найдена для индекса {}", column_name, index_name)
                ));
            }
        }
        
        Ok(())
    }

    /// Валидирует удаление индекса
    fn validate_drop_index(&self, schema: &Schema, index_name: &str) -> Result<()> {
        // Проверяем, что индекс существует
        let exists = schema.base.indexes.iter().any(|i| i.name == *index_name);
        if !exists {
            return Err(Error::validation(format!("Индекс {} не найден", index_name)));
        }
        
        Ok(())
    }

    /// Валидирует изменение первичного ключа
    fn validate_modify_primary_key(&self, schema: &Schema, new_columns: &[String]) -> Result<()> {
        // Проверяем, что все колонки существуют
        for column_name in new_columns {
            if !schema.has_column(column_name) {
                return Err(Error::validation(
                    format!("Колонка {} не найдена для первичного ключа", column_name)
                ));
            }
        }
        
        // Проверяем, что колонки не NULL
        for column_name in new_columns {
            if let Some(column) = schema.get_column(column_name) {
                if !column.not_null {
                    return Err(Error::validation(
                        format!("Колонка {} первичного ключа должна быть NOT NULL", column_name)
                    ));
                }
            }
        }
        
        Ok(())
    }

    /// Проверяет совместимость типов данных
    fn is_type_compatible(&self, old_type: &DataType, new_type: &DataType) -> bool {
        match (old_type, new_type) {
            // Целочисленные типы
            (DataType::TinyInt(_), DataType::SmallInt(_)) |
            (DataType::TinyInt(_), DataType::Integer(_)) |
            (DataType::TinyInt(_), DataType::BigInt(_)) |
            (DataType::SmallInt(_), DataType::Integer(_)) |
            (DataType::SmallInt(_), DataType::BigInt(_)) |
            (DataType::Integer(_), DataType::BigInt(_)) => true,
            
            // Числа с плавающей точкой
            (DataType::Float(_), DataType::Double(_)) => true,
            
            // Строковые типы
            (DataType::Char(_), DataType::Varchar(_)) |
            (DataType::Char(_), DataType::Text(_)) |
            (DataType::Varchar(_), DataType::Text(_)) => true,
            
            // Одинаковые типы
            _ if std::mem::discriminant(old_type) == std::mem::discriminant(new_type) => true,
            
            // По умолчанию несовместимы
            _ => false,
        }
    }

    /// Выполняет операцию изменения схемы
    fn execute_operation(&self, schema: &mut Schema, operation: &SchemaOperation) -> Result<()> {
        Self::execute_operation_static(schema, operation)
    }

    /// Статическая версия выполнения операции изменения схемы
    fn execute_operation_static(schema: &mut Schema, operation: &SchemaOperation) -> Result<()> {
        match operation {
            SchemaOperation::AddColumn { column, after } => {
                Self::execute_add_column_static(schema, column, after)?;
            }
            SchemaOperation::DropColumn { column_name, .. } => {
                Self::execute_drop_column_static(schema, column_name)?;
            }
            SchemaOperation::ModifyColumn { column_name, new_column } => {
                Self::execute_modify_column_static(schema, column_name, new_column)?;
            }
            SchemaOperation::RenameColumn { old_name, new_name } => {
                Self::execute_rename_column_static(schema, old_name, new_name)?;
            }
            SchemaOperation::AddConstraint { constraint } => {
                Self::execute_add_constraint_static(schema, constraint)?;
            }
            SchemaOperation::DropConstraint { constraint_name } => {
                Self::execute_drop_constraint_static(schema, constraint_name)?;
            }
            SchemaOperation::AddIndex { index_name, columns, unique } => {
                Self::execute_add_index_static(schema, index_name, columns, *unique)?;
            }
            SchemaOperation::DropIndex { index_name } => {
                Self::execute_drop_index_static(schema, index_name)?;
            }
            SchemaOperation::ModifyPrimaryKey { new_columns } => {
                Self::execute_modify_primary_key_static(schema, new_columns)?;
            }
            SchemaOperation::ModifyTableOptions { options } => {
                Self::execute_modify_table_options_static(schema, options)?;
            }
        }
        Ok(())
    }

    /// Выполняет добавление колонки
    fn execute_add_column(&self, schema: &mut Schema, column: &Column, after: &Option<String>) -> Result<()> {
        Self::execute_add_column_static(schema, column, after)
    }

    /// Статическая версия добавления колонки
    fn execute_add_column_static(schema: &mut Schema, column: &Column, _after: &Option<String>) -> Result<()> {
        // TODO: Реализовать логику добавления колонки в определенную позицию
        schema.base = schema.base.clone().add_column(column.clone());
        Ok(())
    }

    /// Выполняет удаление колонки
    fn execute_drop_column(&self, schema: &mut Schema, column_name: &str) -> Result<()> {
        Self::execute_drop_column_static(schema, column_name)
    }

    /// Статическая версия удаления колонки
    fn execute_drop_column_static(_schema: &mut Schema, _column_name: &str) -> Result<()> {
        // TODO: Реализовать логику удаления колонки
        // Это сложная операция, требующая перестройки данных
        Ok(())
    }

    /// Выполняет изменение колонки
    fn execute_modify_column(&self, schema: &mut Schema, column_name: &str, new_column: &Column) -> Result<()> {
        Self::execute_modify_column_static(schema, column_name, new_column)
    }

    /// Статическая версия изменения колонки
    fn execute_modify_column_static(_schema: &mut Schema, _column_name: &str, _new_column: &Column) -> Result<()> {
        // TODO: Реализовать логику изменения колонки
        // Это сложная операция, требующая перестройки данных
        Ok(())
    }

    /// Выполняет переименование колонки
    fn execute_rename_column(&self, schema: &mut Schema, old_name: &str, new_name: &str) -> Result<()> {
        Self::execute_rename_column_static(schema, old_name, new_name)
    }

    /// Статическая версия переименования колонки
    fn execute_rename_column_static(_schema: &mut Schema, _old_name: &str, _new_name: &str) -> Result<()> {
        // TODO: Реализовать логику переименования колонки
        Ok(())
    }

    /// Выполняет добавление ограничения
    fn execute_add_constraint(&self, schema: &mut Schema, constraint: &Constraint) -> Result<()> {
        Self::execute_add_constraint_static(schema, constraint)
    }

    /// Статическая версия добавления ограничения
    fn execute_add_constraint_static(schema: &mut Schema, constraint: &Constraint) -> Result<()> {
        schema.constraints.push(constraint.clone());
        Ok(())
    }

    /// Выполняет удаление ограничения
    fn execute_drop_constraint(&self, schema: &mut Schema, constraint_name: &str) -> Result<()> {
        Self::execute_drop_constraint_static(schema, constraint_name)
    }

    /// Статическая версия удаления ограничения
    fn execute_drop_constraint_static(schema: &mut Schema, constraint_name: &str) -> Result<()> {
        schema.constraints.retain(|c| c.name != *constraint_name);
        Ok(())
    }

    /// Выполняет добавление индекса
    fn execute_add_index(&self, schema: &mut Schema, index_name: &str, columns: &[String], unique: bool) -> Result<()> {
        Self::execute_add_index_static(schema, index_name, columns, unique)
    }

    /// Статическая версия добавления индекса
    fn execute_add_index_static(schema: &mut Schema, index_name: &str, columns: &[String], unique: bool) -> Result<()> {
        use crate::common::types::{Index, IndexType};
        
        let index = Index {
            name: index_name.to_string(),
            columns: columns.to_vec(),
            index_type: IndexType::BTree,
            unique,
        };
        
        schema.base = schema.base.clone().index(index);
        Ok(())
    }

    /// Выполняет удаление индекса
    fn execute_drop_index(&self, schema: &mut Schema, index_name: &str) -> Result<()> {
        Self::execute_drop_index_static(schema, index_name)
    }

    /// Статическая версия удаления индекса
    fn execute_drop_index_static(schema: &mut Schema, index_name: &str) -> Result<()> {
        schema.base.indexes.retain(|i| i.name != *index_name);
        Ok(())
    }

    /// Выполняет изменение первичного ключа
    fn execute_modify_primary_key(&self, schema: &mut Schema, new_columns: &[String]) -> Result<()> {
        Self::execute_modify_primary_key_static(schema, new_columns)
    }

    /// Статическая версия изменения первичного ключа
    fn execute_modify_primary_key_static(schema: &mut Schema, new_columns: &[String]) -> Result<()> {
        schema.base = schema.base.clone().primary_key(new_columns.to_vec());
        Ok(())
    }

    /// Выполняет изменение опций таблицы
    fn execute_modify_table_options(&self, schema: &mut Schema, options: &TableOptions) -> Result<()> {
        Self::execute_modify_table_options_static(schema, options)
    }

    /// Статическая версия изменения опций таблицы
    fn execute_modify_table_options_static(schema: &mut Schema, options: &TableOptions) -> Result<()> {
        schema.table_options = options.clone();
        Ok(())
    }

    /// Возвращает историю изменений схем
    pub fn get_change_history(&self) -> &[SchemaChange] {
        &self.change_history
    }

    /// Откатывает последнее изменение схемы
    pub fn rollback_last_change(&mut self) -> Result<()> {
        if let Some(change) = self.change_history.pop() {
            // TODO: Реализовать откат изменений
            log::info!("Откат изменения: {:?}", change);
        }
        Ok(())
    }
}

/// Тип операции изменения схемы
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchemaOperationType {
    /// Создание таблицы
    Create,
    /// Изменение таблицы
    Alter,
    /// Удаление таблицы
    Drop,
}

/// Запись об изменении схемы
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaChange {
    /// Имя таблицы
    pub table_name: String,
    /// Тип операции
    pub operation_type: SchemaOperationType,
    /// Описание изменения
    pub description: String,
    /// Время изменения
    pub timestamp: u64,
}

impl SchemaChange {
    /// Создает новую запись об изменении
    pub fn new(table_name: String, operation_type: SchemaOperationType, description: String) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            table_name,
            operation_type,
            description,
            timestamp,
        }
    }
}

/// Трейт для валидации схем
pub trait SchemaValidator: Send + Sync {
    /// Валидирует схему
    fn validate_schema(&self, schema: &Schema) -> Result<()>;
}

/// Валидатор базовых ограничений схемы
pub struct BasicSchemaValidator;

impl SchemaValidator for BasicSchemaValidator {
    fn validate_schema(&self, schema: &Schema) -> Result<()> {
        // Проверяем, что у таблицы есть колонки
        if schema.get_columns().is_empty() {
            return Err(Error::validation("Таблица должна содержать хотя бы одну колонку"));
        }
        
        // Проверяем, что первичный ключ ссылается на существующие колонки
        if let Some(pk) = &schema.base.primary_key {
            for column_name in pk {
                if !schema.has_column(column_name) {
                    return Err(Error::validation(
                        format!("Колонка первичного ключа {} не найдена", column_name)
                    ));
                }
            }
        }
        
        // Проверяем, что индексы ссылаются на существующие колонки
        for index in &schema.base.indexes {
            for column_name in &index.columns {
                if !schema.has_column(column_name) {
                    return Err(Error::validation(
                        format!("Колонка индекса {} не найдена", column_name)
                    ));
                }
            }
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::{DataType, ColumnValue};

    #[test]
    fn test_schema_manager_creation() {
        let manager = SchemaManager::new();
        assert_eq!(manager.schemas.len(), 0);
        assert_eq!(manager.change_history.len(), 0);
    }

    #[test]
    fn test_create_schema() {
        let mut manager = SchemaManager::new();
        let schema = Schema::new("users".to_string());
        
        manager.create_schema("users".to_string(), schema).unwrap();
        assert!(manager.get_schema("users").is_some());
    }

    #[test]
    fn test_add_column_validation() {
        let mut manager = SchemaManager::new();
        let schema = Schema::new("users".to_string());
        manager.create_schema("users".to_string(), schema).unwrap();
        
        let column = Column::new("age".to_string(), DataType::Integer(0));
        let operation = SchemaOperation::AddColumn {
            column,
            after: None,
        };
        
        manager.alter_table("users", operation).unwrap();
        let updated_schema = manager.get_schema("users").unwrap();
        assert!(updated_schema.has_column("age"));
    }

    #[test]
    fn test_basic_validator() {
        let validator = BasicSchemaValidator;
        let schema = Schema::new("users".to_string());
        
        assert!(validator.validate_schema(&schema).is_ok());
    }
}
