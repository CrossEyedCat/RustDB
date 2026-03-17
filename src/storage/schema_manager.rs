//! Table schema management for rustdb

use crate::common::{
    types::{Column, ColumnValue, DataType, Schema as BaseSchema},
    Error, Result,
};
use crate::storage::tuple::{Constraint, Schema, TableOptions, Trigger};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Schema modification operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchemaOperation {
    /// Adding a column
    AddColumn {
        column: Column,
        after: Option<String>, // After which column to add
    },
    /// Dropping a column
    DropColumn {
        column_name: String,
        cascade: bool, // Cascading deletion
    },
    /// Modifying a column
    ModifyColumn {
        column_name: String,
        new_column: Column,
    },
    /// Renaming a column
    RenameColumn { old_name: String, new_name: String },
    /// Adding a constraint
    AddConstraint { constraint: Constraint },
    /// Dropping a constraint
    DropConstraint { constraint_name: String },
    /// Adding an index
    AddIndex {
        index_name: String,
        columns: Vec<String>,
        unique: bool,
    },
    /// Dropping an index
    DropIndex { index_name: String },
    /// Modifying primary key
    ModifyPrimaryKey { new_columns: Vec<String> },
    /// Modifying table options
    ModifyTableOptions { options: TableOptions },
}

/// Table schema manager
pub struct SchemaManager {
    /// Table schemas
    schemas: HashMap<String, Schema>,
    /// Schema change history
    change_history: Vec<SchemaChange>,
    /// Schema validators
    validators: Vec<Box<dyn SchemaValidator>>,
}

impl Default for SchemaManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaManager {
    /// Creates a new schema manager
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
            change_history: Vec::new(),
            validators: Vec::new(),
        }
    }

    /// Registers a schema validator
    pub fn register_validator(&mut self, validator: Box<dyn SchemaValidator>) {
        self.validators.push(validator);
    }

    /// Creates a new table schema
    pub fn create_schema(&mut self, table_name: String, schema: Schema) -> Result<()> {
        // Validate schema
        self.validate_schema(&schema)?;

        // Check that table doesn't exist
        if self.schemas.contains_key(&table_name) {
            return Err(Error::validation(format!(
                "Table {} already exists",
                table_name
            )));
        }

        // Add schema
        self.schemas.insert(table_name.clone(), schema);

        // Record change
        let change = SchemaChange::new(
            table_name,
            SchemaOperationType::Create,
            "Creating table".to_string(),
        );
        self.change_history.push(change);

        Ok(())
    }

    /// Gets a table schema
    pub fn get_schema(&self, table_name: &str) -> Option<&Schema> {
        self.schemas.get(table_name)
    }

    /// Gets a mutable reference to a table schema
    pub fn get_schema_mut(&mut self, table_name: &str) -> Option<&mut Schema> {
        self.schemas.get_mut(table_name)
    }

    /// Executes an ALTER TABLE operation
    pub fn alter_table(&mut self, table_name: &str, operation: SchemaOperation) -> Result<()> {
        // First validate the operation
        {
            let schema = self
                .get_schema(table_name)
                .ok_or_else(|| Error::validation(format!("Table {} not found", table_name)))?;
            self.validate_operation(schema, &operation)?;
        }

        // Now execute the operation
        {
            let schema = self
                .get_schema_mut(table_name)
                .ok_or_else(|| Error::validation(format!("Table {} not found", table_name)))?;
            Self::execute_operation_static(schema, &operation)?;
        }

        // Record the change
        let change = SchemaChange::new(
            table_name.to_string(),
            SchemaOperationType::Alter,
            format!("{:?}", operation),
        );
        self.change_history.push(change);

        Ok(())
    }

    /// Validates a schema
    fn validate_schema(&self, schema: &Schema) -> Result<()> {
        for validator in &self.validators {
            validator.validate_schema(schema)?;
        }
        Ok(())
    }

    /// Validates a schema modification operation
    fn validate_operation(&self, schema: &Schema, operation: &SchemaOperation) -> Result<()> {
        match operation {
            SchemaOperation::AddColumn { column, .. } => {
                self.validate_add_column(schema, column)?;
            }
            SchemaOperation::DropColumn { column_name, .. } => {
                self.validate_drop_column(schema, column_name)?;
            }
            SchemaOperation::ModifyColumn {
                column_name,
                new_column,
            } => {
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
            SchemaOperation::AddIndex {
                index_name,
                columns,
                ..
            } => {
                self.validate_add_index(schema, index_name, columns)?;
            }
            SchemaOperation::DropIndex { index_name } => {
                self.validate_drop_index(schema, index_name)?;
            }
            SchemaOperation::ModifyPrimaryKey { new_columns } => {
                self.validate_modify_primary_key(schema, new_columns)?;
            }
            SchemaOperation::ModifyTableOptions { .. } => {
                // Table options don't require special validation
            }
        }
        Ok(())
    }

    /// Validates adding a column
    fn validate_add_column(&self, schema: &Schema, column: &Column) -> Result<()> {
        // Check that a column with this name doesn't exist
        if schema.has_column(&column.name) {
            return Err(Error::validation(format!(
                "Column {} already exists",
                column.name
            )));
        }

        // Check column constraints
        if column.not_null && column.default_value.is_none() {
            return Err(Error::validation(format!(
                "Column {} with NOT NULL must have a default value",
                column.name
            )));
        }

        Ok(())
    }

    /// Validates dropping a column
    fn validate_drop_column(&self, schema: &Schema, column_name: &str) -> Result<()> {
        // Check that the column exists
        if !schema.has_column(column_name) {
            return Err(Error::validation(format!(
                "Column {} not found",
                column_name
            )));
        }

        // Check that the column is not part of the primary key
        if let Some(pk) = &schema.base.primary_key {
            if pk.contains(&column_name.to_string()) {
                return Err(Error::validation(format!(
                    "Cannot drop column {} which is part of the primary key",
                    column_name
                )));
            }
        }

        // Check that the column is not used in indexes
        for index in &schema.base.indexes {
            if index.columns.contains(&column_name.to_string()) {
                return Err(Error::validation(format!(
                    "Cannot drop column {} which is used in index {}",
                    column_name, index.name
                )));
            }
        }

        Ok(())
    }

    /// Validates modifying a column
    fn validate_modify_column(
        &self,
        schema: &Schema,
        column_name: &str,
        new_column: &Column,
    ) -> Result<()> {
        // Check that the column exists
        if !schema.has_column(column_name) {
            return Err(Error::validation(format!(
                "Column {} not found",
                column_name
            )));
        }

        // Check data type compatibility
        let old_column = schema.get_column(column_name).unwrap();
        if !self.is_type_compatible(&old_column.data_type, &new_column.data_type) {
            return Err(Error::validation(format!(
                "Data type {:?} is incompatible with {:?}",
                old_column.data_type, new_column.data_type
            )));
        }

        Ok(())
    }

    /// Validates renaming a column
    fn validate_rename_column(
        &self,
        schema: &Schema,
        old_name: &str,
        new_name: &str,
    ) -> Result<()> {
        // Check that the old column exists
        if !schema.has_column(old_name) {
            return Err(Error::validation(format!(
                "Column {} not found",
                old_name
            )));
        }

        // Check that the new name is not taken
        if schema.has_column(new_name) {
            return Err(Error::validation(format!(
                "Column {} already exists",
                new_name
            )));
        }

        Ok(())
    }

    /// Validates adding a constraint
    fn validate_add_constraint(&self, schema: &Schema, constraint: &Constraint) -> Result<()> {
        // Check that all columns exist
        for column_name in &constraint.columns {
            if !schema.has_column(column_name) {
                return Err(Error::validation(format!(
                    "Column {} not found for constraint {}",
                    column_name, constraint.name
                )));
            }
        }

        Ok(())
    }

    /// Validates dropping a constraint
    fn validate_drop_constraint(&self, schema: &Schema, constraint_name: &str) -> Result<()> {
        // Check that the constraint exists
        let exists = schema
            .constraints
            .iter()
            .any(|c| c.name == *constraint_name);
        if !exists {
            return Err(Error::validation(format!(
                "Constraint {} not found",
                constraint_name
            )));
        }

        Ok(())
    }

    /// Validates adding an index
    fn validate_add_index(
        &self,
        schema: &Schema,
        index_name: &str,
        columns: &[String],
    ) -> Result<()> {
        // Check that an index with this name doesn't exist
        if schema.base.indexes.iter().any(|i| i.name == *index_name) {
            return Err(Error::validation(format!(
                "Index {} already exists",
                index_name
            )));
        }

        // Check that all columns exist
        for column_name in columns {
            if !schema.has_column(column_name) {
                return Err(Error::validation(format!(
                    "Column {} not found for index {}",
                    column_name, index_name
                )));
            }
        }

        Ok(())
    }

    /// Validates dropping an index
    fn validate_drop_index(&self, schema: &Schema, index_name: &str) -> Result<()> {
        // Check that the index exists
        let exists = schema.base.indexes.iter().any(|i| i.name == *index_name);
        if !exists {
            return Err(Error::validation(format!(
                "Index {} not found",
                index_name
            )));
        }

        Ok(())
    }

    /// Validates modifying the primary key
    fn validate_modify_primary_key(&self, schema: &Schema, new_columns: &[String]) -> Result<()> {
        // Check that all columns exist
        for column_name in new_columns {
            if !schema.has_column(column_name) {
                return Err(Error::validation(format!(
                    "Column {} not found for primary key",
                    column_name
                )));
            }
        }

        // Check that columns are NOT NULL
        for column_name in new_columns {
            if let Some(column) = schema.get_column(column_name) {
                if !column.not_null {
                    return Err(Error::validation(format!(
                        "Column {} in primary key must be NOT NULL",
                        column_name
                    )));
                }
            }
        }

        Ok(())
    }

    /// Checks data type compatibility
    fn is_type_compatible(&self, old_type: &DataType, new_type: &DataType) -> bool {
        match (old_type, new_type) {
            // Integer types
            (DataType::TinyInt(_), DataType::SmallInt(_))
            | (DataType::TinyInt(_), DataType::Integer(_))
            | (DataType::TinyInt(_), DataType::BigInt(_))
            | (DataType::SmallInt(_), DataType::Integer(_))
            | (DataType::SmallInt(_), DataType::BigInt(_))
            | (DataType::Integer(_), DataType::BigInt(_)) => true,

            // Floating point types
            (DataType::Float(_), DataType::Double(_)) => true,

            // String types
            (DataType::Char(_), DataType::Varchar(_))
            | (DataType::Char(_), DataType::Text(_))
            | (DataType::Varchar(_), DataType::Text(_)) => true,

            // Same types
            _ if std::mem::discriminant(old_type) == std::mem::discriminant(new_type) => true,

            // Incompatible by default
            _ => false,
        }
    }

    /// Executes a schema modification operation
    fn execute_operation(&self, schema: &mut Schema, operation: &SchemaOperation) -> Result<()> {
        Self::execute_operation_static(schema, operation)
    }

    /// Static version of schema modification operation execution
    fn execute_operation_static(schema: &mut Schema, operation: &SchemaOperation) -> Result<()> {
        match operation {
            SchemaOperation::AddColumn { column, after } => {
                Self::execute_add_column_static(schema, column, after)?;
            }
            SchemaOperation::DropColumn { column_name, .. } => {
                Self::execute_drop_column_static(schema, column_name)?;
            }
            SchemaOperation::ModifyColumn {
                column_name,
                new_column,
            } => {
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
            SchemaOperation::AddIndex {
                index_name,
                columns,
                unique,
            } => {
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

    /// Executes adding a column
    fn execute_add_column(
        &self,
        schema: &mut Schema,
        column: &Column,
        after: &Option<String>,
    ) -> Result<()> {
        Self::execute_add_column_static(schema, column, after)
    }

    /// Static version of adding a column
    fn execute_add_column_static(
        schema: &mut Schema,
        column: &Column,
        _after: &Option<String>,
    ) -> Result<()> {
        // TODO: Implement logic for adding a column at a specific position
        schema.base = schema.base.clone().add_column(column.clone());
        Ok(())
    }

    /// Executes dropping a column
    fn execute_drop_column(&self, schema: &mut Schema, column_name: &str) -> Result<()> {
        Self::execute_drop_column_static(schema, column_name)
    }

    /// Static version of dropping a column
    fn execute_drop_column_static(_schema: &mut Schema, _column_name: &str) -> Result<()> {
        // TODO: Implement logic for dropping a column
        // This is a complex operation requiring data restructuring
        Ok(())
    }

    /// Executes modifying a column
    fn execute_modify_column(
        &self,
        schema: &mut Schema,
        column_name: &str,
        new_column: &Column,
    ) -> Result<()> {
        Self::execute_modify_column_static(schema, column_name, new_column)
    }

    /// Static version of modifying a column
    fn execute_modify_column_static(
        _schema: &mut Schema,
        _column_name: &str,
        _new_column: &Column,
    ) -> Result<()> {
        // TODO: Implement logic for modifying a column
        // This is a complex operation requiring data restructuring
        Ok(())
    }

    /// Executes renaming a column
    fn execute_rename_column(
        &self,
        schema: &mut Schema,
        old_name: &str,
        new_name: &str,
    ) -> Result<()> {
        Self::execute_rename_column_static(schema, old_name, new_name)
    }

    /// Static version of renaming a column
    fn execute_rename_column_static(
        _schema: &mut Schema,
        _old_name: &str,
        _new_name: &str,
    ) -> Result<()> {
        // TODO: Implement logic for renaming a column
        Ok(())
    }

    /// Executes adding a constraint
    fn execute_add_constraint(&self, schema: &mut Schema, constraint: &Constraint) -> Result<()> {
        Self::execute_add_constraint_static(schema, constraint)
    }

    /// Static version of adding a constraint
    fn execute_add_constraint_static(schema: &mut Schema, constraint: &Constraint) -> Result<()> {
        schema.constraints.push(constraint.clone());
        Ok(())
    }

    /// Executes dropping a constraint
    fn execute_drop_constraint(&self, schema: &mut Schema, constraint_name: &str) -> Result<()> {
        Self::execute_drop_constraint_static(schema, constraint_name)
    }

    /// Static version of dropping a constraint
    fn execute_drop_constraint_static(schema: &mut Schema, constraint_name: &str) -> Result<()> {
        schema.constraints.retain(|c| c.name != *constraint_name);
        Ok(())
    }

    /// Executes adding an index
    fn execute_add_index(
        &self,
        schema: &mut Schema,
        index_name: &str,
        columns: &[String],
        unique: bool,
    ) -> Result<()> {
        Self::execute_add_index_static(schema, index_name, columns, unique)
    }

    /// Static version of adding an index
    fn execute_add_index_static(
        schema: &mut Schema,
        index_name: &str,
        columns: &[String],
        unique: bool,
    ) -> Result<()> {
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

    /// Executes dropping an index
    fn execute_drop_index(&self, schema: &mut Schema, index_name: &str) -> Result<()> {
        Self::execute_drop_index_static(schema, index_name)
    }

    /// Static version of dropping an index
    fn execute_drop_index_static(schema: &mut Schema, index_name: &str) -> Result<()> {
        schema.base.indexes.retain(|i| i.name != *index_name);
        Ok(())
    }

    /// Executes modifying the primary key
    fn execute_modify_primary_key(
        &self,
        schema: &mut Schema,
        new_columns: &[String],
    ) -> Result<()> {
        Self::execute_modify_primary_key_static(schema, new_columns)
    }

    /// Static version of modifying the primary key
    fn execute_modify_primary_key_static(
        schema: &mut Schema,
        new_columns: &[String],
    ) -> Result<()> {
        schema.base = schema.base.clone().primary_key(new_columns.to_vec());
        Ok(())
    }

    /// Executes modifying table options
    fn execute_modify_table_options(
        &self,
        schema: &mut Schema,
        options: &TableOptions,
    ) -> Result<()> {
        Self::execute_modify_table_options_static(schema, options)
    }

    /// Static version of modifying table options
    fn execute_modify_table_options_static(
        schema: &mut Schema,
        options: &TableOptions,
    ) -> Result<()> {
        schema.table_options = options.clone();
        Ok(())
    }

    /// Returns schema change history
    pub fn get_change_history(&self) -> &[SchemaChange] {
        &self.change_history
    }

    /// Rolls back the last schema change
    pub fn rollback_last_change(&mut self) -> Result<()> {
        if let Some(change) = self.change_history.pop() {
            // TODO: Implement change rollback
            log::info!("Rolling back change: {:?}", change);
        }
        Ok(())
    }
}

/// Schema modification operation type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchemaOperationType {
    /// Table creation
    Create,
    /// Table modification
    Alter,
    /// Table deletion
    Drop,
}

impl std::fmt::Display for SchemaOperationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaOperationType::Create => write!(f, "CREATE"),
            SchemaOperationType::Alter => write!(f, "ALTER"),
            SchemaOperationType::Drop => write!(f, "DROP"),
        }
    }
}

/// Schema change record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaChange {
    /// Table name
    pub table_name: String,
    /// Operation type
    pub operation_type: SchemaOperationType,
    /// Change description
    pub description: String,
    /// Change timestamp
    pub timestamp: u64,
}

impl SchemaChange {
    /// Creates a new change record
    pub fn new(
        table_name: String,
        operation_type: SchemaOperationType,
        description: String,
    ) -> Self {
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

/// Trait for schema validation
pub trait SchemaValidator: Send + Sync {
    /// Validates a schema
    fn validate_schema(&self, schema: &Schema) -> Result<()>;
}

/// Basic schema constraint validator
pub struct BasicSchemaValidator;

impl SchemaValidator for BasicSchemaValidator {
    fn validate_schema(&self, schema: &Schema) -> Result<()> {
        // Check that the table has columns
        if schema.get_columns().is_empty() {
            return Err(Error::validation(
                "Table must contain at least one column",
            ));
        }

        // Check that the primary key references existing columns
        if let Some(pk) = &schema.base.primary_key {
            for column_name in pk {
                if !schema.has_column(column_name) {
                    return Err(Error::validation(format!(
                        "Primary key column {} not found",
                        column_name
                    )));
                }
            }
        }

        // Check that indexes reference existing columns
        for index in &schema.base.indexes {
            for column_name in &index.columns {
                if !schema.has_column(column_name) {
                    return Err(Error::validation(format!(
                        "Index column {} not found",
                        column_name
                    )));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::{ColumnValue, DataType};

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
        let schema = Schema::new("users".to_string())
            .add_column(Column::new("id".to_string(), DataType::Integer(0)))
            .add_column(Column::new(
                "name".to_string(),
                DataType::Text("50".to_string()),
            ));

        assert!(validator.validate_schema(&schema).is_ok());
    }
}
