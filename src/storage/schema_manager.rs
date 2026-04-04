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

        let snapshot_before = self
            .get_schema(table_name)
            .ok_or_else(|| Error::validation(format!("Table {} not found", table_name)))?
            .clone();

        // Now execute the operation
        {
            let schema = self
                .get_schema_mut(table_name)
                .ok_or_else(|| Error::validation(format!("Table {} not found", table_name)))?;
            Self::execute_operation_static(schema, &operation)?;
        }

        // Record the change (snapshot for rollback)
        let change = SchemaChange::new_alter(
            table_name.to_string(),
            format!("{:?}", operation),
            snapshot_before,
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
            return Err(Error::validation(format!("Column {} not found", old_name)));
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
            return Err(Error::validation(format!("Index {} not found", index_name)));
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
        after: &Option<String>,
    ) -> Result<()> {
        let insert_at = if let Some(after_name) = after {
            let pos = schema
                .base
                .columns
                .iter()
                .position(|c| c.name == *after_name);
            match pos {
                Some(i) => i + 1,
                None => {
                    return Err(Error::validation(format!(
                        "AFTER column '{}' not found",
                        after_name
                    )));
                }
            }
        } else {
            schema.base.columns.len()
        };
        schema.base.columns.insert(insert_at, column.clone());
        Ok(())
    }

    /// Executes dropping a column
    fn execute_drop_column(&self, schema: &mut Schema, column_name: &str) -> Result<()> {
        Self::execute_drop_column_static(schema, column_name)
    }

    /// Static version of dropping a column
    fn execute_drop_column_static(schema: &mut Schema, column_name: &str) -> Result<()> {
        let pos = schema
            .base
            .columns
            .iter()
            .position(|c| c.name == column_name)
            .ok_or_else(|| Error::validation(format!("Column {} not found", column_name)))?;
        schema.base.columns.remove(pos);

        if let Some(pk) = &mut schema.base.primary_key {
            pk.retain(|c| c != column_name);
            if pk.is_empty() {
                schema.base.primary_key = None;
            }
        }

        for uc in &mut schema.base.unique_constraints {
            uc.retain(|c| c != column_name);
        }
        schema.base.unique_constraints.retain(|uc| !uc.is_empty());

        schema.base.foreign_keys.retain_mut(|fk| {
            fk.columns.retain(|c| c != column_name);
            !fk.columns.is_empty()
        });

        schema.base.indexes.retain_mut(|idx| {
            idx.columns.retain(|c| c != column_name);
            !idx.columns.is_empty()
        });

        schema.constraints.retain_mut(|c| {
            c.columns.retain(|col| col != column_name);
            !c.columns.is_empty()
        });

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
        schema: &mut Schema,
        column_name: &str,
        new_column: &Column,
    ) -> Result<()> {
        let col = schema
            .base
            .columns
            .iter_mut()
            .find(|c| c.name == column_name)
            .ok_or_else(|| Error::validation(format!("Column {} not found", column_name)))?;
        let mut updated = new_column.clone();
        if updated.name != column_name {
            updated.name = column_name.to_string();
        }
        *col = updated;
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
        schema: &mut Schema,
        old_name: &str,
        new_name: &str,
    ) -> Result<()> {
        let col = schema
            .base
            .columns
            .iter_mut()
            .find(|c| c.name == old_name)
            .ok_or_else(|| Error::validation(format!("Column {} not found", old_name)))?;
        col.name = new_name.to_string();

        if let Some(pk) = &mut schema.base.primary_key {
            for c in pk.iter_mut() {
                if c == old_name {
                    *c = new_name.to_string();
                }
            }
        }
        for uc in &mut schema.base.unique_constraints {
            for c in uc.iter_mut() {
                if c == old_name {
                    *c = new_name.to_string();
                }
            }
        }
        for fk in &mut schema.base.foreign_keys {
            for c in fk.columns.iter_mut() {
                if c == old_name {
                    *c = new_name.to_string();
                }
            }
        }
        for idx in &mut schema.base.indexes {
            for c in idx.columns.iter_mut() {
                if c == old_name {
                    *c = new_name.to_string();
                }
            }
        }
        for c in &mut schema.constraints {
            for col in c.columns.iter_mut() {
                if col == old_name {
                    *col = new_name.to_string();
                }
            }
        }
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
        let change = self
            .change_history
            .pop()
            .ok_or_else(|| Error::validation("No schema changes to roll back"))?;

        log::info!("Rolling back change: {:?}", change);

        match change.operation_type {
            SchemaOperationType::Create => {
                self.schemas.remove(&change.table_name);
            }
            SchemaOperationType::Alter => {
                let prev = change
                    .schema_snapshot_before
                    .ok_or_else(|| Error::internal("ALTER rollback missing schema snapshot"))?;
                self.schemas.insert(change.table_name, prev);
            }
            SchemaOperationType::Drop => {
                return Err(Error::validation(
                    "DROP rollback is not supported (no dropped-schema snapshot)",
                ));
            }
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
    /// Schema before an ALTER (used by [`SchemaManager::rollback_last_change`])
    #[serde(default)]
    pub schema_snapshot_before: Option<Schema>,
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
            schema_snapshot_before: None,
        }
    }

    /// ALTER entry with schema state before the change (for rollback)
    pub fn new_alter(table_name: String, description: String, snapshot_before: Schema) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            table_name,
            operation_type: SchemaOperationType::Alter,
            description,
            timestamp,
            schema_snapshot_before: Some(snapshot_before),
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
            return Err(Error::validation("Table must contain at least one column"));
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

    #[test]
    fn test_alter_add_index_drop_index() {
        let mut m = SchemaManager::new();
        let mut schema = Schema::new("t".into());
        schema = schema.add_column(Column::new("id".into(), DataType::Integer(0)));
        m.create_schema("t".into(), schema).unwrap();
        m.alter_table(
            "t",
            SchemaOperation::AddIndex {
                index_name: "ix".into(),
                columns: vec!["id".into()],
                unique: false,
            },
        )
        .unwrap();
        m.alter_table(
            "t",
            SchemaOperation::DropIndex {
                index_name: "ix".into(),
            },
        )
        .unwrap();
    }

    #[test]
    fn test_alter_add_drop_constraint() {
        let mut m = SchemaManager::new();
        let mut schema = Schema::new("t2".into());
        schema = schema.add_column(Column::new("c".into(), DataType::Integer(0)));
        m.create_schema("t2".into(), schema).unwrap();
        let c = crate::storage::tuple::Constraint::new(
            "chk".into(),
            crate::storage::tuple::ConstraintType::Check,
            "1".into(),
            vec!["c".into()],
        );
        m.alter_table(
            "t2",
            SchemaOperation::AddConstraint {
                constraint: c.clone(),
            },
        )
        .unwrap();
        m.alter_table(
            "t2",
            SchemaOperation::DropConstraint {
                constraint_name: "chk".into(),
            },
        )
        .unwrap();
    }

    #[test]
    fn test_alter_modify_primary_key_and_table_options() {
        let mut m = SchemaManager::new();
        let mut schema = Schema::new("t3".into());
        schema = schema.add_column(
            Column::new("id".into(), DataType::Integer(0))
                .not_null()
                .default_value(ColumnValue::new(DataType::Integer(0))),
        );
        m.create_schema("t3".into(), schema).unwrap();
        m.alter_table(
            "t3",
            SchemaOperation::ModifyPrimaryKey {
                new_columns: vec!["id".into()],
            },
        )
        .unwrap();
        m.alter_table(
            "t3",
            SchemaOperation::ModifyTableOptions {
                options: crate::storage::tuple::TableOptions {
                    engine: "innodb".into(),
                    charset: "utf8".into(),
                    collation: "utf8_bin".into(),
                    comment: None,
                    auto_increment: None,
                    max_rows: None,
                    min_rows: None,
                },
            },
        )
        .unwrap();
    }

    #[test]
    fn test_rename_column_and_rollback() {
        let mut m = SchemaManager::new();
        let mut schema = Schema::new("t4".into());
        schema = schema.add_column(Column::new("a".into(), DataType::Integer(0)));
        m.create_schema("t4".into(), schema).unwrap();
        m.alter_table(
            "t4",
            SchemaOperation::RenameColumn {
                old_name: "a".into(),
                new_name: "b".into(),
            },
        )
        .unwrap();
        assert!(m.get_schema("t4").unwrap().has_column("b"));
        assert!(!m.get_schema("t4").unwrap().has_column("a"));
        m.rollback_last_change().unwrap();
        assert!(m.get_schema("t4").unwrap().has_column("a"));
        assert!(!m.get_schema("t4").unwrap().has_column("b"));
    }

    #[test]
    fn test_add_column_after_position() {
        let mut m = SchemaManager::new();
        let mut schema = Schema::new("t6".into());
        schema = schema
            .add_column(Column::new("first".into(), DataType::Integer(0)))
            .add_column(Column::new("last".into(), DataType::Integer(0)));
        m.create_schema("t6".into(), schema).unwrap();

        let mid = Column::new("mid".into(), DataType::Integer(0));
        m.alter_table(
            "t6",
            SchemaOperation::AddColumn {
                column: mid,
                after: Some("first".into()),
            },
        )
        .unwrap();
        let cols = m.get_schema("t6").unwrap().get_columns();
        assert_eq!(cols[0].name, "first");
        assert_eq!(cols[1].name, "mid");
        assert_eq!(cols[2].name, "last");
    }

    #[test]
    fn test_drop_column_updates_indexes() {
        let mut m = SchemaManager::new();
        let mut schema = Schema::new("t7".into());
        schema = schema.add_column(Column::new("keep".into(), DataType::Integer(0)));
        schema = schema.add_column(Column::new("dropme".into(), DataType::Integer(0)));
        m.create_schema("t7".into(), schema).unwrap();
        m.alter_table(
            "t7",
            SchemaOperation::AddIndex {
                index_name: "ix_only_keep".into(),
                columns: vec!["keep".into()],
                unique: false,
            },
        )
        .unwrap();
        m.alter_table(
            "t7",
            SchemaOperation::DropColumn {
                column_name: "dropme".into(),
                cascade: false,
            },
        )
        .unwrap();
        assert!(m.get_schema("t7").unwrap().has_column("keep"));
        assert!(!m.get_schema("t7").unwrap().has_column("dropme"));
    }

    #[test]
    fn test_rollback_create_removes_table() {
        let mut m = SchemaManager::new();
        let schema = Schema::new("t8".into());
        m.create_schema("t8".into(), schema).unwrap();
        assert!(m.get_schema("t8").is_some());
        m.rollback_last_change().unwrap();
        assert!(m.get_schema("t8").is_none());
    }

    #[test]
    fn test_type_compatibility_paths() {
        let mut m = SchemaManager::new();
        let mut schema = Schema::new("t5".into());
        schema = schema.add_column(Column::new("i".into(), DataType::Integer(0)));
        m.create_schema("t5".into(), schema).unwrap();
        let new_col = Column::new("i".into(), DataType::BigInt(0));
        m.alter_table(
            "t5",
            SchemaOperation::ModifyColumn {
                column_name: "i".into(),
                new_column: new_col,
            },
        )
        .unwrap();
    }
}
