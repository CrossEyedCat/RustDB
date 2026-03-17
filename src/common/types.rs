//! Basic data types for rustdb

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Page identifier
pub type PageId = u64;

/// Record identifier
pub type RecordId = u64;

/// Transaction identifier
pub type TransactionId = u64;

/// Session identifier
pub type SessionId = u64;

/// User identifier
pub type UserId = u32;

/// Page size in bytes
pub const PAGE_SIZE: usize = 4096;

/// Page header size in bytes
pub const PAGE_HEADER_SIZE: usize = 64;

/// Maximum record size in a page
pub const MAX_RECORD_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE;

/// Supported data types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    /// NULL value
    Null,
    /// Boolean value
    Boolean(bool),
    /// 8-bit integer
    TinyInt(i8),
    /// 16-bit integer
    SmallInt(i16),
    /// 32-bit integer
    Integer(i32),
    /// 64-bit integer
    BigInt(i64),
    /// 32-bit floating point number
    Float(f32),
    /// 64-bit floating point number
    Double(f64),
    /// Fixed-length string
    Char(String),
    /// Variable-length string
    Varchar(String),
    /// Text
    Text(String),
    /// Date
    Date(String),
    /// Time
    Time(String),
    /// Date and time
    Timestamp(String),
    /// Binary data
    Blob(Vec<u8>),
}

impl DataType {
    /// Returns the size of the data type in bytes
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
            DataType::Varchar(s) => s.len() + 4, // +4 for length
            DataType::Text(s) => s.len() + 8,    // +8 for length
            DataType::Date(_) => 10,
            DataType::Time(_) => 8,
            DataType::Timestamp(_) => 19,
            DataType::Blob(b) => b.len() + 8, // +8 for length
        }
    }

    /// Checks if the type is NULL
    pub fn is_null(&self) -> bool {
        matches!(self, DataType::Null)
    }

    /// Checks if the type is numeric
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataType::TinyInt(_)
                | DataType::SmallInt(_)
                | DataType::Integer(_)
                | DataType::BigInt(_)
                | DataType::Float(_)
                | DataType::Double(_)
        )
    }

    /// Checks if the type is a string
    pub fn is_string(&self) -> bool {
        matches!(
            self,
            DataType::Char(_) | DataType::Varchar(_) | DataType::Text(_)
        )
    }
}

/// Column value
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnValue {
    /// Data type
    pub data_type: DataType,
    /// NULL flag
    pub is_null: bool,
}

impl ColumnValue {
    /// Creates a new column value
    pub fn new(data_type: DataType) -> Self {
        let is_null = data_type.is_null();
        Self { data_type, is_null }
    }

    /// Creates a NULL value
    pub fn null() -> Self {
        Self {
            data_type: DataType::Null,
            is_null: true,
        }
    }

    /// Checks if the value is NULL
    pub fn is_null(&self) -> bool {
        self.is_null
    }
}

/// Column definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: DataType,
    /// NOT NULL flag
    pub not_null: bool,
    /// Default value
    pub default_value: Option<ColumnValue>,
    /// Comment
    pub comment: Option<String>,
}

impl Column {
    /// Creates a new column
    pub fn new(name: String, data_type: DataType) -> Self {
        Self {
            name,
            data_type,
            not_null: false,
            default_value: None,
            comment: None,
        }
    }

    /// Sets the NOT NULL flag
    pub fn not_null(mut self) -> Self {
        self.not_null = true;
        self
    }

    /// Sets the default value
    pub fn default_value(mut self, value: ColumnValue) -> Self {
        self.default_value = Some(value);
        self
    }

    /// Sets a comment
    pub fn comment(mut self, comment: String) -> Self {
        self.comment = Some(comment);
        self
    }
}

/// Table schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Table name
    pub table_name: String,
    /// Table columns
    pub columns: Vec<Column>,
    /// Primary key
    pub primary_key: Option<Vec<String>>,
    /// Unique constraints
    pub unique_constraints: Vec<Vec<String>>,
    /// Foreign keys
    pub foreign_keys: Vec<ForeignKey>,
    /// Indexes
    pub indexes: Vec<Index>,
}

impl Schema {
    /// Creates a new table schema
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

    /// Adds a column to the schema
    pub fn add_column(mut self, column: Column) -> Self {
        self.columns.push(column);
        self
    }

    /// Sets the primary key
    pub fn primary_key(mut self, columns: Vec<String>) -> Self {
        self.primary_key = Some(columns);
        self
    }

    /// Adds a unique constraint
    pub fn unique(mut self, columns: Vec<String>) -> Self {
        self.unique_constraints.push(columns);
        self
    }

    /// Adds a foreign key
    pub fn foreign_key(mut self, fk: ForeignKey) -> Self {
        self.foreign_keys.push(fk);
        self
    }

    /// Adds an index
    pub fn index(mut self, index: Index) -> Self {
        self.indexes.push(index);
        self
    }
}

/// Foreign key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKey {
    /// Constraint name
    pub name: String,
    /// Columns in the current table
    pub columns: Vec<String>,
    /// Referenced table
    pub referenced_table: String,
    /// Referenced columns
    pub referenced_columns: Vec<String>,
    /// Action on delete
    pub on_delete: Option<ForeignKeyAction>,
    /// Action on update
    pub on_update: Option<ForeignKeyAction>,
}

/// Foreign key action
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ForeignKeyAction {
    /// Cascading delete/update
    Cascade,
    /// Set NULL
    SetNull,
    /// Set default value
    SetDefault,
    /// Restrict
    Restrict,
    /// No action
    NoAction,
}

/// Index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Index {
    /// Index name
    pub name: String,
    /// Index columns
    pub columns: Vec<String>,
    /// Index type
    pub index_type: IndexType,
    /// Index uniqueness
    pub unique: bool,
}

/// Index type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexType {
    /// B+ tree
    BTree,
    /// Hash index
    Hash,
    /// Full-text index
    FullText,
}

/// Table row
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    /// Column values
    pub values: HashMap<String, ColumnValue>,
    /// Row version (for MVCC)
    pub version: u64,
    /// Creation time
    pub created_at: u64,
    /// Last update time
    pub updated_at: u64,
}

impl Row {
    /// Creates a new row
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

    /// Sets a column value
    pub fn set_value(&mut self, column: &str, value: ColumnValue) {
        self.values.insert(column.to_string(), value);
        self.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Gets a column value
    pub fn get_value(&self, column: &str) -> Option<&ColumnValue> {
        self.values.get(column)
    }

    /// Checks if the row contains a column
    pub fn has_column(&self, column: &str) -> bool {
        self.values.contains_key(column)
    }
}
