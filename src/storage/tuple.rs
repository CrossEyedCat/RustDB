//! Data structures for rustdb tables

use crate::common::{
    types::{Column, ColumnValue, DataType, Schema as BaseSchema},
    Error, Result,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Table tuple (row)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tuple {
    /// Tuple ID
    pub id: u64,
    /// Column values
    pub values: HashMap<String, ColumnValue>,
    /// Tuple version (for MVCC)
    pub version: u64,
    /// Creation time
    pub created_at: u64,
    /// Last update time
    pub updated_at: u64,
    /// Deletion flag
    pub is_deleted: bool,
    /// Pointer to the next version
    pub next_version: Option<u64>,
    /// Pointer to the previous version
    pub prev_version: Option<u64>,
}

impl Tuple {
    /// Creates a new tuple
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

    /// Checks if the tuple contains a column
    pub fn has_column(&self, column: &str) -> bool {
        self.values.contains_key(column)
    }

    /// Marks the tuple as deleted
    pub fn mark_deleted(&mut self) {
        self.is_deleted = true;
        self.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Checks if the tuple is deleted
    pub fn is_deleted(&self) -> bool {
        self.is_deleted
    }

    /// Creates a new tuple version
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

        // Update pointer to next version
        self.next_version = Some(new_tuple.version);

        new_tuple
    }

    /// Serializes the tuple to bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        crate::common::bincode_io::serialize(self).map_err(Error::from)
    }

    /// Creates a tuple from bytes (deserialization)
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        crate::common::bincode_io::deserialize(bytes).map_err(Error::from)
    }

    /// Returns the tuple size in bytes
    pub fn size(&self) -> usize {
        self.to_bytes().unwrap_or_default().len()
    }

    /// Checks if the tuple matches the schema
    pub fn validate_against_schema(&self, schema: &Schema) -> Result<()> {
        for column in &schema.base.columns {
            if column.not_null {
                if let Some(value) = self.values.get(&column.name) {
                    if value.is_null() {
                        return Err(Error::validation(format!(
                            "Column {} cannot be NULL",
                            column.name
                        )));
                    }
                } else {
                    return Err(Error::validation(format!(
                        "Missing required column {}",
                        column.name
                    )));
                }
            }
        }
        Ok(())
    }

    /// Checks if the tuple matches the base schema
    pub fn validate_against_base_schema(&self, schema: &BaseSchema) -> Result<()> {
        for column in &schema.columns {
            if column.not_null {
                if let Some(value) = self.values.get(&column.name) {
                    if value.is_null() {
                        return Err(Error::validation(format!(
                            "Column {} cannot be NULL",
                            column.name
                        )));
                    }
                } else {
                    return Err(Error::validation(format!(
                        "Missing required column {}",
                        column.name
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Extended table schema with additional capabilities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Base schema
    pub base: BaseSchema,
    /// Additional constraints
    pub constraints: Vec<Constraint>,
    /// Triggers
    pub triggers: Vec<Trigger>,
    /// Table options
    pub table_options: TableOptions,
}

impl Schema {
    /// Creates a new schema
    pub fn new(table_name: String) -> Self {
        Self {
            base: BaseSchema::new(table_name),
            constraints: Vec::new(),
            triggers: Vec::new(),
            table_options: TableOptions::default(),
        }
    }

    /// Adds a column to the schema
    pub fn add_column(mut self, column: Column) -> Self {
        self.base = self.base.add_column(column);
        self
    }

    /// Sets the primary key
    pub fn primary_key(mut self, columns: Vec<String>) -> Self {
        self.base = self.base.primary_key(columns);
        self
    }

    /// Adds a unique constraint
    pub fn unique(mut self, columns: Vec<String>) -> Self {
        self.base = self.base.unique(columns);
        self
    }

    /// Adds a foreign key
    pub fn foreign_key(mut self, fk: crate::common::types::ForeignKey) -> Self {
        self.base = self.base.foreign_key(fk);
        self
    }

    /// Adds an index
    pub fn index(mut self, index: crate::common::types::Index) -> Self {
        self.base = self.base.index(index);
        self
    }

    /// Adds a constraint
    pub fn add_constraint(mut self, constraint: Constraint) -> Self {
        self.constraints.push(constraint);
        self
    }

    /// Adds a trigger
    pub fn add_trigger(mut self, trigger: Trigger) -> Self {
        self.triggers.push(trigger);
        self
    }

    /// Sets table options
    pub fn with_options(mut self, options: TableOptions) -> Self {
        self.table_options = options;
        self
    }

    /// Checks if the tuple matches the schema
    pub fn validate_tuple(&self, tuple: &Tuple) -> Result<()> {
        // Check base schema
        tuple.validate_against_base_schema(&self.base)?;

        // Check additional constraints
        for constraint in &self.constraints {
            constraint.validate(tuple)?;
        }

        Ok(())
    }

    /// Returns all schema columns
    pub fn get_columns(&self) -> &[Column] {
        &self.base.columns
    }

    /// Returns a column by name
    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.base.columns.iter().find(|c| c.name == name)
    }

    /// Checks if the schema contains a column
    pub fn has_column(&self, name: &str) -> bool {
        self.base.columns.iter().any(|c| c.name == name)
    }
}

/// Table constraint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Constraint {
    /// Constraint name
    pub name: String,
    /// Constraint type
    pub constraint_type: ConstraintType,
    /// Constraint expression
    pub expression: String,
    /// Columns to which the constraint applies
    pub columns: Vec<String>,
}

impl Constraint {
    /// Creates a new constraint
    pub fn new(
        name: String,
        constraint_type: ConstraintType,
        expression: String,
        columns: Vec<String>,
    ) -> Self {
        Self {
            name,
            constraint_type,
            expression,
            columns,
        }
    }

    /// Validates the constraint for a tuple
    pub fn validate(&self, tuple: &Tuple) -> Result<()> {
        match self.constraint_type {
            ConstraintType::NotNull => {
                for col in &self.columns {
                    match tuple.get_value(col) {
                        None => {
                            return Err(Error::validation(format!(
                                "Constraint '{}': column '{}' is missing (NOT NULL)",
                                self.name, col
                            )));
                        }
                        Some(v) if v.is_null() => {
                            return Err(Error::validation(format!(
                                "Constraint '{}': column '{}' cannot be NULL",
                                self.name, col
                            )));
                        }
                        Some(_) => {}
                    }
                }
                Ok(())
            }
            ConstraintType::Default => Ok(()),
            ConstraintType::Check | ConstraintType::Custom => self.validate_check_expression(tuple),
        }
    }

    /// Parses `col op literal` CHECK forms: `>=`, `<=`, `!=`, `=`, `>`, `<`.
    fn validate_check_expression(&self, tuple: &Tuple) -> Result<()> {
        let expr = self.expression.trim();
        if expr.is_empty() {
            return Ok(());
        }

        let Some((left_col, op, right_raw)) = split_binary_comparison(expr) else {
            // Constant-only expressions (e.g. placeholders `1` / `true` = always satisfied)
            let e = expr.trim();
            if e.eq_ignore_ascii_case("true")
                || e == "1"
                || e.eq_ignore_ascii_case("t")
            {
                return Ok(());
            }
            if e.eq_ignore_ascii_case("false") || e == "0" || e.eq_ignore_ascii_case("f") {
                return Err(Error::validation(format!(
                    "Constraint '{}': CHECK constant false",
                    self.name
                )));
            }
            return Err(Error::validation(format!(
                "Constraint '{}': unsupported CHECK expression {:?}",
                self.name, self.expression
            )));
        };

        if !self.columns.is_empty() && !self.columns.iter().any(|c| c == left_col) {
            return Err(Error::validation(format!(
                "Constraint '{}': column '{}' is not listed in constraint columns {:?}",
                self.name, left_col, self.columns
            )));
        }

        let Some(cv) = tuple.get_value(left_col) else {
            return Err(Error::validation(format!(
                "Constraint '{}': column '{}' missing for CHECK",
                self.name, left_col
            )));
        };
        if cv.is_null() {
            return Err(Error::validation(format!(
                "Constraint '{}': column '{}' is NULL in CHECK",
                self.name, left_col
            )));
        }

        let rhs = parse_check_literal(right_raw)?;
        let ok = compare_check_value(&cv, op, &rhs).map_err(|e| {
            Error::validation(format!(
                "Constraint '{}': {} (column '{}')",
                self.name, e, left_col
            ))
        })?;

        if !ok {
            return Err(Error::validation(format!(
                "Constraint '{}': CHECK failed ({})",
                self.name, expr
            )));
        }
        Ok(())
    }
}

/// Splits `a >= 0` style comparisons (longest operator first).
fn split_binary_comparison(expr: &str) -> Option<(&str, &'static str, &str)> {
    let expr = expr.trim();
    const OPS: &[&str] = &[">=", "<=", "!=", "=", ">", "<"];
    for op in OPS {
        if let Some(i) = expr.find(op) {
            let left = expr[..i].trim();
            let rest = i + op.len();
            if rest <= expr.len() {
                let right = expr[rest..].trim();
                if !left.is_empty() && !right.is_empty() {
                    return Some((left, *op, right));
                }
            }
        }
    }
    None
}

#[derive(Debug)]
enum CheckLiteral {
    Number(f64),
    Str(String),
}

fn parse_check_literal(s: &str) -> Result<CheckLiteral> {
    let s = s.trim();
    if s.len() >= 2
        && ((s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')))
    {
        return Ok(CheckLiteral::Str(
            s[1..s.len() - 1].replace("''", "'").replace("\"\"", "\""),
        ));
    }
    if s.eq_ignore_ascii_case("true") {
        return Ok(CheckLiteral::Number(1.0));
    }
    if s.eq_ignore_ascii_case("false") {
        return Ok(CheckLiteral::Number(0.0));
    }
    s.parse::<f64>()
        .map(CheckLiteral::Number)
        .map_err(|_| Error::validation(format!("Invalid CHECK literal {:?}", s)))
}

fn column_value_as_f64(cv: &ColumnValue) -> Option<f64> {
    match &cv.data_type {
        DataType::TinyInt(v) => Some(*v as f64),
        DataType::SmallInt(v) => Some(*v as f64),
        DataType::Integer(v) => Some(*v as f64),
        DataType::BigInt(v) => Some(*v as f64),
        DataType::Float(v) => Some(*v as f64),
        DataType::Double(v) => Some(*v),
        DataType::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

fn column_value_as_string(cv: &ColumnValue) -> Option<String> {
    match &cv.data_type {
        DataType::Char(s) | DataType::Varchar(s) | DataType::Text(s) => Some(s.clone()),
        _ => None,
    }
}

fn compare_check_value(
    cv: &ColumnValue,
    op: &str,
    rhs: &CheckLiteral,
) -> std::result::Result<bool, String> {
    match rhs {
        CheckLiteral::Str(rs) => {
            let lhs = column_value_as_string(cv)
                .ok_or_else(|| "CHECK expects string column for string literal".to_string())?;
            match op {
                "=" => Ok(lhs == *rs),
                "!=" => Ok(lhs != *rs),
                _ => Err("Only = or != supported for string CHECK literals".to_string()),
            }
        }
        CheckLiteral::Number(r) => {
            let lhs = column_value_as_f64(cv).ok_or_else(|| {
                "CHECK numeric comparison requires numeric or boolean column".to_string()
            })?;
            let ok = match op {
                ">=" => lhs >= *r - f64::EPSILON,
                "<=" => lhs <= *r + f64::EPSILON,
                ">" => lhs > *r,
                "<" => lhs < *r,
                "=" => (lhs - *r).abs() <= (1e-9_f64).max(f64::EPSILON * lhs.abs().max(1.0)),
                "!=" => (lhs - *r).abs() > (1e-9_f64).max(f64::EPSILON * lhs.abs().max(1.0)),
                _ => return Err(format!("unsupported operator {:?}", op)),
            };
            Ok(ok)
        }
    }
}

/// Constraint type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ConstraintType {
    /// Check constraint
    Check,
    /// Default constraint
    Default,
    /// NOT NULL constraint
    NotNull,
    /// Custom constraint
    Custom,
}

/// Table trigger
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trigger {
    /// Trigger name
    pub name: String,
    /// Trigger event
    pub event: TriggerEvent,
    /// Trigger timing
    pub timing: TriggerTiming,
    /// Trigger SQL code
    pub sql_code: String,
    /// Trigger execution condition
    pub condition: Option<String>,
}

impl Trigger {
    /// Creates a new trigger
    pub fn new(name: String, event: TriggerEvent, timing: TriggerTiming, sql_code: String) -> Self {
        Self {
            name,
            event,
            timing,
            sql_code,
            condition: None,
        }
    }

    /// Sets the execution condition
    pub fn with_condition(mut self, condition: String) -> Self {
        self.condition = Some(condition);
        self
    }
}

/// Trigger event
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TriggerEvent {
    /// Insert
    Insert,
    /// Update
    Update,
    /// Delete
    Delete,
}

/// Trigger timing
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TriggerTiming {
    /// Before operation
    Before,
    /// After operation
    After,
    /// Instead of operation
    InsteadOf,
}

/// Table options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableOptions {
    /// Table engine
    pub engine: String,
    /// Character set
    pub charset: String,
    /// Collation
    pub collation: String,
    /// Table comment
    pub comment: Option<String>,
    /// Auto-increment
    pub auto_increment: Option<u64>,
    /// Maximum number of rows
    pub max_rows: Option<u64>,
    /// Minimum number of rows
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
    use crate::common::types::{ColumnValue, DataType};

    #[test]
    fn test_tuple_creation() {
        let tuple = Tuple::new(1);
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
        assert_eq!(
            tuple.get_value("age").unwrap().data_type,
            DataType::Integer(42)
        );
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
    fn test_constraint_not_null() {
        let c = Constraint::new(
            "nn".into(),
            ConstraintType::NotNull,
            String::new(),
            vec!["x".into()],
        );
        let mut t = Tuple::new(1);
        assert!(c.validate(&t).is_err());
        t.set_value("x", ColumnValue::null());
        assert!(c.validate(&t).is_err());
        t.set_value("x", ColumnValue::new(DataType::Integer(1)));
        assert!(c.validate(&t).is_ok());
    }

    #[test]
    fn test_constraint_check_numeric() {
        let c = Constraint::new(
            "age_pos".into(),
            ConstraintType::Check,
            "age >= 0".into(),
            vec!["age".into()],
        );
        let mut t = Tuple::new(1);
        t.set_value("age", ColumnValue::new(DataType::Integer(5)));
        assert!(c.validate(&t).is_ok());
        t.set_value("age", ColumnValue::new(DataType::Integer(-1)));
        assert!(c.validate(&t).is_err());
    }

    #[test]
    fn test_constraint_check_constant_true() {
        let c = Constraint::new(
            "trivial".into(),
            ConstraintType::Check,
            "1".into(),
            vec!["c".into()],
        );
        let mut t = Tuple::new(1);
        t.set_value("c", ColumnValue::new(DataType::Integer(0)));
        assert!(c.validate(&t).is_ok());
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
