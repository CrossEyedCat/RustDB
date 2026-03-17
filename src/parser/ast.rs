//! SQL abstract syntax tree for rustdb

use crate::common::{Error, Result};
use crate::parser::token::Token;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Main AST node for SQL operations
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SqlStatement {
    /// SELECT query
    Select(SelectStatement),
    /// INSERT operation
    Insert(InsertStatement),
    /// UPDATE operation
    Update(UpdateStatement),
    /// DELETE operation
    Delete(DeleteStatement),
    /// CREATE TABLE operation
    CreateTable(CreateTableStatement),
    /// CREATE INDEX operation
    CreateIndex(CreateIndexStatement),
    /// ALTER TABLE operation
    AlterTable(AlterTableStatement),
    /// DROP TABLE operation
    DropTable(DropTableStatement),
    /// BEGIN TRANSACTION
    BeginTransaction,
    /// COMMIT TRANSACTION
    CommitTransaction,
    /// ROLLBACK TRANSACTION
    RollbackTransaction,
    /// PREPARE statement
    Prepare(PrepareStatement),
    /// EXECUTE prepared statement
    Execute(ExecuteStatement),
}

/// PREPARE statement: PREPARE name AS SELECT ...
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrepareStatement {
    pub name: String,
    pub statement: Box<SqlStatement>,
}

/// EXECUTE statement: EXECUTE name (param1, param2, ...)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecuteStatement {
    pub name: String,
    pub params: Vec<Expression>,
}

/// SELECT query
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectStatement {
    pub select_list: Vec<SelectItem>,
    pub from: Option<FromClause>,
    pub where_clause: Option<Expression>,
    pub group_by: Vec<Expression>,
    pub having: Option<Expression>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

/// Item in SELECT list
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SelectItem {
    /// All columns (*)
    Wildcard,
    /// Specific expression with alias
    Expression {
        expr: Expression,
        alias: Option<String>,
    },
}

/// FROM clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FromClause {
    pub table: TableReference,
    pub joins: Vec<JoinClause>,
}

/// Table reference
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TableReference {
    /// Simple table
    Table { name: String, alias: Option<String> },
    /// Subquery
    Subquery {
        query: Box<SelectStatement>,
        alias: String,
    },
}

/// JOIN operation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: TableReference,
    pub condition: Option<Expression>,
}

/// JOIN type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// ORDER BY item
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderByItem {
    pub expr: Expression,
    pub direction: OrderDirection,
}

/// Sort direction
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OrderDirection {
    Asc,
    Desc,
}

/// INSERT operation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub values: InsertValues,
}

/// Values for INSERT
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum InsertValues {
    /// VALUES (val1, val2, ...)
    Values(Vec<Vec<Expression>>),
    /// SELECT ...
    Select(Box<SelectStatement>),
}

/// UPDATE operation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UpdateStatement {
    pub table: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<Expression>,
}

/// Assignment in UPDATE
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Assignment {
    pub column: String,
    pub value: Expression,
}

/// DELETE operation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteStatement {
    pub table: String,
    pub where_clause: Option<Expression>,
}

/// CREATE TABLE operation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateTableStatement {
    pub table_name: String,
    pub columns: Vec<ColumnDefinition>,
    pub constraints: Vec<TableConstraint>,
    pub if_not_exists: bool,
}

/// CREATE INDEX operation: CREATE INDEX index_name ON table_name (col1, col2, ...)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateIndexStatement {
    pub index_name: String,
    pub table_name: String,
    pub columns: Vec<String>,
}

/// Column definition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<ColumnConstraint>,
}

/// Data type
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Integer,
    BigInt,
    Real,
    Double,
    Decimal {
        precision: Option<u8>,
        scale: Option<u8>,
    },
    Text,
    Varchar {
        length: Option<u16>,
    },
    Boolean,
    Date,
    Time,
    Timestamp,
    Blob,
}

/// Column constraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnConstraint {
    NotNull,
    Unique,
    PrimaryKey,
    Default(Expression),
    Check(Expression),
    References {
        table: String,
        column: Option<String>,
    },
}

/// Table constraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TableConstraint {
    PrimaryKey(Vec<String>),
    Unique(Vec<String>),
    ForeignKey {
        columns: Vec<String>,
        referenced_table: String,
        referenced_columns: Vec<String>,
    },
    Check(Expression),
}

/// ALTER TABLE operation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlterTableStatement {
    pub table_name: String,
    pub operation: AlterTableOperation,
}

/// ALTER TABLE operation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AlterTableOperation {
    AddColumn(ColumnDefinition),
    DropColumn(String),
    ModifyColumn(ColumnDefinition),
    RenameColumn { old_name: String, new_name: String },
    AddConstraint(TableConstraint),
    DropConstraint(String),
    RenameTable(String),
}

/// DROP TABLE operation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropTableStatement {
    pub table_name: String,
    pub if_exists: bool,
    pub cascade: bool,
}

/// SQL expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    /// Literal
    Literal(Literal),
    /// Identifier (column)
    Identifier(String),
    /// Qualified identifier (table.column)
    QualifiedIdentifier { table: String, column: String },
    /// Binary operation
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    /// Unary operation
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expression>,
    },
    /// Function
    Function { name: String, args: Vec<Expression> },
    /// CASE expression
    Case {
        expr: Option<Box<Expression>>,
        when_clauses: Vec<WhenClause>,
        else_clause: Option<Box<Expression>>,
    },
    /// EXISTS subquery
    Exists(Box<SelectStatement>),
    /// IN operation
    In { expr: Box<Expression>, list: InList },
    /// BETWEEN operation
    Between {
        expr: Box<Expression>,
        low: Box<Expression>,
        high: Box<Expression>,
    },
    /// IS NULL / IS NOT NULL
    IsNull {
        expr: Box<Expression>,
        negated: bool,
    },
    /// LIKE operation
    Like {
        expr: Box<Expression>,
        pattern: Box<Expression>,
        negated: bool,
    },
}

/// Literal
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Literal {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

/// Binary operator
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BinaryOperator {
    // Arithmetic
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    // Comparison
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    // Logical
    And,
    Or,
    // String
    Concat,
}

/// Unary operator
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
}

/// WHEN clause in CASE
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WhenClause {
    pub condition: Expression,
    pub result: Expression,
}

/// List for IN operation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum InList {
    Values(Vec<Expression>),
    Subquery(Box<SelectStatement>),
}

/// AST builder
pub struct AstBuilder {
    // Additional data for building AST
    metadata: HashMap<String, String>,
}

impl AstBuilder {
    /// Creates a new AST builder
    pub fn new() -> Result<Self> {
        Ok(Self {
            metadata: HashMap::new(),
        })
    }

    /// Adds metadata
    pub fn add_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }

    /// Gets metadata
    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }

    /// Creates a simple SELECT query
    pub fn build_simple_select(&self, columns: Vec<String>, table: String) -> Result<SqlStatement> {
        let select_list = if columns.is_empty() || columns[0] == "*" {
            vec![SelectItem::Wildcard]
        } else {
            columns
                .into_iter()
                .map(|col| SelectItem::Expression {
                    expr: Expression::Identifier(col),
                    alias: None,
                })
                .collect()
        };

        let from = Some(FromClause {
            table: TableReference::Table {
                name: table,
                alias: None,
            },
            joins: Vec::new(),
        });

        Ok(SqlStatement::Select(SelectStatement {
            select_list,
            from,
            where_clause: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        }))
    }

    /// Creates a simple INSERT query
    pub fn build_simple_insert(
        &self,
        table: String,
        columns: Vec<String>,
        values: Vec<Vec<String>>,
    ) -> Result<SqlStatement> {
        let insert_values = values
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|val| Expression::Literal(Literal::String(val)))
                    .collect()
            })
            .collect();

        Ok(SqlStatement::Insert(InsertStatement {
            table,
            columns: if columns.is_empty() {
                None
            } else {
                Some(columns)
            },
            values: InsertValues::Values(insert_values),
        }))
    }
}
