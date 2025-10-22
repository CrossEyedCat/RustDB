//! Абстрактное синтаксическое дерево SQL для rustdb

use crate::common::{Error, Result};
use crate::parser::token::Token;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Основной узел AST для SQL операций
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SqlStatement {
    /// SELECT запрос
    Select(SelectStatement),
    /// INSERT операция
    Insert(InsertStatement),
    /// UPDATE операция
    Update(UpdateStatement),
    /// DELETE операция
    Delete(DeleteStatement),
    /// CREATE TABLE операция
    CreateTable(CreateTableStatement),
    /// ALTER TABLE операция
    AlterTable(AlterTableStatement),
    /// DROP TABLE операция
    DropTable(DropTableStatement),
    /// BEGIN TRANSACTION
    BeginTransaction,
    /// COMMIT TRANSACTION
    CommitTransaction,
    /// ROLLBACK TRANSACTION
    RollbackTransaction,
}

/// SELECT запрос
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

/// Элемент в списке SELECT
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SelectItem {
    /// Все колонки (*)
    Wildcard,
    /// Конкретное выражение с псевдонимом
    Expression {
        expr: Expression,
        alias: Option<String>,
    },
}

/// FROM клаузула
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FromClause {
    pub table: TableReference,
    pub joins: Vec<JoinClause>,
}

/// Ссылка на таблицу
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TableReference {
    /// Простая таблица
    Table { name: String, alias: Option<String> },
    /// Подзапрос
    Subquery {
        query: Box<SelectStatement>,
        alias: String,
    },
}

/// JOIN операция
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: TableReference,
    pub condition: Option<Expression>,
}

/// Тип JOIN
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// ORDER BY элемент
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderByItem {
    pub expr: Expression,
    pub direction: OrderDirection,
}

/// Направление сортировки
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OrderDirection {
    Asc,
    Desc,
}

/// INSERT операция
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub values: InsertValues,
}

/// Значения для INSERT
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum InsertValues {
    /// VALUES (val1, val2, ...)
    Values(Vec<Vec<Expression>>),
    /// SELECT ...
    Select(Box<SelectStatement>),
}

/// UPDATE операция
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UpdateStatement {
    pub table: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<Expression>,
}

/// Присваивание в UPDATE
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Assignment {
    pub column: String,
    pub value: Expression,
}

/// DELETE операция
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteStatement {
    pub table: String,
    pub where_clause: Option<Expression>,
}

/// CREATE TABLE операция
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateTableStatement {
    pub table_name: String,
    pub columns: Vec<ColumnDefinition>,
    pub constraints: Vec<TableConstraint>,
    pub if_not_exists: bool,
}

/// Определение колонки
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<ColumnConstraint>,
}

/// Тип данных
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

/// Ограничение колонки
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

/// Ограничение таблицы
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

/// ALTER TABLE операция
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlterTableStatement {
    pub table_name: String,
    pub operation: AlterTableOperation,
}

/// Операция ALTER TABLE
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

/// DROP TABLE операция
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropTableStatement {
    pub table_name: String,
    pub if_exists: bool,
    pub cascade: bool,
}

/// SQL выражение
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    /// Литерал
    Literal(Literal),
    /// Идентификатор (колонка)
    Identifier(String),
    /// Qualified идентификатор (table.column)
    QualifiedIdentifier { table: String, column: String },
    /// Бинарная операция
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    /// Унарная операция
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expression>,
    },
    /// Функция
    Function { name: String, args: Vec<Expression> },
    /// CASE выражение
    Case {
        expr: Option<Box<Expression>>,
        when_clauses: Vec<WhenClause>,
        else_clause: Option<Box<Expression>>,
    },
    /// EXISTS подзапрос
    Exists(Box<SelectStatement>),
    /// IN операция
    In { expr: Box<Expression>, list: InList },
    /// BETWEEN операция
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
    /// LIKE операция
    Like {
        expr: Box<Expression>,
        pattern: Box<Expression>,
        negated: bool,
    },
}

/// Литерал
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Literal {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

/// Бинарный оператор
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BinaryOperator {
    // Арифметические
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    // Сравнения
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    // Логические
    And,
    Or,
    // Строковые
    Concat,
}

/// Унарный оператор
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
}

/// WHEN клаузула в CASE
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WhenClause {
    pub condition: Expression,
    pub result: Expression,
}

/// Список для IN операции
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum InList {
    Values(Vec<Expression>),
    Subquery(Box<SelectStatement>),
}

/// Построитель AST
pub struct AstBuilder {
    // Дополнительные данные для построения AST
    metadata: HashMap<String, String>,
}

impl AstBuilder {
    /// Создает новый построитель AST
    pub fn new() -> Result<Self> {
        Ok(Self {
            metadata: HashMap::new(),
        })
    }

    /// Добавляет метаданные
    pub fn add_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }

    /// Получает метаданные
    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }

    /// Создает простой SELECT запрос
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

    /// Создает простой INSERT запрос
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
