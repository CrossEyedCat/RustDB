//! SQL parser for RustDB

use crate::common::{Error, Result};
use crate::parser::ast::*;
use crate::parser::lexer::Lexer;
use crate::parser::token::{Token, TokenType};
use std::collections::HashMap;

/// Recursive SQL parser with predictive analysis
pub struct SqlParser {
    lexer: Lexer,
    current_token: Option<Token>,
    peek_token: Option<Token>,
    /// Cache for parsing optimization
    parse_cache: HashMap<String, SqlStatement>,
    /// Parser settings
    settings: ParserSettings,
}

/// Parser settings
#[derive(Debug, Clone)]
pub struct ParserSettings {
    /// Maximum recursion depth
    pub max_recursion_depth: usize,
    /// Enable result caching
    pub enable_caching: bool,
    /// Strict validation mode
    pub strict_validation: bool,
}

impl Default for ParserSettings {
    fn default() -> Self {
        Self {
            max_recursion_depth: 100,
            enable_caching: true,
            strict_validation: true,
        }
    }
}

impl SqlParser {
    /// Creates a new SQL parser
    pub fn new(input: &str) -> Result<Self> {
        let mut lexer = Lexer::new(input)?;
        let current_token = lexer.next_token().ok();
        let peek_token = lexer.next_token().ok();

        Ok(Self {
            lexer,
            current_token,
            peek_token,
            parse_cache: HashMap::new(),
            settings: ParserSettings::default(),
        })
    }

    /// Creates parser with settings
    pub fn with_settings(input: &str, settings: ParserSettings) -> Result<Self> {
        let mut parser = Self::new(input)?;
        parser.settings = settings;
        Ok(parser)
    }

    /// Parses SQL query
    pub fn parse(&mut self) -> Result<SqlStatement> {
        self.parse_statement()
    }

    /// Parses multiple SQL queries
    pub fn parse_multiple(&mut self) -> Result<Vec<SqlStatement>> {
        let mut statements = Vec::new();

        while self.current_token.is_some() && !self.match_token(&TokenType::Eof) {
            let stmt = self.parse_statement()?;
            statements.push(stmt);

            // Skip semicolon if present
            if self.match_token(&TokenType::Semicolon) {
                self.advance();
            }

            // If next token is EOF, exit
            if self.match_token(&TokenType::Eof) {
                break;
            }
        }

        Ok(statements)
    }

    /// Parses a single SQL statement
    fn parse_statement(&mut self) -> Result<SqlStatement> {
        match &self.current_token {
            Some(token) => match &token.token_type {
                TokenType::Select => self.parse_select(),
                TokenType::Insert => self.parse_insert(),
                TokenType::Update => self.parse_update(),
                TokenType::Delete => self.parse_delete(),
                TokenType::Create => self.parse_create(),
                TokenType::Alter => self.parse_alter(),
                TokenType::Drop => self.parse_drop(),
                TokenType::Begin => {
                    self.advance();
                    self.expect_keyword("TRANSACTION")?;
                    Ok(SqlStatement::BeginTransaction)
                }
                TokenType::Commit => {
                    self.advance();
                    if self.match_keyword("TRANSACTION") {
                        self.advance();
                    }
                    Ok(SqlStatement::CommitTransaction)
                }
                TokenType::Rollback => {
                    self.advance();
                    if self.match_keyword("TRANSACTION") {
                        self.advance();
                    }
                    Ok(SqlStatement::RollbackTransaction)
                }
                _ => Err(Error::parser(format!(
                    "Unexpected token: {:?}",
                    token.token_type
                ))),
            },
            None => Err(Error::parser("Unexpected end of input".to_string())),
        }
    }

    /// Gets parser settings
    pub fn settings(&self) -> &ParserSettings {
        &self.settings
    }

    /// Clears parser cache
    pub fn clear_cache(&mut self) {
        self.parse_cache.clear();
    }
}

impl SqlParser {
    /// Advances to next token
    fn advance(&mut self) {
        self.current_token = self.peek_token.take();
        self.peek_token = self.lexer.next_token().ok();
    }

    /// Checks if current token matches expected type
    fn match_token(&self, token_type: &TokenType) -> bool {
        match &self.current_token {
            Some(token) => {
                std::mem::discriminant(&token.token_type) == std::mem::discriminant(token_type)
            }
            None => false,
        }
    }

    /// Checks if current token is a keyword
    fn match_keyword(&self, keyword: &str) -> bool {
        match &self.current_token {
            Some(token) => match &token.token_type {
                TokenType::Identifier => token.value.to_uppercase() == keyword.to_uppercase(),
                _ => {
                    // Check special keywords
                    match keyword.to_uppercase().as_str() {
                        "SELECT" => token.token_type == TokenType::Select,
                        "INSERT" => token.token_type == TokenType::Insert,
                        "UPDATE" => token.token_type == TokenType::Update,
                        "DELETE" => token.token_type == TokenType::Delete,
                        "CREATE" => token.token_type == TokenType::Create,
                        "ALTER" => token.token_type == TokenType::Alter,
                        "DROP" => token.token_type == TokenType::Drop,
                        "FROM" => token.token_type == TokenType::From,
                        "WHERE" => token.token_type == TokenType::Where,
                        "AND" => token.token_type == TokenType::And,
                        "OR" => token.token_type == TokenType::Or,
                        "NOT" => token.token_type == TokenType::Not,
                        "NULL" => token.token_type == TokenType::Null,
                        "TRUE" => token.token_type == TokenType::True,
                        "FALSE" => token.token_type == TokenType::False,
                        "BEGIN" => token.token_type == TokenType::Begin,
                        "COMMIT" => token.token_type == TokenType::Commit,
                        "ROLLBACK" => token.token_type == TokenType::Rollback,
                        "TRANSACTION" => token.token_type == TokenType::Transaction,
                        "TABLE" => token.token_type == TokenType::Table,
                        "INTO" => token.token_type == TokenType::Into,
                        "VALUES" => token.token_type == TokenType::Values,
                        "SET" => token.token_type == TokenType::Set,
                        _ => false,
                    }
                }
            },
            None => false,
        }
    }

    /// Expects a specific token and advances to next
    fn expect_token(&mut self, expected: &TokenType) -> Result<()> {
        if self.match_token(expected) {
            self.advance();
            Ok(())
        } else {
            Err(Error::parser(format!(
                "Expected token {:?}, got {:?}",
                expected,
                self.current_token.as_ref().map(|t| &t.token_type)
            )))
        }
    }

    /// Expects a keyword
    fn expect_keyword(&mut self, keyword: &str) -> Result<()> {
        if self.match_keyword(keyword) {
            self.advance();
            Ok(())
        } else {
            Err(Error::parser(format!(
                "Expected keyword '{}', got {:?}",
                keyword,
                self.current_token.as_ref().map(|t| &t.token_type)
            )))
        }
    }

    /// Parses identifier
    fn parse_identifier(&mut self) -> Result<String> {
        match &self.current_token {
            Some(token) => match &token.token_type {
                TokenType::Identifier => {
                    let result = token.value.clone();
                    self.advance();
                    Ok(result)
                }
                _ => Err(Error::parser("Expected identifier".to_string())),
            },
            None => Err(Error::parser("Unexpected end of input".to_string())),
        }
    }

    /// Parses identifier list
    fn parse_identifier_list(&mut self) -> Result<Vec<String>> {
        let mut identifiers = Vec::new();

        loop {
            identifiers.push(self.parse_identifier()?);

            if !self.match_token(&TokenType::Comma) {
                break;
            }
            self.advance();
        }

        Ok(identifiers)
    }

    /// Parses integer
    fn parse_integer(&mut self) -> Result<i64> {
        match &self.current_token {
            Some(token) => match &token.token_type {
                TokenType::IntegerLiteral => {
                    let result = token
                        .value
                        .parse::<i64>()
                        .map_err(|e| Error::parser(format!("Invalid integer: {}", e)))?;
                    self.advance();
                    Ok(result)
                }
                _ => Err(Error::parser("Expected integer".to_string())),
            },
            None => Err(Error::parser("Unexpected end of input".to_string())),
        }
    }

    /// Parses expression list
    fn parse_expression_list(&mut self) -> Result<Vec<Expression>> {
        let mut expressions = Vec::new();

        loop {
            expressions.push(self.parse_simple_expression()?);

            if !self.match_token(&TokenType::Comma) {
                break;
            }
            self.advance();
        }

        Ok(expressions)
    }

    /// Parses simple expression (literal or identifier)
    fn parse_simple_expression(&mut self) -> Result<Expression> {
        match &self.current_token {
            Some(token) => match &token.token_type {
                TokenType::IntegerLiteral => {
                    let value = token
                        .value
                        .parse::<i64>()
                        .map_err(|e| Error::parser(format!("Invalid integer: {}", e)))?;
                    self.advance();
                    Ok(Expression::Literal(Literal::Integer(value)))
                }
                TokenType::StringLiteral => {
                    let value = token.value.clone();
                    self.advance();
                    Ok(Expression::Literal(Literal::String(value)))
                }
                TokenType::FloatLiteral => {
                    let value = token.value.parse::<f64>().map_err(|e| {
                        Error::parser(format!("Invalid floating point number: {}", e))
                    })?;
                    self.advance();
                    Ok(Expression::Literal(Literal::Float(value)))
                }
                TokenType::True => {
                    self.advance();
                    Ok(Expression::Literal(Literal::Boolean(true)))
                }
                TokenType::False => {
                    self.advance();
                    Ok(Expression::Literal(Literal::Boolean(false)))
                }
                TokenType::Null => {
                    self.advance();
                    Ok(Expression::Literal(Literal::Null))
                }
                TokenType::Identifier => {
                    let identifier = self.parse_identifier()?;

                    // Check if it's a function or qualified identifier
                    if self.match_token(&TokenType::LeftParen) {
                        // Function
                        self.advance();
                        let mut args = Vec::new();

                        if !self.match_token(&TokenType::RightParen) {
                            args = self.parse_expression_list()?;
                        }

                        self.expect_token(&TokenType::RightParen)?;

                        Ok(Expression::Function {
                            name: identifier,
                            args,
                        })
                    } else if self.match_token(&TokenType::Dot) {
                        // Qualified identifier
                        self.advance();
                        let column = self.parse_identifier()?;
                        Ok(Expression::QualifiedIdentifier {
                            table: identifier,
                            column,
                        })
                    } else {
                        // Simple identifier
                        Ok(Expression::Identifier(identifier))
                    }
                }
                _ => Err(Error::parser(format!(
                    "Unexpected token in expression: {:?}",
                    token.token_type
                ))),
            },
            None => Err(Error::parser("Unexpected end of input".to_string())),
        }
    }

    // Main parsing methods
    fn parse_select(&mut self) -> Result<SqlStatement> {
        self.expect_keyword("SELECT")?;

        // Simple SELECT implementation
        let mut select_list = Vec::new();

        // Parse column list
        loop {
            if self.match_token(&TokenType::Multiply) {
                self.advance();
                select_list.push(SelectItem::Wildcard);
            } else {
                // Simple expression - identifier or literal
                let expr = if self.match_token(&TokenType::IntegerLiteral) {
                    let value = self
                        .current_token
                        .as_ref()
                        .unwrap()
                        .value
                        .parse::<i64>()
                        .map_err(|e| Error::parser(format!("Invalid integer: {}", e)))?;
                    self.advance();
                    Expression::Literal(Literal::Integer(value))
                } else if self.match_token(&TokenType::StringLiteral) {
                    let value = self.current_token.as_ref().unwrap().value.clone();
                    self.advance();
                    Expression::Literal(Literal::String(value))
                } else {
                    let identifier = self.parse_identifier()?;
                    Expression::Identifier(identifier)
                };
                select_list.push(SelectItem::Expression { expr, alias: None });
            }

            if !self.match_token(&TokenType::Comma) {
                break;
            }
            self.advance();
        }

        // Parse FROM clause (optional)
        let from = if self.match_keyword("FROM") {
            self.advance();
            let table_name = self.parse_identifier()?;
            Some(FromClause {
                table: TableReference::Table {
                    name: table_name,
                    alias: None,
                },
                joins: Vec::new(),
            })
        } else {
            None
        };

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

    fn parse_insert(&mut self) -> Result<SqlStatement> {
        self.expect_keyword("INSERT")?;
        self.expect_keyword("INTO")?;

        let table = self.parse_identifier()?;

        // Parse column list (optional)
        let columns = if self.match_token(&TokenType::LeftParen) {
            self.advance();
            let cols = self.parse_identifier_list()?;
            self.expect_token(&TokenType::RightParen)?;
            Some(cols)
        } else {
            None
        };

        // Parse VALUES or SELECT
        let values = if self.match_keyword("VALUES") {
            self.advance();
            let mut rows = Vec::new();

            loop {
                self.expect_token(&TokenType::LeftParen)?;
                let row = self.parse_expression_list()?;
                self.expect_token(&TokenType::RightParen)?;
                rows.push(row);

                if !self.match_token(&TokenType::Comma) {
                    break;
                }
                self.advance();
            }

            InsertValues::Values(rows)
        } else if self.match_keyword("SELECT") {
            let select_stmt = match self.parse_select()? {
                SqlStatement::Select(select) => select,
                _ => return Err(Error::parser("Expected SELECT".to_string())),
            };
            InsertValues::Select(Box::new(select_stmt))
        } else {
            return Err(Error::parser("Expected VALUES or SELECT".to_string()));
        };

        Ok(SqlStatement::Insert(InsertStatement {
            table,
            columns,
            values,
        }))
    }

    fn parse_update(&mut self) -> Result<SqlStatement> {
        self.expect_keyword("UPDATE")?;

        let table = self.parse_identifier()?;

        self.expect_keyword("SET")?;

        let mut assignments = Vec::new();

        // Parse assignment list
        loop {
            let column = self.parse_identifier()?;
            self.expect_token(&TokenType::Equal)?;
            let value = self.parse_simple_expression()?;

            assignments.push(Assignment { column, value });

            if !self.match_token(&TokenType::Comma) {
                break;
            }
            self.advance();
        }

        // Parse WHERE clause
        let where_clause = if self.match_keyword("WHERE") {
            self.advance();
            Some(self.parse_simple_expression()?)
        } else {
            None
        };

        Ok(SqlStatement::Update(UpdateStatement {
            table,
            assignments,
            where_clause,
        }))
    }

    fn parse_delete(&mut self) -> Result<SqlStatement> {
        self.expect_keyword("DELETE")?;
        self.expect_keyword("FROM")?;

        let table = self.parse_identifier()?;

        // Parse WHERE clause
        let where_clause = if self.match_keyword("WHERE") {
            self.advance();
            Some(self.parse_simple_expression()?)
        } else {
            None
        };

        Ok(SqlStatement::Delete(DeleteStatement {
            table,
            where_clause,
        }))
    }

    fn parse_create(&mut self) -> Result<SqlStatement> {
        self.expect_keyword("CREATE")?;

        if self.match_keyword("TABLE") {
            self.advance();
            self.parse_create_table()
        } else {
            Err(Error::parser("Only CREATE TABLE is supported".to_string()))
        }
    }

    fn parse_create_table(&mut self) -> Result<SqlStatement> {
        let table_name = self.parse_identifier()?;

        self.expect_token(&TokenType::LeftParen)?;

        let mut columns = Vec::new();

        loop {
            let column_name = self.parse_identifier()?;
            let data_type = self.parse_data_type()?;

            columns.push(ColumnDefinition {
                name: column_name,
                data_type,
                constraints: Vec::new(),
            });

            if !self.match_token(&TokenType::Comma) {
                break;
            }
            self.advance();
        }

        self.expect_token(&TokenType::RightParen)?;

        Ok(SqlStatement::CreateTable(CreateTableStatement {
            table_name,
            columns,
            constraints: Vec::new(),
            if_not_exists: false,
        }))
    }

    fn parse_data_type(&mut self) -> Result<DataType> {
        match &self.current_token {
            Some(token) => match &token.token_type {
                TokenType::Integer => {
                    self.advance();
                    Ok(DataType::Integer)
                }
                TokenType::Text => {
                    self.advance();
                    Ok(DataType::Text)
                }
                TokenType::Varchar => {
                    self.advance();
                    // Check if length is in parentheses
                    let length = if self.match_token(&TokenType::LeftParen) {
                        self.advance();
                        let len = self.parse_integer()?;
                        self.expect_token(&TokenType::RightParen)?;
                        Some(len as u16)
                    } else {
                        None
                    };
                    Ok(DataType::Varchar { length })
                }
                TokenType::Boolean => {
                    self.advance();
                    Ok(DataType::Boolean)
                }
                TokenType::Date => {
                    self.advance();
                    Ok(DataType::Date)
                }
                TokenType::Time => {
                    self.advance();
                    Ok(DataType::Time)
                }
                TokenType::Timestamp => {
                    self.advance();
                    Ok(DataType::Timestamp)
                }
                TokenType::Identifier => {
                    let type_name = token.value.to_uppercase();
                    self.advance();

                    match type_name.as_str() {
                        "INTEGER" | "INT" => Ok(DataType::Integer),
                        "TEXT" => Ok(DataType::Text),
                        "REAL" | "FLOAT" => Ok(DataType::Real),
                        "VARCHAR" => {
                            // Check if length is in parentheses
                            let length = if self.match_token(&TokenType::LeftParen) {
                                self.advance();
                                let len = self.parse_integer()?;
                                self.expect_token(&TokenType::RightParen)?;
                                Some(len as u16)
                            } else {
                                None
                            };
                            Ok(DataType::Varchar { length })
                        }
                        "BOOLEAN" | "BOOL" => Ok(DataType::Boolean),
                        "DATE" => Ok(DataType::Date),
                        "TIME" => Ok(DataType::Time),
                        "TIMESTAMP" => Ok(DataType::Timestamp),
                        _ => Err(Error::parser(format!("Unknown data type: {}", type_name))),
                    }
                }
                _ => Err(Error::parser("Expected data type".to_string())),
            },
            None => Err(Error::parser("Unexpected end of input".to_string())),
        }
    }

    fn parse_alter(&mut self) -> Result<SqlStatement> {
        // TODO: Implement full ALTER parsing
        Err(Error::parser("ALTER not implemented".to_string()))
    }

    fn parse_drop(&mut self) -> Result<SqlStatement> {
        // TODO: Implement full DROP parsing
        Err(Error::parser("DROP not implemented".to_string()))
    }
}
