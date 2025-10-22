//! Парсер SQL для RustDB

use crate::common::{Error, Result};
use crate::parser::ast::*;
use crate::parser::lexer::Lexer;
use crate::parser::token::{Token, TokenType};
use std::collections::HashMap;

/// Рекурсивный парсер SQL с предиктивным анализом
pub struct SqlParser {
    lexer: Lexer,
    current_token: Option<Token>,
    peek_token: Option<Token>,
    /// Кэш для оптимизации парсинга
    parse_cache: HashMap<String, SqlStatement>,
    /// Настройки парсера
    settings: ParserSettings,
}

/// Настройки парсера
#[derive(Debug, Clone)]
pub struct ParserSettings {
    /// Максимальная глубина рекурсии
    pub max_recursion_depth: usize,
    /// Включить кэширование результатов
    pub enable_caching: bool,
    /// Строгий режим валидации
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
    /// Создает новый парсер SQL
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

    /// Создает парсер с настройками
    pub fn with_settings(input: &str, settings: ParserSettings) -> Result<Self> {
        let mut parser = Self::new(input)?;
        parser.settings = settings;
        Ok(parser)
    }

    /// Парсит SQL запрос
    pub fn parse(&mut self) -> Result<SqlStatement> {
        self.parse_statement()
    }

    /// Парсит несколько SQL запросов
    pub fn parse_multiple(&mut self) -> Result<Vec<SqlStatement>> {
        let mut statements = Vec::new();

        while self.current_token.is_some() && !self.match_token(&TokenType::Eof) {
            let stmt = self.parse_statement()?;
            statements.push(stmt);

            // Пропускаем точку с запятой если есть
            if self.match_token(&TokenType::Semicolon) {
                self.advance();
            }

            // Если следующий токен EOF, выходим
            if self.match_token(&TokenType::Eof) {
                break;
            }
        }

        Ok(statements)
    }

    /// Парсит одно SQL выражение
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
                    "Неожиданный токен: {:?}",
                    token.token_type
                ))),
            },
            None => Err(Error::parser("Неожиданный конец ввода".to_string())),
        }
    }

    /// Получает настройки парсера
    pub fn settings(&self) -> &ParserSettings {
        &self.settings
    }

    /// Очищает кэш парсера
    pub fn clear_cache(&mut self) {
        self.parse_cache.clear();
    }
}

impl SqlParser {
    /// Переходит к следующему токену
    fn advance(&mut self) {
        self.current_token = self.peek_token.take();
        self.peek_token = self.lexer.next_token().ok();
    }

    /// Проверяет, соответствует ли текущий токен ожидаемому типу
    fn match_token(&self, token_type: &TokenType) -> bool {
        match &self.current_token {
            Some(token) => {
                std::mem::discriminant(&token.token_type) == std::mem::discriminant(token_type)
            }
            None => false,
        }
    }

    /// Проверяет, является ли текущий токен ключевым словом
    fn match_keyword(&self, keyword: &str) -> bool {
        match &self.current_token {
            Some(token) => match &token.token_type {
                TokenType::Identifier => token.value.to_uppercase() == keyword.to_uppercase(),
                _ => {
                    // Проверяем специальные ключевые слова
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

    /// Ожидает определенный токен и переходит к следующему
    fn expect_token(&mut self, expected: &TokenType) -> Result<()> {
        if self.match_token(expected) {
            self.advance();
            Ok(())
        } else {
            Err(Error::parser(format!(
                "Ожидался токен {:?}, получен {:?}",
                expected,
                self.current_token.as_ref().map(|t| &t.token_type)
            )))
        }
    }

    /// Ожидает ключевое слово
    fn expect_keyword(&mut self, keyword: &str) -> Result<()> {
        if self.match_keyword(keyword) {
            self.advance();
            Ok(())
        } else {
            Err(Error::parser(format!(
                "Ожидалось ключевое слово '{}', получено {:?}",
                keyword,
                self.current_token.as_ref().map(|t| &t.token_type)
            )))
        }
    }

    /// Парсит идентификатор
    fn parse_identifier(&mut self) -> Result<String> {
        match &self.current_token {
            Some(token) => match &token.token_type {
                TokenType::Identifier => {
                    let result = token.value.clone();
                    self.advance();
                    Ok(result)
                }
                _ => Err(Error::parser("Ожидался идентификатор".to_string())),
            },
            None => Err(Error::parser("Неожиданный конец ввода".to_string())),
        }
    }

    /// Парсит список идентификаторов
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

    /// Парсит целое число
    fn parse_integer(&mut self) -> Result<i64> {
        match &self.current_token {
            Some(token) => match &token.token_type {
                TokenType::IntegerLiteral => {
                    let result = token
                        .value
                        .parse::<i64>()
                        .map_err(|e| Error::parser(format!("Неверное целое число: {}", e)))?;
                    self.advance();
                    Ok(result)
                }
                _ => Err(Error::parser("Ожидалось целое число".to_string())),
            },
            None => Err(Error::parser("Неожиданный конец ввода".to_string())),
        }
    }

    /// Парсит список выражений
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

    /// Парсит простое выражение (литерал или идентификатор)
    fn parse_simple_expression(&mut self) -> Result<Expression> {
        match &self.current_token {
            Some(token) => match &token.token_type {
                TokenType::IntegerLiteral => {
                    let value = token
                        .value
                        .parse::<i64>()
                        .map_err(|e| Error::parser(format!("Неверное целое число: {}", e)))?;
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
                        Error::parser(format!("Неверное число с плавающей точкой: {}", e))
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

                    // Проверяем, это функция или qualified identifier
                    if self.match_token(&TokenType::LeftParen) {
                        // Функция
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
                        // Простой идентификатор
                        Ok(Expression::Identifier(identifier))
                    }
                }
                _ => Err(Error::parser(format!(
                    "Неожиданный токен в выражении: {:?}",
                    token.token_type
                ))),
            },
            None => Err(Error::parser("Неожиданный конец ввода".to_string())),
        }
    }

    // Основные методы парсинга
    fn parse_select(&mut self) -> Result<SqlStatement> {
        self.expect_keyword("SELECT")?;

        // Простая реализация SELECT
        let mut select_list = Vec::new();

        // Парсим список колонок
        loop {
            if self.match_token(&TokenType::Multiply) {
                self.advance();
                select_list.push(SelectItem::Wildcard);
            } else {
                // Простое выражение - идентификатор или литерал
                let expr = if self.match_token(&TokenType::IntegerLiteral) {
                    let value = self
                        .current_token
                        .as_ref()
                        .unwrap()
                        .value
                        .parse::<i64>()
                        .map_err(|e| Error::parser(format!("Неверное целое число: {}", e)))?;
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

        // Парсим FROM клаузулу (опционально)
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

        // Парсим список колонок (опционально)
        let columns = if self.match_token(&TokenType::LeftParen) {
            self.advance();
            let cols = self.parse_identifier_list()?;
            self.expect_token(&TokenType::RightParen)?;
            Some(cols)
        } else {
            None
        };

        // Парсим VALUES или SELECT
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
                _ => return Err(Error::parser("Ожидался SELECT".to_string())),
            };
            InsertValues::Select(Box::new(select_stmt))
        } else {
            return Err(Error::parser("Ожидалось VALUES или SELECT".to_string()));
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

        // Парсим список присваиваний
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

        // Парсим WHERE клаузулу
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

        // Парсим WHERE клаузулу
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
            Err(Error::parser(
                "Поддерживается только CREATE TABLE".to_string(),
            ))
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
                    // Проверяем, есть ли длина в скобках
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
                            // Проверяем, есть ли длина в скобках
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
                        _ => Err(Error::parser(format!(
                            "Неизвестный тип данных: {}",
                            type_name
                        ))),
                    }
                }
                _ => Err(Error::parser("Ожидался тип данных".to_string())),
            },
            None => Err(Error::parser("Неожиданный конец ввода".to_string())),
        }
    }

    fn parse_alter(&mut self) -> Result<SqlStatement> {
        // TODO: Реализовать полный парсинг ALTER
        Err(Error::parser("ALTER не реализован".to_string()))
    }

    fn parse_drop(&mut self) -> Result<SqlStatement> {
        // TODO: Реализовать полный парсинг DROP
        Err(Error::parser("DROP не реализован".to_string()))
    }
}
