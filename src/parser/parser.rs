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
                TokenType::Prepare => self.parse_prepare(),
                TokenType::Execute => self.parse_execute(),
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
                        "PREPARE" => token.token_type == TokenType::Prepare,
                        "EXECUTE" => token.token_type == TokenType::Execute,
                        "AS" => token.token_type == TokenType::As,
                        "KEY" => token.token_type == TokenType::Key,
                        "REFERENCES" => token.token_type == TokenType::References,
                        "DEFAULT" => token.token_type == TokenType::Default,
                        "CHECK" => token.token_type == TokenType::Check,
                        "PRIMARY" => token.token_type == TokenType::Primary,
                        "FOREIGN" => token.token_type == TokenType::Foreign,
                        "UNIQUE" => token.token_type == TokenType::Unique,
                        "CASE" => token.token_type == TokenType::Case,
                        "WHEN" => token.token_type == TokenType::When,
                        "THEN" => token.token_type == TokenType::Then,
                        "ELSE" => token.token_type == TokenType::Else,
                        "END" => token.token_type == TokenType::End,
                        "INDEX" => token.token_type == TokenType::Index,
                        "ON" => token.token_type == TokenType::On,
                        "TABLE" => token.token_type == TokenType::Table,
                        "INTO" => token.token_type == TokenType::Into,
                        "VALUES" => token.token_type == TokenType::Values,
                        "SET" => token.token_type == TokenType::Set,
                        "EXISTS" => token.token_type == TokenType::Exists,
                        "CONSTRAINT" => token.token_type == TokenType::Constraint,
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
            expressions.push(self.parse_expression()?);

            if !self.match_token(&TokenType::Comma) {
                break;
            }
            self.advance();
        }

        Ok(expressions)
    }

    fn parse_expression(&mut self) -> Result<Expression> {
        self.parse_or_expression()
    }

    fn parse_or_expression(&mut self) -> Result<Expression> {
        let mut left = self.parse_and_expression()?;
        while self.match_token(&TokenType::Or) {
            self.advance();
            let right = self.parse_and_expression()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and_expression(&mut self) -> Result<Expression> {
        let mut left = self.parse_not_expression()?;
        while self.match_token(&TokenType::And) {
            self.advance();
            let right = self.parse_not_expression()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_not_expression(&mut self) -> Result<Expression> {
        if self.match_token(&TokenType::Not) {
            self.advance();
            let expr = self.parse_not_expression()?;
            return Ok(Expression::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(expr),
            });
        }
        self.parse_comparison_expression()
    }

    fn parse_comparison_expression(&mut self) -> Result<Expression> {
        let mut left = self.parse_additive_expression()?;

        // SQL predicates (postfix-ish forms) take precedence here.
        if self.match_token(&TokenType::IsNull) || self.match_token(&TokenType::IsNotNull) {
            let negated = self.match_token(&TokenType::IsNotNull);
            self.advance();
            return Ok(Expression::IsNull {
                expr: Box::new(left),
                negated,
            });
        }

        // [NOT] LIKE
        if self.match_token(&TokenType::Not)
            && matches!(
                self.peek_token.as_ref().map(|t| t.token_type),
                Some(TokenType::Like)
            )
        {
            self.advance(); // NOT
            self.advance(); // LIKE
            let pattern = self.parse_additive_expression()?;
            return Ok(Expression::Like {
                expr: Box::new(left),
                pattern: Box::new(pattern),
                negated: true,
            });
        }
        if self.match_token(&TokenType::Like) {
            self.advance();
            let pattern = self.parse_additive_expression()?;
            return Ok(Expression::Like {
                expr: Box::new(left),
                pattern: Box::new(pattern),
                negated: false,
            });
        }

        // BETWEEN
        if self.match_token(&TokenType::Between) {
            self.advance();
            let low = self.parse_additive_expression()?;
            self.expect_keyword("AND")?;
            let high = self.parse_additive_expression()?;
            return Ok(Expression::Between {
                expr: Box::new(left),
                low: Box::new(low),
                high: Box::new(high),
            });
        }

        // IN ( ... )
        if self.match_token(&TokenType::In) {
            self.advance();
            self.expect_token(&TokenType::LeftParen)?;
            let list = if self.match_keyword("SELECT") {
                let stmt = self.parse_select()?;
                let SqlStatement::Select(sel) = stmt else {
                    return Err(Error::parser("Expected SELECT in IN(subquery)".to_string()));
                };
                self.expect_token(&TokenType::RightParen)?;
                InList::Subquery(Box::new(sel))
            } else {
                let vals = if self.match_token(&TokenType::RightParen) {
                    Vec::new()
                } else {
                    self.parse_expression_list()?
                };
                self.expect_token(&TokenType::RightParen)?;
                InList::Values(vals)
            };
            return Ok(Expression::In {
                expr: Box::new(left),
                list,
            });
        }

        // Standard binary comparisons.
        let op = if self.match_token(&TokenType::Equal) {
            self.advance();
            Some(BinaryOperator::Equal)
        } else if self.match_token(&TokenType::NotEqual) {
            self.advance();
            Some(BinaryOperator::NotEqual)
        } else if self.match_token(&TokenType::Less) {
            self.advance();
            Some(BinaryOperator::LessThan)
        } else if self.match_token(&TokenType::Greater) {
            self.advance();
            Some(BinaryOperator::GreaterThan)
        } else if self.match_token(&TokenType::LessEqual) {
            self.advance();
            Some(BinaryOperator::LessThanOrEqual)
        } else if self.match_token(&TokenType::GreaterEqual) {
            self.advance();
            Some(BinaryOperator::GreaterThanOrEqual)
        } else {
            None
        };

        let Some(op) = op else {
            return Ok(left);
        };
        let right = self.parse_additive_expression()?;
        left = Expression::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        };
        Ok(left)
    }

    fn parse_additive_expression(&mut self) -> Result<Expression> {
        let mut left = self.parse_multiplicative_expression()?;
        loop {
            let op = if self.match_token(&TokenType::Plus) {
                self.advance();
                Some(BinaryOperator::Add)
            } else if self.match_token(&TokenType::Minus) {
                self.advance();
                Some(BinaryOperator::Subtract)
            } else {
                None
            };
            let Some(op) = op else {
                break;
            };
            let right = self.parse_multiplicative_expression()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_multiplicative_expression(&mut self) -> Result<Expression> {
        let mut left = self.parse_primary_expression()?;
        loop {
            let op = if self.match_token(&TokenType::Multiply) {
                self.advance();
                Some(BinaryOperator::Multiply)
            } else if self.match_token(&TokenType::Divide) {
                self.advance();
                Some(BinaryOperator::Divide)
            } else if self.match_token(&TokenType::Modulo) {
                self.advance();
                Some(BinaryOperator::Modulo)
            } else {
                None
            };
            let Some(op) = op else {
                break;
            };
            let right = self.parse_primary_expression()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_primary_expression(&mut self) -> Result<Expression> {
        if self.match_token(&TokenType::Multiply) {
            self.advance();
            return Ok(Expression::Identifier("*".to_string()));
        }
        if self.match_token(&TokenType::LeftParen) {
            self.advance();
            let expr = self.parse_expression()?;
            self.expect_token(&TokenType::RightParen)?;
            return Ok(expr);
        }
        self.parse_simple_expression()
    }

    /// Parses simple expression (literal or identifier)
    fn parse_simple_expression(&mut self) -> Result<Expression> {
        match &self.current_token {
            Some(token) => match &token.token_type {
                TokenType::Exists => {
                    self.advance();
                    self.expect_token(&TokenType::LeftParen)?;
                    let stmt = self.parse_select()?;
                    let SqlStatement::Select(sel) = stmt else {
                        return Err(Error::parser(
                            "Expected SELECT in EXISTS(subquery)".to_string(),
                        ));
                    };
                    self.expect_token(&TokenType::RightParen)?;
                    Ok(Expression::Exists(Box::new(sel)))
                }
                TokenType::Case => {
                    self.advance();
                    // CASE [expr] WHEN ... THEN ... [ELSE ...] END
                    let expr = if self.match_token(&TokenType::When) {
                        None
                    } else {
                        Some(Box::new(self.parse_expression()?))
                    };
                    let mut when_clauses = Vec::new();
                    while self.match_token(&TokenType::When) {
                        self.advance();
                        let condition = self.parse_expression()?;
                        self.expect_keyword("THEN")?;
                        let result = self.parse_expression()?;
                        when_clauses.push(WhenClause { condition, result });
                    }
                    let else_clause = if self.match_token(&TokenType::Else) {
                        self.advance();
                        Some(Box::new(self.parse_expression()?))
                    } else {
                        None
                    };
                    self.expect_keyword("END")?;
                    Ok(Expression::Case {
                        expr,
                        when_clauses,
                        else_clause,
                    })
                }
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
                TokenType::Identifier
                | TokenType::Count
                | TokenType::Sum
                | TokenType::Avg
                | TokenType::Min
                | TokenType::Max => {
                    let identifier = match &token.token_type {
                        TokenType::Identifier => self.parse_identifier()?,
                        _ => {
                            let v = token.value.clone();
                            self.advance();
                            v
                        }
                    };

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

    fn parse_where_expression(&mut self) -> Result<Expression> {
        self.parse_expression()
    }

    // Main parsing methods
    fn parse_select(&mut self) -> Result<SqlStatement> {
        self.expect_keyword("SELECT")?;

        let distinct = if self.match_token(&TokenType::Distinct) {
            self.advance();
            true
        } else {
            false
        };

        // Simple SELECT implementation
        let mut select_list = Vec::new();

        // Parse column list
        loop {
            if self.match_token(&TokenType::Multiply) {
                self.advance();
                select_list.push(SelectItem::Wildcard);
            } else {
                let expr = self.parse_expression()?;
                let alias = if self.match_keyword("AS") {
                    self.advance();
                    Some(self.parse_identifier()?)
                } else {
                    None
                };
                select_list.push(SelectItem::Expression { expr, alias });
            }

            if !self.match_token(&TokenType::Comma) {
                break;
            }
            self.advance();
        }

        // Parse FROM clause (optional)
        let from = if self.match_keyword("FROM") {
            self.advance();

            fn parse_table_ref(p: &mut SqlParser) -> Result<TableReference> {
                if p.match_token(&TokenType::LeftParen) {
                    // Subquery in FROM: (SELECT ...) [AS] alias
                    p.advance();
                    let stmt = p.parse_select()?;
                    let SqlStatement::Select(sel) = stmt else {
                        return Err(Error::parser("Expected SELECT in subquery".to_string()));
                    };
                    p.expect_token(&TokenType::RightParen)?;
                    if p.match_keyword("AS") {
                        p.advance();
                    }
                    let alias = p.parse_identifier()?;
                    Ok(TableReference::Subquery {
                        query: Box::new(sel),
                        alias,
                    })
                } else {
                    // Simple table reference with optional alias.
                    let name = p.parse_identifier()?;
                    if p.match_keyword("AS") {
                        p.advance();
                    }
                    let alias = if matches!(
                        p.current_token.as_ref().map(|t| t.token_type),
                        Some(TokenType::Identifier)
                    ) {
                        Some(p.parse_identifier()?)
                    } else {
                        None
                    };
                    Ok(TableReference::Table { name, alias })
                }
            }

            let mut from = FromClause {
                table: parse_table_ref(self)?,
                joins: Vec::new(),
            };

            loop {
                let join_type = if self.match_token(&TokenType::InnerJoin) {
                    self.advance();
                    Some(JoinType::Inner)
                } else if self.match_token(&TokenType::LeftJoin) {
                    self.advance();
                    Some(JoinType::Left)
                } else if self.match_token(&TokenType::RightJoin) {
                    self.advance();
                    Some(JoinType::Right)
                } else if self.match_token(&TokenType::FullJoin) {
                    self.advance();
                    Some(JoinType::Full)
                } else if self.match_token(&TokenType::CrossJoin) {
                    self.advance();
                    Some(JoinType::Cross)
                } else if self.match_token(&TokenType::Join) {
                    self.advance();
                    Some(JoinType::Inner)
                } else {
                    None
                };

                let Some(join_type) = join_type else {
                    break;
                };

                let table = parse_table_ref(self)?;

                let condition = if self.match_keyword("ON") {
                    self.advance();
                    Some(self.parse_expression()?)
                } else if self.match_token(&TokenType::Using) {
                    self.advance();
                    self.expect_token(&TokenType::LeftParen)?;
                    let cols = self.parse_identifier_list()?;
                    self.expect_token(&TokenType::RightParen)?;
                    // Represent USING at the join clause level; semantic expansion happens later.
                    // We store the columns separately and leave `condition` empty.
                    // (Planner/analyzer can rewrite USING into equality predicates.)
                    from.joins.push(JoinClause {
                        join_type,
                        table,
                        condition: None,
                        using_columns: Some(cols),
                    });
                    continue;
                } else {
                    None
                };

                from.joins.push(JoinClause {
                    join_type,
                    table,
                    condition,
                    using_columns: None,
                });
            }

            Some(from)
        } else {
            None
        };

        let mut where_clause = None;
        if self.match_keyword("WHERE") {
            self.advance();
            where_clause = Some(self.parse_where_expression()?);
        }

        let mut group_by = Vec::new();
        if self.match_token(&TokenType::GroupBy) {
            self.advance();
            loop {
                group_by.push(self.parse_expression()?);
                if !self.match_token(&TokenType::Comma) {
                    break;
                }
                self.advance();
            }
        }

        let mut having = None;
        if self.match_token(&TokenType::Having) {
            self.advance();
            having = Some(self.parse_expression()?);
        }

        let mut order_by = Vec::new();
        if self.match_token(&TokenType::OrderBy) {
            self.advance();
            loop {
                let expr = self.parse_expression()?;
                let direction = if self.match_token(&TokenType::Desc) {
                    self.advance();
                    OrderDirection::Desc
                } else if self.match_token(&TokenType::Asc) {
                    self.advance();
                    OrderDirection::Asc
                } else {
                    OrderDirection::Asc
                };
                order_by.push(OrderByItem { expr, direction });
                if !self.match_token(&TokenType::Comma) {
                    break;
                }
                self.advance();
            }
        }

        let mut limit = None;
        if self.match_token(&TokenType::Limit) {
            self.advance();
            let n = self.parse_integer()?;
            if n < 0 {
                return Err(Error::parser("LIMIT must be non-negative".to_string()));
            }
            limit = Some(n as u64);
        }

        let mut offset = None;
        if self.match_token(&TokenType::Offset) {
            self.advance();
            let n = self.parse_integer()?;
            if n < 0 {
                return Err(Error::parser("OFFSET must be non-negative".to_string()));
            }
            offset = Some(n as u64);
        }

        let base = SelectStatement {
            distinct,
            select_list,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
        };

        // SQL-92 set operations: UNION [ALL], INTERSECT, EXCEPT.
        if self.match_token(&TokenType::Union)
            || self.match_token(&TokenType::Intersect)
            || self.match_token(&TokenType::Except)
        {
            let op = if self.match_token(&TokenType::Union) {
                self.advance();
                SetOperator::Union
            } else if self.match_token(&TokenType::Intersect) {
                self.advance();
                SetOperator::Intersect
            } else {
                self.advance();
                SetOperator::Except
            };

            let all = if op == SetOperator::Union && self.match_token(&TokenType::All) {
                self.advance();
                true
            } else {
                false
            };

            // Right operand must be a SELECT (parentheses not yet supported here).
            if !self.match_keyword("SELECT") {
                return Err(Error::parser(
                    "Expected SELECT after set operator".to_string(),
                ));
            }
            let rhs_stmt = self.parse_select()?;
            let SqlStatement::Select(right) = rhs_stmt else {
                return Err(Error::parser(
                    "Expected SELECT after set operator".to_string(),
                ));
            };

            return Ok(SqlStatement::SetOperation(Box::new(
                SetOperationStatement {
                    left: base,
                    op,
                    all,
                    right,
                },
            )));
        }

        Ok(SqlStatement::Select(base))
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
            Some(self.parse_where_expression()?)
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
            Some(self.parse_where_expression()?)
        } else {
            None
        };

        Ok(SqlStatement::Delete(DeleteStatement {
            table,
            where_clause,
        }))
    }

    fn parse_prepare(&mut self) -> Result<SqlStatement> {
        self.expect_keyword("PREPARE")?;
        let name = self.parse_identifier()?;
        self.expect_keyword("AS")?;

        let statement = match &self.current_token {
            Some(token) => match &token.token_type {
                TokenType::Select => self.parse_select()?,
                TokenType::Insert => self.parse_insert()?,
                TokenType::Update => self.parse_update()?,
                TokenType::Delete => self.parse_delete()?,
                _ => {
                    return Err(Error::parser(
                        "PREPARE only supports SELECT, INSERT, UPDATE, DELETE".to_string(),
                    ));
                }
            },
            None => return Err(Error::parser("Unexpected end after AS".to_string())),
        };

        Ok(SqlStatement::Prepare(PrepareStatement {
            name,
            statement: Box::new(statement),
        }))
    }

    fn parse_execute(&mut self) -> Result<SqlStatement> {
        self.expect_keyword("EXECUTE")?;
        let name = self.parse_identifier()?;

        let params = if self.match_token(&TokenType::LeftParen) {
            self.advance();
            let list = self.parse_expression_list()?;
            self.expect_token(&TokenType::RightParen)?;
            list
        } else {
            Vec::new()
        };

        Ok(SqlStatement::Execute(ExecuteStatement { name, params }))
    }

    fn parse_create(&mut self) -> Result<SqlStatement> {
        self.expect_keyword("CREATE")?;

        if self.match_keyword("TABLE") {
            self.advance();
            self.parse_create_table()
        } else if self.match_keyword("INDEX") {
            self.advance();
            self.parse_create_index()
        } else {
            Err(Error::parser(
                "Only CREATE TABLE and CREATE INDEX are supported".to_string(),
            ))
        }
    }

    fn parse_create_index(&mut self) -> Result<SqlStatement> {
        let index_name = self.parse_identifier()?;
        self.expect_keyword("ON")?;
        let table_name = self.parse_identifier()?;
        self.expect_token(&TokenType::LeftParen)?;

        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_identifier()?);
            if !self.match_token(&TokenType::Comma) {
                break;
            }
            self.advance();
        }
        self.expect_token(&TokenType::RightParen)?;

        Ok(SqlStatement::CreateIndex(CreateIndexStatement {
            index_name,
            table_name,
            columns,
        }))
    }

    fn parse_create_table(&mut self) -> Result<SqlStatement> {
        let table_name = self.parse_identifier()?;

        self.expect_token(&TokenType::LeftParen)?;

        let mut columns: Vec<ColumnDefinition> = Vec::new();
        let mut constraints: Vec<TableConstraint> = Vec::new();

        // CREATE TABLE t ( <coldef or table constraint>, ... )
        loop {
            // Table-level constraints start with keywords.
            if self.match_token(&TokenType::Primary)
                || self.match_token(&TokenType::Unique)
                || self.match_token(&TokenType::Foreign)
                || self.match_token(&TokenType::Check)
                || self.match_token(&TokenType::Constraint)
            {
                // Optional CONSTRAINT <name> prefix (name currently ignored at AST level).
                if self.match_token(&TokenType::Constraint) {
                    self.advance();
                    let _name = self.parse_identifier()?;
                }

                let tc = if self.match_token(&TokenType::Primary) {
                    self.advance();
                    self.expect_keyword("KEY")?;
                    self.expect_token(&TokenType::LeftParen)?;
                    let cols = self.parse_identifier_list()?;
                    self.expect_token(&TokenType::RightParen)?;
                    TableConstraint::PrimaryKey(cols)
                } else if self.match_token(&TokenType::Unique) {
                    self.advance();
                    self.expect_token(&TokenType::LeftParen)?;
                    let cols = self.parse_identifier_list()?;
                    self.expect_token(&TokenType::RightParen)?;
                    TableConstraint::Unique(cols)
                } else if self.match_token(&TokenType::Foreign) {
                    self.advance();
                    self.expect_keyword("KEY")?;
                    self.expect_token(&TokenType::LeftParen)?;
                    let cols = self.parse_identifier_list()?;
                    self.expect_token(&TokenType::RightParen)?;
                    self.expect_keyword("REFERENCES")?;
                    let ref_table = self.parse_identifier()?;
                    let ref_cols = if self.match_token(&TokenType::LeftParen) {
                        self.advance();
                        let cols2 = self.parse_identifier_list()?;
                        self.expect_token(&TokenType::RightParen)?;
                        cols2
                    } else {
                        Vec::new()
                    };
                    TableConstraint::ForeignKey {
                        columns: cols,
                        referenced_table: ref_table,
                        referenced_columns: ref_cols,
                    }
                } else if self.match_token(&TokenType::Check) {
                    self.advance();
                    self.expect_token(&TokenType::LeftParen)?;
                    let expr = self.parse_expression()?;
                    self.expect_token(&TokenType::RightParen)?;
                    TableConstraint::Check(expr)
                } else {
                    return Err(Error::parser("Unsupported table constraint".to_string()));
                };

                constraints.push(tc);
            } else {
                // Column definition: name type [constraints...]
                let column_name = self.parse_identifier()?;
                let data_type = self.parse_data_type()?;

                let mut col_constraints: Vec<ColumnConstraint> = Vec::new();
                loop {
                    if self.match_token(&TokenType::NotNull) {
                        self.advance();
                        col_constraints.push(ColumnConstraint::NotNull);
                        continue;
                    }
                    if self.match_token(&TokenType::Unique) {
                        self.advance();
                        col_constraints.push(ColumnConstraint::Unique);
                        continue;
                    }
                    if self.match_token(&TokenType::Primary) {
                        self.advance();
                        self.expect_keyword("KEY")?;
                        col_constraints.push(ColumnConstraint::PrimaryKey);
                        continue;
                    }
                    if self.match_token(&TokenType::Default) {
                        self.advance();
                        let expr = self.parse_expression()?;
                        col_constraints.push(ColumnConstraint::Default(expr));
                        continue;
                    }
                    if self.match_token(&TokenType::Check) {
                        self.advance();
                        self.expect_token(&TokenType::LeftParen)?;
                        let expr = self.parse_expression()?;
                        self.expect_token(&TokenType::RightParen)?;
                        col_constraints.push(ColumnConstraint::Check(expr));
                        continue;
                    }
                    if self.match_token(&TokenType::References) {
                        self.advance();
                        let table = self.parse_identifier()?;
                        let column = if self.match_token(&TokenType::LeftParen) {
                            self.advance();
                            let c = self.parse_identifier()?;
                            self.expect_token(&TokenType::RightParen)?;
                            Some(c)
                        } else {
                            None
                        };
                        col_constraints.push(ColumnConstraint::References { table, column });
                        continue;
                    }

                    break;
                }

                columns.push(ColumnDefinition {
                    name: column_name,
                    data_type,
                    constraints: col_constraints,
                });
            }

            if self.match_token(&TokenType::Comma) {
                self.advance();
                continue;
            }
            break;
        }

        self.expect_token(&TokenType::RightParen)?;

        Ok(SqlStatement::CreateTable(CreateTableStatement {
            table_name,
            columns,
            constraints,
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

    /// Parses a table-level constraint (without optional `CONSTRAINT name` prefix).
    fn parse_table_constraint_only(&mut self) -> Result<TableConstraint> {
        if self.match_token(&TokenType::Primary) {
            self.advance();
            self.expect_keyword("KEY")?;
            self.expect_token(&TokenType::LeftParen)?;
            let cols = self.parse_identifier_list()?;
            self.expect_token(&TokenType::RightParen)?;
            Ok(TableConstraint::PrimaryKey(cols))
        } else if self.match_token(&TokenType::Unique) {
            self.advance();
            self.expect_token(&TokenType::LeftParen)?;
            let cols = self.parse_identifier_list()?;
            self.expect_token(&TokenType::RightParen)?;
            Ok(TableConstraint::Unique(cols))
        } else if self.match_token(&TokenType::Foreign) {
            self.advance();
            self.expect_keyword("KEY")?;
            self.expect_token(&TokenType::LeftParen)?;
            let cols = self.parse_identifier_list()?;
            self.expect_token(&TokenType::RightParen)?;
            self.expect_keyword("REFERENCES")?;
            let ref_table = self.parse_identifier()?;
            let ref_cols = if self.match_token(&TokenType::LeftParen) {
                self.advance();
                let cols2 = self.parse_identifier_list()?;
                self.expect_token(&TokenType::RightParen)?;
                cols2
            } else {
                Vec::new()
            };
            Ok(TableConstraint::ForeignKey {
                columns: cols,
                referenced_table: ref_table,
                referenced_columns: ref_cols,
            })
        } else if self.match_token(&TokenType::Check) {
            self.advance();
            self.expect_token(&TokenType::LeftParen)?;
            let expr = self.parse_expression()?;
            self.expect_token(&TokenType::RightParen)?;
            Ok(TableConstraint::Check(expr))
        } else {
            Err(Error::parser(
                "Expected PRIMARY KEY, UNIQUE, FOREIGN KEY, or CHECK".to_string(),
            ))
        }
    }

    fn parse_alter(&mut self) -> Result<SqlStatement> {
        self.expect_keyword("ALTER")?;
        self.expect_keyword("TABLE")?;
        let table_name = self.parse_identifier()?;

        let operation = if self.match_keyword("ADD") {
            self.advance();
            if self.match_keyword("COLUMN") {
                self.advance();
                let name = self.parse_identifier()?;
                let data_type = self.parse_data_type()?;
                AlterTableOperation::AddColumn(ColumnDefinition {
                    name,
                    data_type,
                    constraints: Vec::new(),
                })
            } else {
                let mut constraint_name: Option<String> = None;
                if self.match_token(&TokenType::Constraint) {
                    self.advance();
                    constraint_name = Some(self.parse_identifier()?);
                }
                let definition = self.parse_table_constraint_only()?;
                AlterTableOperation::AddConstraint {
                    name: constraint_name,
                    definition,
                }
            }
        } else if self.match_keyword("DROP") {
            self.advance();
            if self.match_keyword("CONSTRAINT") {
                self.advance();
                let name = self.parse_identifier()?;
                AlterTableOperation::DropConstraint(name)
            } else {
                self.expect_keyword("COLUMN")?;
                let col = self.parse_identifier()?;
                AlterTableOperation::DropColumn(col)
            }
        } else if self.match_keyword("MODIFY") {
            self.advance();
            self.expect_keyword("COLUMN")?;
            let name = self.parse_identifier()?;
            let data_type = self.parse_data_type()?;
            AlterTableOperation::ModifyColumn(ColumnDefinition {
                name,
                data_type,
                constraints: Vec::new(),
            })
        } else if self.match_keyword("RENAME") {
            self.advance();
            if self.match_keyword("COLUMN") {
                self.advance();
                let old_name = self.parse_identifier()?;
                self.expect_keyword("TO")?;
                let new_name = self.parse_identifier()?;
                AlterTableOperation::RenameColumn { old_name, new_name }
            } else if self.match_keyword("TO") {
                self.advance();
                let new_name = self.parse_identifier()?;
                AlterTableOperation::RenameTable(new_name)
            } else {
                return Err(Error::parser(
                    "Expected COLUMN or TO after RENAME".to_string(),
                ));
            }
        } else {
            return Err(Error::parser(
                "Unsupported ALTER TABLE: use ADD COLUMN / ADD CONSTRAINT, DROP COLUMN / DROP CONSTRAINT, MODIFY COLUMN, RENAME COLUMN, or RENAME TO"
                    .to_string(),
            ));
        };

        Ok(SqlStatement::AlterTable(AlterTableStatement {
            table_name,
            operation,
        }))
    }

    fn parse_drop(&mut self) -> Result<SqlStatement> {
        self.expect_keyword("DROP")?;
        self.expect_keyword("TABLE")?;
        let if_exists = if self.match_keyword("IF") {
            self.advance();
            self.expect_keyword("EXISTS")?;
            true
        } else {
            false
        };
        let table_name = self.parse_identifier()?;
        let cascade = if self.match_keyword("CASCADE") {
            self.advance();
            true
        } else {
            if self.match_keyword("RESTRICT") {
                self.advance();
            }
            false
        };
        Ok(SqlStatement::DropTable(DropTableStatement {
            table_name,
            if_exists,
            cascade,
        }))
    }
}
