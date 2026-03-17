//! SQL parser tests

use crate::common::Result;
use crate::parser::{
    ColumnDefinition, CreateIndexStatement, CreateTableStatement, DataType, Expression,
    SelectItem, SelectStatement, SqlParser, SqlStatement,
};

#[test]
fn test_parser_creation() -> Result<()> {
    let parser = SqlParser::new("SELECT * FROM users")?;
    assert!(parser.settings().max_recursion_depth > 0);
    Ok(())
}

#[test]
fn test_parse_simple_select() -> Result<()> {
    let mut parser = SqlParser::new("SELECT * FROM users")?;
    let statement = parser.parse()?;

    match statement {
        SqlStatement::Select(select_stmt) => {
            assert_eq!(select_stmt.select_list.len(), 1);
            match &select_stmt.select_list[0] {
                SelectItem::Wildcard => {}
                _ => panic!("Expected wildcard"),
            }
            assert!(select_stmt.from.is_some());
        }
        _ => panic!("Expected SELECT statement"),
    }

    Ok(())
}

#[test]
fn test_parse_select_columns() -> Result<()> {
    let mut parser = SqlParser::new("SELECT name, age FROM users")?;
    let statement = parser.parse()?;

    match statement {
        SqlStatement::Select(select_stmt) => {
            assert_eq!(select_stmt.select_list.len(), 2);

            match &select_stmt.select_list[0] {
                SelectItem::Expression { expr, alias } => {
                    match expr {
                        Expression::Identifier(name) => assert_eq!(name, "name"),
                        _ => panic!("Expected identifier"),
                    }
                    assert!(alias.is_none());
                }
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }

    Ok(())
}

#[test]
fn test_parse_select_without_from() -> Result<()> {
    let mut parser = SqlParser::new("SELECT 1")?;
    let statement = parser.parse()?;

    match statement {
        SqlStatement::Select(select_stmt) => {
            assert_eq!(select_stmt.select_list.len(), 1);
            assert!(select_stmt.from.is_none());
        }
        _ => panic!("Expected SELECT statement"),
    }

    Ok(())
}

#[test]
fn test_parse_create_table() -> Result<()> {
    let mut parser = SqlParser::new("CREATE TABLE users (id INTEGER, name TEXT)")?;
    let statement = parser.parse()?;

    match statement {
        SqlStatement::CreateTable(create_stmt) => {
            assert_eq!(create_stmt.table_name, "users");
            assert_eq!(create_stmt.columns.len(), 2);

            assert_eq!(create_stmt.columns[0].name, "id");
            match create_stmt.columns[0].data_type {
                DataType::Integer => {}
                _ => panic!("Expected INTEGER type"),
            }

            assert_eq!(create_stmt.columns[1].name, "name");
            match create_stmt.columns[1].data_type {
                DataType::Text => {}
                _ => panic!("Expected TEXT type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }

    Ok(())
}

#[test]
fn test_parse_create_index() -> Result<()> {
    let mut parser = SqlParser::new("CREATE INDEX idx_users_email ON users (email)")?;
    let statement = parser.parse()?;

    match statement {
        SqlStatement::CreateIndex(create_idx) => {
            assert_eq!(create_idx.index_name, "idx_users_email");
            assert_eq!(create_idx.table_name, "users");
            assert_eq!(create_idx.columns, vec!["email"]);
        }
        _ => panic!("Expected CREATE INDEX statement"),
    }

    Ok(())
}

#[test]
fn test_parse_create_index_multiple_columns() -> Result<()> {
    let mut parser =
        SqlParser::new("CREATE INDEX idx_orders_user_status ON orders (user_id, status)")?;
    let statement = parser.parse()?;

    match statement {
        SqlStatement::CreateIndex(create_idx) => {
            assert_eq!(create_idx.index_name, "idx_orders_user_status");
            assert_eq!(create_idx.table_name, "orders");
            assert_eq!(create_idx.columns, vec!["user_id", "status"]);
        }
        _ => panic!("Expected CREATE INDEX statement"),
    }

    Ok(())
}

#[test]
fn test_parse_create_table_with_different_types() -> Result<()> {
    let mut parser =
        SqlParser::new("CREATE TABLE test (id INT, flag BOOLEAN, created_at TIMESTAMP)")?;
    let statement = parser.parse()?;

    match statement {
        SqlStatement::CreateTable(create_stmt) => {
            assert_eq!(create_stmt.columns.len(), 3);

            match create_stmt.columns[0].data_type {
                DataType::Integer => {}
                _ => panic!("Expected INTEGER type"),
            }

            match create_stmt.columns[1].data_type {
                DataType::Boolean => {}
                _ => panic!("Expected BOOLEAN type"),
            }

            match create_stmt.columns[2].data_type {
                DataType::Timestamp => {}
                _ => panic!("Expected TIMESTAMP type"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }

    Ok(())
}

#[test]
fn test_parse_transaction_statements() -> Result<()> {
    let mut parser = SqlParser::new("BEGIN TRANSACTION")?;
    let statement = parser.parse()?;
    match statement {
        SqlStatement::BeginTransaction => {}
        _ => panic!("Expected BEGIN TRANSACTION"),
    }

    let mut parser = SqlParser::new("COMMIT")?;
    let statement = parser.parse()?;
    match statement {
        SqlStatement::CommitTransaction => {}
        _ => panic!("Expected COMMIT"),
    }

    let mut parser = SqlParser::new("ROLLBACK")?;
    let statement = parser.parse()?;
    match statement {
        SqlStatement::RollbackTransaction => {}
        _ => panic!("Expected ROLLBACK"),
    }

    Ok(())
}

#[test]
fn test_parse_multiple_statements() -> Result<()> {
    let mut parser = SqlParser::new("SELECT * FROM users; CREATE TABLE test (id INTEGER);")?;
    let statements = parser.parse_multiple()?;

    assert_eq!(statements.len(), 2);

    match &statements[0] {
        SqlStatement::Select(_) => {}
        _ => panic!("First statement must be SELECT"),
    }

    match &statements[1] {
        SqlStatement::CreateTable(_) => {}
        _ => panic!("Second statement must be CREATE TABLE"),
    }

    Ok(())
}

#[test]
fn test_parse_error_handling() {
    // Invalid syntax should fail
    let mut parser = SqlParser::new("SELECT FROM").unwrap();
    let result = parser.parse();
    assert!(result.is_err());

    // Unexpected end-of-input should fail
    let mut parser = SqlParser::new("SELECT").unwrap();
    let result = parser.parse();
    assert!(result.is_err());

    // Unknown keyword should fail
    let mut parser = SqlParser::new("INVALID STATEMENT").unwrap();
    let result = parser.parse();
    assert!(result.is_err());
}

#[test]
fn test_parser_settings() -> Result<()> {
    let settings = crate::parser::ParserSettings {
        max_recursion_depth: 50,
        enable_caching: false,
        strict_validation: false,
    };

    let parser = SqlParser::with_settings("SELECT * FROM users", settings)?;
    assert_eq!(parser.settings().max_recursion_depth, 50);
    assert!(!parser.settings().enable_caching);
    assert!(!parser.settings().strict_validation);

    Ok(())
}

#[test]
fn test_parser_cache() -> Result<()> {
    let mut parser = SqlParser::new("SELECT * FROM users")?;
    parser.clear_cache();

    // Parse twice to exercise cache behavior
    let _stmt1 = parser.parse()?;

    let mut parser = SqlParser::new("SELECT * FROM users")?;
    let _stmt2 = parser.parse()?;

    Ok(())
}

#[test]
fn test_parse_insert_simple() -> Result<()> {
    let mut parser = SqlParser::new("INSERT INTO users VALUES (1, 'John', 'john@example.com')")?;
    let statement = parser.parse()?;

    match statement {
        SqlStatement::Insert(insert_stmt) => {
            assert_eq!(insert_stmt.table, "users");
            assert!(insert_stmt.columns.is_none());

            match insert_stmt.values {
                crate::parser::InsertValues::Values(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0].len(), 3);
                }
                _ => panic!("Expected VALUES"),
            }
        }
        _ => panic!("Expected INSERT statement"),
    }

    Ok(())
}

#[test]
fn test_parse_insert_with_columns() -> Result<()> {
    let mut parser = SqlParser::new(
        "INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com')",
    )?;
    let statement = parser.parse()?;

    match statement {
        SqlStatement::Insert(insert_stmt) => {
            assert_eq!(insert_stmt.table, "users");

            let columns = insert_stmt.columns.unwrap();
            assert_eq!(columns.len(), 3);
            assert_eq!(columns[0], "id");
            assert_eq!(columns[1], "name");
            assert_eq!(columns[2], "email");

            match insert_stmt.values {
                crate::parser::InsertValues::Values(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0].len(), 3);
                }
                _ => panic!("Expected VALUES"),
            }
        }
        _ => panic!("Expected INSERT statement"),
    }

    Ok(())
}

#[test]
fn test_parse_insert_multiple_rows() -> Result<()> {
    let mut parser = SqlParser::new("INSERT INTO users VALUES (1, 'John'), (2, 'Jane')")?;
    let statement = parser.parse()?;

    match statement {
        SqlStatement::Insert(insert_stmt) => {
            assert_eq!(insert_stmt.table, "users");

            match insert_stmt.values {
                crate::parser::InsertValues::Values(rows) => {
                    assert_eq!(rows.len(), 2);
                    assert_eq!(rows[0].len(), 2);
                    assert_eq!(rows[1].len(), 2);
                }
                _ => panic!("Expected VALUES"),
            }
        }
        _ => panic!("Expected INSERT statement"),
    }

    Ok(())
}

#[test]
fn test_parse_update_simple() -> Result<()> {
    let mut parser = SqlParser::new("UPDATE users SET name = 'John Doe'")?;
    let statement = parser.parse()?;

    match statement {
        SqlStatement::Update(update_stmt) => {
            assert_eq!(update_stmt.table, "users");
            assert_eq!(update_stmt.assignments.len(), 1);
            assert_eq!(update_stmt.assignments[0].column, "name");
            assert!(update_stmt.where_clause.is_none());
        }
        _ => panic!("Expected UPDATE statement"),
    }

    Ok(())
}

#[test]
fn test_parse_update_with_where() -> Result<()> {
    let mut parser = SqlParser::new("UPDATE users SET name = 'John Doe', age = 30 WHERE id = 1")?;
    let statement = parser.parse()?;

    match statement {
        SqlStatement::Update(update_stmt) => {
            assert_eq!(update_stmt.table, "users");
            assert_eq!(update_stmt.assignments.len(), 2);
            assert_eq!(update_stmt.assignments[0].column, "name");
            assert_eq!(update_stmt.assignments[1].column, "age");
            assert!(update_stmt.where_clause.is_some());
        }
        _ => panic!("Expected UPDATE statement"),
    }

    Ok(())
}

#[test]
fn test_parse_delete_simple() -> Result<()> {
    let mut parser = SqlParser::new("DELETE FROM users")?;
    let statement = parser.parse()?;

    match statement {
        SqlStatement::Delete(delete_stmt) => {
            assert_eq!(delete_stmt.table, "users");
            assert!(delete_stmt.where_clause.is_none());
        }
        _ => panic!("Expected DELETE statement"),
    }

    Ok(())
}

#[test]
fn test_parse_delete_with_where() -> Result<()> {
    let mut parser = SqlParser::new("DELETE FROM users WHERE id = 1")?;
    let statement = parser.parse()?;

    match statement {
        SqlStatement::Delete(delete_stmt) => {
            assert_eq!(delete_stmt.table, "users");
            assert!(delete_stmt.where_clause.is_some());
        }
        _ => panic!("Expected DELETE statement"),
    }

    Ok(())
}

#[test]
fn test_parse_dml_error_handling() {
    // Invalid INSERT syntax
    let mut parser = SqlParser::new("INSERT INTO").unwrap();
    let result = parser.parse();
    assert!(result.is_err());

    // Invalid UPDATE syntax
    let mut parser = SqlParser::new("UPDATE users SET").unwrap();
    let result = parser.parse();
    assert!(result.is_err());

    // Invalid DELETE syntax
    let mut parser = SqlParser::new("DELETE FROM").unwrap();
    let result = parser.parse();
    assert!(result.is_err());
}

#[test]
fn test_parse_prepare() -> Result<()> {
    let mut parser = SqlParser::new("PREPARE sel AS SELECT * FROM users")?;
    let statement = parser.parse()?;

    match statement {
        SqlStatement::Prepare(prep) => {
            assert_eq!(prep.name, "sel");
            assert!(matches!(*prep.statement, SqlStatement::Select(_)));
        }
        _ => panic!("Expected PREPARE statement"),
    }

    Ok(())
}

#[test]
fn test_parse_execute() -> Result<()> {
    let mut parser = SqlParser::new("EXECUTE sel")?;
    let statement = parser.parse()?;

    match statement {
        SqlStatement::Execute(exec) => {
            assert_eq!(exec.name, "sel");
            assert!(exec.params.is_empty());
        }
        _ => panic!("Expected EXECUTE statement"),
    }

    Ok(())
}

#[test]
fn test_parse_execute_with_params() -> Result<()> {
    let mut parser = SqlParser::new("EXECUTE ins (1, 'test')")?;
    let statement = parser.parse()?;

    match statement {
        SqlStatement::Execute(exec) => {
            assert_eq!(exec.name, "ins");
            assert_eq!(exec.params.len(), 2);
        }
        _ => panic!("Expected EXECUTE statement"),
    }

    Ok(())
}
