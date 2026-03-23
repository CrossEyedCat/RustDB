//! Example of using the SQL parser

use rustdb::common::Result;
use rustdb::parser::{DataType, Expression, SelectItem, SqlParser, SqlStatement};

fn main() -> Result<()> {
    println!("=== Example of using rustdb SQL parser ===\n");

    // Example 1: Simple SELECT query
    println!("1. Parsing a simple SELECT query:");
    let mut parser = SqlParser::new("SELECT * FROM users")?;
    let statement = parser.parse()?;

    match statement {
        SqlStatement::Select(select_stmt) => {
            println!("SELECT query recognized");
            println!("Number of columns: {}", select_stmt.select_list.len());

            match &select_stmt.select_list[0] {
                SelectItem::Wildcard => println!("First column: * (all columns)"),
                SelectItem::Expression { expr, alias } => {
                    match expr {
                        Expression::Identifier(name) => println!("First column: {}", name),
                        _ => println!("First column: complex expression"),
                    }
                    if let Some(alias) = alias {
                        println!("Nickname: {}", alias);
                    }
                }
            }

            if let Some(from_clause) = &select_stmt.from {
                match &from_clause.table {
                    rustdb::parser::TableReference::Table { name, alias } => {
                        println!("Table: {}", name);
                        if let Some(alias) = alias {
                            println!("Table alias: {}", alias);
                        }
                    }
                    rustdb::parser::TableReference::Subquery { alias, .. } => {
                        println!("Subquery with alias: {}", alias);
                    }
                }
            }
        }
        _ => println!("Unexpected statement type"),
    }

    println!();

    // Example 2: SELECT with multiple columns
    println!("2. Parsing SELECT with multiple columns:");
    let mut parser = SqlParser::new("SELECT name, email, age FROM users")?;
    let statement = parser.parse()?;

    if let SqlStatement::Select(select_stmt) = statement {
        println!("Number of columns: {}", select_stmt.select_list.len());
        for (i, item) in select_stmt.select_list.iter().enumerate() {
            match item {
                SelectItem::Expression { expr, .. } => {
                    if let Expression::Identifier(name) = expr {
                        println!("Column {}: {}", i + 1, name);
                    }
                }
                SelectItem::Wildcard => println!("Column {}: *", i + 1),
            }
        }
    }

    println!();

    // Example 3: CREATE TABLE
    println!("3. Parsing CREATE TABLE:");
    let mut parser = SqlParser::new(
        "CREATE TABLE users (id INTEGER, name TEXT, email VARCHAR, active BOOLEAN)",
    )?;
    let statement = parser.parse()?;

    if let SqlStatement::CreateTable(create_stmt) = statement {
        println!("Table name: {}", create_stmt.table_name);
        println!("Number of columns: {}", create_stmt.columns.len());

        for column in &create_stmt.columns {
            let type_name = match &column.data_type {
                DataType::Integer => "INTEGER",
                DataType::Text => "TEXT",
                DataType::Varchar { .. } => "VARCHAR",
                DataType::Boolean => "BOOLEAN",
                DataType::Date => "DATE",
                DataType::Time => "TIME",
                DataType::Timestamp => "TIMESTAMP",
                _ => "UNKNOWN",
            };
            println!("Column: {} {}", column.name, type_name);
        }
    }

    println!();

    // Example 4: Transactions
    println!("4. Parsing transactional commands:");
    let commands = vec!["BEGIN TRANSACTION", "COMMIT", "ROLLBACK"];

    for cmd in commands {
        let mut parser = SqlParser::new(cmd)?;
        let statement = parser.parse()?;

        match statement {
            SqlStatement::BeginTransaction => println!("{}: Start of transaction", cmd),
            SqlStatement::CommitTransaction => println!("{}: Commit transaction", cmd),
            SqlStatement::RollbackTransaction => println!("{}: Rollback transaction", cmd),
            _ => println!("{}: Unexpected command type", cmd),
        }
    }

    println!();

    // Example 5: Several statements
    println!("5. Parsing several SQL commands:");
    let mut parser = SqlParser::new(
        "SELECT * FROM users; CREATE TABLE products (id INTEGER, name TEXT); COMMIT;",
    )?;
    let statements = parser.parse_multiple()?;

    println!("Number of commands: {}", statements.len());
    for (i, stmt) in statements.iter().enumerate() {
        match stmt {
            SqlStatement::Select(_) => println!("Command {}: SELECT", i + 1),
            SqlStatement::CreateTable(create) => {
                println!("Command {}: CREATE TABLE {}", i + 1, create.table_name)
            }
            SqlStatement::CommitTransaction => println!("Command {}: COMMIT", i + 1),
            _ => println!("Command {}: Other", i + 1),
        }
    }

    println!();

    // Example 6: Error Handling
    println!("6. Handling parsing errors:");
    let invalid_queries = vec![
        "SELECT FROM",
        "CREATE TABLE",
        "INVALID STATEMENT",
        "SELECT * FROM",
    ];

    for query in invalid_queries {
        let mut parser = SqlParser::new(query)?;
        match parser.parse() {
            Ok(_) => println!("'{}': Unexpectedly successfully parsed", query),
            Err(e) => println!("'{}': Error - {}", query, e),
        }
    }

    println!();

    // Example 7: Parser settings
    println!("7. Using parser settings:");
    let settings = rustdb::parser::ParserSettings {
        max_recursion_depth: 50,
        enable_caching: true,
        strict_validation: true,
    };

    let parser = SqlParser::with_settings("SELECT * FROM users", settings)?;
    println!(
        "Maximum recursion depth: {}",
        parser.settings().max_recursion_depth
    );
    println!(
        "Caching enabled: {}",
        parser.settings().enable_caching
    );
    println!(
        "Strong validation: {}",
        parser.settings().strict_validation
    );

    println!();

    // Example 8: DML operations (INSERT, UPDATE, DELETE)
    println!("8. Parsing DML operations:");

    // INSERT
    let mut parser =
        SqlParser::new("INSERT INTO users (name, email) VALUES ('John', 'john@example.com')")?;
    let statement = parser.parse()?;
    if let SqlStatement::Insert(insert_stmt) = statement {
        println!("INSERT into table: {}", insert_stmt.table);
        if let Some(cols) = &insert_stmt.columns {
            println!("Columns: {:?}", cols);
        }
        match &insert_stmt.values {
            rustdb::parser::InsertValues::Values(rows) => {
                println!("Number of lines: {}", rows.len());
            }
            rustdb::parser::InsertValues::Select(_) => {
                println!("INSERT from SELECT");
            }
        }
    }

    // UPDATE
    let mut parser = SqlParser::new("UPDATE users SET name = 'Jane', age = 25 WHERE id = 1")?;
    let statement = parser.parse()?;
    if let SqlStatement::Update(update_stmt) = statement {
        println!("UPDATE tables: {}", update_stmt.table);
        println!(
            "Number of assignments: {}",
            update_stmt.assignments.len()
        );
        println!("There is WHERE: {}", update_stmt.where_clause.is_some());
    }

    // DELETE
    let mut parser = SqlParser::new("DELETE FROM users WHERE age > 65")?;
    let statement = parser.parse()?;
    if let SqlStatement::Delete(delete_stmt) = statement {
        println!("DELETE from table: {}", delete_stmt.table);
        println!("There is WHERE: {}", delete_stmt.where_clause.is_some());
    }

    println!();

    // Example 9: Complex DML operations
    println!("9. Complex DML operations:");

    let dml_queries = [
        "INSERT INTO products VALUES (1, 'Laptop', 999.99)",
        "INSERT INTO orders (id, user_id, product_id) VALUES (1, 1, 1), (2, 2, 1)",
        "UPDATE products SET price = 899.99 WHERE id = 1",
        "DELETE FROM orders WHERE user_id = 2",
    ];

    for (i, query) in dml_queries.iter().enumerate() {
        let mut parser = SqlParser::new(query)?;
        match parser.parse() {
            Ok(statement) => match statement {
                SqlStatement::Insert(insert) => {
                    println!("Query {}: INSERT into {}", i + 1, insert.table);
                }
                SqlStatement::Update(update) => {
                    println!(
                        "Query {}: UPDATE table {} ({} changes)",
                        i + 1,
                        update.table,
                        update.assignments.len()
                    );
                }
                SqlStatement::Delete(delete) => {
                    println!("Query {}: DELETE from {}", i + 1, delete.table);
                }
                _ => println!("Request {}: Other type", i + 1),
            },
            Err(e) => println!("Request {}: Error - {}", i + 1, e),
        }
    }

    println!("\n=== Example completed ===");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser_example() -> Result<()> {
        // Run the main function as a test
        main()
    }
}
