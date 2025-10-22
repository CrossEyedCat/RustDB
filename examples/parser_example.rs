//! Пример использования синтаксического анализатора SQL

use rustdb::common::Result;
use rustdb::parser::{SqlParser, SqlStatement, SelectItem, Expression, DataType};

fn main() -> Result<()> {
    println!("=== Пример использования SQL парсера rustdb ===\n");

    // Пример 1: Простой SELECT запрос
    println!("1. Парсинг простого SELECT запроса:");
    let mut parser = SqlParser::new("SELECT * FROM users")?;
    let statement = parser.parse()?;
    
    match statement {
        SqlStatement::Select(select_stmt) => {
            println!("   Распознан SELECT запрос");
            println!("   Количество колонок: {}", select_stmt.select_list.len());
            
            match &select_stmt.select_list[0] {
                SelectItem::Wildcard => println!("   Первая колонка: * (все колонки)"),
                SelectItem::Expression { expr, alias } => {
                    match expr {
                        Expression::Identifier(name) => println!("   Первая колонка: {}", name),
                        _ => println!("   Первая колонка: сложное выражение"),
                    }
                    if let Some(alias) = alias {
                        println!("   Псевдоним: {}", alias);
                    }
                }
            }
            
            if let Some(from_clause) = &select_stmt.from {
                match &from_clause.table {
                    rustdb::parser::TableReference::Table { name, alias } => {
                        println!("   Таблица: {}", name);
                        if let Some(alias) = alias {
                            println!("   Псевдоним таблицы: {}", alias);
                        }
                    }
                    rustdb::parser::TableReference::Subquery { alias, .. } => {
                        println!("   Подзапрос с псевдонимом: {}", alias);
                    }
                }
            }
        }
        _ => println!("   Неожиданный тип statement"),
    }

    println!();

    // Пример 2: SELECT с несколькими колонками
    println!("2. Парсинг SELECT с несколькими колонками:");
    let mut parser = SqlParser::new("SELECT name, email, age FROM users")?;
    let statement = parser.parse()?;
    
    if let SqlStatement::Select(select_stmt) = statement {
        println!("   Количество колонок: {}", select_stmt.select_list.len());
        for (i, item) in select_stmt.select_list.iter().enumerate() {
            match item {
                SelectItem::Expression { expr, .. } => {
                    if let Expression::Identifier(name) = expr {
                        println!("   Колонка {}: {}", i + 1, name);
                    }
                }
                SelectItem::Wildcard => println!("   Колонка {}: *", i + 1),
            }
        }
    }

    println!();

    // Пример 3: CREATE TABLE
    println!("3. Парсинг CREATE TABLE:");
    let mut parser = SqlParser::new("CREATE TABLE users (id INTEGER, name TEXT, email VARCHAR, active BOOLEAN)")?;
    let statement = parser.parse()?;
    
    if let SqlStatement::CreateTable(create_stmt) = statement {
        println!("   Имя таблицы: {}", create_stmt.table_name);
        println!("   Количество колонок: {}", create_stmt.columns.len());
        
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
            println!("   Колонка: {} {}", column.name, type_name);
        }
    }

    println!();

    // Пример 4: Транзакции
    println!("4. Парсинг транзакционных команд:");
    let commands = vec![
        "BEGIN TRANSACTION",
        "COMMIT",
        "ROLLBACK",
    ];
    
    for cmd in commands {
        let mut parser = SqlParser::new(cmd)?;
        let statement = parser.parse()?;
        
        match statement {
            SqlStatement::BeginTransaction => println!("   {}: Начало транзакции", cmd),
            SqlStatement::CommitTransaction => println!("   {}: Фиксация транзакции", cmd),
            SqlStatement::RollbackTransaction => println!("   {}: Откат транзакции", cmd),
            _ => println!("   {}: Неожиданный тип команды", cmd),
        }
    }

    println!();

    // Пример 5: Несколько statement'ов
    println!("5. Парсинг нескольких SQL команд:");
    let mut parser = SqlParser::new("SELECT * FROM users; CREATE TABLE products (id INTEGER, name TEXT); COMMIT;")?;
    let statements = parser.parse_multiple()?;
    
    println!("   Количество команд: {}", statements.len());
    for (i, stmt) in statements.iter().enumerate() {
        match stmt {
            SqlStatement::Select(_) => println!("   Команда {}: SELECT", i + 1),
            SqlStatement::CreateTable(create) => println!("   Команда {}: CREATE TABLE {}", i + 1, create.table_name),
            SqlStatement::CommitTransaction => println!("   Команда {}: COMMIT", i + 1),
            _ => println!("   Команда {}: Другая", i + 1),
        }
    }

    println!();

    // Пример 6: Обработка ошибок
    println!("6. Обработка ошибок парсинга:");
    let invalid_queries = vec![
        "SELECT FROM",           // Отсутствует список колонок
        "CREATE TABLE",          // Отсутствует имя таблицы
        "INVALID STATEMENT",     // Неизвестная команда
        "SELECT * FROM",         // Отсутствует имя таблицы
    ];
    
    for query in invalid_queries {
        let mut parser = SqlParser::new(query)?;
        match parser.parse() {
            Ok(_) => println!("   '{}': Неожиданно успешно распарсен", query),
            Err(e) => println!("   '{}': Ошибка - {}", query, e),
        }
    }

    println!();

    // Пример 7: Настройки парсера
    println!("7. Использование настроек парсера:");
    let settings = rustdb::parser::ParserSettings {
        max_recursion_depth: 50,
        enable_caching: true,
        strict_validation: true,
    };
    
    let parser = SqlParser::with_settings("SELECT * FROM users", settings)?;
    println!("   Максимальная глубина рекурсии: {}", parser.settings().max_recursion_depth);
    println!("   Кэширование включено: {}", parser.settings().enable_caching);
    println!("   Строгая валидация: {}", parser.settings().strict_validation);

    println!();

    // Пример 8: DML операции (INSERT, UPDATE, DELETE)
    println!("8. Парсинг DML операций:");
    
    // INSERT
    let mut parser = SqlParser::new("INSERT INTO users (name, email) VALUES ('John', 'john@example.com')")?;
    let statement = parser.parse()?;
    if let SqlStatement::Insert(insert_stmt) = statement {
        println!("   INSERT в таблицу: {}", insert_stmt.table);
        if let Some(cols) = &insert_stmt.columns {
            println!("   Колонки: {:?}", cols);
        }
        match &insert_stmt.values {
            rustdb::parser::InsertValues::Values(rows) => {
                println!("   Количество строк: {}", rows.len());
            }
            rustdb::parser::InsertValues::Select(_) => {
                println!("   INSERT из SELECT");
            }
        }
    }
    
    // UPDATE
    let mut parser = SqlParser::new("UPDATE users SET name = 'Jane', age = 25 WHERE id = 1")?;
    let statement = parser.parse()?;
    if let SqlStatement::Update(update_stmt) = statement {
        println!("   UPDATE таблицы: {}", update_stmt.table);
        println!("   Количество присваиваний: {}", update_stmt.assignments.len());
        println!("   Есть WHERE: {}", update_stmt.where_clause.is_some());
    }
    
    // DELETE
    let mut parser = SqlParser::new("DELETE FROM users WHERE age > 65")?;
    let statement = parser.parse()?;
    if let SqlStatement::Delete(delete_stmt) = statement {
        println!("   DELETE из таблицы: {}", delete_stmt.table);
        println!("   Есть WHERE: {}", delete_stmt.where_clause.is_some());
    }

    println!();

    // Пример 9: Комплексные DML операции
    println!("9. Комплексные DML операции:");
    
    let dml_queries = [
        "INSERT INTO products VALUES (1, 'Laptop', 999.99)",
        "INSERT INTO orders (id, user_id, product_id) VALUES (1, 1, 1), (2, 2, 1)",
        "UPDATE products SET price = 899.99 WHERE id = 1",
        "DELETE FROM orders WHERE user_id = 2",
    ];
    
    for (i, query) in dml_queries.iter().enumerate() {
        let mut parser = SqlParser::new(query)?;
        match parser.parse() {
            Ok(statement) => {
                match statement {
                    SqlStatement::Insert(insert) => {
                        println!("   Запрос {}: INSERT в {}", i + 1, insert.table);
                    }
                    SqlStatement::Update(update) => {
                        println!("   Запрос {}: UPDATE таблицы {} ({} изменений)", 
                                i + 1, update.table, update.assignments.len());
                    }
                    SqlStatement::Delete(delete) => {
                        println!("   Запрос {}: DELETE из {}", i + 1, delete.table);
                    }
                    _ => println!("   Запрос {}: Другой тип", i + 1),
                }
            }
            Err(e) => println!("   Запрос {}: Ошибка - {}", i + 1, e),
        }
    }

    println!("\n=== Пример завершен ===");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser_example() -> Result<()> {
        // Запускаем основную функцию как тест
        main()
    }
}
