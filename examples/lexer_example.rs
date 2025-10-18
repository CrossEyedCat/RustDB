//! Пример использования лексического анализатора rustdb

use rustdb::parser::Lexer;

fn main() {
    println!("🔍 Демонстрация лексического анализатора rustdb\n");
    
    // Тест 1: Базовые ключевые слова
    test_basic_keywords();
    
    // Тест 2: Составные ключевые слова
    test_compound_keywords();
    
    // Тест 3: Комплексный SQL запрос
    test_complex_query();
    
    println!("✅ Демонстрация завершена успешно!");
}

fn test_basic_keywords() {
    println!("📝 1. Базовые ключевые слова");
    println!("============================");
    
    let sql = "SELECT FROM WHERE INSERT UPDATE DELETE";
    let mut lexer = Lexer::new(sql).unwrap();
    let tokens = lexer.tokenize().unwrap();
    
    for token in &tokens {
        if token.token_type != rustdb::parser::TokenType::Eof {
            println!("   {:?}: '{}'", token.token_type, token.value);
        }
    }
    println!();
}

fn test_compound_keywords() {
    println!("🔗 2. Составные ключевые слова");
    println!("==============================");
    
    let sql = "IS NULL IS NOT NULL NOT NULL GROUP BY ORDER BY INNER JOIN";
    let mut lexer = Lexer::new(sql).unwrap();
    
    loop {
        let token = lexer.next_token().unwrap();
        if token.token_type == rustdb::parser::TokenType::Eof {
            break;
        }
        println!("   {:?}: '{}'", token.token_type, token.value);
    }
    println!();
    
    // Тест операторов
    println!("🔧 Тест операторов:");
    let mut lexer2 = rustdb::parser::Lexer::new("+ - * / % = <> < > <= >= != :=").unwrap();
    let tokens = lexer2.tokenize().unwrap();
    
    println!("   Всего токенов: {}", tokens.len());
    for (i, token) in tokens.iter().enumerate() {
        println!("   {}: {:?} = '{}'", i, token.token_type, token.value);
    }
    println!();
}

fn test_complex_query() {
    println!("🗃️  3. Комплексный SQL запрос");
    println!("=============================");
    
    let sql = r#"
        SELECT u.name, u.email, COUNT(o.id) as order_count
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.created_at >= '2023-01-01'
          AND u.status IS NOT NULL
        GROUP BY u.id
        HAVING COUNT(o.id) > 0
        ORDER BY order_count DESC
        LIMIT 10;
    "#;
    
    let mut lexer = Lexer::new(sql).unwrap();
    let tokens = lexer.tokenize().unwrap();
    
    println!("   Всего токенов: {}", tokens.len());
    
    // Показываем только ключевые слова
    let keywords: Vec<_> = tokens.iter()
        .filter(|t| t.token_type.is_keyword())
        .collect();
    
    println!("   Ключевые слова:");
    for token in keywords {
        println!("     {:?}", token.token_type);
    }
    println!();
}
