//! An example of using the rustdb lexical analyzer

use rustdb::parser::Lexer;

fn main() {
    println!("🔍 Demonstration of the rustdb lexical analyzer\n");

    // Test 1: Basic Keywords
    test_basic_keywords();

    // Test 2: Compound Keywords
    test_compound_keywords();

    // Test 3: Complex SQL query
    test_complex_query();

    println!("✅ Demonstration completed successfully!");
}

fn test_basic_keywords() {
    println!("📝 1. Basic keywords");
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
    println!("🔗 2. Compound keywords");
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

    // Operator test
    println!("🔧 Operator test:");
    let mut lexer2 = rustdb::parser::Lexer::new("+ - * / % = <> < > <= >= != :=").unwrap();
    let tokens = lexer2.tokenize().unwrap();

    println!("Total tokens: {}", tokens.len());
    for (i, token) in tokens.iter().enumerate() {
        println!("   {}: {:?} = '{}'", i, token.token_type, token.value);
    }
    println!();
}

fn test_complex_query() {
    println!("🗃️ 3. Complex SQL query");
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

    println!("Total tokens: {}", tokens.len());

    // Show only keywords
    let keywords: Vec<_> = tokens
        .iter()
        .filter(|t| t.token_type.is_keyword())
        .collect();

    println!("Key words:");
    for token in keywords {
        println!("     {:?}", token.token_type);
    }
    println!();
}
