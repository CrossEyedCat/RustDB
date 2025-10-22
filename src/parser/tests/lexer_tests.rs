//! Тесты для лексического анализатора rustdb

use crate::parser::{Lexer, Position, TokenType};

#[test]
fn test_lexer_creation() {
    let lexer = Lexer::new("SELECT * FROM users").unwrap();
    // Просто проверяем, что лексер создается без ошибок
    assert!(true);
}

#[test]
fn test_keywords() {
    let mut lexer = Lexer::new("SELECT FROM WHERE INSERT UPDATE DELETE").unwrap();
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(tokens.len(), 7); // 6 keywords + EOF
    assert_eq!(tokens[0].token_type, TokenType::Select);
    assert_eq!(tokens[1].token_type, TokenType::From);
    assert_eq!(tokens[2].token_type, TokenType::Where);
    assert_eq!(tokens[3].token_type, TokenType::Insert);
    assert_eq!(tokens[4].token_type, TokenType::Update);
    assert_eq!(tokens[5].token_type, TokenType::Delete);
    assert_eq!(tokens[6].token_type, TokenType::Eof);
}

#[test]
fn test_case_insensitive_keywords() {
    let mut lexer = Lexer::new("select SELECT Select sElEcT").unwrap();
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(tokens.len(), 5); // 4 SELECT + EOF
    for i in 0..4 {
        assert_eq!(tokens[i].token_type, TokenType::Select);
    }
    assert_eq!(tokens[4].token_type, TokenType::Eof);
}

#[test]
fn test_identifiers() {
    let mut lexer = Lexer::new("user_name table123 _private column1").unwrap();
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(tokens.len(), 5); // 4 identifiers + EOF
    for i in 0..4 {
        assert_eq!(tokens[i].token_type, TokenType::Identifier);
    }

    assert_eq!(tokens[0].value, "user_name");
    assert_eq!(tokens[1].value, "table123");
    assert_eq!(tokens[2].value, "_private");
    assert_eq!(tokens[3].value, "column1");
}

#[test]
fn test_quoted_identifiers() {
    let mut lexer = Lexer::new("\"user name\" \"SELECT\"").unwrap();
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(tokens.len(), 3); // 2 quoted identifiers + EOF
    assert_eq!(tokens[0].token_type, TokenType::Identifier);
    assert_eq!(tokens[1].token_type, TokenType::Identifier);

    assert_eq!(tokens[0].value, "\"user name\"");
    assert_eq!(tokens[1].value, "\"SELECT\"");
}

#[test]
fn test_string_literals() {
    let mut lexer = Lexer::new("'hello' 'world with spaces' 'it\\'s escaped'").unwrap();
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(tokens.len(), 4); // 3 strings + EOF
    for i in 0..3 {
        assert_eq!(tokens[i].token_type, TokenType::StringLiteral);
    }

    assert_eq!(tokens[0].value, "'hello'");
    assert_eq!(tokens[1].value, "'world with spaces'");
    assert_eq!(tokens[2].value, "'it\\'s escaped'");
}

#[test]
fn test_integer_literals() {
    let mut lexer = Lexer::new("123 0 999999").unwrap();
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(tokens.len(), 4); // 3 integers + EOF
    for i in 0..3 {
        assert_eq!(tokens[i].token_type, TokenType::IntegerLiteral);
    }

    assert_eq!(tokens[0].value, "123");
    assert_eq!(tokens[1].value, "0");
    assert_eq!(tokens[2].value, "999999");
}

#[test]
fn test_float_literals() {
    let mut lexer = Lexer::new("123.456 0.0 3.14159 1e10 2.5e-3").unwrap();
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(tokens.len(), 6); // 5 floats + EOF
    for i in 0..5 {
        assert_eq!(tokens[i].token_type, TokenType::FloatLiteral);
    }

    assert_eq!(tokens[0].value, "123.456");
    assert_eq!(tokens[1].value, "0.0");
    assert_eq!(tokens[2].value, "3.14159");
    assert_eq!(tokens[3].value, "1e10");
    assert_eq!(tokens[4].value, "2.5e-3");
}

#[test]
fn test_operators() {
    let mut lexer = Lexer::new("+ - * / % = <> < > <= >= != :=").unwrap();
    let tokens = lexer.tokenize().unwrap();

    let expected_types = vec![
        TokenType::Plus,
        TokenType::Minus,
        TokenType::Multiply,
        TokenType::Divide,
        TokenType::Modulo,
        TokenType::Equal,
        TokenType::NotEqual,
        TokenType::Less,
        TokenType::Greater,
        TokenType::LessEqual,
        TokenType::GreaterEqual,
        TokenType::NotEqual, // !=
        TokenType::Assign,
        TokenType::Eof,
    ];

    assert_eq!(tokens.len(), expected_types.len());
    for (i, expected_type) in expected_types.iter().enumerate() {
        assert_eq!(tokens[i].token_type, *expected_type);
    }
}

#[test]
fn test_delimiters() {
    let mut lexer = Lexer::new("() [] {} , ; . : :: ?").unwrap();
    let tokens = lexer.tokenize().unwrap();

    let expected_types = vec![
        TokenType::LeftParen,
        TokenType::RightParen,
        TokenType::LeftBracket,
        TokenType::RightBracket,
        TokenType::LeftBrace,
        TokenType::RightBrace,
        TokenType::Comma,
        TokenType::Semicolon,
        TokenType::Dot,
        TokenType::Colon,
        TokenType::DoubleColon,
        TokenType::Question,
        TokenType::Eof,
    ];

    assert_eq!(tokens.len(), expected_types.len());
    for (i, expected_type) in expected_types.iter().enumerate() {
        assert_eq!(tokens[i].token_type, *expected_type);
    }
}

#[test]
fn test_single_line_comments() {
    let mut lexer = Lexer::new("SELECT -- это комментарий\nFROM").unwrap();
    let mut all_tokens = Vec::new();

    loop {
        let token = lexer.next_token().unwrap();
        let is_eof = token.token_type == TokenType::Eof;
        all_tokens.push(token);
        if is_eof {
            break;
        }
    }

    // Должны получить: SELECT, комментарий, FROM, EOF
    assert_eq!(all_tokens.len(), 4);
    assert_eq!(all_tokens[0].token_type, TokenType::Select);
    assert_eq!(all_tokens[1].token_type, TokenType::Comment);
    assert_eq!(all_tokens[2].token_type, TokenType::From);
    assert_eq!(all_tokens[3].token_type, TokenType::Eof);

    assert!(all_tokens[1].value.starts_with("-- это комментарий"));
}

#[test]
fn test_multi_line_comments() {
    let mut lexer = Lexer::new("SELECT /* многострочный\nкомментарий */ FROM").unwrap();
    let mut all_tokens = Vec::new();

    loop {
        let token = lexer.next_token().unwrap();
        let is_eof = token.token_type == TokenType::Eof;
        all_tokens.push(token);
        if is_eof {
            break;
        }
    }

    // Должны получить: SELECT, комментарий, FROM, EOF
    assert_eq!(all_tokens.len(), 4);
    assert_eq!(all_tokens[0].token_type, TokenType::Select);
    assert_eq!(all_tokens[1].token_type, TokenType::Comment);
    assert_eq!(all_tokens[2].token_type, TokenType::From);
    assert_eq!(all_tokens[3].token_type, TokenType::Eof);

    assert!(all_tokens[1].value.contains("многострочный"));
    assert!(all_tokens[1].value.contains("комментарий"));
}

#[test]
fn test_compound_keywords() {
    let mut lexer = Lexer::new("GROUP BY ORDER BY INNER JOIN LEFT JOIN").unwrap();
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(tokens.len(), 5); // GROUP BY, ORDER BY, INNER JOIN, LEFT JOIN, EOF
    assert_eq!(tokens[0].token_type, TokenType::GroupBy);
    assert_eq!(tokens[1].token_type, TokenType::OrderBy);
    assert_eq!(tokens[2].token_type, TokenType::InnerJoin);
    assert_eq!(tokens[3].token_type, TokenType::LeftJoin);
    assert_eq!(tokens[4].token_type, TokenType::Eof);
}

#[test]
fn test_is_null_keywords() {
    let mut lexer = Lexer::new("IS NULL IS NOT NULL").unwrap();
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(tokens.len(), 3); // IS NULL, IS NOT NULL, EOF
    assert_eq!(tokens[0].token_type, TokenType::IsNull);
    assert_eq!(tokens[1].token_type, TokenType::IsNotNull);
    assert_eq!(tokens[2].token_type, TokenType::Eof);
}

#[test]
fn test_not_null_keyword() {
    let mut lexer = Lexer::new("name VARCHAR(50) NOT NULL").unwrap();
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(tokens.len(), 7); // name, VARCHAR, (, 50, ), NOT NULL, EOF
    assert_eq!(tokens[0].token_type, TokenType::Identifier);
    assert_eq!(tokens[1].token_type, TokenType::Varchar);
    assert_eq!(tokens[2].token_type, TokenType::LeftParen);
    assert_eq!(tokens[3].token_type, TokenType::IntegerLiteral);
    assert_eq!(tokens[4].token_type, TokenType::RightParen);
    assert_eq!(tokens[5].token_type, TokenType::NotNull);
    assert_eq!(tokens[6].token_type, TokenType::Eof);
}

#[test]
fn test_complex_sql_query() {
    let sql = r#"
        SELECT u.name, u.email, COUNT(o.id) as order_count
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.created_at >= '2023-01-01'
        GROUP BY u.id
        HAVING COUNT(o.id) > 0
        ORDER BY order_count DESC
        LIMIT 10;
    "#;

    let mut lexer = Lexer::new(sql).unwrap();
    let tokens = lexer.tokenize().unwrap();

    // Проверяем, что получили разумное количество токенов
    assert!(tokens.len() > 20);

    // Проверяем некоторые ключевые токены
    assert_eq!(tokens[0].token_type, TokenType::Select);

    // Находим FROM
    let from_pos = tokens
        .iter()
        .position(|t| t.token_type == TokenType::From)
        .unwrap();
    assert!(from_pos > 0);

    // Находим LEFT JOIN
    let left_join_pos = tokens
        .iter()
        .position(|t| t.token_type == TokenType::LeftJoin)
        .unwrap();
    assert!(left_join_pos > from_pos);

    // Находим WHERE
    let where_pos = tokens
        .iter()
        .position(|t| t.token_type == TokenType::Where)
        .unwrap();
    assert!(where_pos > left_join_pos);

    // Находим GROUP BY
    let group_by_pos = tokens
        .iter()
        .position(|t| t.token_type == TokenType::GroupBy)
        .unwrap();
    assert!(group_by_pos > where_pos);

    // Последний токен должен быть EOF
    assert_eq!(tokens.last().unwrap().token_type, TokenType::Eof);
}

#[test]
fn test_position_tracking() {
    let sql = "SELECT\nFROM\n  WHERE";
    let mut lexer = Lexer::new(sql).unwrap();
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(tokens.len(), 4); // SELECT, FROM, WHERE, EOF

    // SELECT на строке 1
    assert_eq!(tokens[0].position.line, 1);
    assert_eq!(tokens[0].position.column, 1);

    // FROM на строке 2
    assert_eq!(tokens[1].position.line, 2);
    assert_eq!(tokens[1].position.column, 1);

    // WHERE на строке 3 (с отступом)
    assert_eq!(tokens[2].position.line, 3);
    assert_eq!(tokens[2].position.column, 3);
}

#[test]
fn test_peek_token() {
    let mut lexer = Lexer::new("SELECT FROM").unwrap();

    // Заглядываем вперед
    let peeked = lexer.peek_token().unwrap();
    assert_eq!(peeked.token_type, TokenType::Select);

    // Получаем тот же токен
    let actual = lexer.next_token().unwrap();
    assert_eq!(actual.token_type, TokenType::Select);

    // Следующий токен
    let next = lexer.next_token().unwrap();
    assert_eq!(next.token_type, TokenType::From);
}

#[test]
fn test_unknown_characters() {
    let mut lexer = Lexer::new("SELECT @ FROM").unwrap();
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(tokens.len(), 4); // SELECT, @, FROM, EOF
    assert_eq!(tokens[0].token_type, TokenType::Select);
    assert_eq!(tokens[1].token_type, TokenType::Unknown);
    assert_eq!(tokens[1].value, "@");
    assert_eq!(tokens[2].token_type, TokenType::From);
}

#[test]
fn test_data_types() {
    let mut lexer = Lexer::new("INTEGER VARCHAR BOOLEAN DATE TIMESTAMP DECIMAL").unwrap();
    let tokens = lexer.tokenize().unwrap();

    let expected_types = vec![
        TokenType::Integer,
        TokenType::Varchar,
        TokenType::Boolean,
        TokenType::Date,
        TokenType::Timestamp,
        TokenType::Decimal,
        TokenType::Eof,
    ];

    assert_eq!(tokens.len(), expected_types.len());
    for (i, expected_type) in expected_types.iter().enumerate() {
        assert_eq!(tokens[i].token_type, *expected_type);
    }
}

#[test]
fn test_transaction_keywords() {
    let mut lexer = Lexer::new("BEGIN TRANSACTION COMMIT ROLLBACK").unwrap();
    let tokens = lexer.tokenize().unwrap();

    let expected_types = vec![
        TokenType::Begin,
        TokenType::Transaction,
        TokenType::Commit,
        TokenType::Rollback,
        TokenType::Eof,
    ];

    assert_eq!(tokens.len(), expected_types.len());
    for (i, expected_type) in expected_types.iter().enumerate() {
        assert_eq!(tokens[i].token_type, *expected_type);
    }
}

#[test]
fn test_boolean_and_null_literals() {
    let mut lexer = Lexer::new("TRUE FALSE NULL").unwrap();
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(tokens.len(), 4); // TRUE, FALSE, NULL, EOF
    assert_eq!(tokens[0].token_type, TokenType::True);
    assert_eq!(tokens[1].token_type, TokenType::False);
    assert_eq!(tokens[2].token_type, TokenType::Null);
    assert_eq!(tokens[3].token_type, TokenType::Eof);
}
