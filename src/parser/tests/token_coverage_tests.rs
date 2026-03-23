//! Coverage `token.rs`: Position, Token, all variants of `TokenType`, `keyword_map`.

use crate::parser::token::{keyword_map, Position, Token, TokenType};

#[test]
fn test_position_display_and_start() {
    let p = Position::new(2, 5, 10);
    assert_eq!(format!("{}", p), "2:5");
    assert_eq!(p.offset, 10);
    let s = Position::start();
    assert_eq!(s.line, 1);
    assert_eq!(s.column, 1);
}

#[test]
fn test_token_display() {
    let t = Token::new(TokenType::Select, "SELECT".into(), Position::start());
    let d = format!("{}", t);
    assert!(d.contains("Select"));
    assert!(d.contains("SELECT"));
}

// / Calls `Display`, `is_keyword`, `is_literal`, `is_operator`, `is_delimiter`, `should_skip`, `precedence` for each option.
#[test]
fn test_token_type_all_variants_exercised() {
    macro_rules! exercise {
        ($($t:ident),* $(,)?) => {
            $(
                let tt = TokenType::$t;
                let disp = format!("{}", tt);
                assert!(!disp.is_empty(), "empty Display for {:?}", tt);
                let _ = tt.is_keyword();
                let _ = tt.is_literal();
                let _ = tt.is_operator();
                let _ = tt.is_delimiter();
                let _ = tt.should_skip();
                let _ = tt.precedence();
            )*
        };
    }
    exercise!(
        Create,
        Drop,
        Alter,
        Table,
        Index,
        Database,
        Schema,
        View,
        Constraint,
        Primary,
        Foreign,
        Key,
        References,
        Unique,
        NotNull,
        Default,
        Check,
        Select,
        Insert,
        Update,
        Delete,
        From,
        Into,
        Values,
        Set,
        Where,
        Having,
        GroupBy,
        OrderBy,
        Limit,
        Offset,
        Join,
        InnerJoin,
        LeftJoin,
        RightJoin,
        FullJoin,
        CrossJoin,
        On,
        Using,
        And,
        Or,
        Not,
        In,
        Exists,
        Between,
        Like,
        Is,
        IsNull,
        IsNotNull,
        Count,
        Sum,
        Avg,
        Min,
        Max,
        Distinct,
        All,
        Integer,
        Varchar,
        Char,
        Text,
        Boolean,
        Date,
        Time,
        Timestamp,
        Decimal,
        Float,
        Double,
        Begin,
        Commit,
        Rollback,
        Transaction,
        Prepare,
        Execute,
        Case,
        When,
        Then,
        Else,
        End,
        Union,
        Intersect,
        Except,
        As,
        Asc,
        Desc,
        True,
        False,
        Null,
        Identifier,
        StringLiteral,
        IntegerLiteral,
        FloatLiteral,
        BooleanLiteral,
        NullLiteral,
        Plus,
        Minus,
        Multiply,
        Divide,
        Modulo,
        Equal,
        NotEqual,
        Less,
        Greater,
        LessEqual,
        GreaterEqual,
        Assign,
        LeftParen,
        RightParen,
        LeftBracket,
        RightBracket,
        LeftBrace,
        RightBrace,
        Comma,
        Semicolon,
        Dot,
        Colon,
        DoubleColon,
        Question,
        Comment,
        Whitespace,
        Newline,
        Eof,
        Unknown,
    );
}

#[test]
fn test_keyword_map_covers_registered_keywords() {
    let m = keyword_map();
    assert!(m.len() >= 80);
    for (kw, tt) in &m {
        assert!(!kw.is_empty());
        let _ = format!("{}", tt);
    }
    assert_eq!(m.get("SELECT"), Some(&TokenType::Select));
    assert_eq!(m.get("INT"), Some(&TokenType::Integer));
    assert_eq!(m.get("BOOL"), Some(&TokenType::Boolean));
}
