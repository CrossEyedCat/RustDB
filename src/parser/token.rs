//! Токены для SQL лексера rustdb
//! 
//! Определяет все типы токенов, которые может распознать лексический анализатор,
//! включая ключевые слова SQL, идентификаторы, литералы и операторы.

use std::fmt;

/// Позиция токена в исходном тексте
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Position {
    pub line: usize,
    pub column: usize,
    pub offset: usize,
}

impl Position {
    pub fn new(line: usize, column: usize, offset: usize) -> Self {
        Self { line, column, offset }
    }
    
    pub fn start() -> Self {
        Self::new(1, 1, 0)
    }
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.line, self.column)
    }
}

/// Токен с позицией и значением
#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub token_type: TokenType,
    pub value: String,
    pub position: Position,
}

impl Token {
    pub fn new(token_type: TokenType, value: String, position: Position) -> Self {
        Self {
            token_type,
            value,
            position,
        }
    }
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}('{}') at {}", self.token_type, self.value, self.position)
    }
}

/// Типы токенов SQL
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenType {
    // === Ключевые слова SQL ===
    // DDL (Data Definition Language)
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
    
    // DML (Data Manipulation Language)
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
    
    // JOIN операции
    Join,
    InnerJoin,
    LeftJoin,
    RightJoin,
    FullJoin,
    CrossJoin,
    On,
    Using,
    
    // Логические операторы
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
    
    // Функции и агрегаты
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Distinct,
    All,
    
    // Типы данных
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
    
    // Транзакции
    Begin,
    Commit,
    Rollback,
    Transaction,
    
    // Условные операторы
    Case,
    When,
    Then,
    Else,
    End,
    
    // Подзапросы
    Union,
    Intersect,
    Except,
    
    // Прочие ключевые слова
    As,
    Asc,
    Desc,
    True,
    False,
    Null,
    
    // === Идентификаторы и литералы ===
    /// Идентификатор (имя таблицы, колонки, etc.)
    Identifier,
    
    /// Строковый литерал
    StringLiteral,
    
    /// Целое число
    IntegerLiteral,
    
    /// Число с плавающей точкой
    FloatLiteral,
    
    /// Булевый литерал
    BooleanLiteral,
    
    /// NULL литерал
    NullLiteral,
    
    // === Операторы ===
    // Арифметические
    Plus,          // +
    Minus,         // -
    Multiply,      // *
    Divide,        // /
    Modulo,        // %
    
    // Сравнения
    Equal,         // =
    NotEqual,      // <> или !=
    Less,          // <
    Greater,       // >
    LessEqual,     // <=
    GreaterEqual,  // >=
    
    // Присваивания
    Assign,        // :=
    
    // === Разделители и символы ===
    LeftParen,     // (
    RightParen,    // )
    LeftBracket,   // [
    RightBracket,  // ]
    LeftBrace,     // {
    RightBrace,    // }
    Comma,         // ,
    Semicolon,     // ;
    Dot,           // .
    Colon,         // :
    DoubleColon,   // ::
    Question,      // ?
    
    // === Специальные токены ===
    /// Комментарий (однострочный или многострочный)
    Comment,
    
    /// Пробельный символ
    Whitespace,
    
    /// Конец строки
    Newline,
    
    /// Конец файла
    Eof,
    
    /// Неизвестный символ (ошибка)
    Unknown,
}

impl TokenType {
    /// Проверяет, является ли токен ключевым словом
    pub fn is_keyword(&self) -> bool {
        match self {
            TokenType::Create | TokenType::Drop | TokenType::Alter | TokenType::Table |
            TokenType::Index | TokenType::Database | TokenType::Schema | TokenType::View |
            TokenType::Constraint | TokenType::Primary | TokenType::Foreign | TokenType::Key |
            TokenType::References | TokenType::Unique | TokenType::NotNull | TokenType::Default |
            TokenType::Check | TokenType::Select | TokenType::Insert | TokenType::Update |
            TokenType::Delete | TokenType::From | TokenType::Into | TokenType::Values |
            TokenType::Set | TokenType::Where | TokenType::Having | TokenType::GroupBy |
            TokenType::OrderBy | TokenType::Limit | TokenType::Offset | TokenType::Join |
            TokenType::InnerJoin | TokenType::LeftJoin | TokenType::RightJoin | TokenType::FullJoin |
            TokenType::CrossJoin | TokenType::On | TokenType::Using | TokenType::And |
            TokenType::Or | TokenType::Not | TokenType::In | TokenType::Exists |
            TokenType::Between | TokenType::Like | TokenType::Is | TokenType::IsNull | TokenType::IsNotNull |
            TokenType::Count | TokenType::Sum | TokenType::Avg | TokenType::Min |
            TokenType::Max | TokenType::Distinct | TokenType::All | TokenType::Integer |
            TokenType::Varchar | TokenType::Char | TokenType::Text | TokenType::Boolean |
            TokenType::Date | TokenType::Time | TokenType::Timestamp | TokenType::Decimal |
            TokenType::Float | TokenType::Double | TokenType::Begin | TokenType::Commit |
            TokenType::Rollback | TokenType::Transaction | TokenType::Case | TokenType::When |
            TokenType::Then | TokenType::Else | TokenType::End | TokenType::Union |
            TokenType::Intersect | TokenType::Except | TokenType::As | TokenType::Asc |
            TokenType::Desc | TokenType::True | TokenType::False | TokenType::Null => true,
            _ => false,
        }
    }
    
    /// Проверяет, является ли токен литералом
    pub fn is_literal(&self) -> bool {
        match self {
            TokenType::StringLiteral | TokenType::IntegerLiteral | TokenType::FloatLiteral |
            TokenType::BooleanLiteral | TokenType::NullLiteral => true,
            _ => false,
        }
    }
    
    /// Проверяет, является ли токен оператором
    pub fn is_operator(&self) -> bool {
        match self {
            TokenType::Plus | TokenType::Minus | TokenType::Multiply | TokenType::Divide |
            TokenType::Modulo | TokenType::Equal | TokenType::NotEqual | TokenType::Less |
            TokenType::Greater | TokenType::LessEqual | TokenType::GreaterEqual |
            TokenType::Assign => true,
            _ => false,
        }
    }
    
    /// Проверяет, является ли токен разделителем
    pub fn is_delimiter(&self) -> bool {
        match self {
            TokenType::LeftParen | TokenType::RightParen | TokenType::LeftBracket |
            TokenType::RightBracket | TokenType::LeftBrace | TokenType::RightBrace |
            TokenType::Comma | TokenType::Semicolon | TokenType::Dot | TokenType::Colon |
            TokenType::DoubleColon | TokenType::Question => true,
            _ => false,
        }
    }
    
    /// Проверяет, следует ли пропустить токен при парсинге
    pub fn should_skip(&self) -> bool {
        match self {
            TokenType::Whitespace | TokenType::Newline | TokenType::Comment => true,
            _ => false,
        }
    }
    
    /// Возвращает приоритет оператора (для парсинга выражений)
    pub fn precedence(&self) -> u8 {
        match self {
            TokenType::Or => 1,
            TokenType::And => 2,
            TokenType::Not => 3,
            TokenType::Equal | TokenType::NotEqual | TokenType::Less | TokenType::Greater |
            TokenType::LessEqual | TokenType::GreaterEqual | TokenType::Like | TokenType::In |
            TokenType::Between | TokenType::Is | TokenType::IsNull | TokenType::IsNotNull => 4,
            TokenType::Plus | TokenType::Minus => 5,
            TokenType::Multiply | TokenType::Divide | TokenType::Modulo => 6,
            _ => 0,
        }
    }
}

impl fmt::Display for TokenType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            TokenType::Create => "CREATE",
            TokenType::Drop => "DROP",
            TokenType::Alter => "ALTER",
            TokenType::Table => "TABLE",
            TokenType::Index => "INDEX",
            TokenType::Database => "DATABASE",
            TokenType::Schema => "SCHEMA",
            TokenType::View => "VIEW",
            TokenType::Constraint => "CONSTRAINT",
            TokenType::Primary => "PRIMARY",
            TokenType::Foreign => "FOREIGN",
            TokenType::Key => "KEY",
            TokenType::References => "REFERENCES",
            TokenType::Unique => "UNIQUE",
            TokenType::NotNull => "NOT NULL",
            TokenType::Default => "DEFAULT",
            TokenType::Check => "CHECK",
            TokenType::Select => "SELECT",
            TokenType::Insert => "INSERT",
            TokenType::Update => "UPDATE",
            TokenType::Delete => "DELETE",
            TokenType::From => "FROM",
            TokenType::Into => "INTO",
            TokenType::Values => "VALUES",
            TokenType::Set => "SET",
            TokenType::Where => "WHERE",
            TokenType::Having => "HAVING",
            TokenType::GroupBy => "GROUP BY",
            TokenType::OrderBy => "ORDER BY",
            TokenType::Limit => "LIMIT",
            TokenType::Offset => "OFFSET",
            TokenType::Join => "JOIN",
            TokenType::InnerJoin => "INNER JOIN",
            TokenType::LeftJoin => "LEFT JOIN",
            TokenType::RightJoin => "RIGHT JOIN",
            TokenType::FullJoin => "FULL JOIN",
            TokenType::CrossJoin => "CROSS JOIN",
            TokenType::On => "ON",
            TokenType::Using => "USING",
            TokenType::And => "AND",
            TokenType::Or => "OR",
            TokenType::Not => "NOT",
            TokenType::In => "IN",
            TokenType::Exists => "EXISTS",
            TokenType::Between => "BETWEEN",
            TokenType::Like => "LIKE",
            TokenType::Is => "IS",
            TokenType::IsNull => "IS NULL",
            TokenType::IsNotNull => "IS NOT NULL",
            TokenType::Count => "COUNT",
            TokenType::Sum => "SUM",
            TokenType::Avg => "AVG",
            TokenType::Min => "MIN",
            TokenType::Max => "MAX",
            TokenType::Distinct => "DISTINCT",
            TokenType::All => "ALL",
            TokenType::Integer => "INTEGER",
            TokenType::Varchar => "VARCHAR",
            TokenType::Char => "CHAR",
            TokenType::Text => "TEXT",
            TokenType::Boolean => "BOOLEAN",
            TokenType::Date => "DATE",
            TokenType::Time => "TIME",
            TokenType::Timestamp => "TIMESTAMP",
            TokenType::Decimal => "DECIMAL",
            TokenType::Float => "FLOAT",
            TokenType::Double => "DOUBLE",
            TokenType::Begin => "BEGIN",
            TokenType::Commit => "COMMIT",
            TokenType::Rollback => "ROLLBACK",
            TokenType::Transaction => "TRANSACTION",
            TokenType::Case => "CASE",
            TokenType::When => "WHEN",
            TokenType::Then => "THEN",
            TokenType::Else => "ELSE",
            TokenType::End => "END",
            TokenType::Union => "UNION",
            TokenType::Intersect => "INTERSECT",
            TokenType::Except => "EXCEPT",
            TokenType::As => "AS",
            TokenType::Asc => "ASC",
            TokenType::Desc => "DESC",
            TokenType::True => "TRUE",
            TokenType::False => "FALSE",
            TokenType::Null => "NULL",
            TokenType::Identifier => "IDENTIFIER",
            TokenType::StringLiteral => "STRING",
            TokenType::IntegerLiteral => "INTEGER",
            TokenType::FloatLiteral => "FLOAT",
            TokenType::BooleanLiteral => "BOOLEAN",
            TokenType::NullLiteral => "NULL",
            TokenType::Plus => "+",
            TokenType::Minus => "-",
            TokenType::Multiply => "*",
            TokenType::Divide => "/",
            TokenType::Modulo => "%",
            TokenType::Equal => "=",
            TokenType::NotEqual => "<>",
            TokenType::Less => "<",
            TokenType::Greater => ">",
            TokenType::LessEqual => "<=",
            TokenType::GreaterEqual => ">=",
            TokenType::Assign => ":=",
            TokenType::LeftParen => "(",
            TokenType::RightParen => ")",
            TokenType::LeftBracket => "[",
            TokenType::RightBracket => "]",
            TokenType::LeftBrace => "{",
            TokenType::RightBrace => "}",
            TokenType::Comma => ",",
            TokenType::Semicolon => ";",
            TokenType::Dot => ".",
            TokenType::Colon => ":",
            TokenType::DoubleColon => "::",
            TokenType::Question => "?",
            TokenType::Comment => "COMMENT",
            TokenType::Whitespace => "WHITESPACE",
            TokenType::Newline => "NEWLINE",
            TokenType::Eof => "EOF",
            TokenType::Unknown => "UNKNOWN",
        };
        write!(f, "{}", name)
    }
}

/// Карта ключевых слов для быстрого поиска
pub fn keyword_map() -> std::collections::HashMap<&'static str, TokenType> {
    let mut map = std::collections::HashMap::new();
    
    // DDL
    map.insert("CREATE", TokenType::Create);
    map.insert("DROP", TokenType::Drop);
    map.insert("ALTER", TokenType::Alter);
    map.insert("TABLE", TokenType::Table);
    map.insert("INDEX", TokenType::Index);
    map.insert("DATABASE", TokenType::Database);
    map.insert("SCHEMA", TokenType::Schema);
    map.insert("VIEW", TokenType::View);
    map.insert("CONSTRAINT", TokenType::Constraint);
    map.insert("PRIMARY", TokenType::Primary);
    map.insert("FOREIGN", TokenType::Foreign);
    map.insert("KEY", TokenType::Key);
    map.insert("REFERENCES", TokenType::References);
    map.insert("UNIQUE", TokenType::Unique);
    map.insert("DEFAULT", TokenType::Default);
    map.insert("CHECK", TokenType::Check);
    
    // DML
    map.insert("SELECT", TokenType::Select);
    map.insert("INSERT", TokenType::Insert);
    map.insert("UPDATE", TokenType::Update);
    map.insert("DELETE", TokenType::Delete);
    map.insert("FROM", TokenType::From);
    map.insert("INTO", TokenType::Into);
    map.insert("VALUES", TokenType::Values);
    map.insert("SET", TokenType::Set);
    map.insert("WHERE", TokenType::Where);
    map.insert("HAVING", TokenType::Having);
    map.insert("LIMIT", TokenType::Limit);
    map.insert("OFFSET", TokenType::Offset);
    
    // JOIN
    map.insert("JOIN", TokenType::Join);
    map.insert("ON", TokenType::On);
    map.insert("USING", TokenType::Using);
    
    // Логические операторы
    map.insert("AND", TokenType::And);
    map.insert("OR", TokenType::Or);
    map.insert("NOT", TokenType::Not);
    map.insert("IN", TokenType::In);
    map.insert("EXISTS", TokenType::Exists);
    map.insert("BETWEEN", TokenType::Between);
    map.insert("LIKE", TokenType::Like);
    map.insert("IS", TokenType::Is);
    
    // Функции
    map.insert("COUNT", TokenType::Count);
    map.insert("SUM", TokenType::Sum);
    map.insert("AVG", TokenType::Avg);
    map.insert("MIN", TokenType::Min);
    map.insert("MAX", TokenType::Max);
    map.insert("DISTINCT", TokenType::Distinct);
    map.insert("ALL", TokenType::All);
    
    // Типы данных
    map.insert("INTEGER", TokenType::Integer);
    map.insert("INT", TokenType::Integer);
    map.insert("VARCHAR", TokenType::Varchar);
    map.insert("CHAR", TokenType::Char);
    map.insert("TEXT", TokenType::Text);
    map.insert("BOOLEAN", TokenType::Boolean);
    map.insert("BOOL", TokenType::Boolean);
    map.insert("DATE", TokenType::Date);
    map.insert("TIME", TokenType::Time);
    map.insert("TIMESTAMP", TokenType::Timestamp);
    map.insert("DECIMAL", TokenType::Decimal);
    map.insert("FLOAT", TokenType::Float);
    map.insert("DOUBLE", TokenType::Double);
    
    // Транзакции
    map.insert("BEGIN", TokenType::Begin);
    map.insert("COMMIT", TokenType::Commit);
    map.insert("ROLLBACK", TokenType::Rollback);
    map.insert("TRANSACTION", TokenType::Transaction);
    
    // Условные операторы
    map.insert("CASE", TokenType::Case);
    map.insert("WHEN", TokenType::When);
    map.insert("THEN", TokenType::Then);
    map.insert("ELSE", TokenType::Else);
    map.insert("END", TokenType::End);
    
    // Подзапросы
    map.insert("UNION", TokenType::Union);
    map.insert("INTERSECT", TokenType::Intersect);
    map.insert("EXCEPT", TokenType::Except);
    
    // Прочие
    map.insert("AS", TokenType::As);
    map.insert("ASC", TokenType::Asc);
    map.insert("DESC", TokenType::Desc);
    map.insert("TRUE", TokenType::True);
    map.insert("FALSE", TokenType::False);
    map.insert("NULL", TokenType::Null);
    
    // Составные ключевые слова
    map.insert("GROUP", TokenType::GroupBy); // будет обрабатываться отдельно с BY
    map.insert("ORDER", TokenType::OrderBy); // будет обрабатываться отдельно с BY
    map.insert("INNER", TokenType::InnerJoin); // будет обрабатываться отдельно с JOIN
    map.insert("LEFT", TokenType::LeftJoin); // будет обрабатываться отдельно с JOIN
    map.insert("RIGHT", TokenType::RightJoin); // будет обрабатываться отдельно с JOIN
    map.insert("FULL", TokenType::FullJoin); // будет обрабатываться отдельно с JOIN
    map.insert("CROSS", TokenType::CrossJoin); // будет обрабатываться отдельно с JOIN
    
    map
}
