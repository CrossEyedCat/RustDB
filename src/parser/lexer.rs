//! Лексический анализатор SQL для rustdb
//! 
//! Преобразует входной SQL текст в последовательность токенов для дальнейшего парсинга.
//! Поддерживает все основные конструкции SQL, включая ключевые слова, идентификаторы,
//! литералы, операторы и комментарии.

use crate::common::{Error, Result};
use crate::parser::token::{Token, TokenType, Position, keyword_map};
use std::collections::HashMap;

/// Лексический анализатор SQL
pub struct Lexer {
    /// Исходный текст
    input: Vec<char>,
    /// Текущая позиция в тексте
    position: usize,
    /// Текущая позиция для отображения ошибок
    current_position: Position,
    /// Карта ключевых слов
    keywords: HashMap<&'static str, TokenType>,
    /// Буфер токенов для lookahead
    token_buffer: Vec<Token>,
    /// Позиция в буфере токенов
    buffer_position: usize,
}

impl Lexer {
    /// Создает новый лексический анализатор
    pub fn new(input: &str) -> Result<Self> {
        Ok(Self {
            input: input.chars().collect(),
            position: 0,
            current_position: Position::start(),
            keywords: keyword_map(),
            token_buffer: Vec::new(),
            buffer_position: 0,
        })
    }
}

// Подключаем методы из отдельных файлов
include!("lexer_methods.rs");
include!("lexer_readers.rs");
