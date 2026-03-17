//! SQL lexical analyzer for rustdb
//!
//! Converts input SQL text into a sequence of tokens for further parsing.
//! Supports all major SQL constructs, including keywords, identifiers,
//! literals, operators and comments.

use crate::common::{Error, Result};
use crate::parser::token::{keyword_map, Position, Token, TokenType};
use std::collections::HashMap;

/// SQL lexical analyzer
pub struct Lexer {
    /// Source text
    input: Vec<char>,
    /// Current position in text
    position: usize,
    /// Current position for error display
    current_position: Position,
    /// Keywords map
    keywords: HashMap<&'static str, TokenType>,
    /// Token buffer for lookahead
    token_buffer: Vec<Token>,
    /// Position in token buffer
    buffer_position: usize,
}

impl Lexer {
    /// Creates a new lexical analyzer
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

// Include methods from separate files
include!("lexer_methods.rs");
include!("lexer_readers.rs");
