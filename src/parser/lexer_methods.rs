// Lexer methods

impl Lexer {
    /// Returns next token
    pub fn next_token(&mut self) -> Result<Token> {
        // Check token buffer
        if self.buffer_position < self.token_buffer.len() {
            let token = self.token_buffer[self.buffer_position].clone();
            self.buffer_position += 1;
            return Ok(token);
        }
        
        // Skip whitespace
        self.skip_whitespace();
        
        // Check end of file
        if self.position >= self.input.len() {
            return Ok(Token::new(
                TokenType::Eof,
                String::new(),
                self.current_position.clone(),
            ));
        }
        
        let start_position = self.current_position.clone();
        let current_char = self.input[self.position];
        
        // Determine token type by first character
        let token = match current_char {
            // String literals
            '\'' => self.read_string_literal()?,
            '"' => self.read_quoted_identifier()?,
            
            // Numeric literals
            '0'..='9' => self.read_number()?,
            
            // Identifiers and keywords
            'a'..='z' | 'A'..='Z' | '_' => self.read_identifier_or_keyword()?,
            
            // Comments (check after operators)
            '-' if self.position + 1 < self.input.len() && self.input[self.position + 1] == '-' => self.read_single_line_comment()?,
            '/' if self.position + 1 < self.input.len() && self.input[self.position + 1] == '*' => self.read_multi_line_comment()?,
            
            // Operators and symbols
            '+' => self.read_single_char_token(TokenType::Plus),
            '-' => self.read_single_char_token(TokenType::Minus),
            '*' => self.read_single_char_token(TokenType::Multiply),
            '/' => self.read_single_char_token(TokenType::Divide),
            '%' => self.read_single_char_token(TokenType::Modulo),
            '=' => self.read_single_char_token(TokenType::Equal),
            '<' => self.read_comparison_operator()?,
            '>' => self.read_comparison_operator()?,
            '!' if self.position + 1 < self.input.len() && self.input[self.position + 1] == '=' => self.read_two_char_token(TokenType::NotEqual),
            ':' if self.position + 1 < self.input.len() && self.input[self.position + 1] == '=' => self.read_two_char_token(TokenType::Assign),
            ':' if self.position + 1 < self.input.len() && self.input[self.position + 1] == ':' => self.read_two_char_token(TokenType::DoubleColon),
            ':' => self.read_single_char_token(TokenType::Colon),
            
            // Delimiters
            '(' => self.read_single_char_token(TokenType::LeftParen),
            ')' => self.read_single_char_token(TokenType::RightParen),
            '[' => self.read_single_char_token(TokenType::LeftBracket),
            ']' => self.read_single_char_token(TokenType::RightBracket),
            '{' => self.read_single_char_token(TokenType::LeftBrace),
            '}' => self.read_single_char_token(TokenType::RightBrace),
            ',' => self.read_single_char_token(TokenType::Comma),
            ';' => self.read_single_char_token(TokenType::Semicolon),
            '.' => self.read_single_char_token(TokenType::Dot),
            '?' => self.read_single_char_token(TokenType::Question),
            
            // Unknown character
            _ => {
                let unknown_char = self.advance();
                Token::new(
                    TokenType::Unknown,
                    unknown_char.to_string(),
                    start_position,
                )
            }
        };
        
        Ok(token)
    }
    
    /// Returns next token without consuming it (lookahead)
    pub fn peek_token(&mut self) -> Result<Token> {
        let token = self.next_token()?;
        
        // Add token to buffer
        if self.buffer_position == self.token_buffer.len() {
            self.token_buffer.push(token.clone());
        } else {
            self.token_buffer[self.buffer_position] = token.clone();
        }
        
        // Move buffer position back
        if self.buffer_position > 0 {
            self.buffer_position -= 1;
        }
        
        Ok(token)
    }
    
    /// Returns all tokens from input text
    pub fn tokenize(&mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();
        
        loop {
            let token = self.next_token()?;
            let is_eof = token.token_type == TokenType::Eof;
            
            // Skip whitespace and comments in final token list
            if !token.token_type.should_skip() {
                tokens.push(token);
            }
            
            if is_eof {
                break;
            }
        }
        
        Ok(tokens)
    }
    
    // === Helper methods ===
    
    /// Returns current character and advances position
    pub(crate) fn advance(&mut self) -> char {
        if self.position >= self.input.len() {
            return '\0';
        }
        
        let ch = self.input[self.position];
        self.position += 1;
        
        if ch == '\n' {
            self.current_position.line += 1;
            self.current_position.column = 1;
        } else {
            self.current_position.column += 1;
        }
        self.current_position.offset += 1;
        
        ch
    }
    
    /// Returns next character without advancing position
    pub(crate) fn peek(&self) -> Option<char> {
        if self.position >= self.input.len() {
            None
        } else {
            Some(self.input[self.position])
        }
    }
    
    /// Returns character at specified distance from current position
    pub(crate) fn peek_ahead(&self, offset: usize) -> Option<char> {
        let pos = self.position + offset;
        if pos >= self.input.len() {
            None
        } else {
            Some(self.input[pos])
        }
    }
    
    /// Skips whitespace characters
    pub(crate) fn skip_whitespace(&mut self) {
        while let Some(ch) = self.peek() {
            if ch.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }
    
    /// Reads token from single character
    pub(crate) fn read_single_char_token(&mut self, token_type: TokenType) -> Token {
        let start_position = self.current_position.clone();
        let ch = self.advance();
        Token::new(token_type, ch.to_string(), start_position)
    }
    
    /// Reads token from two characters
    pub(crate) fn read_two_char_token(&mut self, token_type: TokenType) -> Token {
        let start_position = self.current_position.clone();
        let first = self.advance();
        let second = self.advance();
        let value = format!("{}{}", first, second);
        Token::new(token_type, value, start_position)
    }
}
