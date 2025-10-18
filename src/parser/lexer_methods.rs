// Методы лексического анализатора

impl Lexer {
    /// Возвращает следующий токен
    pub fn next_token(&mut self) -> Result<Token> {
        // Проверяем буфер токенов
        if self.buffer_position < self.token_buffer.len() {
            let token = self.token_buffer[self.buffer_position].clone();
            self.buffer_position += 1;
            return Ok(token);
        }
        
        // Пропускаем пробелы
        self.skip_whitespace();
        
        // Проверяем конец файла
        if self.position >= self.input.len() {
            return Ok(Token::new(
                TokenType::Eof,
                String::new(),
                self.current_position.clone(),
            ));
        }
        
        let start_position = self.current_position.clone();
        let current_char = self.input[self.position];
        
        // Определяем тип токена по первому символу
        let token = match current_char {
            // Строковые литералы
            '\'' => self.read_string_literal()?,
            '"' => self.read_quoted_identifier()?,
            
            // Числовые литералы
            '0'..='9' => self.read_number()?,
            
            // Идентификаторы и ключевые слова
            'a'..='z' | 'A'..='Z' | '_' => self.read_identifier_or_keyword()?,
            
            // Комментарии (проверяем после операторов)
            '-' if self.position + 1 < self.input.len() && self.input[self.position + 1] == '-' => self.read_single_line_comment()?,
            '/' if self.position + 1 < self.input.len() && self.input[self.position + 1] == '*' => self.read_multi_line_comment()?,
            
            // Операторы и символы
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
            
            // Разделители
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
            
            // Неизвестный символ
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
    
    /// Возвращает следующий токен без его потребления (lookahead)
    pub fn peek_token(&mut self) -> Result<Token> {
        let token = self.next_token()?;
        
        // Добавляем токен в буфер
        if self.buffer_position == self.token_buffer.len() {
            self.token_buffer.push(token.clone());
        } else {
            self.token_buffer[self.buffer_position] = token.clone();
        }
        
        // Возвращаем позицию буфера назад
        if self.buffer_position > 0 {
            self.buffer_position -= 1;
        }
        
        Ok(token)
    }
    
    /// Возвращает все токены из входного текста
    pub fn tokenize(&mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();
        
        loop {
            let token = self.next_token()?;
            let is_eof = token.token_type == TokenType::Eof;
            
            // Пропускаем пробелы и комментарии в финальном списке токенов
            if !token.token_type.should_skip() {
                tokens.push(token);
            }
            
            if is_eof {
                break;
            }
        }
        
        Ok(tokens)
    }
    
    // === Вспомогательные методы ===
    
    /// Возвращает текущий символ и продвигает позицию
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
    
    /// Возвращает следующий символ без продвижения позиции
    pub(crate) fn peek(&self) -> Option<char> {
        if self.position >= self.input.len() {
            None
        } else {
            Some(self.input[self.position])
        }
    }
    
    /// Возвращает символ на определенном расстоянии от текущей позиции
    pub(crate) fn peek_ahead(&self, offset: usize) -> Option<char> {
        let pos = self.position + offset;
        if pos >= self.input.len() {
            None
        } else {
            Some(self.input[pos])
        }
    }
    
    /// Пропускает пробельные символы
    pub(crate) fn skip_whitespace(&mut self) {
        while let Some(ch) = self.peek() {
            if ch.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }
    
    /// Читает токен из одного символа
    pub(crate) fn read_single_char_token(&mut self, token_type: TokenType) -> Token {
        let start_position = self.current_position.clone();
        let ch = self.advance();
        Token::new(token_type, ch.to_string(), start_position)
    }
    
    /// Читает токен из двух символов
    pub(crate) fn read_two_char_token(&mut self, token_type: TokenType) -> Token {
        let start_position = self.current_position.clone();
        let first = self.advance();
        let second = self.advance();
        let value = format!("{}{}", first, second);
        Token::new(token_type, value, start_position)
    }
}
