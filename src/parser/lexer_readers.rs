// Методы чтения специальных токенов для лексического анализатора

impl Lexer {
    /// Читает однострочный комментарий
    pub(crate) fn read_single_line_comment(&mut self) -> Result<Token> {
        let start_position = self.current_position.clone();
        let mut value = String::new();
        
        // Проверяем, что действительно есть "--"
        if self.input[self.position] == '-' && self.peek() == Some('-') {
            // Пропускаем "--"
            value.push(self.advance());
            value.push(self.advance());
            
            // Читаем до конца строки
            while let Some(ch) = self.peek() {
                if ch == '\n' {
                    break;
                }
                value.push(self.advance());
            }
            
            Ok(Token::new(TokenType::Comment, value, start_position))
        } else {
            // Это не комментарий, возвращаем ошибку
            Err(crate::common::Error::internal("Expected '--' for single line comment".to_string()))
        }
    }
    
    /// Читает многострочный комментарий
    pub(crate) fn read_multi_line_comment(&mut self) -> Result<Token> {
        let start_position = self.current_position.clone();
        let mut value = String::new();
        
        // Пропускаем "/*"
        value.push(self.advance());
        value.push(self.advance());
        
        // Читаем до "*/"
        while self.position < self.input.len() {
            let ch = self.advance();
            value.push(ch);
            
            if ch == '*' && self.peek() == Some('/') {
                value.push(self.advance());
                break;
            }
        }
        
        Ok(Token::new(TokenType::Comment, value, start_position))
    }
    
    /// Читает строковый литерал
    pub(crate) fn read_string_literal(&mut self) -> Result<Token> {
        let start_position = self.current_position.clone();
        let mut value = String::new();
        
        let quote_char = self.advance(); // '
        value.push(quote_char);
        
        while let Some(ch) = self.peek() {
            if ch == quote_char {
                value.push(self.advance());
                break;
            } else if ch == '\\' {
                // Обрабатываем экранированные символы
                value.push(self.advance()); // \
                if let Some(_escaped) = self.peek() {
                    value.push(self.advance());
                }
            } else {
                value.push(self.advance());
            }
        }
        
        Ok(Token::new(TokenType::StringLiteral, value, start_position))
    }
    
    /// Читает идентификатор в кавычках
    pub(crate) fn read_quoted_identifier(&mut self) -> Result<Token> {
        let start_position = self.current_position.clone();
        let mut value = String::new();
        
        let quote_char = self.advance(); // "
        value.push(quote_char);
        
        while let Some(ch) = self.peek() {
            if ch == quote_char {
                value.push(self.advance());
                break;
            } else {
                value.push(self.advance());
            }
        }
        
        Ok(Token::new(TokenType::Identifier, value, start_position))
    }
    
    /// Читает числовой литерал
    pub(crate) fn read_number(&mut self) -> Result<Token> {
        let start_position = self.current_position.clone();
        let mut value = String::new();
        let mut is_float = false;
        
        // Читаем цифры
        while let Some(ch) = self.peek() {
            if ch.is_ascii_digit() {
                value.push(self.advance());
            } else if ch == '.' && !is_float {
                // Проверяем, что после точки есть цифра
                if let Some(next_ch) = self.peek_ahead(1) {
                    if next_ch.is_ascii_digit() {
                        is_float = true;
                        value.push(self.advance());
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            } else if ch == 'e' || ch == 'E' {
                // Научная нотация
                is_float = true;
                value.push(self.advance());
                
                // Опциональный знак
                if let Some(sign) = self.peek() {
                    if sign == '+' || sign == '-' {
                        value.push(self.advance());
                    }
                }
            } else {
                break;
            }
        }
        
        let token_type = if is_float {
            TokenType::FloatLiteral
        } else {
            TokenType::IntegerLiteral
        };
        
        Ok(Token::new(token_type, value, start_position))
    }
    
    /// Читает простой идентификатор без обработки составных ключевых слов
    pub(crate) fn read_simple_identifier(&mut self) -> Result<Token> {
        let start_position = self.current_position.clone();
        let mut value = String::new();
        
        // Читаем буквы, цифры и подчеркивания
        while let Some(ch) = self.peek() {
            if ch.is_alphanumeric() || ch == '_' {
                value.push(self.advance());
            } else {
                break;
            }
        }
        
        // Проверяем, является ли это ключевым словом (но без составных слов)
        let upper_value = value.to_uppercase();
        let token_type = if let Some(keyword_type) = self.keywords.get(upper_value.as_str()) {
            *keyword_type
        } else {
            TokenType::Identifier
        };
        
        Ok(Token::new(token_type, value, start_position))
    }

    /// Читает идентификатор или ключевое слово
    pub(crate) fn read_identifier_or_keyword(&mut self) -> Result<Token> {
        let start_position = self.current_position.clone();
        let mut value = String::new();
        
        // Читаем буквы, цифры и подчеркивания
        while let Some(ch) = self.peek() {
            if ch.is_alphanumeric() || ch == '_' {
                value.push(self.advance());
            } else {
                break;
            }
        }
        
        // Проверяем, является ли это ключевым словом
        let upper_value = value.to_uppercase();
        let (token_type, final_value) = if let Some(keyword_type) = self.keywords.get(upper_value.as_str()) {
            // Обрабатываем составные ключевые слова
            self.handle_compound_keywords(*keyword_type, &upper_value)?
        } else {
            (TokenType::Identifier, value.clone())
        };
        
        Ok(Token::new(token_type, final_value, start_position))
    }
    
    /// Обрабатывает составные ключевые слова (GROUP BY, ORDER BY, etc.)
    pub(crate) fn handle_compound_keywords(&mut self, token_type: TokenType, value: &str) -> Result<(TokenType, String)> {
        match value {
            "GROUP" => {
                // Проверяем, следует ли "BY"
                let saved_pos = self.position;
                let saved_current_pos = self.current_position.clone();
                
                self.skip_whitespace();
                if let Ok(next_token) = self.read_simple_identifier() {
                    if next_token.value.to_uppercase() == "BY" {
                        // НЕ возвращаем позицию - потребляем второй токен
                        return Ok((TokenType::GroupBy, "GROUP BY".to_string()));
                    }
                }
                
                // Возвращаем позицию обратно только если не нашли составное слово
                self.position = saved_pos;
                self.current_position = saved_current_pos;
                Ok((TokenType::Identifier, value.to_string()))
            },
            "ORDER" => {
                // Проверяем, следует ли "BY"
                let saved_pos = self.position;
                let saved_current_pos = self.current_position.clone();
                
                self.skip_whitespace();
                if let Ok(next_token) = self.read_simple_identifier() {
                    if next_token.value.to_uppercase() == "BY" {
                        // НЕ возвращаем позицию - потребляем второй токен
                        return Ok((TokenType::OrderBy, "ORDER BY".to_string()));
                    }
                }
                
                // Возвращаем позицию обратно только если не нашли составное слово
                self.position = saved_pos;
                self.current_position = saved_current_pos;
                Ok((TokenType::Identifier, value.to_string()))
            },
            "INNER" | "LEFT" | "RIGHT" | "FULL" | "CROSS" => {
                // Проверяем, следует ли "JOIN"
                let saved_pos = self.position;
                let saved_current_pos = self.current_position.clone();
                
                self.skip_whitespace();
                if let Ok(next_token) = self.read_simple_identifier() {
                    if next_token.value.to_uppercase() == "JOIN" {
                        // НЕ возвращаем позицию - потребляем второй токен
                        return Ok(match value {
                            "INNER" => (TokenType::InnerJoin, "INNER JOIN".to_string()),
                            "LEFT" => (TokenType::LeftJoin, "LEFT JOIN".to_string()),
                            "RIGHT" => (TokenType::RightJoin, "RIGHT JOIN".to_string()),
                            "FULL" => (TokenType::FullJoin, "FULL JOIN".to_string()),
                            "CROSS" => (TokenType::CrossJoin, "CROSS JOIN".to_string()),
                            _ => unreachable!(),
                        });
                    }
                }
                
                // Возвращаем позицию обратно только если не нашли составное слово
                self.position = saved_pos;
                self.current_position = saved_current_pos;
                Ok((TokenType::Identifier, value.to_string()))
            },
            "IS" => {
                // Проверяем "IS NULL" или "IS NOT NULL"
                let saved_pos = self.position;
                let saved_current_pos = self.current_position.clone();
                
                self.skip_whitespace();
                if let Ok(next_token) = self.read_simple_identifier() {
                    let next_upper = next_token.value.to_uppercase();
                    if next_upper == "NULL" {
                        // НЕ возвращаем позицию - потребляем второй токен
                        return Ok((TokenType::IsNull, "IS NULL".to_string()));
                    } else if next_upper == "NOT" {
                        self.skip_whitespace();
                        if let Ok(third_token) = self.read_simple_identifier() {
                            if third_token.value.to_uppercase() == "NULL" {
                                // НЕ возвращаем позицию - потребляем все три токена
                                return Ok((TokenType::IsNotNull, "IS NOT NULL".to_string()));
                            }
                        }
                    }
                }
                
                // Возвращаем позицию обратно только если не нашли составное слово
                self.position = saved_pos;
                self.current_position = saved_current_pos;
                Ok((TokenType::Identifier, value.to_string()))
            },
            "NOT" => {
                // Проверяем "NOT NULL"
                let saved_pos = self.position;
                let saved_current_pos = self.current_position.clone();
                
                self.skip_whitespace();
                if let Ok(next_token) = self.read_simple_identifier() {
                    if next_token.value.to_uppercase() == "NULL" {
                        // НЕ возвращаем позицию - потребляем второй токен
                        return Ok((TokenType::NotNull, "NOT NULL".to_string()));
                    }
                }
                
                // Возвращаем позицию обратно только если не нашли составное слово
                self.position = saved_pos;
                self.current_position = saved_current_pos;
                Ok((token_type, value.to_string()))
            },
            _ => Ok((token_type, value.to_string())),
        }
    }
    
    /// Читает операторы сравнения
    pub(crate) fn read_comparison_operator(&mut self) -> Result<Token> {
        let start_position = self.current_position.clone();
        let first_char = self.advance();
        
        match first_char {
            '<' => {
                if self.peek() == Some('=') {
                    let second_char = self.advance();
                    Ok(Token::new(
                        TokenType::LessEqual,
                        format!("{}{}", first_char, second_char),
                        start_position,
                    ))
                } else if self.peek() == Some('>') {
                    let second_char = self.advance();
                    Ok(Token::new(
                        TokenType::NotEqual,
                        format!("{}{}", first_char, second_char),
                        start_position,
                    ))
                } else {
                    Ok(Token::new(TokenType::Less, first_char.to_string(), start_position))
                }
            },
            '>' => {
                if self.peek() == Some('=') {
                    let second_char = self.advance();
                    Ok(Token::new(
                        TokenType::GreaterEqual,
                        format!("{}{}", first_char, second_char),
                        start_position,
                    ))
                } else {
                    Ok(Token::new(TokenType::Greater, first_char.to_string(), start_position))
                }
            },
            _ => unreachable!(),
        }
    }
}
