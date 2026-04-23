use super::*;

impl Parser {
    pub(crate) fn parse_explain(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Explain)?;
        let analyze = if *self.current_token() == Token::Analyze {
            self.advance();
            true
        } else {
            false
        };
        let select_stmt = if *self.current_token() == Token::With {
            self.parse_with_select()?
        } else {
            self.parse_select_statement(Vec::new())?
        };
        if let Statement::Select(select_stmt) = select_stmt {
            if analyze {
                Ok(Statement::ExplainAnalyze(select_stmt))
            } else {
                Ok(Statement::Explain(select_stmt))
            }
        } else {
            Err(RustqlError::ParseError(
                "EXPLAIN must be followed by a SELECT statement".to_string(),
            ))
        }
    }

    pub(crate) fn parse_describe(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Describe)?;
        let table_name = match self.advance() {
            Token::Identifier(name) => name,
            _ => {
                return Err(RustqlError::ParseError(
                    "Expected table name after DESCRIBE".to_string(),
                ));
            }
        };
        Ok(Statement::Describe(table_name))
    }

    pub(crate) fn parse_show(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Show)?;
        if *self.current_token() == Token::Tables {
            self.advance();
            Ok(Statement::ShowTables)
        } else {
            Err(RustqlError::ParseError(
                "SHOW must be followed by TABLES".to_string(),
            ))
        }
    }

    pub(crate) fn parse_analyze(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Analyze)?;
        let table_name = match self.advance() {
            Token::Identifier(name) => name,
            _ => {
                return Err(RustqlError::ParseError(
                    "Expected table name after ANALYZE".to_string(),
                ));
            }
        };
        Ok(Statement::Analyze(table_name))
    }

    pub(crate) fn parse_truncate(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Truncate)?;
        if *self.current_token() == Token::Table {
            self.advance();
        }
        let table_name = match self.advance() {
            Token::Identifier(name) => name,
            _ => {
                return Err(RustqlError::ParseError(
                    "Expected table name after TRUNCATE".to_string(),
                ));
            }
        };
        Ok(Statement::TruncateTable { table_name })
    }

    pub(crate) fn parse_do_block(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Do)?;
        self.consume(Token::Begin)?;
        let mut statements = Vec::new();
        while *self.current_token() != Token::End && *self.current_token() != Token::Eof {
            let stmt = self.parse_statement()?;
            statements.push(stmt);
            if *self.current_token() == Token::Semicolon {
                self.advance();
            }
        }
        self.consume(Token::End)?;
        Ok(Statement::Do { statements })
    }
}
