use super::*;

impl Parser {
    pub(crate) fn parse_begin_transaction(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Begin)?;
        if *self.current_token() == Token::Transaction {
            self.advance();
        }
        Ok(Statement::BeginTransaction)
    }

    pub(crate) fn parse_commit_transaction(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Commit)?;
        if *self.current_token() == Token::Transaction {
            self.advance();
        }
        Ok(Statement::CommitTransaction)
    }

    pub(crate) fn parse_rollback(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Rollback)?;
        if *self.current_token() == Token::Transaction {
            self.advance();
        }
        if *self.current_token() == Token::To {
            self.advance();
            if *self.current_token() == Token::Savepoint {
                self.advance();
            }
            let name = match self.advance() {
                Token::Identifier(name) => name,
                _ => {
                    return Err(RustqlError::ParseError(
                        "Expected savepoint name after ROLLBACK TO".to_string(),
                    ));
                }
            };
            return Ok(Statement::RollbackToSavepoint(name));
        }
        Ok(Statement::RollbackTransaction)
    }

    pub(crate) fn parse_savepoint(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Savepoint)?;
        let name = match self.advance() {
            Token::Identifier(name) => name,
            _ => {
                return Err(RustqlError::ParseError(
                    "Expected savepoint name".to_string(),
                ));
            }
        };
        Ok(Statement::Savepoint(name))
    }

    pub(crate) fn parse_release_savepoint(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Release)?;
        if *self.current_token() == Token::Savepoint {
            self.advance();
        }
        let name = match self.advance() {
            Token::Identifier(name) => name,
            _ => {
                return Err(RustqlError::ParseError(
                    "Expected savepoint name after RELEASE".to_string(),
                ));
            }
        };
        Ok(Statement::ReleaseSavepoint(name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::tokenize;
    use crate::parser::parse;

    fn parse_sql(sql: &str) -> Result<Statement, RustqlError> {
        parse(tokenize(sql).expect("SQL should lex"))
    }

    #[test]
    fn rollback_transaction_to_savepoint_parses_savepoint_target() {
        let statement = parse_sql("ROLLBACK TRANSACTION TO SAVEPOINT sp1")
            .expect("ROLLBACK TRANSACTION TO SAVEPOINT should parse");

        assert_eq!(statement, Statement::RollbackToSavepoint("sp1".to_string()));
    }

    #[test]
    fn rollback_transaction_without_target_stays_full_transaction_rollback() {
        let statement =
            parse_sql("ROLLBACK TRANSACTION").expect("ROLLBACK TRANSACTION should parse");

        assert_eq!(statement, Statement::RollbackTransaction);
    }
}
