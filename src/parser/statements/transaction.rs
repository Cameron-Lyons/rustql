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
            return Ok(Statement::RollbackTransaction);
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
