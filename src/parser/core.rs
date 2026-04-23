use super::*;

impl Parser {
    pub(super) fn new(tokens: Vec<Token>) -> Self {
        let spans = vec![None; tokens.len()];
        Parser {
            tokens,
            spans,
            current: 0,
        }
    }

    pub(super) fn new_spanned(tokens: Vec<SpannedToken>) -> Self {
        let (tokens, spans): (Vec<_>, Vec<_>) = tokens
            .into_iter()
            .map(|spanned| (spanned.token, Some(spanned.span)))
            .unzip();
        Parser {
            tokens,
            spans,
            current: 0,
        }
    }

    pub(super) fn current_token(&self) -> &Token {
        self.tokens.get(self.current).unwrap_or(&Token::Eof)
    }

    pub(super) fn current_span(&self) -> Option<SourceSpan> {
        self.spans.get(self.current).copied().flatten()
    }

    pub(super) fn with_current_location(&self, err: RustqlError) -> RustqlError {
        match self.current_span() {
            Some(span) => err.with_parse_span(span),
            None => err,
        }
    }

    pub(super) fn consume(&mut self, expected: Token) -> Result<(), RustqlError> {
        if *self.current_token() == expected {
            self.current += 1;
            Ok(())
        } else {
            Err(RustqlError::ParseError(format!(
                "Expected {:?}, found {:?}",
                expected,
                self.current_token()
            )))
        }
    }

    pub(super) fn advance(&mut self) -> Token {
        let token = self.current_token().clone();
        self.current += 1;
        token
    }

    pub(super) fn parse_statement(&mut self) -> Result<Statement, RustqlError> {
        match self.current_token() {
            Token::Explain => self.parse_explain(),
            Token::With => self.parse_with_select(),
            Token::Select => self.parse_select_statement(Vec::new()),
            Token::Insert => self.parse_insert(),
            Token::Update => self.parse_update(),
            Token::Delete => self.parse_delete(),
            Token::Create => self.parse_create(),
            Token::Drop => self.parse_drop(),
            Token::Alter => self.parse_alter(),
            Token::Begin => self.parse_begin_transaction(),
            Token::Commit => self.parse_commit_transaction(),
            Token::Rollback => self.parse_rollback(),
            Token::Savepoint => self.parse_savepoint(),
            Token::Release => self.parse_release_savepoint(),
            Token::Describe => self.parse_describe(),
            Token::Show => self.parse_show(),
            Token::Analyze => self.parse_analyze(),
            Token::Truncate => self.parse_truncate(),
            Token::Merge => self.parse_merge(),
            Token::Do => self.parse_do_block(),
            _ => Err(RustqlError::ParseError(format!(
                "Unexpected token: {:?}",
                self.current_token()
            ))),
        }
    }

    pub(super) fn finish_single_statement(
        &mut self,
        statement: Statement,
    ) -> Result<Statement, RustqlError> {
        while *self.current_token() == Token::Semicolon {
            self.advance();
        }

        if *self.current_token() != Token::Eof {
            return Err(RustqlError::ParseError(format!(
                "Unexpected trailing token: {:?}",
                self.current_token()
            )));
        }

        Ok(statement)
    }
}
