use super::Parser;
use crate::ast::Statement;
use crate::error::RustqlError;
use crate::lexer::Token;

pub fn parse(tokens: Vec<Token>) -> Result<Statement, RustqlError> {
    let mut parser = Parser::new(tokens);
    parser.parse_statement()
}

pub fn parse_script(tokens: Vec<Token>) -> Result<Vec<Statement>, RustqlError> {
    let mut parser = Parser::new(tokens);
    let mut statements = Vec::new();

    while *parser.current_token() != Token::Eof {
        if *parser.current_token() == Token::Semicolon {
            parser.advance();
            continue;
        }

        statements.push(parser.parse_statement()?);

        if *parser.current_token() == Token::Semicolon {
            parser.advance();
        }
    }

    Ok(statements)
}
