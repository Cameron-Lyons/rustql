use super::Parser;
use crate::ast::Statement;
use crate::error::RustqlError;
use crate::lexer::{SpannedToken, Token};

pub fn parse(tokens: Vec<Token>) -> Result<Statement, RustqlError> {
    let mut parser = Parser::new(tokens);
    parser
        .parse_statement()
        .map_err(|err| parser.with_current_location(err))
}

pub fn parse_spanned(tokens: Vec<SpannedToken>) -> Result<Statement, RustqlError> {
    let mut parser = Parser::new_spanned(tokens);
    parser
        .parse_statement()
        .map_err(|err| parser.with_current_location(err))
}

pub fn parse_script(tokens: Vec<Token>) -> Result<Vec<Statement>, RustqlError> {
    let mut parser = Parser::new(tokens);
    let mut statements = Vec::new();

    while *parser.current_token() != Token::Eof {
        if *parser.current_token() == Token::Semicolon {
            parser.advance();
            continue;
        }

        let statement = parser
            .parse_statement()
            .map_err(|err| parser.with_current_location(err))?;
        statements.push(statement);

        if *parser.current_token() == Token::Semicolon {
            parser.advance();
        }
    }

    Ok(statements)
}

pub fn parse_script_spanned(tokens: Vec<SpannedToken>) -> Result<Vec<Statement>, RustqlError> {
    let mut parser = Parser::new_spanned(tokens);
    let mut statements = Vec::new();

    while *parser.current_token() != Token::Eof {
        if *parser.current_token() == Token::Semicolon {
            parser.advance();
            continue;
        }

        let statement = parser
            .parse_statement()
            .map_err(|err| parser.with_current_location(err))?;
        statements.push(statement);

        if *parser.current_token() == Token::Semicolon {
            parser.advance();
        }
    }

    Ok(statements)
}
