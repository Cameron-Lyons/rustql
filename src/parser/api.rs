use super::*;

pub fn parse(tokens: Vec<Token>) -> Result<Statement, RustqlError> {
    let mut parser = Parser::new(tokens);
    let statement = parser
        .parse_statement()
        .map_err(|err| parser.with_current_location(err))?;
    parser
        .finish_single_statement(statement)
        .map_err(|err| parser.with_current_location(err))
}

pub fn parse_spanned(tokens: Vec<SpannedToken>) -> Result<Statement, RustqlError> {
    let mut parser = Parser::new_spanned(tokens);
    let statement = parser
        .parse_statement()
        .map_err(|err| parser.with_current_location(err))?;
    parser
        .finish_single_statement(statement)
        .map_err(|err| parser.with_current_location(err))
}

pub fn parse_script(tokens: Vec<Token>) -> Result<Vec<Statement>, RustqlError> {
    parse_script_with(Parser::new(tokens))
}

pub fn parse_script_spanned(tokens: Vec<SpannedToken>) -> Result<Vec<Statement>, RustqlError> {
    parse_script_with(Parser::new_spanned(tokens))
}

fn parse_script_with(mut parser: Parser) -> Result<Vec<Statement>, RustqlError> {
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
        } else if *parser.current_token() != Token::Eof {
            return Err(
                parser.with_current_location(RustqlError::ParseError(format!(
                    "Expected semicolon or end of input, found {:?}",
                    parser.current_token()
                ))),
            );
        }
    }

    Ok(statements)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::{tokenize, tokenize_spanned};

    #[test]
    fn script_entry_points_parse_same_statement_boundaries() {
        let sql = "; SELECT 1; ; SELECT 2;";

        let plain = parse_script(tokenize(sql).expect("plain tokens should lex"))
            .expect("plain script should parse");
        let spanned =
            parse_script_spanned(tokenize_spanned(sql).expect("spanned tokens should lex"))
                .expect("spanned script should parse");

        assert_eq!(plain.len(), 2);
        assert_eq!(plain, spanned);
    }

    #[test]
    fn script_entry_points_reject_missing_statement_separator() {
        let sql = "SELECT 1 SELECT 2";

        let plain = parse_script(tokenize(sql).expect("plain tokens should lex"))
            .expect_err("plain script should reject missing separator");
        let spanned =
            parse_script_spanned(tokenize_spanned(sql).expect("spanned tokens should lex"))
                .expect_err("spanned script should reject missing separator");

        assert!(
            plain
                .to_string()
                .contains("Expected semicolon or end of input")
        );
        assert!(
            spanned
                .to_string()
                .contains("Expected semicolon or end of input")
        );
        assert!(spanned.span().is_some());
    }
}
