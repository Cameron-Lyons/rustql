use crate::ast::*;
use crate::error::RustqlError;
use crate::error::SourceSpan;
use crate::lexer::{SpannedToken, Token};

type ParseOverClauseResult =
    Result<(Vec<Expression>, Vec<OrderByExpr>, Option<WindowFrame>), RustqlError>;

mod api;
mod core;
mod expressions;
mod select_clauses;
mod statements;
mod tokens;

#[allow(unused_imports)]
pub use api::{parse, parse_script, parse_script_spanned, parse_spanned};
use tokens::{token_to_sql, token_to_string};

pub struct Parser {
    tokens: Vec<Token>,
    spans: Vec<Option<SourceSpan>>,
    current: usize,
}

#[cfg(test)]
mod tests;
