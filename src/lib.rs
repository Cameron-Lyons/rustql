pub mod ast;
pub mod database;
pub mod executor;
pub mod lexer;
pub mod parser;

use std::fs;
use std::path::Path;

pub fn process_query(query: &str) -> Result<String, String> {
    let tokens = lexer::tokenize(query)?;
    let statement = parser::parse(tokens)?;
    executor::execute(statement)
}

pub fn reset_database() {
    #[cfg(test)]
    executor::reset_database_state();

    let db_file = "rustql_data.json";
    if Path::new(db_file).exists() {
        fs::remove_file(db_file).ok();
    }
}
