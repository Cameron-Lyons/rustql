pub mod ast;
pub mod database;
pub mod error;
pub mod executor;
pub mod lexer;
pub mod parser;
pub mod plan_executor;
pub mod planner;
pub mod storage;
pub mod wal;

use std::fs;
use std::path::Path;

pub fn process_query(query: &str) -> Result<String, String> {
    let tokens = lexer::tokenize(query).map_err(|e| e.to_string())?;
    let statement = parser::parse(tokens).map_err(|e| e.to_string())?;
    executor::execute(statement).map_err(|e| e.to_string())
}

pub fn reset_database() {
    let db_file = "rustql_data.json";

    executor::reset_database_state();

    if Path::new(db_file).exists() {
        fs::remove_file(db_file).ok();
    }
}
