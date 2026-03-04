pub mod ast;
pub mod database;
pub mod engine;
pub mod error;
pub mod executor;
pub mod legacy;
pub mod lexer;
pub mod parser;
pub mod plan_executor;
pub mod planner;
pub mod storage;
pub mod wal;

use std::fs;
use std::path::Path;

pub use engine::{
    ColumnMeta, CommandResult, CommandTag, Engine, EngineOptions, QueryResult, QueryRows, Session,
    format_query_result, format_query_results,
};

pub fn process_query(query: &str) -> Result<String, String> {
    let engine = engine::default_engine();
    let mut session = engine.session();
    session.execute_legacy(query).map_err(|e| e.to_string())
}

pub fn reset_database() {
    let db_file = "rustql_data.json";

    engine::default_engine().reset_state();

    if Path::new(db_file).exists() {
        fs::remove_file(db_file).ok();
    }
}
