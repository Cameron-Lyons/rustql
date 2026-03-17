pub mod ast;
pub mod database;
pub mod engine;
pub mod error;
pub mod executor;
pub mod lexer;
pub mod parser;
pub mod plan_executor;
pub mod planner;
pub mod storage;
pub mod wal;

use std::fs;

pub use engine::{Engine, EngineBuilder};
pub use storage::{DefaultStorageBackend, DefaultStoragePaths};

pub fn process_query(query: &str) -> Result<String, String> {
    engine::default_engine().process_query(query)
}

pub fn reset_database() {
    engine::default_engine().reset_state();

    let paths = storage::default_storage_paths();
    for path in [paths.json_path, paths.btree_path] {
        if path.exists() {
            fs::remove_file(path).ok();
        }
    }
}
