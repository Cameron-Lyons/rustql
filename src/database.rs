use crate::ast::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

const DATABASE_FILE: &str = "rustql_data.json";

#[derive(Serialize, Deserialize, Default)]
pub struct Database {
    pub tables: HashMap<String, Table>,
}

#[derive(Serialize, Deserialize)]
pub struct Table {
    pub columns: Vec<ColumnDefinition>,
    pub rows: Vec<Vec<Value>>,
}

impl Database {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn load() -> Self {
        if Path::new(DATABASE_FILE).exists() {
            let data = fs::read_to_string(DATABASE_FILE).unwrap_or_default();
            serde_json::from_str(&data).unwrap_or_default()
        } else {
            Self::default()
        }
    }

    pub fn save(&self) -> Result<(), String> {
        let data = serde_json::to_string_pretty(self)
            .map_err(|e| format!("Failed to serialize database: {}", e))?;
        fs::write(DATABASE_FILE, data)
            .map_err(|e| format!("Failed to write database file: {}", e))?;
        Ok(())
    }
}
