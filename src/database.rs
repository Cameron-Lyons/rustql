use crate::ast::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
#[cfg(not(test))]
use std::fs;
#[cfg(not(test))]
use std::path::Path;

#[cfg(not(test))]
const DATABASE_FILE: &str = "rustql_data.json";

#[derive(Serialize, Deserialize)]
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
        Database {
            tables: HashMap::new(),
        }
    }

    #[cfg(not(test))]
    pub fn load() -> Self {
        if Path::new(DATABASE_FILE).exists() {
            let data = fs::read_to_string(DATABASE_FILE).unwrap_or_default();
            serde_json::from_str(&data).unwrap_or_else(|_| Database::new())
        } else {
            Database::new()
        }
    }

    #[cfg(not(test))]
    pub fn save(&self) -> Result<(), String> {
        let data = serde_json::to_string_pretty(self)
            .map_err(|e| format!("Failed to serialize database: {}", e))?;
        fs::write(DATABASE_FILE, data)
            .map_err(|e| format!("Failed to write database file: {}", e))?;
        Ok(())
    }
}
