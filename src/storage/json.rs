use super::StorageEngine;
use super::atomic_file::atomic_write;
use crate::database::Database;
use crate::error::RustqlError;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

pub struct JsonStorageEngine {
    path: PathBuf,
    lock: Arc<RwLock<()>>,
}

impl JsonStorageEngine {
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        JsonStorageEngine {
            path: path.into(),
            lock: Arc::new(RwLock::new(())),
        }
    }
}

impl StorageEngine for JsonStorageEngine {
    fn load(&self) -> Result<Database, RustqlError> {
        let _guard = self.lock.read().map_err(|e| {
            RustqlError::StorageError(format!("Failed to acquire JSON storage read lock: {}", e))
        })?;
        if Path::new(&self.path).exists() {
            let data = fs::read_to_string(&self.path).map_err(|e| {
                RustqlError::StorageError(format!(
                    "Failed to read JSON storage file '{}': {}",
                    self.path.display(),
                    e
                ))
            })?;
            if data.trim().is_empty() {
                return Err(RustqlError::StorageError(format!(
                    "JSON storage file '{}' is empty",
                    self.path.display()
                )));
            }
            let mut db: Database = serde_json::from_str(&data).map_err(|e| {
                RustqlError::StorageError(format!(
                    "Failed to parse JSON storage file '{}': {}",
                    self.path.display(),
                    e
                ))
            })?;
            db.normalize_row_ids();
            Ok(db)
        } else {
            Ok(Database::new())
        }
    }

    fn save(&self, db: &Database) -> Result<(), RustqlError> {
        let _guard = self.lock.write().map_err(|e| {
            RustqlError::StorageError(format!("Failed to acquire JSON storage write lock: {}", e))
        })?;
        let normalized;
        let db = if db.has_normalized_row_ids() {
            db
        } else {
            normalized = {
                let mut db = db.clone();
                db.normalize_row_ids();
                db
            };
            &normalized
        };
        let data = serde_json::to_string_pretty(db).map_err(|e| {
            RustqlError::StorageError(format!("Failed to serialize database: {}", e))
        })?;
        atomic_write(&self.path, data.as_bytes())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{ColumnDefinition, DataType, Value};
    use crate::database::{RowId, Table};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_table() -> Table {
        Table::new(
            vec![ColumnDefinition {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
            }],
            vec![vec![Value::Integer(10)], vec![Value::Integer(20)]],
            Vec::new(),
        )
    }

    fn test_database(table: Table) -> Database {
        let mut db = Database::new();
        db.tables.insert("items".to_string(), table);
        db
    }

    fn unique_temp_path(name: &str) -> PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        std::env::temp_dir().join(format!(
            "rustql_json_{}_{}_{}.json",
            name,
            std::process::id(),
            timestamp
        ))
    }

    #[test]
    fn save_serializes_normalized_legacy_row_metadata() {
        let mut table = test_table();
        table.row_ids.clear();
        table.next_row_id = 0;
        let db = test_database(table);

        let path = unique_temp_path("legacy_rows");
        let _ = fs::remove_file(&path);

        let storage = JsonStorageEngine::new(path.clone());
        storage.save(&db).unwrap();

        let saved_json = fs::read_to_string(&path).unwrap();
        let saved: Database = serde_json::from_str(&saved_json).unwrap();
        let table = saved.tables.get("items").unwrap();
        assert_eq!(table.row_ids, vec![RowId(1), RowId(2)]);
        assert_eq!(table.next_row_id, 3);

        fs::remove_file(path).unwrap();
    }
}
