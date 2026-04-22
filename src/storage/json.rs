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
        let mut db = db.clone();
        db.normalize_row_ids();
        let data = serde_json::to_string_pretty(&db).map_err(|e| {
            RustqlError::StorageError(format!("Failed to serialize database: {}", e))
        })?;
        atomic_write(&self.path, data.as_bytes())?;
        Ok(())
    }
}
