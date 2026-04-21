use super::StorageEngine;
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
                    "Failed to read database file '{}': {}",
                    self.path.display(),
                    e
                ))
            })?;
            if data.trim().is_empty() {
                return Ok(Database::new());
            }
            let mut db: Database = serde_json::from_str(&data).map_err(|e| {
                RustqlError::StorageError(format!(
                    "Failed to parse database file '{}': {}",
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
        super::atomic_write(&self.path, data.as_bytes())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_json_path(label: &str) -> PathBuf {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock should be after Unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "rustql_json_{}_{}_{}.json",
            label,
            std::process::id(),
            timestamp
        ))
    }

    fn remove_storage_artifacts(path: &Path) {
        let _ = std::fs::remove_file(path);
        let mut journal = path.as_os_str().to_os_string();
        journal.push(".wal");
        let _ = std::fs::remove_file(PathBuf::from(journal));
    }

    #[test]
    fn json_storage_rejects_invalid_json() {
        let temp_path = temp_json_path("invalid");
        remove_storage_artifacts(&temp_path);

        std::fs::write(&temp_path, b"{not valid json").expect("failed to write invalid JSON");

        let engine = JsonStorageEngine::new(&temp_path);
        let error = match engine.load() {
            Ok(_) => panic!("expected invalid JSON error"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("Failed to parse database file"));

        remove_storage_artifacts(&temp_path);
    }

    #[test]
    fn json_storage_loads_empty_file_as_empty_database() {
        let temp_path = temp_json_path("empty");
        remove_storage_artifacts(&temp_path);

        std::fs::write(&temp_path, b"").expect("failed to write empty JSON file");

        let engine = JsonStorageEngine::new(&temp_path);
        let db = engine.load().expect("empty JSON file should load");
        assert!(db.tables.is_empty());

        remove_storage_artifacts(&temp_path);
    }
}
