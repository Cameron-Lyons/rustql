use crate::database::Database;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// Abstraction over how the `Database` is persisted.
///
/// Today we just have a JSON-file-backed engine, but this trait is the
/// foundation for adding more advanced engines (e.g. B-tree / LSM-tree).
pub trait StorageEngine: Send + Sync {
    /// Load a database from the underlying storage.
    fn load(&self) -> Database;

    /// Persist the given database to the underlying storage.
    fn save(&self, db: &Database) -> Result<(), String>;
}

/// JSON file based storage engine.
///
/// This preserves the existing behaviour of reading / writing a single
/// `rustql_data.json` file in the current working directory.
pub struct JsonStorageEngine {
    path: PathBuf,
}

impl JsonStorageEngine {
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        JsonStorageEngine { path: path.into() }
    }
}

impl StorageEngine for JsonStorageEngine {
    fn load(&self) -> Database {
        if Path::new(&self.path).exists() {
            let data = fs::read_to_string(&self.path).unwrap_or_default();
            serde_json::from_str(&data).unwrap_or_default()
        } else {
            Database::new()
        }
    }

    fn save(&self, db: &Database) -> Result<(), String> {
        let data = serde_json::to_string_pretty(db)
            .map_err(|e| format!("Failed to serialize database: {}", e))?;
        fs::write(&self.path, data)
            .map_err(|e| format!("Failed to write database file: {}", e))?;
        Ok(())
    }
}

/// Global storage engine used by the executor.
///
/// For now this is always a `JsonStorageEngine` pointing at
/// `rustql_data.json`, but this indirection will allow swapping in
/// a different engine (e.g. B-tree) in the future.
static STORAGE_ENGINE: OnceLock<Box<dyn StorageEngine>> = OnceLock::new();

fn default_engine() -> Box<dyn StorageEngine> {
    Box::new(JsonStorageEngine::new("rustql_data.json"))
}

pub fn storage_engine() -> &'static dyn StorageEngine {
    let _ = STORAGE_ENGINE.get_or_init(default_engine);
    // Safe unwrap: we just initialised it above if it was absent.
    &**STORAGE_ENGINE.get().unwrap()
}

/// Skeleton for a future B-tree / LSM-tree based storage engine.
///
/// This is not wired into the system yet; it exists to make it easier
/// to experiment with a more advanced on-disk layout without changing
/// call sites again.
#[allow(dead_code)]
pub struct BTreeStorageEngine {
    #[allow(dead_code)]
    data_path: PathBuf,
}

#[allow(dead_code)]
impl BTreeStorageEngine {
    pub fn new<P: Into<PathBuf>>(data_path: P) -> Self {
        BTreeStorageEngine {
            data_path: data_path.into(),
        }
    }
}

#[allow(dead_code)]
impl StorageEngine for BTreeStorageEngine {
    fn load(&self) -> Database {
        // For now, just start with an empty in-memory database.
        // Future work: read pages / segments from `data_path` and build
        // table / index structures backed by a B-tree or LSM-tree layout.
        Database::new()
    }

    fn save(&self, _db: &Database) -> Result<(), String> {
        // Future work: write modified pages / segments to disk.
        Ok(())
    }
}


