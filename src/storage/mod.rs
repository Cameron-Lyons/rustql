use crate::database::Database;
use crate::engine::{EngineOptions, StorageMode};
use crate::error::RustqlError;

mod atomic_file;
mod btree;
mod json;

pub use btree::BTreeStorageEngine;
pub use json::JsonStorageEngine;

pub trait StorageEngine: Send + Sync {
    fn load(&self) -> Result<Database, RustqlError>;

    fn save(&self, db: &Database) -> Result<(), RustqlError>;

    fn begin_transaction(&self) -> Result<(), RustqlError> {
        Ok(())
    }

    fn prepare_commit(&self, _db: &Database) -> Result<(), RustqlError> {
        Ok(())
    }

    fn clear_transaction(&self) -> Result<(), RustqlError> {
        Ok(())
    }
}

#[allow(deprecated)]
fn default_engine() -> Result<Box<dyn StorageEngine>, RustqlError> {
    match EngineOptions::from_env()?.storage {
        StorageMode::Json { path } => Ok(Box::new(JsonStorageEngine::new(path))),
        StorageMode::BTree { path } | StorageMode::Disk { path } => {
            Ok(Box::new(BTreeStorageEngine::new(path)))
        }
        StorageMode::Memory => Err(RustqlError::StorageError(
            "Legacy Database::load/save helpers require persistent storage".to_string(),
        )),
    }
}

pub(crate) fn load_database() -> Result<Database, RustqlError> {
    default_engine()?.load()
}

pub(crate) fn save_database(db: &Database) -> Result<(), RustqlError> {
    default_engine()?.save(db)
}
