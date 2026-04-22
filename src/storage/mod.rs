use crate::database::Database;
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
