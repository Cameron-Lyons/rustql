use crate::database::Database;
use crate::error::RustqlError;

pub mod btree;
pub mod json;

pub use btree::{BTREE_PAGE_SIZE, BTreeEntry, BTreePage, BTreeStorageEngine, PageHeader, PageKind};
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

fn default_engine() -> Box<dyn StorageEngine> {
    match std::env::var("RUSTQL_STORAGE") {
        Ok(v) if v.eq_ignore_ascii_case("btree") => {
            Box::new(BTreeStorageEngine::new("rustql_btree.dat"))
        }
        _ => Box::new(JsonStorageEngine::new("rustql_data.json")),
    }
}

pub fn load_database() -> Database {
    default_engine().load().unwrap_or_default()
}

pub fn save_database(db: &Database) -> Result<(), RustqlError> {
    default_engine().save(db)
}
