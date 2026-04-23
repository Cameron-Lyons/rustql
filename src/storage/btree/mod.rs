mod cache;
mod file;
mod header;
mod journal;
mod page;
mod records;

#[cfg(test)]
mod tests;

use super::StorageEngine;
use super::atomic_file::{cleanup_temp_file, rename_synced, storage_temp_path};
use crate::database::Database;
use crate::error::RustqlError;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use cache::PageCache;
use file::{BTreeFile, CachedBTreeFile};
use journal::TransactionJournal;
use page::BTreePage;

const MAX_CACHE_SIZE: usize = 1000;

/// Snapshot-oriented B-tree storage backend.
///
/// The on-disk format is page-based and versioned, but persistence is not
/// incremental: each save normalizes the whole [`Database`], writes a complete
/// page image to a temporary file, syncs it, and atomically replaces the
/// storage file. The transaction journal records enough redo frames to recover
/// a committed snapshot, not a mutation log for replaying individual writes.
pub struct BTreeStorageEngine {
    data_path: PathBuf,
    path_lock: Arc<RwLock<()>>,
    page_cache: Arc<RwLock<PageCache>>,
}

impl BTreeStorageEngine {
    pub fn new<P: Into<PathBuf>>(data_path: P) -> Self {
        BTreeStorageEngine {
            data_path: data_path.into(),
            path_lock: Arc::new(RwLock::new(())),
            page_cache: Arc::new(RwLock::new(PageCache::new())),
        }
    }

    pub(super) fn read_page_cached(&self, page_id: u64) -> Result<BTreePage, RustqlError> {
        {
            let mut cache = self.page_cache.write().map_err(|e| {
                RustqlError::StorageError(format!("Failed to acquire cache write lock: {}", e))
            })?;
            if let Some(page) = cache.get(&page_id) {
                return Ok(page.clone());
            }
        }

        let mut file = BTreeFile::open(&self.data_path)?;
        let page = file.read_page(page_id)?;
        {
            let mut cache = self.page_cache.write().map_err(|e| {
                RustqlError::StorageError(format!("Failed to acquire cache write lock: {}", e))
            })?;
            cache.insert(page_id, page.clone());
        }
        Ok(page)
    }

    pub fn cache_stats(&self) -> (u64, u64, usize) {
        let cache = self
            .page_cache
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cache.stats()
    }

    pub fn invalidate_page(&self, page_id: u64) {
        let mut cache = self
            .page_cache
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cache.pages.remove(&page_id);
        cache.access_order.retain(|&id| id != page_id);
    }

    pub fn clear_cache(&self) {
        let mut cache = self
            .page_cache
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cache.clear();
    }

    pub fn invalidate_pages(&self, page_ids: &[u64]) {
        let mut cache = self
            .page_cache
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for &page_id in page_ids {
            cache.pages.remove(&page_id);
            cache.access_order.retain(|&id| id != page_id);
        }
    }

    pub(super) fn save_locked(&self, db: &Database) -> Result<(), RustqlError> {
        // Snapshot write: build a fresh page image and atomically replace the
        // current file. This keeps crash semantics simple, but it is not an
        // incremental page-delta update.
        let mut db = db.clone();
        db.normalize_row_ids();
        let temp_path = storage_temp_path(&self.data_path);
        let result = (|| {
            let mut file = BTreeFile::create(&temp_path)?;
            file.write_database_via_pages(&db)?;
            file.sync_all()?;
            drop(file);
            rename_synced(&temp_path, &self.data_path)
        })();
        if result.is_err() {
            cleanup_temp_file(&temp_path);
        }
        self.clear_cache();
        result
    }
}

impl StorageEngine for BTreeStorageEngine {
    fn load(&self) -> Result<Database, RustqlError> {
        let _path_guard = self.path_lock.write().map_err(|e| {
            RustqlError::StorageError(format!("Failed to acquire BTree storage write lock: {}", e))
        })?;
        self.recover_if_needed_locked()?;
        let mut cached_file = CachedBTreeFile { engine: self };
        let mut db = cached_file.read_database_via_pages()?;
        db.normalize_row_ids();
        Ok(db)
    }

    fn save(&self, db: &Database) -> Result<(), RustqlError> {
        let _path_guard = self.path_lock.write().map_err(|e| {
            RustqlError::StorageError(format!("Failed to acquire BTree storage write lock: {}", e))
        })?;
        self.save_locked(db)
    }

    fn begin_transaction(&self) -> Result<(), RustqlError> {
        let _path_guard = self.path_lock.write().map_err(|e| {
            RustqlError::StorageError(format!("Failed to acquire BTree storage write lock: {}", e))
        })?;
        self.write_journal_locked(&TransactionJournal::Pending)
    }

    fn prepare_commit(&self, db: &Database) -> Result<(), RustqlError> {
        let _path_guard = self.path_lock.write().map_err(|e| {
            RustqlError::StorageError(format!("Failed to acquire BTree storage write lock: {}", e))
        })?;
        self.write_journal_locked(&TransactionJournal::Committed {
            redo: self.build_redo_journal_locked(db)?,
        })
    }

    fn clear_transaction(&self) -> Result<(), RustqlError> {
        let _path_guard = self.path_lock.write().map_err(|e| {
            RustqlError::StorageError(format!("Failed to acquire BTree storage write lock: {}", e))
        })?;
        self.clear_journal_locked()
    }
}
