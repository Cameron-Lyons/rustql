use crate::ast::{ColumnDefinition, TableConstraint, Value};
use crate::database::{Database, RowId};
use crate::error::RustqlError;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

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
        let _guard = self.lock.read().unwrap();
        if Path::new(&self.path).exists() {
            let data = fs::read_to_string(&self.path).unwrap_or_default();
            let mut db: Database = serde_json::from_str(&data).unwrap_or_default();
            db.normalize_row_ids();
            Ok(db)
        } else {
            Ok(Database::new())
        }
    }

    fn save(&self, db: &Database) -> Result<(), RustqlError> {
        let _guard = self.lock.write().unwrap();
        let mut db = db.clone();
        db.normalize_row_ids();
        let data = serde_json::to_string_pretty(&db).map_err(|e| {
            RustqlError::StorageError(format!("Failed to serialize database: {}", e))
        })?;
        fs::write(&self.path, data).map_err(|e| {
            RustqlError::StorageError(format!("Failed to write database file: {}", e))
        })?;
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

const MAX_CACHE_SIZE: usize = 1000;
const FILE_HEADER_SIZE: usize = 16;
const HEADER_RESERVED: u32 = 0;
const JOURNAL_MAGIC: [u8; 8] = *b"RSTQLJW\0";
const JOURNAL_VERSION: u32 = 1;
const LEGACY_ROW_KEY_PREFIX: &str = "row:";
const ROW_KEY_PREFIX: &str = "table_row:";
const ROW_ID_KEY_WIDTH: usize = 20;

#[derive(Serialize, Deserialize)]
enum TransactionJournal {
    Pending,
    Committed { database: Database },
}

#[derive(Serialize, Deserialize)]
struct TableStorageRecord {
    columns: Vec<ColumnDefinition>,
    constraints: Vec<TableConstraint>,
    next_row_id: u64,
}

fn format_row_storage_key(table_name: &str, row_id: RowId) -> String {
    format!(
        "{ROW_KEY_PREFIX}{table_name}:{:0width$}",
        row_id.0,
        width = ROW_ID_KEY_WIDTH
    )
}

fn parse_row_storage_key(key: &str) -> Option<(&str, RowId, bool)> {
    if let Some(row_key) = key.strip_prefix(ROW_KEY_PREFIX) {
        let (table_name, row_id_str) = row_key.rsplit_once(':')?;
        let row_id = row_id_str.parse::<u64>().ok()?;
        return Some((table_name, RowId(row_id), true));
    }

    let row_key = key.strip_prefix(LEGACY_ROW_KEY_PREFIX)?;
    let (table_name, row_id_str) = row_key.rsplit_once(':')?;
    let row_id = row_id_str.parse::<u64>().ok()?;
    Some((table_name, RowId(row_id), false))
}

fn insert_loaded_row(table: &mut crate::database::Table, row_id: RowId, row: Vec<Value>) {
    if table
        .row_ids
        .last()
        .is_none_or(|last_row_id| *last_row_id < row_id)
    {
        table.row_ids.push(row_id);
        table.rows.push(row);
    } else {
        let position = table
            .row_ids
            .binary_search(&row_id)
            .unwrap_or_else(|pos| pos);
        table.row_ids.insert(position, row_id);
        table.rows.insert(position, row);
    }

    if table.next_row_id <= row_id.0 {
        table.next_row_id = row_id.0 + 1;
    }
}

enum VersionedFileState {
    Missing,
    Empty,
    Valid,
}

fn read_versioned_header(
    path: &Path,
    expected_magic: [u8; 8],
    expected_version: u32,
    label: &str,
) -> Result<VersionedFileState, RustqlError> {
    if !path.exists() {
        return Ok(VersionedFileState::Missing);
    }

    let metadata = fs::metadata(path)
        .map_err(|e| RustqlError::StorageError(format!("Failed to stat {}: {}", label, e)))?;

    if metadata.len() == 0 {
        return Ok(VersionedFileState::Empty);
    }

    if metadata.len() < FILE_HEADER_SIZE as u64 {
        return Err(RustqlError::StorageError(format!(
            "{} header is truncated",
            label
        )));
    }

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|e| RustqlError::StorageError(format!("Failed to open {}: {}", label, e)))?;

    let mut header = [0u8; FILE_HEADER_SIZE];
    file.read_exact(&mut header).map_err(|e| {
        RustqlError::StorageError(format!("Failed to read {} header: {}", label, e))
    })?;

    if header[0..8] != expected_magic {
        return Err(RustqlError::StorageError(format!(
            "{} has an invalid header",
            label
        )));
    }

    let version = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
    if version != expected_version {
        return Err(RustqlError::StorageError(format!(
            "Unsupported {} format version: {}",
            label, version
        )));
    }

    Ok(VersionedFileState::Valid)
}

fn write_versioned_header(
    file: &mut std::fs::File,
    magic: [u8; 8],
    version: u32,
    label: &str,
) -> Result<(), RustqlError> {
    file.seek(SeekFrom::Start(0))
        .map_err(|e| RustqlError::StorageError(format!("Failed to seek {}: {}", label, e)))?;

    let mut header = Vec::with_capacity(FILE_HEADER_SIZE);
    header.extend_from_slice(&magic);
    header.extend_from_slice(&version.to_le_bytes());
    header.extend_from_slice(&HEADER_RESERVED.to_le_bytes());

    file.write_all(&header)
        .map_err(|e| RustqlError::StorageError(format!("Failed to write {} header: {}", label, e)))
}

struct PageCache {
    pages: HashMap<u64, BTreePage>,
    access_order: VecDeque<u64>,
    hits: u64,
    misses: u64,
}

impl PageCache {
    fn new() -> Self {
        PageCache {
            pages: HashMap::new(),
            access_order: VecDeque::new(),
            hits: 0,
            misses: 0,
        }
    }

    fn get(&mut self, page_id: &u64) -> Option<&BTreePage> {
        if self.pages.contains_key(page_id) {
            self.hits += 1;
            self.access_order.retain(|&id| id != *page_id);
            self.access_order.push_back(*page_id);
            self.pages.get(page_id)
        } else {
            self.misses += 1;
            None
        }
    }

    fn insert(&mut self, page_id: u64, page: BTreePage) {
        if self.pages.contains_key(&page_id) {
            self.pages.insert(page_id, page);
            self.access_order.retain(|&id| id != page_id);
            self.access_order.push_back(page_id);
        } else {
            while self.pages.len() >= MAX_CACHE_SIZE {
                if let Some(oldest_id) = self.access_order.pop_front() {
                    self.pages.remove(&oldest_id);
                } else {
                    break;
                }
            }
            self.pages.insert(page_id, page);
            self.access_order.push_back(page_id);
        }
    }

    fn clear(&mut self) {
        self.pages.clear();
        self.access_order.clear();
    }

    fn stats(&self) -> (u64, u64, usize) {
        (self.hits, self.misses, self.pages.len())
    }
}

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

    fn read_page_cached(&self, page_id: u64) -> Result<BTreePage, RustqlError> {
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
        let cache = self.page_cache.read().unwrap();
        cache.stats()
    }

    pub fn invalidate_page(&self, page_id: u64) {
        let mut cache = self.page_cache.write().unwrap();
        cache.pages.remove(&page_id);
        cache.access_order.retain(|&id| id != page_id);
    }

    pub fn clear_cache(&self) {
        let mut cache = self.page_cache.write().unwrap();
        cache.clear();
    }

    pub fn invalidate_pages(&self, page_ids: &[u64]) {
        let mut cache = self.page_cache.write().unwrap();
        for &page_id in page_ids {
            cache.pages.remove(&page_id);
            cache.access_order.retain(|&id| id != page_id);
        }
    }

    fn journal_path(&self) -> PathBuf {
        let mut path = self.data_path.as_os_str().to_os_string();
        path.push(".wal");
        PathBuf::from(path)
    }

    fn write_journal_locked(&self, journal: &TransactionJournal) -> Result<(), RustqlError> {
        let path = self.journal_path();
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| {
                RustqlError::StorageError(format!(
                    "Failed to create transaction journal '{}': {}",
                    path.display(),
                    e
                ))
            })?;

        write_versioned_header(
            &mut file,
            JOURNAL_MAGIC,
            JOURNAL_VERSION,
            "transaction journal",
        )?;
        let payload = serde_json::to_vec(journal).map_err(|e| {
            RustqlError::StorageError(format!(
                "Failed to serialize transaction journal '{}': {}",
                path.display(),
                e
            ))
        })?;
        file.write_all(&payload).map_err(|e| {
            RustqlError::StorageError(format!(
                "Failed to write transaction journal '{}': {}",
                path.display(),
                e
            ))
        })?;
        file.flush().map_err(|e| {
            RustqlError::StorageError(format!(
                "Failed to flush transaction journal '{}': {}",
                path.display(),
                e
            ))
        })
    }

    fn read_journal_locked(&self) -> Result<Option<TransactionJournal>, RustqlError> {
        let path = self.journal_path();
        match read_versioned_header(&path, JOURNAL_MAGIC, JOURNAL_VERSION, "transaction journal")? {
            VersionedFileState::Missing | VersionedFileState::Empty => Ok(None),
            VersionedFileState::Valid => {
                let mut file = std::fs::OpenOptions::new()
                    .read(true)
                    .open(&path)
                    .map_err(|e| {
                        RustqlError::StorageError(format!(
                            "Failed to open transaction journal '{}': {}",
                            path.display(),
                            e
                        ))
                    })?;
                file.seek(SeekFrom::Start(FILE_HEADER_SIZE as u64))
                    .map_err(|e| {
                        RustqlError::StorageError(format!(
                            "Failed to seek transaction journal '{}': {}",
                            path.display(),
                            e
                        ))
                    })?;
                let mut payload = Vec::new();
                file.read_to_end(&mut payload).map_err(|e| {
                    RustqlError::StorageError(format!(
                        "Failed to read transaction journal '{}': {}",
                        path.display(),
                        e
                    ))
                })?;
                if payload.is_empty() {
                    return Ok(None);
                }
                let journal = serde_json::from_slice(&payload).map_err(|e| {
                    RustqlError::StorageError(format!(
                        "Failed to decode transaction journal '{}': {}",
                        path.display(),
                        e
                    ))
                })?;
                Ok(Some(journal))
            }
        }
    }

    fn clear_journal_locked(&self) -> Result<(), RustqlError> {
        let path = self.journal_path();
        if !path.exists() {
            return Ok(());
        }
        fs::remove_file(&path).map_err(|e| {
            RustqlError::StorageError(format!(
                "Failed to remove transaction journal '{}': {}",
                path.display(),
                e
            ))
        })
    }

    fn recover_if_needed_locked(&self) -> Result<(), RustqlError> {
        let Some(journal) = self.read_journal_locked()? else {
            return Ok(());
        };

        match journal {
            TransactionJournal::Pending => {
                self.clear_journal_locked()?;
            }
            TransactionJournal::Committed { database } => {
                self.save_locked(&database)?;
                self.clear_journal_locked()?;
            }
        }

        Ok(())
    }

    fn save_locked(&self, db: &Database) -> Result<(), RustqlError> {
        let mut db = db.clone();
        db.normalize_row_ids();
        let mut file = BTreeFile::create(&self.data_path)?;
        let result = file.write_database_via_pages(&db);
        self.clear_cache();
        result
    }
}

impl StorageEngine for BTreeStorageEngine {
    fn load(&self) -> Result<Database, RustqlError> {
        let _path_guard = self.path_lock.write().unwrap();
        self.recover_if_needed_locked()?;
        let mut cached_file = CachedBTreeFile { engine: self };
        let mut db = cached_file.read_database_via_pages()?;
        db.normalize_row_ids();
        Ok(db)
    }

    fn save(&self, db: &Database) -> Result<(), RustqlError> {
        let _path_guard = self.path_lock.write().unwrap();
        self.save_locked(db)
    }

    fn begin_transaction(&self) -> Result<(), RustqlError> {
        let _path_guard = self.path_lock.write().unwrap();
        self.write_journal_locked(&TransactionJournal::Pending)
    }

    fn prepare_commit(&self, db: &Database) -> Result<(), RustqlError> {
        let _path_guard = self.path_lock.write().unwrap();
        self.write_journal_locked(&TransactionJournal::Committed {
            database: db.clone(),
        })
    }

    fn clear_transaction(&self) -> Result<(), RustqlError> {
        let _path_guard = self.path_lock.write().unwrap();
        self.clear_journal_locked()
    }
}

struct CachedBTreeFile<'a> {
    engine: &'a BTreeStorageEngine,
}

impl<'a> CachedBTreeFile<'a> {
    fn read_page(&self, page_id: u64) -> Result<BTreePage, RustqlError> {
        self.engine.read_page_cached(page_id)
    }

    fn read_database_via_pages(&mut self) -> Result<Database, RustqlError> {
        match read_versioned_header(
            &self.engine.data_path,
            BTreeFile::MAGIC,
            BTreeFile::VERSION,
            "BTree storage file",
        )? {
            VersionedFileState::Missing | VersionedFileState::Empty => return Ok(Database::new()),
            VersionedFileState::Valid => {}
        }

        let file_size = fs::metadata(&self.engine.data_path)
            .map_err(|e| RustqlError::StorageError(format!("Failed to get file metadata: {}", e)))?
            .len();

        if file_size < FILE_HEADER_SIZE as u64 + BTREE_PAGE_SIZE as u64 {
            return Ok(Database::new());
        }

        let meta_page = match self.read_page(0) {
            Ok(page) => page,
            Err(_) => return Ok(Database::new()),
        };
        let root_page_id = meta_page
            .entries
            .iter()
            .find(|e| {
                if let Value::Text(ref s) = e.key {
                    s == "root"
                } else {
                    false
                }
            })
            .map(|e| e.pointer)
            .unwrap_or(1);

        self.load_database_from_rows(root_page_id)
    }

    fn load_database_from_rows(&self, root_page_id: u64) -> Result<Database, RustqlError> {
        let mut db = Database::new();
        let mut pending_rows: HashMap<String, Vec<(RowId, Vec<Value>)>> = HashMap::new();

        for (key, pointer) in self.range_scan(None, None, root_page_id)? {
            let Value::Text(key_str) = key else {
                continue;
            };

            if let Some(table_name) = key_str.strip_prefix("schema:") {
                let schema: TableStorageRecord = self
                    .read_data_from_pointer(pointer, format!("schema for table {}", table_name))?;

                db.tables.insert(
                    table_name.to_string(),
                    crate::database::Table {
                        columns: schema.columns,
                        rows: Vec::new(),
                        row_ids: Vec::new(),
                        next_row_id: schema.next_row_id,
                        constraints: schema.constraints,
                    },
                );
                if let Some(mut rows) = pending_rows.remove(table_name) {
                    rows.sort_by_key(|(row_id, _)| *row_id);
                    if let Some(table_ref) = db.tables.get_mut(table_name) {
                        for (row_id, row) in rows {
                            insert_loaded_row(table_ref, row_id, row);
                        }
                    }
                }
                continue;
            }

            if let Some(index_name) = key_str.strip_prefix("index:") {
                let index: crate::database::Index =
                    self.read_data_from_pointer(pointer, format!("index {}", index_name))?;
                db.indexes.insert(index_name.to_string(), index);
                continue;
            }

            if let Some(index_name) = key_str.strip_prefix("cindex:") {
                let index: crate::database::CompositeIndex = self
                    .read_data_from_pointer(pointer, format!("composite index {}", index_name))?;
                db.composite_indexes.insert(index_name.to_string(), index);
                continue;
            }

            if let Some(view_name) = key_str.strip_prefix("view:") {
                let view: crate::database::View =
                    self.read_data_from_pointer(pointer, format!("view {}", view_name))?;
                db.views.insert(view_name.to_string(), view);
                continue;
            }

            if let Some((table_name, row_id, can_insert_in_order)) = parse_row_storage_key(&key_str)
            {
                let row: Vec<Value> = self.read_data_from_pointer(
                    pointer,
                    format!("row {} for table {}", row_id.0, table_name),
                )?;
                if can_insert_in_order && let Some(table_ref) = db.tables.get_mut(table_name) {
                    insert_loaded_row(table_ref, row_id, row);
                    continue;
                }
                pending_rows
                    .entry(table_name.to_string())
                    .or_default()
                    .push((row_id, row));
            }
        }

        for (table_name, mut row_indices) in pending_rows {
            row_indices.sort_by_key(|(row_id, _)| *row_id);
            if let Some(table_ref) = db.tables.get_mut(&table_name) {
                for (row_id, row) in row_indices {
                    insert_loaded_row(table_ref, row_id, row);
                }
            }
        }

        Ok(db)
    }

    fn read_data_from_pointer<T>(
        &self,
        pointer: u64,
        label: impl Into<String>,
    ) -> Result<T, RustqlError>
    where
        T: DeserializeOwned,
    {
        let label = label.into();
        let data_page = self.read_page(pointer)?;

        if let Some(entry) = data_page.entries.first()
            && let Value::Text(ref json_str) = entry.key
        {
            return serde_json::from_str(json_str).map_err(|e| {
                RustqlError::StorageError(format!("Failed to deserialize {}: {}", label, e))
            });
        }

        Err(RustqlError::StorageError(format!(
            "Data page for {} does not contain valid JSON",
            label
        )))
    }

    fn range_scan(
        &self,
        start_key: Option<&Value>,
        end_key: Option<&Value>,
        root_page_id: u64,
    ) -> Result<Vec<(Value, u64)>, RustqlError> {
        scan_pages_in_order(
            |page_id| self.read_page(page_id),
            start_key,
            end_key,
            root_page_id,
        )
    }
}

struct BTreeFile {
    file: std::fs::File,
}

impl BTreeFile {
    const MAGIC: [u8; 8] = *b"RSTQLBT\0";
    const VERSION: u32 = 2;

    fn open(path: &Path) -> Result<Self, RustqlError> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|e| {
                RustqlError::StorageError(format!("Failed to open BTree storage file: {}", e))
            })?;
        Ok(BTreeFile { file })
    }

    fn create(path: &Path) -> Result<Self, RustqlError> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(|e| {
                RustqlError::StorageError(format!("Failed to create BTree storage file: {}", e))
            })?;
        Ok(BTreeFile { file })
    }

    fn write_data_to_pointer(&mut self, data: &str) -> Result<u64, RustqlError> {
        let page_id = self.get_next_page_id()?;
        let mut data_page = BTreePage::new(page_id, PageKind::Leaf);

        data_page
            .entries
            .push(BTreeEntry::new(Value::Text(data.to_string()), 0));
        data_page.header.entry_count = data_page.entries.len() as u16;

        self.write_page(&data_page)?;
        Ok(page_id)
    }

    fn write_database_via_pages(&mut self, db: &Database) -> Result<(), RustqlError> {
        write_versioned_header(
            &mut self.file,
            Self::MAGIC,
            Self::VERSION,
            "BTree storage file",
        )?;

        let mut meta_page = BTreePage::new(0, PageKind::Meta);
        meta_page
            .entries
            .push(BTreeEntry::new(Value::Text("root".to_string()), 1));
        meta_page
            .entries
            .push(BTreeEntry::new(Value::Text("next_page_id".to_string()), 2));
        meta_page.header.entry_count = meta_page.entries.len() as u16;
        self.write_page(&meta_page)?;

        let root_page_id = 1;
        let root_page = BTreePage::new(root_page_id, PageKind::Leaf);
        self.write_page(&root_page)?;

        let mut current_root_id = root_page_id;

        for (table_name, table) in &db.tables {
            let schema_key = Value::Text(format!("schema:{}", table_name));
            let schema_json = serde_json::to_string(&TableStorageRecord {
                columns: table.columns.clone(),
                constraints: table.constraints.clone(),
                next_row_id: table.next_row_id,
            })
            .map_err(|e| format!("Failed to serialize schema for table {}: {}", table_name, e))?;
            let data_pointer = self.write_data_to_pointer(&schema_json)?;
            current_root_id = self.insert(schema_key, data_pointer, current_root_id)?;
        }

        for (table_name, table) in &db.tables {
            for (row_id, row) in table.iter_rows_with_ids() {
                let row_key = Value::Text(format_row_storage_key(table_name, row_id));
                let row_json = serde_json::to_string(row).map_err(|e| {
                    format!(
                        "Failed to serialize row {} for table {}: {}",
                        row_id.0, table_name, e
                    )
                })?;
                let data_pointer = self.write_data_to_pointer(&row_json)?;
                current_root_id = self.insert(row_key, data_pointer, current_root_id)?;
            }
        }

        for (index_name, index) in &db.indexes {
            let index_key = Value::Text(format!("index:{}", index_name));
            let index_json = serde_json::to_string(index)
                .map_err(|e| format!("Failed to serialize index {}: {}", index_name, e))?;
            let data_pointer = self.write_data_to_pointer(&index_json)?;
            current_root_id = self.insert(index_key, data_pointer, current_root_id)?;
        }

        for (index_name, index) in &db.composite_indexes {
            let index_key = Value::Text(format!("cindex:{}", index_name));
            let index_json = serde_json::to_string(index).map_err(|e| {
                format!("Failed to serialize composite index {}: {}", index_name, e)
            })?;
            let data_pointer = self.write_data_to_pointer(&index_json)?;
            current_root_id = self.insert(index_key, data_pointer, current_root_id)?;
        }

        for (view_name, view) in &db.views {
            let view_key = Value::Text(format!("view:{}", view_name));
            let view_json = serde_json::to_string(view)
                .map_err(|e| format!("Failed to serialize view {}: {}", view_name, e))?;
            let data_pointer = self.write_data_to_pointer(&view_json)?;
            current_root_id = self.insert(view_key, data_pointer, current_root_id)?;
        }

        let mut meta_page = self.read_page(0)?;
        if let Some(root_entry) = meta_page
            .entries
            .iter_mut()
            .find(|e| matches!(&e.key, Value::Text(value) if value == "root"))
        {
            root_entry.pointer = current_root_id;
        }
        meta_page.header.entry_count = meta_page.entries.len() as u16;
        self.write_page(&meta_page)?;

        self.file.flush().map_err(|e| {
            RustqlError::StorageError(format!("Failed to flush BTree storage file: {}", e))
        })
    }
}

pub const BTREE_PAGE_SIZE: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PageKind {
    Meta,
    Internal,
    Leaf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageHeader {
    pub page_id: u64,

    pub kind: PageKind,

    pub entry_count: u16,

    pub reserved: u16,
}

impl PageHeader {
    pub fn new(page_id: u64, kind: PageKind) -> Self {
        PageHeader {
            page_id,
            kind,
            entry_count: 0,
            reserved: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTreePage {
    pub header: PageHeader,

    pub entries: Vec<BTreeEntry>,
}

impl BTreePage {
    pub fn new(page_id: u64, kind: PageKind) -> Self {
        BTreePage {
            header: PageHeader::new(page_id, kind),
            entries: Vec::new(),
        }
    }

    pub fn can_accept_entry(&self, entry: &BTreeEntry) -> bool {
        let current_size = self.estimated_size();
        let added = entry.estimated_size();
        current_size + added <= BTREE_PAGE_SIZE
    }

    fn estimated_size(&self) -> usize {
        let header_size = 16usize;
        let entries_size: usize = self.entries.iter().map(|e| e.estimated_size()).sum();
        header_size + entries_size
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTreeEntry {
    pub key: Value,

    pub pointer: u64,
}

impl BTreeEntry {
    pub fn new(key: Value, pointer: u64) -> Self {
        BTreeEntry { key, pointer }
    }

    fn estimated_size(&self) -> usize {
        let pointer_size = 8usize;
        let key_size = match &self.key {
            Value::Null => 1,
            Value::Integer(_) => 9,
            Value::Float(_) => 9,
            Value::Boolean(_) => 2,
            Value::Text(s) | Value::Date(s) | Value::Time(s) | Value::DateTime(s) => 1 + s.len(),
        };
        pointer_size + key_size
    }
}

const TAG_NULL: u8 = 0x00;
const TAG_INTEGER: u8 = 0x01;
const TAG_FLOAT: u8 = 0x02;
const TAG_TEXT: u8 = 0x03;
const TAG_BOOLEAN: u8 = 0x04;
const TAG_DATE: u8 = 0x05;
const TAG_TIME: u8 = 0x06;
const TAG_DATETIME: u8 = 0x07;

fn encode_value(buf: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => buf.push(TAG_NULL),
        Value::Integer(i) => {
            buf.push(TAG_INTEGER);
            buf.extend_from_slice(&i.to_le_bytes());
        }
        Value::Float(f) => {
            buf.push(TAG_FLOAT);
            buf.extend_from_slice(&f.to_le_bytes());
        }
        Value::Text(s) => {
            buf.push(TAG_TEXT);
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        Value::Boolean(b) => {
            buf.push(TAG_BOOLEAN);
            buf.push(if *b { 1 } else { 0 });
        }
        Value::Date(s) => {
            buf.push(TAG_DATE);
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        Value::Time(s) => {
            buf.push(TAG_TIME);
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        Value::DateTime(s) => {
            buf.push(TAG_DATETIME);
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
    }
}

fn decode_value(data: &[u8], offset: &mut usize) -> Result<Value, RustqlError> {
    if *offset >= data.len() {
        return Err(RustqlError::StorageError(
            "Unexpected end of binary entry data".to_string(),
        ));
    }
    let tag = data[*offset];
    *offset += 1;
    match tag {
        TAG_NULL => Ok(Value::Null),
        TAG_INTEGER => {
            if *offset + 8 > data.len() {
                return Err(RustqlError::StorageError(
                    "Truncated integer in binary entry".to_string(),
                ));
            }
            let val = i64::from_le_bytes(data[*offset..*offset + 8].try_into().unwrap());
            *offset += 8;
            Ok(Value::Integer(val))
        }
        TAG_FLOAT => {
            if *offset + 8 > data.len() {
                return Err(RustqlError::StorageError(
                    "Truncated float in binary entry".to_string(),
                ));
            }
            let val = f64::from_le_bytes(data[*offset..*offset + 8].try_into().unwrap());
            *offset += 8;
            Ok(Value::Float(val))
        }
        TAG_TEXT | TAG_DATE | TAG_TIME | TAG_DATETIME => {
            if *offset + 4 > data.len() {
                return Err(RustqlError::StorageError(
                    "Truncated string length in binary entry".to_string(),
                ));
            }
            let len = u32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap()) as usize;
            *offset += 4;
            if *offset + len > data.len() {
                return Err(RustqlError::StorageError(
                    "Truncated string data in binary entry".to_string(),
                ));
            }
            let s = String::from_utf8(data[*offset..*offset + len].to_vec()).map_err(|e| {
                RustqlError::StorageError(format!("Invalid UTF-8 in binary entry: {}", e))
            })?;
            *offset += len;
            match tag {
                TAG_TEXT => Ok(Value::Text(s)),
                TAG_DATE => Ok(Value::Date(s)),
                TAG_TIME => Ok(Value::Time(s)),
                TAG_DATETIME => Ok(Value::DateTime(s)),
                _ => unreachable!(),
            }
        }
        TAG_BOOLEAN => {
            if *offset >= data.len() {
                return Err(RustqlError::StorageError(
                    "Truncated boolean in binary entry".to_string(),
                ));
            }
            let val = data[*offset] != 0;
            *offset += 1;
            Ok(Value::Boolean(val))
        }
        other => Err(RustqlError::StorageError(format!(
            "Unknown value type tag: 0x{:02x}",
            other
        ))),
    }
}

impl BTreePage {
    pub fn to_bytes(&self) -> Result<[u8; BTREE_PAGE_SIZE], RustqlError> {
        let mut buf = [0u8; BTREE_PAGE_SIZE];

        let kind_byte = match self.header.kind {
            PageKind::Meta => 0u8,
            PageKind::Internal => 1u8,
            PageKind::Leaf => 2u8,
        };

        buf[0..8].copy_from_slice(&self.header.page_id.to_le_bytes());
        buf[8] = kind_byte;
        buf[9..11].copy_from_slice(&self.header.entry_count.to_le_bytes());
        buf[11..13].copy_from_slice(&self.header.reserved.to_le_bytes());

        let mut payload = Vec::new();
        for entry in &self.entries {
            encode_value(&mut payload, &entry.key);
            payload.extend_from_slice(&entry.pointer.to_le_bytes());
        }

        let header_size = 16usize;
        if header_size + payload.len() > BTREE_PAGE_SIZE {
            return Err(RustqlError::StorageError(
                "BTreePage too large to fit in fixed page size".to_string(),
            ));
        }

        buf[header_size..header_size + payload.len()].copy_from_slice(&payload);
        Ok(buf)
    }

    pub fn from_bytes(buf: &[u8; BTREE_PAGE_SIZE]) -> Result<Self, RustqlError> {
        let page_id = u64::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);
        let kind = match buf[8] {
            0 => PageKind::Meta,
            1 => PageKind::Internal,
            2 => PageKind::Leaf,
            other => {
                return Err(RustqlError::StorageError(format!(
                    "Unknown BTree page kind byte: {}",
                    other
                )));
            }
        };
        let entry_count = u16::from_le_bytes([buf[9], buf[10]]);
        let reserved = u16::from_le_bytes([buf[11], buf[12]]);

        let header = PageHeader {
            page_id,
            kind,
            entry_count,
            reserved,
        };

        let header_size = 16usize;

        if entry_count == 0 {
            return Ok(BTreePage {
                header,
                entries: Vec::new(),
            });
        }

        if buf[header_size] == b'[' {
            let mut payload = &buf[header_size..];
            if let Some(last) = payload.iter().rposition(|b| *b != 0) {
                payload = &payload[..=last];
            } else {
                payload = &[];
            }
            let entries: Vec<BTreeEntry> = if payload.is_empty() {
                Vec::new()
            } else {
                serde_json::from_slice(payload).map_err(|e| {
                    RustqlError::StorageError(format!(
                        "Failed to decode legacy JSON BTree entries: {}",
                        e
                    ))
                })?
            };
            return Ok(BTreePage { header, entries });
        }

        let mut entries = Vec::with_capacity(entry_count as usize);
        let mut offset = header_size;
        for _ in 0..entry_count {
            let key = decode_value(buf, &mut offset)?;
            if offset + 8 > buf.len() {
                return Err(RustqlError::StorageError(
                    "Truncated pointer in binary entry".to_string(),
                ));
            }
            let pointer = u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap());
            offset += 8;
            entries.push(BTreeEntry { key, pointer });
        }

        Ok(BTreePage { header, entries })
    }
}

impl BTreeFile {
    pub fn read_page(&mut self, page_id: u64) -> Result<BTreePage, RustqlError> {
        let header_size = 16u64;
        let offset = header_size + page_id * BTREE_PAGE_SIZE as u64;

        self.file.seek(SeekFrom::Start(offset)).map_err(|e| {
            RustqlError::StorageError(format!("Failed to seek to BTree page: {}", e))
        })?;

        let mut buf = [0u8; BTREE_PAGE_SIZE];
        self.file
            .read_exact(&mut buf)
            .map_err(|e| RustqlError::StorageError(format!("Failed to read BTree page: {}", e)))?;

        BTreePage::from_bytes(&buf)
    }

    pub fn write_page(&mut self, page: &BTreePage) -> Result<(), RustqlError> {
        let header_size = 16u64;
        let offset = header_size + page.header.page_id * BTREE_PAGE_SIZE as u64;

        let buf = page.to_bytes()?;

        self.file.seek(SeekFrom::Start(offset)).map_err(|e| {
            RustqlError::StorageError(format!("Failed to seek to BTree page for write: {}", e))
        })?;

        self.file
            .write_all(&buf)
            .map_err(|e| RustqlError::StorageError(format!("Failed to write BTree page: {}", e)))?;

        self.file
            .flush()
            .map_err(|e| RustqlError::StorageError(format!("Failed to flush BTree page: {}", e)))
    }

    fn get_next_page_id(&mut self) -> Result<u64, RustqlError> {
        let meta_page = self.read_page(0)?;

        for entry in &meta_page.entries {
            if let Value::Text(ref s) = entry.key
                && s == "next_page_id"
            {
                let next_id = entry.pointer;
                let mut new_meta = meta_page.clone();
                if let Some(e) = new_meta.entries.iter_mut().find(|e| {
                    if let Value::Text(ref s) = e.key {
                        s == "next_page_id"
                    } else {
                        false
                    }
                }) {
                    e.pointer = next_id + 1;
                } else {
                    new_meta.entries.push(BTreeEntry::new(
                        Value::Text("next_page_id".to_string()),
                        next_id + 1,
                    ));
                }
                new_meta.header.entry_count = new_meta.entries.len() as u16;
                self.write_page(&new_meta)?;
                return Ok(next_id);
            }
        }

        let next_id = 2;
        let mut new_meta = meta_page;
        new_meta.entries.push(BTreeEntry::new(
            Value::Text("next_page_id".to_string()),
            next_id + 1,
        ));
        new_meta.header.entry_count = new_meta.entries.len() as u16;
        self.write_page(&new_meta)?;
        Ok(next_id)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn search(
        &mut self,
        key: &Value,
        root_page_id: u64,
    ) -> Result<Option<(u64, usize)>, RustqlError> {
        let mut current_page_id = root_page_id;

        loop {
            let page = self.read_page(current_page_id)?;

            match page.header.kind {
                PageKind::Leaf => {
                    for (idx, entry) in page.entries.iter().enumerate() {
                        if &entry.key == key {
                            return Ok(Some((current_page_id, idx)));
                        }
                    }
                    return Ok(None);
                }
                PageKind::Internal => {
                    let mut found = false;
                    for entry in &page.entries {
                        if &entry.key > key {
                            current_page_id = entry.pointer;
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        if let Some(last_entry) = page.entries.last() {
                            current_page_id = last_entry.pointer;
                        } else {
                            return Ok(None);
                        }
                    }
                }
                PageKind::Meta => {
                    return Err(RustqlError::StorageError(
                        "Cannot search in meta page".to_string(),
                    ));
                }
            }
        }
    }

    fn find_path_to_leaf(
        &mut self,
        key: &Value,
        root_page_id: u64,
    ) -> Result<Vec<u64>, RustqlError> {
        let mut path = Vec::new();
        let mut current_page_id = root_page_id;

        loop {
            path.push(current_page_id);
            let page = self.read_page(current_page_id)?;

            match page.header.kind {
                PageKind::Leaf => {
                    return Ok(path);
                }
                PageKind::Internal => {
                    let mut next_page_id = None;
                    for entry in &page.entries {
                        if &entry.key > key {
                            next_page_id = Some(entry.pointer);
                            break;
                        }
                    }
                    current_page_id = next_page_id
                        .or_else(|| page.entries.last().map(|e| e.pointer))
                        .ok_or_else(|| "Invalid internal page structure".to_string())?;
                }
                PageKind::Meta => {
                    return Err(RustqlError::StorageError(
                        "Cannot traverse meta page for insert".to_string(),
                    ));
                }
            }
        }
    }

    pub fn insert(
        &mut self,
        key: Value,
        data_pointer: u64,
        root_page_id: u64,
    ) -> Result<u64, RustqlError> {
        let path = self.find_path_to_leaf(&key, root_page_id)?;
        let leaf_page_id = *path.last().unwrap();
        let mut leaf_page = self.read_page(leaf_page_id)?;

        for (idx, entry) in leaf_page.entries.iter().enumerate() {
            if entry.key == key {
                leaf_page.entries[idx].pointer = data_pointer;
                self.write_page(&leaf_page)?;
                return Ok(root_page_id);
            }
        }

        let new_entry = BTreeEntry::new(key.clone(), data_pointer);

        if leaf_page.can_accept_entry(&new_entry) {
            let insert_pos = leaf_page
                .entries
                .binary_search_by(|e| e.key.cmp(&key))
                .unwrap_or_else(|pos| pos);
            leaf_page.entries.insert(insert_pos, new_entry);
            leaf_page.header.entry_count = leaf_page.entries.len() as u16;
            self.write_page(&leaf_page)?;
            Ok(root_page_id)
        } else {
            self.split_and_insert_recursive(leaf_page, new_entry, path, root_page_id)
        }
    }

    fn split_and_insert_recursive(
        &mut self,
        mut page: BTreePage,
        new_entry: BTreeEntry,
        mut path: Vec<u64>,
        root_page_id: u64,
    ) -> Result<u64, RustqlError> {
        let insert_pos = page
            .entries
            .binary_search_by(|e| e.key.cmp(&new_entry.key))
            .unwrap_or_else(|pos| pos);
        page.entries.insert(insert_pos, new_entry);

        let mid = page.entries.len() / 2;
        let right_entries = page.entries.split_off(mid);
        let split_key = right_entries[0].key.clone();

        page.header.entry_count = page.entries.len() as u16;
        self.write_page(&page)?;

        let right_page_id = self.get_next_page_id()?;
        let mut right_page = BTreePage::new(right_page_id, page.header.kind);
        right_page.entries = right_entries;
        right_page.header.entry_count = right_page.entries.len() as u16;
        self.write_page(&right_page)?;

        if page.header.page_id == root_page_id {
            let new_root_id = self.get_next_page_id()?;
            let mut new_root = BTreePage::new(new_root_id, PageKind::Internal);

            let left_key = page.entries[0].key.clone();

            new_root
                .entries
                .push(BTreeEntry::new(left_key, page.header.page_id));
            new_root
                .entries
                .push(BTreeEntry::new(split_key, right_page_id));
            new_root.header.entry_count = new_root.entries.len() as u16;
            self.write_page(&new_root)?;

            let mut meta_page = self.read_page(0)?;
            if let Some(root_entry) = meta_page.entries.iter_mut().find(|e| {
                if let Value::Text(ref s) = e.key {
                    s == "root"
                } else {
                    false
                }
            }) {
                root_entry.pointer = new_root_id;
            } else {
                meta_page.entries.push(BTreeEntry::new(
                    Value::Text("root".to_string()),
                    new_root_id,
                ));
            }
            meta_page.header.entry_count = meta_page.entries.len() as u16;
            self.write_page(&meta_page)?;

            return Ok(new_root_id);
        }

        path.pop();
        if let Some(parent_page_id) = path.last().copied() {
            let mut parent_page = self.read_page(parent_page_id)?;

            let parent_entry = BTreeEntry::new(split_key, right_page_id);

            if parent_page.can_accept_entry(&parent_entry) {
                let insert_pos = parent_page
                    .entries
                    .binary_search_by(|e| e.key.cmp(&parent_entry.key))
                    .unwrap_or_else(|pos| pos);
                parent_page.entries.insert(insert_pos, parent_entry);
                parent_page.header.entry_count = parent_page.entries.len() as u16;
                self.write_page(&parent_page)?;
                Ok(root_page_id)
            } else {
                self.split_and_insert_recursive(parent_page, parent_entry, path, root_page_id)
            }
        } else {
            Ok(root_page_id)
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn delete(&mut self, key: &Value, root_page_id: u64) -> Result<bool, RustqlError> {
        match self.search(key, root_page_id)? {
            Some((page_id, entry_idx)) => {
                let mut page = self.read_page(page_id)?;
                page.entries.remove(entry_idx);
                page.header.entry_count = page.entries.len() as u16;
                self.write_page(&page)?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn range_scan(
        &mut self,
        start_key: Option<&Value>,
        end_key: Option<&Value>,
        root_page_id: u64,
    ) -> Result<Vec<(Value, u64)>, RustqlError> {
        scan_pages_in_order(
            |page_id| self.read_page(page_id),
            start_key,
            end_key,
            root_page_id,
        )
    }
}

fn scan_pages_in_order<F>(
    mut read_page: F,
    start_key: Option<&Value>,
    end_key: Option<&Value>,
    root_page_id: u64,
) -> Result<Vec<(Value, u64)>, RustqlError>
where
    F: FnMut(u64) -> Result<BTreePage, RustqlError>,
{
    let mut entries = Vec::new();
    scan_page_in_order_recursive(
        &mut read_page,
        root_page_id,
        start_key,
        end_key,
        &mut entries,
    )?;
    Ok(entries)
}

fn scan_page_in_order_recursive<F>(
    read_page: &mut F,
    page_id: u64,
    start_key: Option<&Value>,
    end_key: Option<&Value>,
    entries: &mut Vec<(Value, u64)>,
) -> Result<(), RustqlError>
where
    F: FnMut(u64) -> Result<BTreePage, RustqlError>,
{
    let page = read_page(page_id)?;

    match page.header.kind {
        PageKind::Leaf => {
            for entry in page.entries {
                if start_key.is_some_and(|start| &entry.key < start) {
                    continue;
                }
                if end_key.is_some_and(|end| &entry.key > end) {
                    break;
                }
                entries.push((entry.key, entry.pointer));
            }
            Ok(())
        }
        PageKind::Internal => {
            for (idx, entry) in page.entries.iter().enumerate() {
                if end_key.is_some_and(|end| &entry.key > end) {
                    break;
                }

                let next_min = page.entries.get(idx + 1).map(|next| &next.key);
                if start_key.is_some_and(|start| next_min.is_some_and(|min| min <= start)) {
                    continue;
                }

                scan_page_in_order_recursive(
                    read_page,
                    entry.pointer,
                    start_key,
                    end_key,
                    entries,
                )?;
            }
            Ok(())
        }
        PageKind::Meta => Err(RustqlError::StorageError(
            "Cannot scan entries from meta page".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{ColumnDefinition, DataType, TableConstraint};
    use crate::database::{CompositeIndex, RowId, Table, View};
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};

    fn remove_storage_artifacts(path: &Path) {
        let _ = std::fs::remove_file(path);
        let mut journal = path.as_os_str().to_os_string();
        journal.push(".wal");
        let _ = std::fs::remove_file(PathBuf::from(journal));
    }

    #[test]
    fn btree_storage_round_trip() {
        let temp_path = std::env::temp_dir().join("rustql_btree_test.dat");

        remove_storage_artifacts(&temp_path);

        let engine = BTreeStorageEngine::new(&temp_path);

        let mut db = Database::new();

        let columns = vec![
            ColumnDefinition {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
            },
            ColumnDefinition {
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: false,
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
            },
        ];

        let rows = vec![vec![Value::Integer(1), Value::Text("Alice".to_string())]];

        let table = Table::new(columns, rows, vec![]);

        let mut tables = HashMap::new();
        tables.insert("users".to_string(), table);

        db.tables = tables;

        engine
            .save(&db)
            .expect("failed to save via BTreeStorageEngine");
        let loaded = engine
            .load()
            .expect("failed to load via BTreeStorageEngine");

        let users = loaded
            .tables
            .get("users")
            .expect("users table missing after load");
        assert_eq!(users.rows.len(), 1);
        assert_eq!(users.columns.len(), 2);
        assert_eq!(users.rows[0][0], Value::Integer(1));
        assert_eq!(users.rows[0][1], Value::Text("Alice".to_string()));

        remove_storage_artifacts(&temp_path);
    }

    #[test]
    fn btree_storage_loads_legacy_row_keys_in_row_id_order() {
        let temp_path = std::env::temp_dir().join("rustql_btree_legacy_rows_test.dat");
        remove_storage_artifacts(&temp_path);

        let mut file = BTreeFile::create(&temp_path).expect("Failed to create BTree file");
        write_versioned_header(
            &mut file.file,
            BTreeFile::MAGIC,
            BTreeFile::VERSION,
            "BTree storage file",
        )
        .expect("Failed to write versioned header");

        let mut meta_page = BTreePage::new(0, PageKind::Meta);
        meta_page
            .entries
            .push(BTreeEntry::new(Value::Text("root".to_string()), 1));
        meta_page.header.entry_count = meta_page.entries.len() as u16;
        file.write_page(&meta_page)
            .expect("Failed to write meta page");

        let root_page_id = 1;
        let root_page = BTreePage::new(root_page_id, PageKind::Leaf);
        file.write_page(&root_page)
            .expect("Failed to write root page");

        let schema_json = serde_json::to_string(&TableStorageRecord {
            columns: vec![ColumnDefinition {
                name: "value".to_string(),
                data_type: DataType::Text,
                nullable: false,
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
            }],
            constraints: Vec::<TableConstraint>::new(),
            next_row_id: 11,
        })
        .expect("Failed to encode schema");
        let schema_pointer = file
            .write_data_to_pointer(&schema_json)
            .expect("Failed to write schema");

        let mut current_root_id = file
            .insert(
                Value::Text("schema:test".to_string()),
                schema_pointer,
                root_page_id,
            )
            .expect("Failed to insert schema");

        for (row_id, value) in [(10u64, "ten"), (2u64, "two")] {
            let row_json =
                serde_json::to_string(&vec![Value::Text(value.to_string())]).expect("row json");
            let row_pointer = file
                .write_data_to_pointer(&row_json)
                .unwrap_or_else(|_| panic!("Failed to write legacy row {}", row_id));
            current_root_id = file
                .insert(
                    Value::Text(format!("row:test:{}", row_id)),
                    row_pointer,
                    current_root_id,
                )
                .unwrap_or_else(|_| panic!("Failed to insert legacy row {}", row_id));
        }

        let mut meta_page = file.read_page(0).expect("Failed to reload meta page");
        meta_page
            .entries
            .iter_mut()
            .find(|entry| matches!(&entry.key, Value::Text(text) if text == "root"))
            .expect("root entry should exist")
            .pointer = current_root_id;
        meta_page.header.entry_count = meta_page.entries.len() as u16;
        file.write_page(&meta_page)
            .expect("Failed to update root pointer");
        drop(file);

        let engine = BTreeStorageEngine::new(&temp_path);
        let loaded = engine.load().expect("Failed to load legacy rows");
        let table = loaded.tables.get("test").expect("table should exist");

        assert_eq!(table.row_ids, vec![RowId(2), RowId(10)]);
        assert_eq!(
            table.rows,
            vec![
                vec![Value::Text("two".to_string())],
                vec![Value::Text("ten".to_string())]
            ]
        );

        remove_storage_artifacts(&temp_path);
    }

    #[test]
    fn btree_search_insert_delete() {
        let temp_path = std::env::temp_dir().join("rustql_btree_ops_test.dat");
        remove_storage_artifacts(&temp_path);

        let mut file = BTreeFile::create(&temp_path).expect("Failed to create BTree file");

        let mut meta_page = BTreePage::new(0, PageKind::Meta);
        meta_page
            .entries
            .push(BTreeEntry::new(Value::Text("root".to_string()), 1));
        meta_page.header.entry_count = meta_page.entries.len() as u16;
        file.write_page(&meta_page)
            .expect("Failed to write meta page");

        let mut root_page = BTreePage::new(1, PageKind::Leaf);
        root_page.header.entry_count = 0;
        file.write_page(&root_page)
            .expect("Failed to write root page");

        let key1 = Value::Integer(10);
        let value1_json = serde_json::to_string(&Value::Text("value1".to_string())).unwrap();
        let data_pointer1 = file
            .write_data_to_pointer(&value1_json)
            .expect("Failed to write data");
        let root_id = file
            .insert(key1.clone(), data_pointer1, 1)
            .expect("Failed to insert");

        let result = file.search(&key1, root_id).expect("Failed to search");
        assert!(result.is_some(), "Key should be found after insert");

        let key2 = Value::Integer(20);
        let value2_json = serde_json::to_string(&Value::Text("value2".to_string())).unwrap();
        let data_pointer2 = file
            .write_data_to_pointer(&value2_json)
            .expect("Failed to write data");
        file.insert(key2.clone(), data_pointer2, root_id)
            .expect("Failed to insert second key");

        let result2 = file
            .search(&key2, root_id)
            .expect("Failed to search for second key");
        assert!(result2.is_some(), "Second key should be found");

        let deleted = file.delete(&key1, root_id).expect("Failed to delete");
        assert!(deleted, "Delete should return true for existing key");

        let result_after_delete = file
            .search(&key1, root_id)
            .expect("Failed to search after delete");
        assert!(
            result_after_delete.is_none(),
            "Key should not be found after delete"
        );

        let result2_after_delete = file
            .search(&key2, root_id)
            .expect("Failed to search for second key after delete");
        assert!(
            result2_after_delete.is_some(),
            "Second key should still exist"
        );

        remove_storage_artifacts(&temp_path);
    }

    #[test]
    fn btree_range_scan() {
        let temp_path = std::env::temp_dir().join("rustql_btree_range_test.dat");
        remove_storage_artifacts(&temp_path);

        let mut file = BTreeFile::create(&temp_path).expect("Failed to create BTree file");

        let mut meta_page = BTreePage::new(0, PageKind::Meta);
        meta_page
            .entries
            .push(BTreeEntry::new(Value::Text("root".to_string()), 1));
        meta_page.header.entry_count = meta_page.entries.len() as u16;
        file.write_page(&meta_page)
            .expect("Failed to write meta page");

        let mut root_page = BTreePage::new(1, PageKind::Leaf);
        root_page.header.entry_count = 0;
        file.write_page(&root_page)
            .expect("Failed to write root page");

        let root_id = 1;
        for i in 1..=10 {
            let key = Value::Integer(i * 10);
            let value_json = serde_json::to_string(&Value::Text(format!("value{}", i))).unwrap();
            let data_pointer = file
                .write_data_to_pointer(&value_json)
                .unwrap_or_else(|_| panic!("Failed to write data for key {}", i));
            file.insert(key, data_pointer, root_id)
                .unwrap_or_else(|_| panic!("Failed to insert key {}", i));
        }

        let start = Value::Integer(30);
        let end = Value::Integer(70);
        let results = file
            .range_scan(Some(&start), Some(&end), root_id)
            .expect("Failed to range scan");

        assert!(results.len() >= 5, "Range scan should find multiple keys");

        remove_storage_artifacts(&temp_path);
    }

    #[test]
    fn btree_range_scan_after_split_stays_ordered() {
        let temp_path = std::env::temp_dir().join("rustql_btree_range_split_test.dat");
        remove_storage_artifacts(&temp_path);

        let mut file = BTreeFile::create(&temp_path).expect("Failed to create BTree file");

        let mut meta_page = BTreePage::new(0, PageKind::Meta);
        meta_page
            .entries
            .push(BTreeEntry::new(Value::Text("root".to_string()), 1));
        meta_page.header.entry_count = meta_page.entries.len() as u16;
        file.write_page(&meta_page)
            .expect("Failed to write meta page");

        let mut root_page = BTreePage::new(1, PageKind::Leaf);
        root_page.header.entry_count = 0;
        file.write_page(&root_page)
            .expect("Failed to write root page");

        let mut root_id = 1;
        for i in 1..=400 {
            let key = Value::Integer(i);
            let value_json = serde_json::to_string(&Value::Text(format!("value{}", i))).unwrap();
            let data_pointer = file
                .write_data_to_pointer(&value_json)
                .unwrap_or_else(|_| panic!("Failed to write data for key {}", i));
            root_id = file
                .insert(key, data_pointer, root_id)
                .unwrap_or_else(|_| panic!("Failed to insert key {}", i));
        }

        let start = Value::Integer(101);
        let end = Value::Integer(199);
        let results = file
            .range_scan(Some(&start), Some(&end), root_id)
            .expect("Failed to range scan after split");

        assert_eq!(results.len(), 99);
        assert!(matches!(results.first(), Some((Value::Integer(101), _))));
        assert!(matches!(results.last(), Some((Value::Integer(199), _))));
        assert!(results.windows(2).all(|pair| pair[0].0 <= pair[1].0));

        remove_storage_artifacts(&temp_path);
    }

    #[test]
    fn btree_concurrent_loads() {
        let temp_path = std::env::temp_dir().join("rustql_btree_concurrent_test.dat");
        remove_storage_artifacts(&temp_path);

        let engine = Arc::new(BTreeStorageEngine::new(&temp_path));

        let mut db = Database::new();
        let columns = vec![ColumnDefinition {
            name: "id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: true,
            unique: false,
            default_value: None,
            foreign_key: None,
            check: None,
            auto_increment: false,
            generated: None,
        }];
        let table = Table::new(columns, vec![vec![Value::Integer(1)]], vec![]);
        db.tables.insert("test".to_string(), table);

        engine.save(&db).expect("Failed to save");

        let loaded_before = engine.load().expect("initial load failed");
        assert_eq!(loaded_before.tables.len(), 1, "Initial load should work");

        use std::thread;
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let engine = Arc::clone(&engine);
                thread::spawn(move || {
                    let loaded = engine.load().expect("concurrent load failed");
                    assert_eq!(loaded.tables.len(), 1, "Expected 1 table");
                    assert!(loaded.tables.contains_key("test"), "Expected 'test' table");
                })
            })
            .collect();

        for handle in handles {
            if let Err(e) = handle.join() {
                panic!("Thread panicked: {:?}", e);
            }
        }

        let (hits, misses, _size) = engine.cache_stats();
        assert!(hits + misses > 0, "Cache should have activity");

        remove_storage_artifacts(&temp_path);
    }

    #[test]
    fn btree_cache_lru_eviction() {
        let temp_path = std::env::temp_dir().join("rustql_btree_cache_test.dat");
        remove_storage_artifacts(&temp_path);

        let engine = BTreeStorageEngine::new(&temp_path);

        let mut db = Database::new();
        let columns = vec![ColumnDefinition {
            name: "id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: true,
            unique: false,
            default_value: None,
            foreign_key: None,
            check: None,
            auto_increment: false,
            generated: None,
        }];
        let table = Table::new(columns, vec![vec![Value::Integer(1)]], vec![]);
        db.tables.insert("test".to_string(), table);

        engine.save(&db).expect("Failed to save");

        let _ = engine.load();
        let (_, _, size1) = engine.cache_stats();
        assert!(size1 > 0, "Cache should have pages after load");

        engine.clear_cache();
        let (_, _, size2) = engine.cache_stats();
        assert_eq!(size2, 0, "Cache should be empty after clear");

        remove_storage_artifacts(&temp_path);
    }

    #[test]
    fn btree_storage_rejects_invalid_header() {
        let temp_path = std::env::temp_dir().join("rustql_btree_invalid_header.dat");
        remove_storage_artifacts(&temp_path);

        std::fs::write(&temp_path, [0u8; FILE_HEADER_SIZE]).expect("failed to write invalid file");

        let engine = BTreeStorageEngine::new(&temp_path);
        let error = match engine.load() {
            Ok(_) => panic!("expected invalid header error"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("invalid header"));

        remove_storage_artifacts(&temp_path);
    }

    #[test]
    fn btree_storage_rejects_unsupported_version() {
        let temp_path = std::env::temp_dir().join("rustql_btree_unsupported_version.dat");
        remove_storage_artifacts(&temp_path);

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)
            .expect("failed to create version test file");
        write_versioned_header(&mut file, BTreeFile::MAGIC, 999, "BTree storage file")
            .expect("failed to write test header");

        let engine = BTreeStorageEngine::new(&temp_path);
        let error = match engine.load() {
            Ok(_) => panic!("expected unsupported version error"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("Unsupported BTree storage file format version")
        );

        remove_storage_artifacts(&temp_path);
    }

    #[test]
    fn btree_recovers_committed_journal_on_load() {
        let temp_path = std::env::temp_dir().join("rustql_btree_committed_journal.dat");
        remove_storage_artifacts(&temp_path);

        let engine = BTreeStorageEngine::new(&temp_path);

        let mut db = Database::new();
        db.tables.insert(
            "test".to_string(),
            Table::new(
                vec![ColumnDefinition {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    primary_key: true,
                    unique: false,
                    default_value: None,
                    foreign_key: None,
                    check: None,
                    auto_increment: false,
                    generated: None,
                }],
                vec![vec![Value::Integer(1)]],
                vec![],
            ),
        );
        engine.save(&db).expect("failed to save base database");

        let mut committed = db.clone();
        committed.tables.get_mut("test").unwrap().rows = vec![vec![Value::Integer(2)]];
        engine
            .write_journal_locked(&TransactionJournal::Committed {
                database: committed.clone(),
            })
            .expect("failed to write committed journal");

        let loaded = engine.load().expect("failed to recover committed journal");
        assert_eq!(loaded.tables["test"].rows, committed.tables["test"].rows);
        assert!(
            !engine.journal_path().exists(),
            "journal should be cleared after recovery"
        );

        let loaded_again = engine.load().expect("failed to reload recovered database");
        assert_eq!(
            loaded_again.tables["test"].rows,
            committed.tables["test"].rows
        );

        remove_storage_artifacts(&temp_path);
    }

    #[test]
    fn btree_discards_pending_journal_on_load() {
        let temp_path = std::env::temp_dir().join("rustql_btree_pending_journal.dat");
        remove_storage_artifacts(&temp_path);

        let engine = BTreeStorageEngine::new(&temp_path);

        let mut db = Database::new();
        db.tables.insert(
            "test".to_string(),
            Table::new(
                vec![ColumnDefinition {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    primary_key: true,
                    unique: false,
                    default_value: None,
                    foreign_key: None,
                    check: None,
                    auto_increment: false,
                    generated: None,
                }],
                vec![vec![Value::Integer(1)]],
                vec![],
            ),
        );
        engine.save(&db).expect("failed to save base database");
        engine
            .write_journal_locked(&TransactionJournal::Pending)
            .expect("failed to write pending journal");

        let loaded = engine.load().expect("failed to load database");
        assert_eq!(loaded.tables["test"].rows, db.tables["test"].rows);
        assert!(
            !engine.journal_path().exists(),
            "pending journal should be cleared during load"
        );

        remove_storage_artifacts(&temp_path);
    }

    #[test]
    fn btree_persists_views_constraints_and_composite_indexes() {
        let temp_path = std::env::temp_dir().join("rustql_btree_metadata_roundtrip.dat");
        remove_storage_artifacts(&temp_path);

        let engine = BTreeStorageEngine::new(&temp_path);

        let mut db = Database::new();
        db.tables.insert(
            "orders".to_string(),
            Table::new(
                vec![
                    ColumnDefinition {
                        name: "id".to_string(),
                        data_type: DataType::Integer,
                        nullable: false,
                        primary_key: true,
                        unique: false,
                        default_value: None,
                        foreign_key: None,
                        check: None,
                        auto_increment: false,
                        generated: None,
                    },
                    ColumnDefinition {
                        name: "customer_id".to_string(),
                        data_type: DataType::Integer,
                        nullable: false,
                        primary_key: false,
                        unique: false,
                        default_value: None,
                        foreign_key: None,
                        check: None,
                        auto_increment: false,
                        generated: None,
                    },
                ],
                vec![vec![Value::Integer(1), Value::Integer(10)]],
                vec![TableConstraint::Unique {
                    name: Some("orders_customer_unique".to_string()),
                    columns: vec!["customer_id".to_string()],
                }],
            ),
        );
        db.views.insert(
            "order_ids".to_string(),
            View {
                name: "order_ids".to_string(),
                query_sql: "SELECT id FROM orders".to_string(),
            },
        );
        db.composite_indexes.insert(
            "orders_customer_idx".to_string(),
            CompositeIndex {
                name: "orders_customer_idx".to_string(),
                table: "orders".to_string(),
                columns: vec!["id".to_string(), "customer_id".to_string()],
                entries: std::collections::BTreeMap::from([(
                    vec![Value::Integer(1), Value::Integer(10)],
                    vec![RowId(1)],
                )]),
                filter_expr: None,
            },
        );

        engine.save(&db).expect("failed to save metadata database");
        let loaded = engine.load().expect("failed to load metadata database");

        assert_eq!(
            loaded.tables["orders"].constraints,
            db.tables["orders"].constraints
        );
        assert_eq!(loaded.views["order_ids"].query_sql, "SELECT id FROM orders");
        assert!(loaded.composite_indexes.contains_key("orders_customer_idx"));

        remove_storage_artifacts(&temp_path);
    }
}
