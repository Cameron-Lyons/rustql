use crate::database::Database;
use crate::ast::Value;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
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
        // First-cut implementation: if the file does not exist, return a new DB.
        // If it exists but is empty or invalid, also fall back to a new DB.
        //
        // Future work: interpret the file as a sequence of fixed-size pages,
        // starting with a meta page that contains root pointers for each table
        // and index B-tree.
        match BTreeFile::open(&self.data_path) {
            Ok(mut file) => match file.read_database() {
                Ok(db) => db,
                Err(_) => Database::new(),
            },
            Err(_) => Database::new(),
        }
    }

    fn save(&self, _db: &Database) -> Result<(), String> {
        // First-cut implementation: delegate to a very simple paged file that
        // currently just writes the entire Database as a single blob behind a
        // small header. This keeps the on-disk format independent from the
        // JsonStorageEngine while we experiment with page layout.
        let mut file = BTreeFile::create(&self.data_path)?;
        file.write_database(_db)
    }
}

/// Very small, experimental paged file format used by `BTreeStorageEngine`.
///
/// Layout (all little-endian):
/// - 8 bytes: magic header: b\"RSTQLBT\\0\"
/// - 4 bytes: format version (u32)
/// - 4 bytes: reserved
/// - remaining bytes: opaque payload (currently a single JSON blob of Database)
///
/// This is intentionally simple while we design proper fixed-size pages:
/// in the future, the payload region will be replaced by a sequence of
/// fixed-size pages with meta / internal / leaf nodes for B-trees.
struct BTreeFile {
    file: std::fs::File,
}

impl BTreeFile {
    const MAGIC: [u8; 8] = *b"RSTQLBT\0";
    const VERSION: u32 = 1;

    fn open(path: &Path) -> Result<Self, String> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .map_err(|e| format!("Failed to open BTree storage file: {}", e))?;
        Ok(BTreeFile { file })
    }

    fn create(path: &Path) -> Result<Self, String> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(|e| format!("Failed to create BTree storage file: {}", e))?;
        Ok(BTreeFile { file })
    }

    fn read_database(&mut self) -> Result<Database, String> {
        let mut header = [0u8; 16];
        let bytes_read = self
            .file
            .read(&mut header)
            .map_err(|e| format!("Failed to read BTree storage header: {}", e))?;

        if bytes_read < header.len() || header[0..8] != Self::MAGIC {
            // Treat missing or invalid header as empty database for now.
            return Ok(Database::new());
        }

        // In the future we may branch on version; for now we just ensure it's non-zero.
        let _version =
            u32::from_le_bytes([header[8], header[9], header[10], header[11]]);

        let mut buf = Vec::new();
        self.file
            .read_to_end(&mut buf)
            .map_err(|e| format!("Failed to read BTree storage payload: {}", e))?;

        if buf.is_empty() {
            return Ok(Database::new());
        }

        serde_json::from_slice(&buf)
            .map_err(|e| format!("Failed to decode BTree storage payload: {}", e))
    }

    fn write_database(&mut self, db: &Database) -> Result<(), String> {
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(|e| format!("Failed to seek BTree storage file: {}", e))?;

        let mut header = Vec::with_capacity(16);
        header.extend_from_slice(&Self::MAGIC);
        header.extend_from_slice(&Self::VERSION.to_le_bytes());
        header.extend_from_slice(&0u32.to_le_bytes()); // reserved

        self.file
            .write_all(&header)
            .map_err(|e| format!("Failed to write BTree storage header: {}", e))?;

        let payload = serde_json::to_vec(db)
            .map_err(|e| format!("Failed to encode database for BTree storage: {}", e))?;

        self.file
            .write_all(&payload)
            .map_err(|e| format!("Failed to write BTree storage payload: {}", e))?;

        self.file
            .flush()
            .map_err(|e| format!("Failed to flush BTree storage file: {}", e))
    }
}

// ============================================================
// B-tree page and node layout (not yet wired into BTreeFile)
// ============================================================

/// Fixed page size to target for on-disk B-tree pages.
///
/// This is a reasonable default that keeps pages aligned with typical
/// filesystem blocks while staying small enough for experimentation.
pub const BTREE_PAGE_SIZE: usize = 4096;

/// Logical type of a page in the B-tree file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageKind {
    Meta,
    Internal,
    Leaf,
}

/// On-disk page header describing the contents of a single page.
#[derive(Debug, Clone)]
pub struct PageHeader {
    /// Logical page identifier (0 is typically the meta page).
    pub page_id: u64,
    /// Type of this page (meta / internal / leaf).
    pub kind: PageKind,
    /// Number of key/value entries currently stored in the page.
    pub entry_count: u16,
    /// Reserved for future use (split pointers, sibling links, etc.).
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

/// Single B-tree page in memory.
///
/// For now we expose a unified representation; in a more advanced design
/// you might use distinct structs for meta / internal / leaf pages.
#[derive(Debug, Clone)]
pub struct BTreePage {
    pub header: PageHeader,
    /// For internal pages, `entries` hold (key, child_page_id) pairs.
    /// For leaf pages, `entries` hold (key, row_pointer) pairs where
    /// `row_pointer` could later be a physical offset.
    pub entries: Vec<BTreeEntry>,
}

impl BTreePage {
    pub fn new(page_id: u64, kind: PageKind) -> Self {
        BTreePage {
            header: PageHeader::new(page_id, kind),
            entries: Vec::new(),
        }
    }

    /// Estimate whether another entry would fit in this page, given the
    /// target on-disk page size. This is deliberately approximate: once
    /// the format stabilises we can tighten the calculation.
    pub fn can_accept_entry(&self, entry: &BTreeEntry) -> bool {
        let current_size = self.estimated_size();
        let added = entry.estimated_size();
        current_size + added <= BTREE_PAGE_SIZE
    }

    /// Rough size estimate for this page when serialised.
    fn estimated_size(&self) -> usize {
        // Header: 8 (page_id) + 1 (kind) + 2 (entry_count) + 2 (reserved) ~= 16 bytes
        let header_size = 16usize;
        let entries_size: usize = self.entries.iter().map(|e| e.estimated_size()).sum();
        header_size + entries_size
    }
}

/// Single key/value entry inside a B-tree page.
#[derive(Debug, Clone)]
pub struct BTreeEntry {
    /// Logical key stored in this node.
    pub key: Value,
    /// For internal nodes: child page id.
    /// For leaf nodes: offset or logical row id. For now we store it as
    /// a simple 64-bit integer to keep the layout flexible.
    pub pointer: u64,
}

impl BTreeEntry {
    pub fn new(key: Value, pointer: u64) -> Self {
        BTreeEntry { key, pointer }
    }

    /// Very rough size estimate used by `BTreePage::can_accept_entry`.
    fn estimated_size(&self) -> usize {
        // Pointer is always 8 bytes.
        let pointer_size = 8usize;
        let key_size = match &self.key {
            Value::Null => 1,
            Value::Integer(_) => 9, // tag + i64
            Value::Float(_) => 9,   // tag + f64
            Value::Boolean(_) => 2,
            Value::Text(s) | Value::Date(s) | Value::Time(s) | Value::DateTime(s) => {
                1 + s.len()
            }
        };
        pointer_size + key_size
    }
}




