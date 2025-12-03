use crate::ast::Value;
use crate::database::Database;
use serde::{Deserialize, Serialize};
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
        fs::write(&self.path, data).map_err(|e| format!("Failed to write database file: {}", e))?;
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
        match BTreeFile::open(&self.data_path) {
            Ok(mut file) => match file.read_database_via_pages() {
                Ok(db) => db,
                Err(_) => Database::new(),
            },
            Err(_) => Database::new(),
        }
    }

    fn save(&self, db: &Database) -> Result<(), String> {
        // Persist the database using a minimal page-based format:
        // - Page 0 (Meta): points to the logical root page for the database.
        // - Page 1 (Leaf): stores the entire Database as a JSON blob inside a
        //   single entry's key. This uses the page + entry machinery end-to-end
        //   while keeping the mapping logic simple for now.
        let mut file = BTreeFile::create(&self.data_path)?;
        file.write_database_via_pages(db)
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

    fn read_database_legacy(&mut self) -> Result<Database, String> {
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
        let _version = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);

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

    /// New page-based representation for the database:
    /// - Global file header (magic + version) at offset 0.
    /// - Page 0: Meta page, whose first entry points to the root page (page 1).
    /// - Page 1: Leaf page containing a single entry whose key holds the
    ///   entire Database encoded as JSON text.
    fn read_database_via_pages(&mut self) -> Result<Database, String> {
        // Read and validate file header.
        let mut header = [0u8; 16];
        let bytes_read = self
            .file
            .read(&mut header)
            .map_err(|e| format!("Failed to read BTree storage header: {}", e))?;

        if bytes_read < header.len() || header[0..8] != Self::MAGIC {
            // If header is missing/invalid, try legacy format; otherwise start fresh.
            return self.read_database_legacy().or_else(|_| Ok(Database::new()));
        }

        let _version = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        // Reserved bytes [12..16] currently ignored.

        // Read meta page (page 0) and determine root page id.
        let meta_page = self.read_page(0)?;
        let root_page_id = meta_page
            .entries
            .get(0)
            .map(|e| e.pointer)
            .unwrap_or(1);

        // Read root page and extract JSON-encoded Database from the first entry.
        let root_page = self.read_page(root_page_id)?;
        let json_entry = root_page
            .entries
            .get(0)
            .ok_or_else(|| "Root BTree page has no entries".to_string())?;

        let json_str = match &json_entry.key {
            Value::Text(s) => s.clone(),
            _ => return Err("Root BTree entry does not contain JSON text".to_string()),
        };

        serde_json::from_str(&json_str)
            .map_err(|e| format!("Failed to decode Database from BTree root page: {}", e))
    }

    fn write_database_via_pages(&mut self, db: &Database) -> Result<(), String> {
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

        // Encode Database as a JSON string stored in the first entry of the
        // root leaf page.
        let json = serde_json::to_string(db)
            .map_err(|e| format!("Failed to encode database for BTree root page: {}", e))?;

        // Root leaf page (page 1).
        let mut root_page = BTreePage::new(1, PageKind::Leaf);
        root_page
            .entries
            .push(BTreeEntry::new(Value::Text(json), 0));
        root_page.header.entry_count = root_page.entries.len() as u16;

        // Meta page (page 0) points to root page id 1.
        let mut meta_page = BTreePage::new(0, PageKind::Meta);
        meta_page
            .entries
            .push(BTreeEntry::new(Value::Text("root".to_string()), 1));
        meta_page.header.entry_count = meta_page.entries.len() as u16;

        // Write pages.
        self.write_page(&meta_page)?;
        self.write_page(&root_page)?;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PageKind {
    Meta,
    Internal,
    Leaf,
}

/// On-disk page header describing the contents of a single page.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
            Value::Text(s) | Value::Date(s) | Value::Time(s) | Value::DateTime(s) => 1 + s.len(),
        };
        pointer_size + key_size
    }
}

// ---------------- Page <-> bytes helpers ----------------

impl BTreePage {
    /// Serialise this page into a fixed-size byte buffer suitable for writing
    /// to disk. Fails if the estimated encoded size would exceed
    /// `BTREE_PAGE_SIZE`.
    pub fn to_bytes(&self) -> Result<[u8; BTREE_PAGE_SIZE], String> {
        let mut buf = [0u8; BTREE_PAGE_SIZE];

        // Encode header using a simple fixed layout.
        let kind_byte = match self.header.kind {
            PageKind::Meta => 0u8,
            PageKind::Internal => 1u8,
            PageKind::Leaf => 2u8,
        };

        buf[0..8].copy_from_slice(&self.header.page_id.to_le_bytes());
        buf[8] = kind_byte;
        buf[9..11].copy_from_slice(&self.header.entry_count.to_le_bytes());
        buf[11..13].copy_from_slice(&self.header.reserved.to_le_bytes());
        // bytes 13..16 reserved/padding (left as zero)

        // Encode entries as a compact JSON blob for now.
        let entries_json = serde_json::to_vec(&self.entries)
            .map_err(|e| format!("Failed to encode BTree entries: {}", e))?;

        let header_size = 16usize;
        if header_size + entries_json.len() > BTREE_PAGE_SIZE {
            return Err("BTreePage too large to fit in fixed page size".to_string());
        }

        buf[header_size..header_size + entries_json.len()].copy_from_slice(&entries_json);
        Ok(buf)
    }

    /// Deserialize a `BTreePage` from a fixed-size page buffer.
    pub fn from_bytes(buf: &[u8; BTREE_PAGE_SIZE]) -> Result<Self, String> {
        let page_id = u64::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);
        let kind = match buf[8] {
            0 => PageKind::Meta,
            1 => PageKind::Internal,
            2 => PageKind::Leaf,
            other => {
                return Err(format!("Unknown BTree page kind byte: {}", other));
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

        // Remaining bytes contain the JSON-encoded entries; trim trailing zeros
        // before decoding to avoid parse errors on unwritten space.
        let header_size = 16usize;
        let mut payload = &buf[header_size..];
        // Find last non-zero byte.
        if let Some(last) = payload.iter().rposition(|b| *b != 0) {
            payload = &payload[..=last];
        } else {
            payload = &[];
        }

        let entries: Vec<BTreeEntry> = if payload.is_empty() {
            Vec::new()
        } else {
            serde_json::from_slice(payload)
                .map_err(|e| format!("Failed to decode BTree entries: {}", e))?
        };

        Ok(BTreePage { header, entries })
    }
}

impl BTreeFile {
    /// Read a single page at the given `page_id` from disk.
    ///
    /// This uses the current simple header+payload layout and does not yet
    /// integrate with `read_database` / `write_database`.
    pub fn read_page(&mut self, page_id: u64) -> Result<BTreePage, String> {
        let header_size = 16u64;
        let offset = header_size + page_id * BTREE_PAGE_SIZE as u64;

        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(|e| format!("Failed to seek to BTree page: {}", e))?;

        let mut buf = [0u8; BTREE_PAGE_SIZE];
        self.file
            .read_exact(&mut buf)
            .map_err(|e| format!("Failed to read BTree page: {}", e))?;

        BTreePage::from_bytes(&buf)
    }

    /// Write a single page to disk at its `header.page_id`.
    pub fn write_page(&mut self, page: &BTreePage) -> Result<(), String> {
        let header_size = 16u64;
        let offset = header_size + page.header.page_id * BTREE_PAGE_SIZE as u64;

        let buf = page.to_bytes()?;

        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(|e| format!("Failed to seek to BTree page for write: {}", e))?;

        self.file
            .write_all(&buf)
            .map_err(|e| format!("Failed to write BTree page: {}", e))?;

        self.file
            .flush()
            .map_err(|e| format!("Failed to flush BTree page: {}", e))
    }
}
