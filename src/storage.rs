use crate::ast::Value;
use crate::database::Database;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

pub trait StorageEngine: Send + Sync {
    fn load(&self) -> Database;

    fn save(&self, db: &Database) -> Result<(), String>;
}

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

static STORAGE_ENGINE: OnceLock<Box<dyn StorageEngine>> = OnceLock::new();

fn default_engine() -> Box<dyn StorageEngine> {
    match std::env::var("RUSTQL_STORAGE") {
        Ok(v) if v.eq_ignore_ascii_case("btree") => {
            Box::new(BTreeStorageEngine::new("rustql_btree.dat"))
        }
        _ => Box::new(JsonStorageEngine::new("rustql_data.json")),
    }
}

pub fn storage_engine() -> &'static dyn StorageEngine {
    let _ = STORAGE_ENGINE.get_or_init(default_engine);

    &**STORAGE_ENGINE.get().unwrap()
}

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
        let mut file = BTreeFile::create(&self.data_path)?;
        file.write_database_via_pages(db)
    }
}

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
            return Ok(Database::new());
        }

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

    fn read_database_via_pages(&mut self) -> Result<Database, String> {
        let mut header = [0u8; 16];
        let bytes_read = self
            .file
            .read(&mut header)
            .map_err(|e| format!("Failed to read BTree storage header: {}", e))?;

        if bytes_read < header.len() || header[0..8] != Self::MAGIC {
            return self.read_database_legacy().or_else(|_| Ok(Database::new()));
        }

        let _version = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);

        let meta_page = self.read_page(0)?;
        let root_page_id = meta_page.entries.get(0).map(|e| e.pointer).unwrap_or(1);

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

        let json = serde_json::to_string(db)
            .map_err(|e| format!("Failed to encode database for BTree root page: {}", e))?;

        let mut root_page = BTreePage::new(1, PageKind::Leaf);
        root_page
            .entries
            .push(BTreeEntry::new(Value::Text(json), 0));
        root_page.header.entry_count = root_page.entries.len() as u16;

        let mut meta_page = BTreePage::new(0, PageKind::Meta);
        meta_page
            .entries
            .push(BTreeEntry::new(Value::Text("root".to_string()), 1));
        meta_page.header.entry_count = meta_page.entries.len() as u16;

        self.write_page(&meta_page)?;
        self.write_page(&root_page)?;

        self.file
            .flush()
            .map_err(|e| format!("Failed to flush BTree storage file: {}", e))
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
            Value::Integer(_) => 9, // tag + i64
            Value::Float(_) => 9,   // tag + f64
            Value::Boolean(_) => 2,
            Value::Text(s) | Value::Date(s) | Value::Time(s) | Value::DateTime(s) => 1 + s.len(),
        };
        pointer_size + key_size
    }
}

impl BTreePage {
    pub fn to_bytes(&self) -> Result<[u8; BTREE_PAGE_SIZE], String> {
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

        let entries_json = serde_json::to_vec(&self.entries)
            .map_err(|e| format!("Failed to encode BTree entries: {}", e))?;

        let header_size = 16usize;
        if header_size + entries_json.len() > BTREE_PAGE_SIZE {
            return Err("BTreePage too large to fit in fixed page size".to_string());
        }

        buf[header_size..header_size + entries_json.len()].copy_from_slice(&entries_json);
        Ok(buf)
    }

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

        let header_size = 16usize;
        let mut payload = &buf[header_size..];

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{ColumnDefinition, DataType};
    use crate::database::Table;
    use std::collections::HashMap;

    #[test]
    fn btree_storage_round_trip() {
        let temp_path = std::env::temp_dir().join("rustql_btree_test.dat");

        let _ = std::fs::remove_file(&temp_path);

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
            },
            ColumnDefinition {
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: false,
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
            },
        ];

        let rows = vec![vec![Value::Integer(1), Value::Text("Alice".to_string())]];

        let table = Table { columns, rows };

        let mut tables = HashMap::new();
        tables.insert("users".to_string(), table);

        db.tables = tables;

        engine
            .save(&db)
            .expect("failed to save via BTreeStorageEngine");
        let loaded = engine.load();

        let users = loaded
            .tables
            .get("users")
            .expect("users table missing after load");
        assert_eq!(users.rows.len(), 1);
        assert_eq!(users.columns.len(), 2);
        assert_eq!(users.rows[0][0], Value::Integer(1));
        assert_eq!(users.rows[0][1], Value::Text("Alice".to_string()));

        let _ = std::fs::remove_file(&temp_path);
    }
}
