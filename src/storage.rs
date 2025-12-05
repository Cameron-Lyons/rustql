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
        let file_size = self
            .file
            .metadata()
            .map_err(|e| format!("Failed to get file metadata: {}", e))?
            .len();

        if file_size < 16 {
            return Ok(Database::new());
        }

        let mut header = [0u8; 16];
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(|e| format!("Failed to seek to start: {}", e))?;
        let bytes_read = self
            .file
            .read(&mut header)
            .map_err(|e| format!("Failed to read BTree storage header: {}", e))?;

        if bytes_read < header.len() || header[0..8] != Self::MAGIC {
            return self.read_database_legacy().or_else(|_| Ok(Database::new()));
        }

        let _version = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);

        if file_size < 16 + BTREE_PAGE_SIZE as u64 {
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

        let root_page = self.read_page(root_page_id)?;

        if root_page.entries.len() == 1 {
            if let Some(entry) = root_page.entries.get(0) {
                if let Value::Text(ref json_str) = entry.key {
                    if json_str.starts_with('{') && json_str.len() > 100 {
                        return serde_json::from_str(json_str).map_err(|e| {
                            format!("Failed to decode Database from BTree root page: {}", e)
                        });
                    }
                }
            }
        }

        self.load_database_from_rows(root_page_id)
    }

    fn load_database_from_rows(&mut self, root_page_id: u64) -> Result<Database, String> {
        let mut db = Database::new();

        let schema_prefix = Value::Text("schema:".to_string());
        let schema_end = Value::Text("schema;".to_string());
        let schemas = self.range_scan(Some(&schema_prefix), Some(&schema_end), root_page_id)?;

        for (key, pointer) in schemas {
            if let Value::Text(ref key_str) = key {
                if let Some(table_name) = key_str.strip_prefix("schema:") {
                    let schema_json = self.read_data_from_pointer(pointer)?;
                    let columns: Vec<crate::ast::ColumnDefinition> =
                        serde_json::from_str(&schema_json).map_err(|e| {
                            format!(
                                "Failed to deserialize schema for table {}: {}",
                                table_name, e
                            )
                        })?;

                    db.tables.insert(
                        table_name.to_string(),
                        crate::database::Table {
                            columns,
                            rows: Vec::new(),
                        },
                    );
                }
            }
        }

        for table_name in db.tables.keys().cloned().collect::<Vec<_>>() {
            let row_prefix = Value::Text(format!("row:{}:", table_name));
            let row_end = Value::Text(format!("row:{};", table_name));
            let rows = self.range_scan(Some(&row_prefix), Some(&row_end), root_page_id)?;

            let mut row_indices: Vec<(usize, Vec<Value>)> = Vec::new();

            for (key, pointer) in rows {
                if let Value::Text(ref key_str) = key {
                    if let Some(row_id_str) = key_str.strip_prefix(&format!("row:{}:", table_name))
                    {
                        if let Ok(row_index) = row_id_str.parse::<usize>() {
                            let row_json = self.read_data_from_pointer(pointer)?;
                            let row: Vec<Value> = serde_json::from_str(&row_json).map_err(|e| {
                                format!(
                                    "Failed to deserialize row {} for table {}: {}",
                                    row_id_str, table_name, e
                                )
                            })?;
                            row_indices.push((row_index, row));
                        }
                    }
                }
            }

            row_indices.sort_by_key(|(idx, _)| *idx);
            let sorted_rows: Vec<Vec<Value>> =
                row_indices.into_iter().map(|(_, row)| row).collect();

            if let Some(table_ref) = db.tables.get_mut(&table_name) {
                table_ref.rows = sorted_rows;
            }
        }

        let index_prefix = Value::Text("index:".to_string());
        let index_end = Value::Text("index;".to_string());
        let indexes = self.range_scan(Some(&index_prefix), Some(&index_end), root_page_id)?;

        for (key, pointer) in indexes {
            if let Value::Text(ref key_str) = key {
                if let Some(index_name) = key_str.strip_prefix("index:") {
                    let index_json = self.read_data_from_pointer(pointer)?;
                    let index: crate::database::Index =
                        serde_json::from_str(&index_json).map_err(|e| {
                            format!("Failed to deserialize index {}: {}", index_name, e)
                        })?;
                    db.indexes.insert(index_name.to_string(), index);
                }
            }
        }

        Ok(db)
    }

    fn read_data_from_pointer(&mut self, pointer: u64) -> Result<String, String> {
        let data_page = self.read_page(pointer)?;

        if let Some(entry) = data_page.entries.get(0) {
            if let Value::Text(ref json_str) = entry.key {
                return Ok(json_str.clone());
            }
        }

        Err("Data page does not contain valid JSON".to_string())
    }

    fn write_data_to_pointer(&mut self, data: &str) -> Result<u64, String> {
        let page_id = self.get_next_page_id()?;
        let mut data_page = BTreePage::new(page_id, PageKind::Leaf);

        data_page
            .entries
            .push(BTreeEntry::new(Value::Text(data.to_string()), 0));
        data_page.header.entry_count = data_page.entries.len() as u16;

        self.write_page(&data_page)?;
        Ok(page_id)
    }

    fn write_database_via_pages(&mut self, db: &Database) -> Result<(), String> {
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(|e| format!("Failed to seek BTree storage file: {}", e))?;

        let mut header = Vec::with_capacity(16);
        header.extend_from_slice(&Self::MAGIC);
        header.extend_from_slice(&Self::VERSION.to_le_bytes());
        header.extend_from_slice(&0u32.to_le_bytes());

        self.file
            .write_all(&header)
            .map_err(|e| format!("Failed to write BTree storage header: {}", e))?;

        let meta_page = match self.read_page(1) {
            Ok(page) => page,
            Err(_) => {
                let new_meta = BTreePage::new(0, PageKind::Meta);
                self.write_page(&new_meta)?;
                new_meta
            }
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
            .unwrap_or_else(|| {
                let new_root_id = 1;
                let mut new_meta = meta_page.clone();
                new_meta.entries.push(BTreeEntry::new(
                    Value::Text("root".to_string()),
                    new_root_id,
                ));
                new_meta.header.entry_count = new_meta.entries.len() as u16;
                self.write_page(&new_meta).ok();

                let root_page = BTreePage::new(new_root_id, PageKind::Leaf);
                self.write_page(&root_page).ok();
                new_root_id
            });

        let tables_to_delete: Vec<String> = db.tables.keys().cloned().collect();
        for table_name in &tables_to_delete {
            let schema_key = Value::Text(format!("schema:{}", table_name));
            let _ = self.delete(&schema_key, root_page_id);

            let row_prefix = Value::Text(format!("row:{}:", table_name));
            let row_end = Value::Text(format!("row:{};", table_name));
            if let Ok(rows) = self.range_scan(Some(&row_prefix), Some(&row_end), root_page_id) {
                for (key, _) in rows {
                    let _ = self.delete(&key, root_page_id);
                }
            }
        }

        let indexes_to_delete: Vec<String> = db.indexes.keys().cloned().collect();
        for index_name in &indexes_to_delete {
            let index_key = Value::Text(format!("index:{}", index_name));
            let _ = self.delete(&index_key, root_page_id);
        }

        let mut current_root_id = root_page_id;

        for (table_name, table) in &db.tables {
            let schema_key = Value::Text(format!("schema:{}", table_name));
            let schema_json = serde_json::to_string(&table.columns).map_err(|e| {
                format!("Failed to serialize schema for table {}: {}", table_name, e)
            })?;
            let data_pointer = self.write_data_to_pointer(&schema_json)?;
            current_root_id = self.insert(schema_key, data_pointer, current_root_id)?;
        }

        for (table_name, table) in &db.tables {
            for (row_index, row) in table.rows.iter().enumerate() {
                let row_key = Value::Text(format!("row:{}:{}", table_name, row_index));
                let row_json = serde_json::to_string(row).map_err(|e| {
                    format!(
                        "Failed to serialize row {} for table {}: {}",
                        row_index, table_name, e
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

        let mut meta_page = self.read_page(0)?;
        if let Some(root_entry) = meta_page.entries.iter_mut().find(|e| {
            if let Value::Text(ref s) = e.key {
                s == "root"
            } else {
                false
            }
        }) {
            root_entry.pointer = current_root_id;
        } else {
            meta_page.entries.push(BTreeEntry::new(
                Value::Text("root".to_string()),
                current_root_id,
            ));
        }
        meta_page.header.entry_count = meta_page.entries.len() as u16;
        self.write_page(&meta_page)?;

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
            Value::Integer(_) => 9,
            Value::Float(_) => 9,
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

    fn get_next_page_id(&mut self) -> Result<u64, String> {
        let meta_page = self.read_page(0)?;

        for entry in &meta_page.entries {
            if let Value::Text(ref s) = entry.key {
                if s == "next_page_id" {
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

    pub fn search(
        &mut self,
        key: &Value,
        root_page_id: u64,
    ) -> Result<Option<(u64, usize)>, String> {
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
                    return Err("Cannot search in meta page".to_string());
                }
            }
        }
    }

    fn find_leaf_for_insert(&mut self, key: &Value, root_page_id: u64) -> Result<u64, String> {
        let mut current_page_id = root_page_id;

        loop {
            let page = self.read_page(current_page_id)?;

            match page.header.kind {
                PageKind::Leaf => {
                    return Ok(current_page_id);
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
                    return Err("Cannot traverse meta page for insert".to_string());
                }
            }
        }
    }

    pub fn insert(
        &mut self,
        key: Value,
        data_pointer: u64,
        root_page_id: u64,
    ) -> Result<u64, String> {
        let leaf_page_id = self.find_leaf_for_insert(&key, root_page_id)?;
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
            return Ok(root_page_id);
        } else {
            return self.split_and_insert(leaf_page, new_entry, root_page_id);
        }
    }

    fn split_and_insert(
        &mut self,
        mut page: BTreePage,
        new_entry: BTreeEntry,
        root_page_id: u64,
    ) -> Result<u64, String> {
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
            new_root.entries.push(BTreeEntry::new(
                page.entries[0].key.clone(),
                page.header.page_id,
            ));
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
            }
            self.write_page(&meta_page)?;

            return Ok(new_root_id);
        }

        Ok(root_page_id)
    }

    pub fn delete(&mut self, key: &Value, root_page_id: u64) -> Result<bool, String> {
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

    pub fn range_scan(
        &mut self,
        start_key: Option<&Value>,
        end_key: Option<&Value>,
        root_page_id: u64,
    ) -> Result<Vec<(Value, u64)>, String> {
        let mut results = Vec::new();

        let current_page_id = self.find_starting_leaf(start_key, root_page_id)?;

        loop {
            let page = self.read_page(current_page_id)?;

            match page.header.kind {
                PageKind::Leaf => {
                    for entry in &page.entries {
                        let in_range = match (start_key, end_key) {
                            (Some(start), Some(end)) => entry.key >= *start && entry.key <= *end,
                            (Some(start), None) => entry.key >= *start,
                            (None, Some(end)) => entry.key <= *end,
                            (None, None) => true,
                        };

                        if in_range {
                            results.push((entry.key.clone(), entry.pointer));
                        } else if end_key.is_some() && &entry.key > end_key.unwrap() {
                            return Ok(results);
                        }
                    }

                    break;
                }
                _ => {
                    return Err("Expected leaf page in range scan".to_string());
                }
            }
        }

        Ok(results)
    }

    fn find_starting_leaf(
        &mut self,
        start_key: Option<&Value>,
        root_page_id: u64,
    ) -> Result<u64, String> {
        let mut current_page_id = root_page_id;

        loop {
            let page = self.read_page(current_page_id)?;

            match page.header.kind {
                PageKind::Leaf => {
                    return Ok(current_page_id);
                }
                PageKind::Internal => {
                    let mut next_page_id = None;
                    if let Some(start) = start_key {
                        for entry in &page.entries {
                            if &entry.key > start {
                                next_page_id = Some(entry.pointer);
                                break;
                            }
                        }
                    }
                    current_page_id = next_page_id
                        .or_else(|| page.entries.first().map(|e| e.pointer))
                        .ok_or_else(|| "Invalid internal page structure".to_string())?;
                }
                PageKind::Meta => {
                    return Err("Cannot find leaf in meta page".to_string());
                }
            }
        }
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

    #[test]
    fn btree_search_insert_delete() {
        let temp_path = std::env::temp_dir().join("rustql_btree_ops_test.dat");
        let _ = std::fs::remove_file(&temp_path);

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

        let _ = std::fs::remove_file(&temp_path);
    }

    #[test]
    fn btree_range_scan() {
        let temp_path = std::env::temp_dir().join("rustql_btree_range_test.dat");
        let _ = std::fs::remove_file(&temp_path);

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
                .expect(&format!("Failed to write data for key {}", i));
            file.insert(key, data_pointer, root_id)
                .expect(&format!("Failed to insert key {}", i));
        }

        let start = Value::Integer(30);
        let end = Value::Integer(70);
        let results = file
            .range_scan(Some(&start), Some(&end), root_id)
            .expect("Failed to range scan");

        assert!(results.len() >= 5, "Range scan should find multiple keys");

        let _ = std::fs::remove_file(&temp_path);
    }
}
