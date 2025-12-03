use crate::database::Database;
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



