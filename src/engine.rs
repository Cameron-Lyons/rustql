use crate::ast::Statement;
use crate::database::Database;
use crate::error::RustqlError;
use crate::executor;
use crate::lexer;
use crate::parser;
use crate::storage::{
    self, BTreeStorageEngine, InMemoryStorageEngine, JsonStorageEngine, StorageEngine,
};
use crate::wal::WalLog;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub struct Engine {
    inner: Arc<EngineInner>,
}

pub struct EngineBuilder {
    storage: EngineStorage,
}

#[derive(Clone)]
pub(crate) struct ExecutionContext {
    engine: Arc<EngineInner>,
}

pub(crate) struct EngineInner {
    database: RwLock<Database>,
    wal: Mutex<Option<WalLog>>,
    storage: Box<dyn StorageEngine>,
}

pub(crate) struct DatabaseReadGuard {
    // This field must drop before `_engine` so the lock never outlives the Arc.
    guard: RwLockReadGuard<'static, Database>,
    _engine: Arc<EngineInner>,
}

pub(crate) struct DatabaseWriteGuard {
    // This field must drop before `_engine` so the lock never outlives the Arc.
    guard: RwLockWriteGuard<'static, Database>,
    _engine: Arc<EngineInner>,
}

pub(crate) struct WalGuard {
    // This field must drop before `_engine` so the lock never outlives the Arc.
    guard: MutexGuard<'static, Option<WalLog>>,
    _engine: Arc<EngineInner>,
}

enum EngineStorage {
    InMemory,
    Default,
    Json(PathBuf),
    BTree(PathBuf),
    Custom(Box<dyn StorageEngine>),
}

static DEFAULT_ENGINE: OnceLock<Engine> = OnceLock::new();

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for EngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl Engine {
    pub fn builder() -> EngineBuilder {
        EngineBuilder::new()
    }

    pub fn new() -> Self {
        Self::builder().in_memory().build()
    }

    pub fn with_default_storage() -> Self {
        Self::builder().default_storage().build()
    }

    pub fn with_json_path<P: Into<PathBuf>>(path: P) -> Self {
        Self::builder().json_file(path).build()
    }

    pub fn with_btree_path<P: Into<PathBuf>>(path: P) -> Self {
        Self::builder().btree_file(path).build()
    }

    pub fn with_storage(storage: Box<dyn StorageEngine>) -> Self {
        let database = storage.load();
        Self {
            inner: Arc::new(EngineInner {
                database: RwLock::new(database),
                wal: Mutex::new(None),
                storage,
            }),
        }
    }

    pub fn process_query(&self, query: &str) -> Result<String, String> {
        let tokens = lexer::tokenize(query).map_err(|e| e.to_string())?;
        let statement = parser::parse(tokens).map_err(|e| e.to_string())?;
        self.execute(statement).map_err(|e| e.to_string())
    }

    pub fn execute(&self, statement: Statement) -> Result<String, RustqlError> {
        let ctx = self.execution_context();
        executor::execute_with_context(statement, &ctx)
    }

    pub fn reset_state(&self) {
        let mut db = self.inner.database.write().unwrap();
        db.tables.clear();
        db.indexes.clear();
        db.views.clear();
        db.composite_indexes.clear();
        drop(db);
        self.inner.reset_wal();
    }

    pub fn reload_from_storage(&self) {
        let loaded = self.inner.storage.load();
        let mut db = self.inner.database.write().unwrap();
        *db = loaded;
        drop(db);
        self.inner.reset_wal();
    }

    pub fn database_snapshot(&self) -> Database {
        self.inner.database.read().unwrap().clone()
    }

    pub(crate) fn execution_context(&self) -> ExecutionContext {
        ExecutionContext {
            engine: self.inner.clone(),
        }
    }
}

impl EngineBuilder {
    pub fn new() -> Self {
        Self {
            storage: EngineStorage::InMemory,
        }
    }

    pub fn in_memory(mut self) -> Self {
        self.storage = EngineStorage::InMemory;
        self
    }

    pub fn default_storage(mut self) -> Self {
        self.storage = EngineStorage::Default;
        self
    }

    pub fn json_file<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.storage = EngineStorage::Json(path.into());
        self
    }

    pub fn btree_file<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.storage = EngineStorage::BTree(path.into());
        self
    }

    pub fn custom_storage(mut self, storage: Box<dyn StorageEngine>) -> Self {
        self.storage = EngineStorage::Custom(storage);
        self
    }

    pub fn build(self) -> Engine {
        let storage: Box<dyn StorageEngine> = match self.storage {
            EngineStorage::InMemory => Box::new(InMemoryStorageEngine::new()),
            EngineStorage::Default => storage::default_storage_engine(),
            EngineStorage::Json(path) => Box::new(JsonStorageEngine::new(path)),
            EngineStorage::BTree(path) => Box::new(BTreeStorageEngine::new(path)),
            EngineStorage::Custom(storage) => storage,
        };
        Engine::with_storage(storage)
    }
}

impl EngineInner {
    pub(crate) fn read_database(self: &Arc<Self>) -> DatabaseReadGuard {
        let guard = self.database.read().unwrap();
        let guard = unsafe {
            // Safety: DatabaseReadGuard stores an Arc clone of `self`, and `guard` is dropped
            // before that Arc, so the RwLock outlives the guard.
            std::mem::transmute::<RwLockReadGuard<'_, Database>, RwLockReadGuard<'static, Database>>(
                guard,
            )
        };
        DatabaseReadGuard {
            guard,
            _engine: self.clone(),
        }
    }

    pub(crate) fn write_database(self: &Arc<Self>) -> DatabaseWriteGuard {
        let guard = self.database.write().unwrap();
        let guard = unsafe {
            // Safety: DatabaseWriteGuard stores an Arc clone of `self`, and `guard` is dropped
            // before that Arc, so the RwLock outlives the guard.
            std::mem::transmute::<RwLockWriteGuard<'_, Database>, RwLockWriteGuard<'static, Database>>(
                guard,
            )
        };
        DatabaseWriteGuard {
            guard,
            _engine: self.clone(),
        }
    }

    pub(crate) fn lock_wal(self: &Arc<Self>) -> WalGuard {
        let guard = self.wal.lock().unwrap();
        let guard = unsafe {
            // Safety: WalGuard stores an Arc clone of `self`, and `guard` is dropped before
            // that Arc, so the Mutex outlives the guard.
            std::mem::transmute::<MutexGuard<'_, Option<WalLog>>, MutexGuard<'static, Option<WalLog>>>(
                guard,
            )
        };
        WalGuard {
            guard,
            _engine: self.clone(),
        }
    }

    pub(crate) fn save_database(&self, db: &Database) -> Result<(), RustqlError> {
        self.storage.save(db)
    }

    pub(crate) fn reset_wal(&self) {
        let mut wal = self.wal.lock().unwrap();
        *wal = None;
    }
}

impl ExecutionContext {
    pub(crate) fn read_database(&self) -> DatabaseReadGuard {
        self.engine.read_database()
    }

    pub(crate) fn write_database(&self) -> DatabaseWriteGuard {
        self.engine.write_database()
    }

    pub(crate) fn lock_wal(&self) -> WalGuard {
        self.engine.lock_wal()
    }

    pub(crate) fn save_database(&self, db: &Database) -> Result<(), RustqlError> {
        self.engine.save_database(db)
    }
}

impl Deref for DatabaseReadGuard {
    type Target = Database;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl Deref for DatabaseWriteGuard {
    type Target = Database;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl DerefMut for DatabaseWriteGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl Deref for WalGuard {
    type Target = Option<WalLog>;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl DerefMut for WalGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

pub fn default_engine() -> &'static Engine {
    DEFAULT_ENGINE.get_or_init(Engine::with_default_storage)
}
