use crate::ast::{DataType, Statement, Value};
use crate::database::Database;
use crate::error::RustqlError;
use crate::storage::StorageEngine;
use crate::{executor, lexer, parser};
use std::path::PathBuf;
use std::sync::Arc;

const ENV_STORAGE_KIND: &str = "RUSTQL_STORAGE";
const ENV_STORAGE_PATH: &str = "RUSTQL_STORAGE_PATH";
const DEFAULT_JSON_PATH: &str = "rustql_data.json";
const DEFAULT_BTREE_PATH: &str = "rustql_btree.dat";

pub type Row = Vec<Value>;
pub type PlanTree = crate::planner::PlanNode;

#[derive(Debug, Clone)]
pub enum StorageMode {
    Memory,
    Json {
        path: PathBuf,
    },
    BTree {
        path: PathBuf,
    },
    #[deprecated(note = "Use StorageMode::BTree instead")]
    Disk {
        path: PathBuf,
    },
}

#[derive(Debug, Clone)]
pub struct EngineOptions {
    pub storage: StorageMode,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            storage: StorageMode::Json {
                path: PathBuf::from(DEFAULT_JSON_PATH),
            },
        }
    }
}

impl EngineOptions {
    pub fn memory() -> Self {
        Self {
            storage: StorageMode::Memory,
        }
    }

    pub fn json(path: impl Into<PathBuf>) -> Self {
        Self {
            storage: StorageMode::Json { path: path.into() },
        }
    }

    pub fn btree(path: impl Into<PathBuf>) -> Self {
        Self {
            storage: StorageMode::BTree { path: path.into() },
        }
    }

    pub fn from_env() -> Result<Self, RustqlError> {
        let storage = match std::env::var(ENV_STORAGE_KIND) {
            Ok(value) if value.eq_ignore_ascii_case("btree") => Ok(Self {
                storage: StorageMode::BTree {
                    path: storage_path_from_env(DEFAULT_BTREE_PATH)?,
                },
            }),
            Ok(value) if value.eq_ignore_ascii_case("json") => Ok(Self {
                storage: StorageMode::Json {
                    path: storage_path_from_env(DEFAULT_JSON_PATH)?,
                },
            }),
            Ok(value) => Err(RustqlError::StorageError(format!(
                "Unsupported RUSTQL_STORAGE value '{}'. Expected 'json' or 'btree'",
                value
            ))),
            Err(std::env::VarError::NotPresent) => Ok(Self {
                storage: StorageMode::Json {
                    path: storage_path_from_env(DEFAULT_JSON_PATH)?,
                },
            }),
            Err(err) => Err(RustqlError::StorageError(format!(
                "Failed to read RUSTQL_STORAGE: {}",
                err
            ))),
        }?;

        Ok(storage)
    }
}

fn storage_path_from_env(default_path: &str) -> Result<PathBuf, RustqlError> {
    match std::env::var_os(ENV_STORAGE_PATH) {
        Some(path) if path.is_empty() => Err(RustqlError::StorageError(format!(
            "{} cannot be empty",
            ENV_STORAGE_PATH
        ))),
        Some(path) => Ok(PathBuf::from(path)),
        None => Ok(PathBuf::from(default_path)),
    }
}

pub struct Engine {
    context: executor::ExecutionContext,
}

impl Engine {
    pub fn open_default() -> Result<Self, RustqlError> {
        Self::open(EngineOptions::default())
    }

    pub fn from_env() -> Result<Self, RustqlError> {
        Self::open(EngineOptions::from_env()?)
    }

    pub fn in_memory() -> Result<Self, RustqlError> {
        Self::open(EngineOptions::memory())
    }

    #[allow(deprecated)]
    pub fn open(options: EngineOptions) -> Result<Self, RustqlError> {
        let (database, storage) = match &options.storage {
            StorageMode::Memory => (Database::new(), None),
            StorageMode::Json { path } => {
                let storage = Arc::new(crate::storage::JsonStorageEngine::new(path.clone()));
                let database = storage.load()?;
                (
                    database,
                    Some(storage as Arc<dyn crate::storage::StorageEngine>),
                )
            }
            StorageMode::BTree { path } | StorageMode::Disk { path } => {
                let storage = Arc::new(crate::storage::BTreeStorageEngine::new(path.clone()));
                let database = storage.load()?;
                (
                    database,
                    Some(storage as Arc<dyn crate::storage::StorageEngine>),
                )
            }
        };

        Ok(Self {
            context: executor::ExecutionContext::new(database, storage),
        })
    }

    pub fn session(&self) -> Session<'_> {
        Session { engine: self }
    }

    pub fn snapshot_database(&self) -> Database {
        self.context.database_snapshot()
    }
}

pub struct Session<'e> {
    engine: &'e Engine,
}

impl Session<'_> {
    pub fn execute(&mut self, sql: &str) -> Result<Vec<QueryResult>, RustqlError> {
        self.execute_script(sql)
    }

    pub fn execute_script(&mut self, sql: &str) -> Result<Vec<QueryResult>, RustqlError> {
        if sql.trim().is_empty() {
            return Ok(Vec::new());
        }

        let tokens = lexer::tokenize_spanned(sql)?;
        let statements = parser::parse_script_spanned(tokens)?;
        let mut results = Vec::with_capacity(statements.len());
        for statement in statements {
            results.push(self.execute_statement(statement)?);
        }
        Ok(results)
    }

    pub fn execute_one(&mut self, sql: &str) -> Result<QueryResult, RustqlError> {
        let tokens = lexer::tokenize_spanned(sql)?;
        let statement = parser::parse_spanned(tokens)?;
        self.execute_statement(statement)
    }

    pub fn execute_statement(&mut self, statement: Statement) -> Result<QueryResult, RustqlError> {
        executor::execute(&self.engine.context, statement)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnMeta {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RowBatch {
    pub columns: Vec<ColumnMeta>,
    pub rows: Vec<Row>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandTag {
    CreateTable,
    DropTable,
    AlterTable,
    CreateIndex,
    DropIndex,
    Insert,
    Update,
    Delete,
    BeginTransaction,
    CommitTransaction,
    RollbackTransaction,
    Savepoint,
    ReleaseSavepoint,
    RollbackToSavepoint,
    Analyze,
    TruncateTable,
    CreateView,
    DropView,
    Merge,
    Do,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandResult {
    pub tag: CommandTag,
    pub affected: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExplainAnalyzeResult {
    pub plan: PlanTree,
    pub planning_ms: f64,
    pub execution_ms: f64,
    pub actual_rows: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueryResult {
    Rows(RowBatch),
    Command(CommandResult),
    Explain(PlanTree),
    ExplainAnalyze(ExplainAnalyzeResult),
}
