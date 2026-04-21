use crate::ast::{DataType, Statement, Value};
use crate::database::Database;
use crate::error::RustqlError;
use crate::storage::StorageEngine;
use crate::{executor, lexer, parser};
use std::path::PathBuf;
use std::sync::Arc;

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
                path: PathBuf::from("rustql_data.json"),
            },
        }
    }
}

impl EngineOptions {
    pub fn from_env() -> Result<Self, RustqlError> {
        match std::env::var("RUSTQL_STORAGE") {
            Ok(value) if value.eq_ignore_ascii_case("btree") => Ok(Self {
                storage: StorageMode::BTree {
                    path: PathBuf::from("rustql_btree.dat"),
                },
            }),
            Ok(value) if value.eq_ignore_ascii_case("json") => Ok(Self::default()),
            Ok(value) => Err(RustqlError::StorageError(format!(
                "Unsupported RUSTQL_STORAGE value '{}'. Expected 'json' or 'btree'",
                value
            ))),
            Err(std::env::VarError::NotPresent) => Ok(Self::default()),
            Err(err) => Err(RustqlError::StorageError(format!(
                "Failed to read RUSTQL_STORAGE: {}",
                err
            ))),
        }
    }
}

pub struct Engine {
    context: executor::ExecutionContext,
}

impl Engine {
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
    pub fn execute_script(&mut self, sql: &str) -> Result<Vec<QueryResult>, RustqlError> {
        if sql.trim().is_empty() {
            return Ok(Vec::new());
        }

        let tokens = lexer::tokenize(sql)?;
        let statements = parser::parse_script(tokens)?;
        let mut results = Vec::with_capacity(statements.len());
        for statement in statements {
            results.push(self.execute_statement(statement)?);
        }
        Ok(results)
    }

    pub fn execute_one(&mut self, sql: &str) -> Result<QueryResult, RustqlError> {
        let tokens = lexer::tokenize(sql)?;
        let statement = parser::parse(tokens)?;
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
pub enum QueryResult {
    Rows(RowBatch),
    Command(CommandResult),
    Explain(PlanTree),
}
