pub(crate) mod aggregate;
pub(crate) mod ddl;
pub(crate) mod dml;
pub(crate) mod expr;
pub(crate) mod select;

use crate::ast::*;
use crate::database::Database;
use crate::engine::{
    ColumnMeta, CommandResult, CommandTag, ExplainAnalyzeResult, QueryResult, RowBatch,
    plan_tree_from_node,
};
use crate::error::RustqlError;
use crate::plan_executor::PlanExecutor;
use crate::planner::QueryPlanner;
use crate::storage::StorageEngine;
use crate::wal::{self, WalState};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

/// Per-engine execution state.
///
/// The WAL state lives here because an `Engine` is the connection and
/// transaction boundary. Public `Session` values are borrowed execution
/// handles over this shared context.
pub(crate) struct ExecutionContext {
    database: RwLock<Database>,
    wal_state: Mutex<WalState>,
    statement_lock: Mutex<()>,
    storage: Option<Arc<dyn StorageEngine>>,
}

impl ExecutionContext {
    pub(crate) fn new(database: Database, storage: Option<Arc<dyn StorageEngine>>) -> Self {
        Self {
            database: RwLock::new(database),
            wal_state: Mutex::new(WalState::default()),
            statement_lock: Mutex::new(()),
            storage,
        }
    }

    #[cfg_attr(not(feature = "testing-api"), allow(dead_code))]
    pub(crate) fn database_snapshot(&self) -> Database {
        self.database
            .read()
            .unwrap_or_else(|err| err.into_inner())
            .clone()
    }

    pub(crate) fn database_read(&self) -> std::sync::RwLockReadGuard<'_, Database> {
        self.database.read().unwrap_or_else(|err| err.into_inner())
    }

    pub(crate) fn database_write(&self) -> std::sync::RwLockWriteGuard<'_, Database> {
        self.database.write().unwrap_or_else(|err| err.into_inner())
    }

    fn statement_guard(&self) -> std::sync::MutexGuard<'_, ()> {
        self.statement_lock
            .lock()
            .unwrap_or_else(|err| err.into_inner())
    }

    fn persist_database(&self, db: &Database) -> Result<(), RustqlError> {
        match &self.storage {
            Some(storage) => storage.save(db),
            None => Ok(()),
        }
    }

    fn begin_transaction_persistence(&self) -> Result<(), RustqlError> {
        match &self.storage {
            Some(storage) => storage.begin_transaction(),
            None => Ok(()),
        }
    }

    fn prepare_commit_persistence(&self, db: &Database) -> Result<(), RustqlError> {
        match &self.storage {
            Some(storage) => storage.prepare_commit(db),
            None => Ok(()),
        }
    }

    fn clear_transaction_persistence(&self) -> Result<(), RustqlError> {
        match &self.storage {
            Some(storage) => storage.clear_transaction(),
            None => Ok(()),
        }
    }

    fn with_wal_state_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut WalState) -> R,
    {
        let mut wal_state = self.wal_state.lock().unwrap_or_else(|err| err.into_inner());
        f(&mut wal_state)
    }

    fn with_wal_state<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&WalState) -> R,
    {
        let wal_state = self.wal_state.lock().unwrap_or_else(|err| err.into_inner());
        f(&wal_state)
    }
}

pub(crate) fn get_database_read(
    context: &ExecutionContext,
) -> std::sync::RwLockReadGuard<'_, Database> {
    context.database_read()
}

pub(crate) fn get_database_write(
    context: &ExecutionContext,
) -> std::sync::RwLockWriteGuard<'_, Database> {
    context.database_write()
}

pub(crate) fn record_wal_entry(context: &ExecutionContext, entry: wal::WalEntry) {
    context.with_wal_state_mut(|state| {
        state.record_wal_entry(entry);
    });
}

#[derive(Debug, Clone)]
pub(crate) struct SelectResult {
    pub(crate) headers: Vec<String>,
    pub(crate) rows: Vec<Vec<Value>>,
}

pub(crate) fn infer_row_batch(headers: Vec<String>, rows: Vec<Vec<Value>>) -> RowBatch {
    let columns = headers
        .iter()
        .enumerate()
        .map(|(idx, name)| ColumnMeta {
            name: name.clone(),
            data_type: infer_data_type(&rows, idx),
            nullable: rows
                .iter()
                .any(|row| matches!(row.get(idx), None | Some(Value::Null))),
        })
        .collect();

    RowBatch { columns, rows }
}

pub(crate) fn rows_result(result: SelectResult) -> QueryResult {
    QueryResult::Rows(infer_row_batch(result.headers, result.rows))
}

pub(crate) fn command_result(tag: CommandTag, affected: u64) -> QueryResult {
    QueryResult::Command(CommandResult { tag, affected })
}

fn infer_data_type(rows: &[Vec<Value>], idx: usize) -> DataType {
    rows.iter()
        .filter_map(|row| row.get(idx))
        .find_map(|value| match value {
            Value::Null => None,
            Value::Integer(_) => Some(DataType::Integer),
            Value::Float(_) => Some(DataType::Float),
            Value::Text(_) => Some(DataType::Text),
            Value::Boolean(_) => Some(DataType::Boolean),
            Value::Date(_) => Some(DataType::Date),
            Value::Time(_) => Some(DataType::Time),
            Value::DateTime(_) => Some(DataType::DateTime),
        })
        .unwrap_or(DataType::Text)
}

pub(crate) fn execute(
    context: &ExecutionContext,
    statement: Statement,
) -> Result<QueryResult, RustqlError> {
    let _statement_guard = context.statement_guard();

    if requires_statement_savepoint(&statement) {
        return execute_atomic_statement(context, statement);
    }

    execute_statement_inner(context, statement)
}

fn execute_statement_inner(
    context: &ExecutionContext,
    statement: Statement,
) -> Result<QueryResult, RustqlError> {
    match statement {
        Statement::CreateTable(stmt) => ddl::execute_create_table(context, stmt),
        Statement::DropTable(stmt) => ddl::execute_drop_table(context, stmt),
        Statement::Insert(stmt) => dml::execute_insert(context, stmt),
        Statement::Select(stmt) => select::execute_select(context, stmt),
        Statement::Update(stmt) => dml::execute_update(context, stmt),
        Statement::Delete(stmt) => dml::execute_delete(context, stmt),
        Statement::AlterTable(stmt) => ddl::execute_alter_table(context, stmt),
        Statement::CreateIndex(stmt) => ddl::execute_create_index(context, stmt),
        Statement::DropIndex(stmt) => ddl::execute_drop_index(context, stmt),
        Statement::BeginTransaction => execute_begin_transaction(context),
        Statement::CommitTransaction => execute_commit_transaction(context),
        Statement::RollbackTransaction => execute_rollback_transaction(context),
        Statement::Explain(stmt) => execute_explain(context, stmt),
        Statement::ExplainAnalyze(stmt) => execute_explain_analyze(context, stmt),
        Statement::Describe(table_name) => ddl::execute_describe(context, table_name),
        Statement::ShowTables => ddl::execute_show_tables(context),
        Statement::Savepoint(name) => execute_savepoint(context, name),
        Statement::ReleaseSavepoint(name) => execute_release_savepoint(context, name),
        Statement::RollbackToSavepoint(name) => execute_rollback_to_savepoint(context, name),
        Statement::Analyze(table_name) => ddl::execute_analyze(context, table_name),
        Statement::TruncateTable { table_name } => ddl::execute_truncate_table(context, table_name),
        Statement::CreateView { name, query_sql } => {
            ddl::execute_create_view(context, name, query_sql)
        }
        Statement::DropView { name, if_exists } => ddl::execute_drop_view(context, name, if_exists),
        Statement::Merge(stmt) => dml::execute_merge(context, stmt),
        Statement::Do { statements } => {
            let mut affected = 0u64;
            for statement in statements {
                if let QueryResult::Command(result) = execute_statement_inner(context, statement)? {
                    affected += result.affected;
                }
            }
            Ok(command_result(CommandTag::Do, affected))
        }
    }
}

fn requires_statement_savepoint(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::CreateTable(_)
            | Statement::DropTable(_)
            | Statement::Insert(_)
            | Statement::Update(_)
            | Statement::Delete(_)
            | Statement::AlterTable(_)
            | Statement::CreateIndex(_)
            | Statement::DropIndex(_)
            | Statement::Analyze(_)
            | Statement::TruncateTable { .. }
            | Statement::CreateView { .. }
            | Statement::DropView { .. }
            | Statement::Merge(_)
            | Statement::Do { .. }
    )
}

fn execute_atomic_statement(
    context: &ExecutionContext,
    statement: Statement,
) -> Result<QueryResult, RustqlError> {
    let savepoint = context.with_wal_state_mut(|state| state.begin_statement())?;
    let result = execute_statement_inner(context, statement);

    match result {
        Ok(result) => {
            if savepoint.is_autocommit_statement() {
                let persist_result = {
                    let db = get_database_read(context);
                    context.persist_database(&db)
                };
                if let Err(err) = persist_result {
                    rollback_statement(context, savepoint)?;
                    return Err(err);
                }
            }

            context.with_wal_state_mut(|state| state.commit_statement(savepoint))?;
            Ok(result)
        }
        Err(err) => {
            rollback_statement(context, savepoint)?;
            Err(err)
        }
    }
}

fn rollback_statement(
    context: &ExecutionContext,
    savepoint: wal::StatementSavepoint,
) -> Result<(), RustqlError> {
    let mut db = get_database_write(context);
    context.with_wal_state_mut(|state| state.rollback_statement(savepoint, &mut db))
}

pub(crate) fn save_if_not_in_transaction(
    context: &ExecutionContext,
    db: &Database,
) -> Result<(), RustqlError> {
    if !context.with_wal_state(|state| state.has_active_log()) {
        context.persist_database(db)?;
    }
    Ok(())
}

fn execute_begin_transaction(context: &ExecutionContext) -> Result<QueryResult, RustqlError> {
    context.with_wal_state_mut(|state| state.begin_transaction())?;
    if let Err(err) = context.begin_transaction_persistence() {
        context.with_wal_state_mut(|state| state.reset());
        return Err(err);
    }
    Ok(command_result(CommandTag::BeginTransaction, 0))
}

fn execute_commit_transaction(context: &ExecutionContext) -> Result<QueryResult, RustqlError> {
    if !context.with_wal_state(|state| state.is_in_transaction()) {
        return Err(RustqlError::TransactionError(
            "No transaction in progress".to_string(),
        ));
    }

    let db = get_database_read(context);
    context.prepare_commit_persistence(&db)?;
    context.persist_database(&db)?;
    context.clear_transaction_persistence()?;
    drop(db);
    context.with_wal_state_mut(|state| state.commit_transaction())?;
    Ok(command_result(CommandTag::CommitTransaction, 0))
}

fn execute_rollback_transaction(context: &ExecutionContext) -> Result<QueryResult, RustqlError> {
    let mut db = get_database_write(context);
    context.with_wal_state_mut(|state| state.rollback_transaction(&mut db))?;
    drop(db);
    context.clear_transaction_persistence()?;
    Ok(command_result(CommandTag::RollbackTransaction, 0))
}

fn execute_explain(
    context: &ExecutionContext,
    stmt: SelectStatement,
) -> Result<QueryResult, RustqlError> {
    let plan = select::explain_select(context, stmt)?;
    Ok(QueryResult::Explain(plan_tree_from_node(plan)))
}

fn execute_explain_analyze(
    context: &ExecutionContext,
    stmt: SelectStatement,
) -> Result<QueryResult, RustqlError> {
    let db = get_database_read(context);
    let planner = QueryPlanner::new(&db);

    let planning_start = Instant::now();
    let plan = planner.plan_select(&stmt)?;
    let planning_ms = planning_start.elapsed().as_secs_f64() * 1000.0;

    let executor = PlanExecutor::new(&db);
    let execution_start = Instant::now();
    let result = executor.execute(&plan, &stmt)?;
    let execution_ms = execution_start.elapsed().as_secs_f64() * 1000.0;

    Ok(QueryResult::ExplainAnalyze(ExplainAnalyzeResult {
        plan: plan_tree_from_node(plan),
        planning_ms,
        execution_ms,
        actual_rows: result.rows.len(),
    }))
}

fn execute_savepoint(context: &ExecutionContext, name: String) -> Result<QueryResult, RustqlError> {
    context.with_wal_state_mut(|state| state.savepoint(&name))?;
    Ok(command_result(CommandTag::Savepoint, 0))
}

fn execute_release_savepoint(
    context: &ExecutionContext,
    name: String,
) -> Result<QueryResult, RustqlError> {
    context.with_wal_state_mut(|state| state.release_savepoint(&name))?;
    Ok(command_result(CommandTag::ReleaseSavepoint, 0))
}

fn execute_rollback_to_savepoint(
    context: &ExecutionContext,
    name: String,
) -> Result<QueryResult, RustqlError> {
    let mut db = get_database_write(context);
    context.with_wal_state_mut(|state| state.rollback_to_savepoint(&name, &mut db))?;
    Ok(command_result(CommandTag::RollbackToSavepoint, 0))
}
