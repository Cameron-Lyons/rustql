pub mod aggregate;
pub mod ddl;
pub mod dml;
pub mod expr;
pub mod join;
pub mod select;

use crate::ast::*;
use crate::database::Database;
use crate::error::RustqlError;
use crate::planner;
use crate::wal::{self, WalState};
use std::cell::RefCell;
use std::sync::{Mutex, RwLock};

thread_local! {
    static CONTEXT_STACK: RefCell<Vec<*const ExecutionContext>> = const { RefCell::new(Vec::new()) };
}

pub struct ExecutionContext {
    database: RwLock<Database>,
    wal_state: Mutex<WalState>,
}

impl ExecutionContext {
    pub fn new(database: Database) -> Self {
        Self {
            database: RwLock::new(database),
            wal_state: Mutex::new(WalState::default()),
        }
    }

    pub fn database_snapshot(&self) -> Database {
        self.database.read().unwrap().clone()
    }
}

pub struct ContextBinding;

impl Drop for ContextBinding {
    fn drop(&mut self) {
        CONTEXT_STACK.with(|stack| {
            let _ = stack.borrow_mut().pop();
        });
    }
}

pub fn bind_context(context: &ExecutionContext) -> ContextBinding {
    CONTEXT_STACK.with(|stack| {
        stack.borrow_mut().push(context as *const ExecutionContext);
    });
    ContextBinding
}

fn current_context() -> &'static ExecutionContext {
    let ptr = CONTEXT_STACK.with(|stack| stack.borrow().last().copied());

    match ptr {
        Some(ptr) => {
            // The context pointer is bound to the current thread for the duration of execution.
            unsafe { &*ptr }
        }
        None => panic!("Executor context is not bound"),
    }
}

fn has_bound_context() -> bool {
    CONTEXT_STACK.with(|stack| !stack.borrow().is_empty())
}

fn get_database_read() -> std::sync::RwLockReadGuard<'static, Database> {
    current_context().database.read().unwrap()
}

fn get_database_write() -> std::sync::RwLockWriteGuard<'static, Database> {
    current_context().database.write().unwrap()
}

pub fn get_database_for_testing() -> Database {
    crate::engine::default_engine().snapshot_database()
}

fn with_wal_state_mut<F, R>(f: F) -> R
where
    F: FnOnce(&mut WalState) -> R,
{
    let context = current_context();
    let mut wal_state = context.wal_state.lock().unwrap();
    f(&mut wal_state)
}

fn with_wal_state<F, R>(f: F) -> R
where
    F: FnOnce(&WalState) -> R,
{
    let context = current_context();
    let wal_state = context.wal_state.lock().unwrap();
    f(&wal_state)
}

pub(crate) fn record_wal_entry(entry: wal::WalEntry) {
    with_wal_state_mut(|state| {
        state.record_wal_entry(entry);
    });
}

pub(crate) struct SelectResult {
    pub(crate) headers: Vec<String>,
    pub(crate) rows: Vec<Vec<Value>>,
}

pub fn execute(statement: Statement) -> Result<String, RustqlError> {
    let default_binding = if has_bound_context() {
        None
    } else {
        Some(crate::engine::bind_default_context())
    };

    let result = match statement {
        Statement::CreateTable(stmt) => ddl::execute_create_table(stmt),
        Statement::DropTable(stmt) => ddl::execute_drop_table(stmt),
        Statement::Insert(stmt) => dml::execute_insert(stmt),
        Statement::Select(stmt) => select::execute_select(stmt),
        Statement::Update(stmt) => dml::execute_update(stmt),
        Statement::Delete(stmt) => dml::execute_delete(stmt),
        Statement::AlterTable(stmt) => ddl::execute_alter_table(stmt),
        Statement::CreateIndex(stmt) => ddl::execute_create_index(stmt),
        Statement::DropIndex(stmt) => ddl::execute_drop_index(stmt),
        Statement::BeginTransaction => execute_begin_transaction(),
        Statement::CommitTransaction => execute_commit_transaction(),
        Statement::RollbackTransaction => execute_rollback_transaction(),
        Statement::Explain(stmt) => execute_explain(stmt),
        Statement::Describe(table_name) => ddl::execute_describe(table_name),
        Statement::ShowTables => ddl::execute_show_tables(),
        Statement::Savepoint(name) => execute_savepoint(name),
        Statement::ReleaseSavepoint(name) => execute_release_savepoint(name),
        Statement::RollbackToSavepoint(name) => execute_rollback_to_savepoint(name),
        Statement::Analyze(table_name) => ddl::execute_analyze(table_name),
        Statement::TruncateTable { table_name } => ddl::execute_truncate_table(table_name),
        Statement::CreateView { name, query_sql } => ddl::execute_create_view(name, query_sql),
        Statement::DropView { name, if_exists } => ddl::execute_drop_view(name, if_exists),
        Statement::Merge(stmt) => dml::execute_merge(stmt),
        Statement::Do { statements } => {
            let mut results = Vec::new();
            for s in statements {
                results.push(execute(s)?);
            }
            Ok(results.join("\n"))
        }
    };

    drop(default_binding);
    result
}

pub fn reset_database_state() {
    let default_binding = if has_bound_context() {
        None
    } else {
        Some(crate::engine::bind_default_context())
    };

    {
        let mut db = get_database_write();
        db.tables.clear();
        db.indexes.clear();
        db.views.clear();
    }

    with_wal_state_mut(|state| state.reset());

    drop(default_binding);
}

fn save_if_not_in_transaction(db: &Database) -> Result<(), RustqlError> {
    if !with_wal_state(|state| state.is_in_transaction()) {
        db.save()?;
    }
    Ok(())
}

fn execute_begin_transaction() -> Result<String, RustqlError> {
    with_wal_state_mut(|state| state.begin_transaction())?;
    Ok("Transaction begun".to_string())
}

fn execute_commit_transaction() -> Result<String, RustqlError> {
    with_wal_state_mut(|state| state.commit_transaction())?;
    let db = get_database_read();
    db.save()?;
    Ok("Transaction committed".to_string())
}

fn execute_rollback_transaction() -> Result<String, RustqlError> {
    let mut db = get_database_write();
    with_wal_state_mut(|state| state.rollback_transaction(&mut db))?;
    Ok("Transaction rolled back".to_string())
}

fn execute_explain(stmt: SelectStatement) -> Result<String, RustqlError> {
    let db = get_database_read();
    planner::explain_query(&db, &stmt)
}

fn execute_savepoint(name: String) -> Result<String, RustqlError> {
    with_wal_state_mut(|state| state.savepoint(&name))?;
    Ok(format!("Savepoint '{}' created", name))
}

fn execute_release_savepoint(name: String) -> Result<String, RustqlError> {
    with_wal_state_mut(|state| state.release_savepoint(&name))?;
    Ok(format!("Savepoint '{}' released", name))
}

fn execute_rollback_to_savepoint(name: String) -> Result<String, RustqlError> {
    let mut db = get_database_write();
    with_wal_state_mut(|state| state.rollback_to_savepoint(&name, &mut db))?;
    Ok(format!("Rolled back to savepoint '{}'", name))
}

pub use expr::format_value;
