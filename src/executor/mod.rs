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
use crate::wal;
use std::sync::{OnceLock, RwLock};

static DATABASE: OnceLock<RwLock<Database>> = OnceLock::new();

fn get_database() -> &'static RwLock<Database> {
    #[cfg(test)]
    {
        DATABASE.get_or_init(|| RwLock::new(Database::new()))
    }
    #[cfg(not(test))]
    {
        DATABASE.get_or_init(|| RwLock::new(Database::load()))
    }
}

fn get_database_read() -> std::sync::RwLockReadGuard<'static, Database> {
    get_database().read().unwrap()
}

fn get_database_write() -> std::sync::RwLockWriteGuard<'static, Database> {
    get_database().write().unwrap()
}

pub fn get_database_for_testing() -> Database {
    (*get_database_read()).clone()
}

pub(crate) struct SelectResult {
    pub(crate) headers: Vec<String>,
    pub(crate) rows: Vec<Vec<Value>>,
}

pub fn execute(statement: Statement) -> Result<String, RustqlError> {
    match statement {
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
    }
}

pub fn reset_database_state() {
    let mut db = get_database_write();
    db.tables.clear();
    db.indexes.clear();
    db.views.clear();
    wal::reset_wal_state();
}

fn save_if_not_in_transaction(db: &Database) -> Result<(), RustqlError> {
    if !wal::is_in_transaction() {
        db.save()?;
    }
    Ok(())
}

fn execute_begin_transaction() -> Result<String, RustqlError> {
    wal::begin_transaction()?;
    Ok("Transaction begun".to_string())
}

fn execute_commit_transaction() -> Result<String, RustqlError> {
    wal::commit_transaction()?;
    let db = get_database_read();
    db.save()?;
    Ok("Transaction committed".to_string())
}

fn execute_rollback_transaction() -> Result<String, RustqlError> {
    let mut db = get_database_write();
    wal::rollback_transaction(&mut db)?;
    Ok("Transaction rolled back".to_string())
}

fn execute_explain(stmt: SelectStatement) -> Result<String, RustqlError> {
    let db = get_database_read();
    planner::explain_query(&db, &stmt)
}

fn execute_savepoint(name: String) -> Result<String, RustqlError> {
    wal::savepoint(&name)?;
    Ok(format!("Savepoint '{}' created", name))
}

fn execute_release_savepoint(name: String) -> Result<String, RustqlError> {
    wal::release_savepoint(&name)?;
    Ok(format!("Savepoint '{}' released", name))
}

fn execute_rollback_to_savepoint(name: String) -> Result<String, RustqlError> {
    let mut db = get_database_write();
    wal::rollback_to_savepoint(&name, &mut db)?;
    Ok(format!("Rolled back to savepoint '{}'", name))
}

pub use expr::format_value;
