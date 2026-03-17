pub mod aggregate;
pub mod ddl;
pub mod dml;
pub mod expr;
pub mod join;
pub mod select;

use crate::ast::*;
use crate::database::Database;
use crate::engine::{DatabaseReadGuard, DatabaseWriteGuard, ExecutionContext, default_engine};
use crate::error::RustqlError;
use crate::plan_executor::PlanExecutor;
use crate::planner;
use crate::planner::QueryPlanner;
use crate::wal;
use std::time::Instant;

fn get_database_read(ctx: &ExecutionContext) -> DatabaseReadGuard {
    ctx.read_database()
}

fn get_database_write(ctx: &ExecutionContext) -> DatabaseWriteGuard {
    ctx.write_database()
}

pub fn get_database_for_testing() -> Database {
    default_engine().database_snapshot()
}

pub(crate) struct SelectResult {
    pub(crate) headers: Vec<String>,
    pub(crate) rows: Vec<Vec<Value>>,
}

pub fn execute(statement: Statement) -> Result<String, RustqlError> {
    let ctx = default_engine().execution_context();
    execute_with_context(statement, &ctx)
}

pub(crate) fn execute_with_context(
    statement: Statement,
    ctx: &ExecutionContext,
) -> Result<String, RustqlError> {
    match statement {
        Statement::CreateTable(stmt) => ddl::execute_create_table(stmt, ctx),
        Statement::DropTable(stmt) => ddl::execute_drop_table(stmt, ctx),
        Statement::Insert(stmt) => dml::execute_insert(stmt, ctx),
        Statement::Select(stmt) => select::execute_select(stmt, ctx),
        Statement::Update(stmt) => dml::execute_update(stmt, ctx),
        Statement::Delete(stmt) => dml::execute_delete(stmt, ctx),
        Statement::AlterTable(stmt) => ddl::execute_alter_table(stmt, ctx),
        Statement::CreateIndex(stmt) => ddl::execute_create_index(stmt, ctx),
        Statement::DropIndex(stmt) => ddl::execute_drop_index(stmt, ctx),
        Statement::BeginTransaction => execute_begin_transaction(ctx),
        Statement::CommitTransaction => execute_commit_transaction(ctx),
        Statement::RollbackTransaction => execute_rollback_transaction(ctx),
        Statement::Explain(stmt) => execute_explain(stmt, ctx),
        Statement::ExplainAnalyze(stmt) => execute_explain_analyze(stmt, ctx),
        Statement::Describe(table_name) => ddl::execute_describe(table_name, ctx),
        Statement::ShowTables => ddl::execute_show_tables(ctx),
        Statement::Savepoint(name) => execute_savepoint(name, ctx),
        Statement::ReleaseSavepoint(name) => execute_release_savepoint(name, ctx),
        Statement::RollbackToSavepoint(name) => execute_rollback_to_savepoint(name, ctx),
        Statement::Analyze(table_name) => ddl::execute_analyze(table_name, ctx),
        Statement::TruncateTable { table_name } => ddl::execute_truncate_table(table_name, ctx),
        Statement::CreateView { name, query_sql } => ddl::execute_create_view(name, query_sql, ctx),
        Statement::DropView { name, if_exists } => ddl::execute_drop_view(name, if_exists, ctx),
        Statement::Merge(stmt) => dml::execute_merge(stmt, ctx),
        Statement::Do { statements } => {
            let mut results = Vec::new();
            for s in statements {
                results.push(execute_with_context(s, ctx)?);
            }
            Ok(results.join("\n"))
        }
    }
}

pub fn reset_database_state() {
    default_engine().reset_state();
}

fn save_if_not_in_transaction(ctx: &ExecutionContext, db: &Database) -> Result<(), RustqlError> {
    if !wal::is_in_transaction(ctx) {
        ctx.save_database(db)?;
    }
    Ok(())
}

fn execute_begin_transaction(ctx: &ExecutionContext) -> Result<String, RustqlError> {
    wal::begin_transaction(ctx)?;
    Ok("Transaction begun".to_string())
}

fn execute_commit_transaction(ctx: &ExecutionContext) -> Result<String, RustqlError> {
    wal::commit_transaction(ctx)?;
    let db = get_database_read(ctx);
    ctx.save_database(&db)?;
    Ok("Transaction committed".to_string())
}

fn execute_rollback_transaction(ctx: &ExecutionContext) -> Result<String, RustqlError> {
    let mut db = get_database_write(ctx);
    wal::rollback_transaction(ctx, &mut db)?;
    Ok("Transaction rolled back".to_string())
}

fn execute_explain(stmt: SelectStatement, ctx: &ExecutionContext) -> Result<String, RustqlError> {
    let db = get_database_read(ctx);
    planner::explain_query(&db, &stmt)
}

fn execute_explain_analyze(
    stmt: SelectStatement,
    ctx: &ExecutionContext,
) -> Result<String, RustqlError> {
    let db = get_database_read(ctx);
    let planner = QueryPlanner::new(&db);

    let planning_start = Instant::now();
    let plan = planner.plan_select(&stmt)?;
    let planning_ms = planning_start.elapsed().as_secs_f64() * 1000.0;

    let executor = PlanExecutor::new(&db);
    let execution_start = Instant::now();
    let result = executor.execute(&plan, &stmt)?;
    let execution_ms = execution_start.elapsed().as_secs_f64() * 1000.0;

    Ok(format!(
        "Query Plan:\n{}\nPlanning Time: {:.3} ms\nExecution Time: {:.3} ms\nActual Rows: {}",
        plan,
        planning_ms,
        execution_ms,
        result.rows.len()
    ))
}

fn execute_savepoint(name: String, ctx: &ExecutionContext) -> Result<String, RustqlError> {
    wal::savepoint(ctx, &name)?;
    Ok(format!("Savepoint '{}' created", name))
}

fn execute_release_savepoint(name: String, ctx: &ExecutionContext) -> Result<String, RustqlError> {
    wal::release_savepoint(ctx, &name)?;
    Ok(format!("Savepoint '{}' released", name))
}

fn execute_rollback_to_savepoint(
    name: String,
    ctx: &ExecutionContext,
) -> Result<String, RustqlError> {
    let mut db = get_database_write(ctx);
    wal::rollback_to_savepoint(ctx, &name, &mut db)?;
    Ok(format!("Rolled back to savepoint '{}'", name))
}

pub use expr::format_value;

pub fn reload_database_from_storage_for_testing() {
    default_engine().reload_from_storage();
}
