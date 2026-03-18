use crate::ast::{Statement, Value};
use crate::database::Database;
use crate::{
    CommandResult, CommandTag, Engine, EngineOptions, ExplainAnalyzeResult, QueryResult, RowBatch,
    StorageMode,
};
use crate::{lexer, parser};
use std::sync::{Mutex, OnceLock};

struct TestHarness {
    engine: Engine,
}

impl TestHarness {
    fn new() -> Self {
        Self {
            engine: Engine::open(EngineOptions {
                storage: StorageMode::Memory,
            })
            .expect("failed to create test engine"),
        }
    }
}

fn harness() -> &'static Mutex<TestHarness> {
    static HARNESS: OnceLock<Mutex<TestHarness>> = OnceLock::new();
    HARNESS.get_or_init(|| Mutex::new(TestHarness::new()))
}

pub fn reset_database() {
    let mut harness = harness().lock().unwrap_or_else(|err| err.into_inner());
    *harness = TestHarness::new();
}

pub fn process_query(sql: &str) -> Result<String, String> {
    let harness = harness().lock().unwrap_or_else(|err| err.into_inner());
    let mut session = harness.engine.session();
    let statements = parser::parse_script(lexer::tokenize(sql).map_err(|err| err.to_string())?)
        .map_err(|err| err.to_string())?;
    let results = session.execute_script(sql).map_err(|err| err.to_string())?;
    Ok(statements
        .iter()
        .zip(results.iter())
        .map(|(statement, result)| render_result_for_statement(statement, result))
        .collect::<Vec<_>>()
        .join("\n"))
}

pub fn execute_sql(sql: &str) -> Result<QueryResult, String> {
    let harness = harness().lock().unwrap_or_else(|err| err.into_inner());
    let mut session = harness.engine.session();
    session.execute_one(sql).map_err(|err| err.to_string())
}

pub fn execute_script_results(sql: &str) -> Result<Vec<QueryResult>, String> {
    let harness = harness().lock().unwrap_or_else(|err| err.into_inner());
    let mut session = harness.engine.session();
    session.execute_script(sql).map_err(|err| err.to_string())
}

pub fn execute_statement(statement: Statement) -> Result<QueryResult, String> {
    let harness = harness().lock().unwrap_or_else(|err| err.into_inner());
    let mut session = harness.engine.session();
    session
        .execute_statement(statement)
        .map_err(|err| err.to_string())
}

pub fn execute(statement: Statement) -> Result<String, String> {
    let result = execute_statement(statement.clone())?;
    Ok(render_result_for_statement(&statement, &result))
}

pub fn query_rows(sql: &str) -> Result<RowBatch, String> {
    match execute_sql(sql)? {
        QueryResult::Rows(rows) => Ok(rows),
        other => Err(format!("Expected row result, got {other:?}")),
    }
}

pub fn command_result(sql: &str) -> Result<CommandResult, String> {
    match execute_sql(sql)? {
        QueryResult::Command(command) => Ok(command),
        other => Err(format!("Expected command result, got {other:?}")),
    }
}

pub fn snapshot_database() -> Database {
    let harness = harness().lock().unwrap_or_else(|err| err.into_inner());
    harness.engine.snapshot_database()
}

pub fn render_results(results: &[QueryResult]) -> String {
    results
        .iter()
        .map(render_result)
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn render_result(result: &QueryResult) -> String {
    match result {
        QueryResult::Rows(rows) => render_rows(rows),
        QueryResult::Command(command) => render_command(command.tag, command.affected),
        QueryResult::Explain(plan) => format!("Query Plan:\n{}", plan),
        QueryResult::ExplainAnalyze(result) => render_explain_analyze(result),
    }
}

fn render_result_for_statement(statement: &Statement, result: &QueryResult) -> String {
    match result {
        QueryResult::Rows(rows) => render_rows(rows),
        QueryResult::Command(command) => {
            render_command_for_statement(statement, command.tag, command.affected)
        }
        QueryResult::Explain(plan) => format!("Query Plan:\n{}", plan),
        QueryResult::ExplainAnalyze(result) => render_explain_analyze(result),
    }
}

fn render_explain_analyze(result: &ExplainAnalyzeResult) -> String {
    format!(
        "Query Plan:\n{}\nPlanning Time: {:.3} ms\nExecution Time: {:.3} ms\nActual Rows: {}",
        result.plan, result.planning_ms, result.execution_ms, result.actual_rows
    )
}

fn render_command_for_statement(statement: &Statement, tag: CommandTag, affected: u64) -> String {
    match statement {
        Statement::CreateTable(stmt) => format!("Table '{}' created", stmt.name),
        Statement::DropTable(stmt) => format!("Table '{}' dropped", stmt.name),
        Statement::AlterTable(stmt) => match &stmt.operation {
            crate::ast::AlterOperation::AddColumn(col) => {
                format!("Column '{}' added to table '{}'", col.name, stmt.table)
            }
            crate::ast::AlterOperation::DropColumn(col) => {
                format!("Column '{}' dropped from table '{}'", col, stmt.table)
            }
            crate::ast::AlterOperation::RenameColumn { old, new } => {
                format!(
                    "Column '{}' renamed to '{}' in table '{}'",
                    old, new, stmt.table
                )
            }
            crate::ast::AlterOperation::RenameTable(new_name) => {
                format!("Table '{}' renamed to '{}'", stmt.table, new_name)
            }
            crate::ast::AlterOperation::AddConstraint(_) => {
                format!("Constraint added to table '{}'", stmt.table)
            }
            crate::ast::AlterOperation::DropConstraint(name) => {
                format!("Constraint '{}' dropped from table '{}'", name, stmt.table)
            }
        },
        Statement::CreateIndex(stmt) => {
            format!(
                "Index '{}' created on {}.{}",
                stmt.name,
                stmt.table,
                stmt.columns.join(", ")
            )
        }
        Statement::DropIndex(stmt) => format!("Index '{}' dropped", stmt.name),
        Statement::Insert(_) => format!("{} row(s) inserted", affected),
        Statement::Update(_) => format!("{} row(s) updated", affected),
        Statement::Delete(_) => format!("{} row(s) deleted", affected),
        Statement::BeginTransaction => "Transaction begun".to_string(),
        Statement::CommitTransaction => "Transaction committed".to_string(),
        Statement::RollbackTransaction => "Transaction rolled back".to_string(),
        Statement::Savepoint(name) => format!("Savepoint '{}' created", name),
        Statement::ReleaseSavepoint(name) => format!("Savepoint '{}' released", name),
        Statement::RollbackToSavepoint(name) => format!("Rolled back to savepoint '{}'", name),
        Statement::Analyze(table_name) => format!("Table: {}\nRow count: {}", table_name, affected),
        Statement::TruncateTable { table_name } => format!("Table '{}' truncated", table_name),
        Statement::CreateView { name, .. } => format!("View '{}' created", name),
        Statement::DropView { name, .. } => format!("View '{}' dropped", name),
        Statement::Merge(_) => format!("{} row(s) affected", affected),
        Statement::Do { .. } => render_command(tag, affected),
        Statement::Describe(_)
        | Statement::ShowTables
        | Statement::Select(_)
        | Statement::Explain(_)
        | Statement::ExplainAnalyze(_) => render_command(tag, affected),
    }
}

fn render_command(tag: CommandTag, affected: u64) -> String {
    match tag {
        CommandTag::CreateTable => "Table created".to_string(),
        CommandTag::DropTable => "Table dropped".to_string(),
        CommandTag::AlterTable => "Table altered".to_string(),
        CommandTag::CreateIndex => "Index created".to_string(),
        CommandTag::DropIndex => "Index dropped".to_string(),
        CommandTag::Insert => format!("{} row(s) inserted", affected),
        CommandTag::Update => format!("{} row(s) updated", affected),
        CommandTag::Delete => format!("{} row(s) deleted", affected),
        CommandTag::BeginTransaction => "Transaction begun".to_string(),
        CommandTag::CommitTransaction => "Transaction committed".to_string(),
        CommandTag::RollbackTransaction => "Transaction rolled back".to_string(),
        CommandTag::Savepoint => "Savepoint created".to_string(),
        CommandTag::ReleaseSavepoint => "Savepoint released".to_string(),
        CommandTag::RollbackToSavepoint => "Rolled back to savepoint".to_string(),
        CommandTag::Analyze => format!("ANALYZE {}", affected),
        CommandTag::TruncateTable => "Table truncated".to_string(),
        CommandTag::CreateView => "View created".to_string(),
        CommandTag::DropView => "View dropped".to_string(),
        CommandTag::Merge => format!("{} row(s) affected", affected),
        CommandTag::Do => format!("DO {}", affected),
    }
}

fn render_rows(rows: &RowBatch) -> String {
    let mut output = String::new();

    for (idx, column) in rows.columns.iter().enumerate() {
        if idx > 0 {
            output.push('\t');
        }
        output.push_str(&column.name);
    }
    output.push('\n');
    output.push_str(&"-".repeat(40));
    output.push('\n');

    for row in &rows.rows {
        for (idx, value) in row.iter().enumerate() {
            if idx > 0 {
                output.push('\t');
            }
            output.push_str(&render_value(value));
        }
        output.push('\n');
    }

    output
}

fn render_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Integer(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::Text(value) => value.clone(),
        Value::Boolean(value) => value.to_string(),
        Value::Date(value) => value.clone(),
        Value::Time(value) => value.clone(),
        Value::DateTime(value) => value.clone(),
    }
}
