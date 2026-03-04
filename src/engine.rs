use crate::ast::{Statement, Value};
use crate::database::Database;
use crate::error::RustqlError;
use crate::{executor, lexer, parser};
use regex::Regex;
use std::sync::OnceLock;

#[derive(Debug, Clone, Default)]
pub struct EngineOptions {}

pub struct Engine {
    context: executor::ExecutionContext,
    _options: EngineOptions,
}

impl Engine {
    pub fn open(options: EngineOptions) -> Result<Self, RustqlError> {
        #[cfg(test)]
        let database = Database::new();

        #[cfg(not(test))]
        let database = Database::load();

        Ok(Self {
            context: executor::ExecutionContext::new(database),
            _options: options,
        })
    }

    pub fn open_default() -> Result<Self, RustqlError> {
        Self::open(EngineOptions::default())
    }

    pub fn session(&self) -> Session<'_> {
        Session { engine: self }
    }

    pub fn snapshot_database(&self) -> Database {
        self.context.database_snapshot()
    }

    pub fn reset_state(&self) {
        let _binding = executor::bind_context(&self.context);
        executor::reset_database_state();
    }
}

pub fn default_engine() -> &'static Engine {
    static ENGINE: OnceLock<Engine> = OnceLock::new();
    ENGINE.get_or_init(|| Engine::open_default().expect("failed to open default engine"))
}

pub(crate) fn bind_default_context() -> executor::ContextBinding {
    let engine = default_engine();
    executor::bind_context(&engine.context)
}

pub struct Session<'e> {
    engine: &'e Engine,
}

impl Session<'_> {
    pub fn execute(&mut self, sql: &str) -> Result<Vec<QueryResult>, RustqlError> {
        if sql.trim().is_empty() {
            return Ok(Vec::new());
        }

        Ok(vec![self.execute_one(sql)?])
    }

    pub fn execute_legacy(&mut self, sql: &str) -> Result<String, RustqlError> {
        if sql.trim().is_empty() {
            return Ok(String::new());
        }

        let tokens = lexer::tokenize(sql)?;
        let statement = parser::parse(tokens)?;
        let _binding = executor::bind_context(&self.engine.context);
        executor::execute(statement)
    }

    pub fn execute_one(&mut self, sql: &str) -> Result<QueryResult, RustqlError> {
        let tokens = lexer::tokenize(sql)?;
        let statement = parser::parse(tokens)?;
        self.execute_statement(statement)
    }

    pub fn execute_statement(&mut self, statement: Statement) -> Result<QueryResult, RustqlError> {
        let statement_kind = statement.clone();
        let _binding = executor::bind_context(&self.engine.context);
        let output = executor::execute(statement)?;
        Ok(map_output_to_result(&statement_kind, output))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnMeta {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryRows {
    pub columns: Vec<ColumnMeta>,
    pub rows: Vec<Vec<Value>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
    Describe,
    ShowTables,
    Do,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandResult {
    pub tag: CommandTag,
    pub affected: u64,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueryResult {
    Rows(QueryRows),
    Command(CommandResult),
    Explain { plan: String },
    Message(String),
}

fn map_output_to_result(statement: &Statement, output: String) -> QueryResult {
    if matches!(statement, Statement::Explain(_)) {
        return QueryResult::Explain { plan: output };
    }

    if let Some(rows) = parse_rows(&output) {
        return QueryResult::Rows(rows);
    }

    if let Some(tag) = command_tag(statement) {
        return QueryResult::Command(CommandResult {
            tag,
            affected: extract_affected_rows(&output),
            message: output,
        });
    }

    QueryResult::Message(output)
}

fn parse_rows(output: &str) -> Option<QueryRows> {
    let lines: Vec<&str> = output.lines().collect();
    if lines.len() < 2 || !is_separator_line(lines[1]) {
        return None;
    }

    let columns = split_tabular_line(lines[0])
        .into_iter()
        .map(|name| ColumnMeta { name })
        .collect();

    let mut rows = Vec::new();
    for line in lines.iter().skip(2) {
        if line.trim().is_empty() {
            continue;
        }

        let values = split_tabular_line(line)
            .into_iter()
            .map(|value| parse_output_value(&value))
            .collect();
        rows.push(values);
    }

    Some(QueryRows { columns, rows })
}

fn split_tabular_line(line: &str) -> Vec<String> {
    let mut values: Vec<String> = line.split('\t').map(str::to_string).collect();
    while matches!(values.last(), Some(last) if last.is_empty()) {
        values.pop();
    }
    values
}

fn parse_output_value(raw: &str) -> Value {
    if raw == "NULL" {
        return Value::Null;
    }

    if raw == "true" {
        return Value::Boolean(true);
    }

    if raw == "false" {
        return Value::Boolean(false);
    }

    if let Ok(i) = raw.parse::<i64>() {
        return Value::Integer(i);
    }

    if let Ok(f) = raw.parse::<f64>() {
        return Value::Float(f);
    }

    Value::Text(raw.to_string())
}

fn is_separator_line(line: &str) -> bool {
    let trimmed = line.trim();
    !trimmed.is_empty()
        && trimmed
            .chars()
            .all(|ch| matches!(ch, '-' | '+' | '|' | ':' | '='))
}

fn command_tag(statement: &Statement) -> Option<CommandTag> {
    match statement {
        Statement::CreateTable(_) => Some(CommandTag::CreateTable),
        Statement::DropTable(_) => Some(CommandTag::DropTable),
        Statement::AlterTable(_) => Some(CommandTag::AlterTable),
        Statement::CreateIndex(_) => Some(CommandTag::CreateIndex),
        Statement::DropIndex(_) => Some(CommandTag::DropIndex),
        Statement::Insert(_) => Some(CommandTag::Insert),
        Statement::Update(_) => Some(CommandTag::Update),
        Statement::Delete(_) => Some(CommandTag::Delete),
        Statement::BeginTransaction => Some(CommandTag::BeginTransaction),
        Statement::CommitTransaction => Some(CommandTag::CommitTransaction),
        Statement::RollbackTransaction => Some(CommandTag::RollbackTransaction),
        Statement::Savepoint(_) => Some(CommandTag::Savepoint),
        Statement::ReleaseSavepoint(_) => Some(CommandTag::ReleaseSavepoint),
        Statement::RollbackToSavepoint(_) => Some(CommandTag::RollbackToSavepoint),
        Statement::Analyze(_) => Some(CommandTag::Analyze),
        Statement::TruncateTable { .. } => Some(CommandTag::TruncateTable),
        Statement::CreateView { .. } => Some(CommandTag::CreateView),
        Statement::DropView { .. } => Some(CommandTag::DropView),
        Statement::Merge(_) => Some(CommandTag::Merge),
        Statement::Describe(_) => Some(CommandTag::Describe),
        Statement::ShowTables => Some(CommandTag::ShowTables),
        Statement::Do { .. } => Some(CommandTag::Do),
        Statement::Select(_) | Statement::Explain(_) => None,
    }
}

fn extract_affected_rows(output: &str) -> u64 {
    affected_rows_regex()
        .captures_iter(output)
        .filter_map(|captures| captures.get(1))
        .filter_map(|m| m.as_str().parse::<u64>().ok())
        .sum()
}

fn affected_rows_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?i)(\d+)\s+row\(s\)\s+(?:inserted|updated|deleted|affected)")
            .expect("invalid affected rows regex")
    })
}

pub fn format_query_results(results: &[QueryResult]) -> String {
    let mut rendered = String::new();

    for (idx, result) in results.iter().enumerate() {
        if idx > 0 {
            rendered.push('\n');
        }
        rendered.push_str(&format_query_result(result));
    }

    rendered
}

pub fn format_query_result(result: &QueryResult) -> String {
    match result {
        QueryResult::Rows(rows) => format_rows(rows),
        QueryResult::Command(command) => command.message.clone(),
        QueryResult::Explain { plan } => plan.clone(),
        QueryResult::Message(message) => message.clone(),
    }
}

fn format_rows(rows: &QueryRows) -> String {
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
            output.push_str(&crate::executor::format_value(value));
        }
        output.push('\n');
    }

    output
}
