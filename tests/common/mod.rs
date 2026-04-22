#![allow(dead_code)]

pub use rustql::CommandTag;
use rustql::ast::{Statement, Value};
use rustql::{CommandResult, Database, Engine, EngineOptions, QueryResult, RowBatch, StorageMode};
use std::cell::RefCell;

struct TestHarness {
    engine: Engine,
}

impl TestHarness {
    fn new() -> Result<Self, String> {
        let engine = Engine::open(EngineOptions {
            storage: StorageMode::Memory,
        })
        .map_err(|err| format!("failed to create test engine: {}", err))?;

        Ok(Self { engine })
    }
}

thread_local! {
    static HARNESS: RefCell<Result<TestHarness, String>> = RefCell::new(TestHarness::new());
}

fn with_harness<T>(operation: impl FnOnce(&TestHarness) -> Result<T, String>) -> Result<T, String> {
    HARNESS.with(|harness| {
        let harness = harness.borrow();
        let harness = harness.as_ref().map_err(|err| err.clone())?;
        operation(harness)
    })
}

pub fn reset_database() {
    HARNESS.with(|harness| {
        *harness.borrow_mut() = TestHarness::new();
    });
}

pub fn execute_script(sql: &str) -> Result<Vec<QueryResult>, String> {
    with_harness(|harness| {
        let mut session = harness.engine.session();
        session.execute_script(sql).map_err(|err| err.to_string())
    })
}

pub fn execute_sql(sql: &str) -> Result<QueryResult, String> {
    with_harness(|harness| {
        let mut session = harness.engine.session();
        session.execute_one(sql).map_err(|err| err.to_string())
    })
}

pub fn execute_statement(statement: Statement) -> Result<QueryResult, String> {
    with_harness(|harness| {
        let mut session = harness.engine.session();
        session
            .execute_statement(statement)
            .map_err(|err| err.to_string())
    })
}

pub fn execute(statement: Statement) -> Result<QueryResult, String> {
    execute_statement(statement)
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

pub fn snapshot_database() -> Result<Database, String> {
    with_harness(|harness| Ok(harness.engine.snapshot_database()))
}

pub fn assert_command(result: QueryResult, expected_tag: CommandTag, expected_affected: u64) {
    let command = result.expect_command();
    assert_eq!(command.tag, expected_tag);
    assert_eq!(command.affected, expected_affected);
}

pub fn assert_command_sql(sql: &str, expected_tag: CommandTag, expected_affected: u64) {
    assert_command(execute_sql(sql).unwrap(), expected_tag, expected_affected);
}

pub fn assert_rows(sql: &str, expected_columns: &[&str], expected_rows: Vec<Vec<Value>>) {
    let rows = query_rows(sql).unwrap();
    rows.assert_columns(expected_columns);
    assert_eq!(rows.rows, expected_rows);
}

pub trait QueryResultAssertions {
    fn expect_rows(&self) -> &RowBatch;
    fn expect_command(&self) -> &CommandResult;
    fn contains<N: QueryResultNeedle>(&self, needle: N) -> bool;
    fn matches(&self, needle: &str) -> std::vec::IntoIter<String>;
    fn lines(&self) -> std::vec::IntoIter<String>;
    fn output_text(&self) -> String;
    fn to_lowercase(&self) -> String;
    fn is_empty(&self) -> bool;
}

impl QueryResultAssertions for QueryResult {
    fn expect_rows(&self) -> &RowBatch {
        match self {
            QueryResult::Rows(rows) => rows,
            other => panic!("expected row result, got {other:?}"),
        }
    }

    fn expect_command(&self) -> &CommandResult {
        match self {
            QueryResult::Command(command) => command,
            other => panic!("expected command result, got {other:?}"),
        }
    }

    fn contains<N: QueryResultNeedle>(&self, needle: N) -> bool {
        match self {
            QueryResult::Rows(rows) => rows.contains(needle),
            QueryResult::Command(command) => needle.matches_command(command),
            QueryResult::Explain(plan) => {
                needle.matches_plan("Query Plan") || needle.matches_plan(&plan.to_string())
            }
            QueryResult::ExplainAnalyze(result) => {
                needle.matches_plan("Query Plan")
                    || needle.matches_plan("Planning Time")
                    || needle.matches_plan("Execution Time")
                    || needle.matches_plan("Actual Rows")
                    || needle.matches_plan(&result.plan.to_string())
                    || needle.matches_usize(result.actual_rows)
                    || needle.matches_float(result.planning_ms)
                    || needle.matches_float(result.execution_ms)
                    || query_output_lines(self)
                        .iter()
                        .any(|line| needle.matches_line(line))
            }
        }
    }

    fn matches(&self, needle: &str) -> std::vec::IntoIter<String> {
        query_output_lines(self)
            .into_iter()
            .filter(|line| line.contains(needle))
            .collect::<Vec<_>>()
            .into_iter()
    }

    fn lines(&self) -> std::vec::IntoIter<String> {
        rendered_lines(self).into_iter()
    }

    fn output_text(&self) -> String {
        rendered_lines(self).join("\n")
    }

    fn to_lowercase(&self) -> String {
        self.output_text().to_lowercase()
    }

    fn is_empty(&self) -> bool {
        match self {
            QueryResult::Rows(rows) => rows.columns.is_empty() && rows.rows.is_empty(),
            QueryResult::Command(_) | QueryResult::Explain(_) | QueryResult::ExplainAnalyze(_) => {
                false
            }
        }
    }
}

pub trait RowBatchAssertions {
    fn assert_columns(&self, expected: &[&str]);
    fn contains<N: QueryResultNeedle>(&self, needle: N) -> bool;
}

impl RowBatchAssertions for RowBatch {
    fn assert_columns(&self, expected: &[&str]) {
        let actual = self
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    fn contains<N: QueryResultNeedle>(&self, needle: N) -> bool {
        self.columns
            .iter()
            .any(|column| needle.matches_column(column.name.as_str()))
            || self
                .rows
                .iter()
                .flatten()
                .any(|value| needle.matches_value(value))
    }
}

pub trait QueryResultNeedle {
    fn matches_column(&self, column: &str) -> bool;
    fn matches_value(&self, value: &Value) -> bool;
    fn matches_command(&self, command: &CommandResult) -> bool;
    fn matches_plan(&self, plan: &str) -> bool;
    fn matches_line(&self, line: &str) -> bool {
        self.matches_plan(line)
    }

    fn matches_usize(&self, value: usize) -> bool;
    fn matches_float(&self, value: f64) -> bool;
}

impl QueryResultNeedle for &str {
    fn matches_column(&self, column: &str) -> bool {
        column == *self
    }

    fn matches_value(&self, value: &Value) -> bool {
        match value {
            Value::Null => *self == "NULL",
            Value::Integer(value) => self.parse::<i64>().is_ok_and(|expected| expected == *value),
            Value::Float(value) => self.matches_float(*value),
            Value::Boolean(value) => self
                .parse::<bool>()
                .is_ok_and(|expected| expected == *value),
            Value::Text(value)
            | Value::Date(value)
            | Value::Time(value)
            | Value::DateTime(value) => value.contains(*self),
        }
    }

    fn matches_command(&self, command: &CommandResult) -> bool {
        command_tag_name(command.tag) == *self || self.matches_usize(command.affected as usize)
    }

    fn matches_plan(&self, plan: &str) -> bool {
        plan.contains(*self)
    }

    fn matches_usize(&self, value: usize) -> bool {
        self.parse::<usize>()
            .is_ok_and(|expected| expected == value)
    }

    fn matches_float(&self, value: f64) -> bool {
        self.parse::<f64>().is_ok_and(|expected| {
            (value - expected).abs() < f64::EPSILON || value.to_string().contains(*self)
        })
    }
}

impl QueryResultNeedle for String {
    fn matches_column(&self, column: &str) -> bool {
        self.as_str().matches_column(column)
    }

    fn matches_value(&self, value: &Value) -> bool {
        self.as_str().matches_value(value)
    }

    fn matches_command(&self, command: &CommandResult) -> bool {
        self.as_str().matches_command(command)
    }

    fn matches_plan(&self, plan: &str) -> bool {
        self.as_str().matches_plan(plan)
    }

    fn matches_usize(&self, value: usize) -> bool {
        self.as_str().matches_usize(value)
    }

    fn matches_float(&self, value: f64) -> bool {
        self.as_str().matches_float(value)
    }
}

impl QueryResultNeedle for &String {
    fn matches_column(&self, column: &str) -> bool {
        self.as_str().matches_column(column)
    }

    fn matches_value(&self, value: &Value) -> bool {
        self.as_str().matches_value(value)
    }

    fn matches_command(&self, command: &CommandResult) -> bool {
        self.as_str().matches_command(command)
    }

    fn matches_plan(&self, plan: &str) -> bool {
        self.as_str().matches_plan(plan)
    }

    fn matches_usize(&self, value: usize) -> bool {
        self.as_str().matches_usize(value)
    }

    fn matches_float(&self, value: f64) -> bool {
        self.as_str().matches_float(value)
    }
}

impl QueryResultNeedle for char {
    fn matches_column(&self, column: &str) -> bool {
        column.contains(*self)
    }

    fn matches_value(&self, value: &Value) -> bool {
        match value {
            Value::Text(value)
            | Value::Date(value)
            | Value::Time(value)
            | Value::DateTime(value) => value.contains(*self),
            other => other.to_string().contains(*self),
        }
    }

    fn matches_command(&self, command: &CommandResult) -> bool {
        command_tag_name(command.tag).contains(*self)
            || command.affected.to_string().contains(*self)
    }

    fn matches_plan(&self, plan: &str) -> bool {
        plan.contains(*self)
    }

    fn matches_usize(&self, value: usize) -> bool {
        value.to_string().contains(*self)
    }

    fn matches_float(&self, value: f64) -> bool {
        value.to_string().contains(*self)
    }
}

pub fn query_output_lines(result: &QueryResult) -> Vec<String> {
    match result {
        QueryResult::Rows(rows) => row_batch_lines(rows),
        QueryResult::Command(command) => vec![format!(
            "{} {}",
            command_tag_name(command.tag),
            command.affected
        )],
        QueryResult::Explain(plan) => vec!["Query Plan:".to_string(), plan.to_string()],
        QueryResult::ExplainAnalyze(result) => vec![
            "Query Plan:".to_string(),
            result.plan.to_string(),
            format!("Planning Time: {:.3} ms", result.planning_ms),
            format!("Execution Time: {:.3} ms", result.execution_ms),
            format!("Actual Rows: {}", result.actual_rows),
        ],
    }
}

fn row_batch_lines(rows: &RowBatch) -> Vec<String> {
    let mut output = Vec::with_capacity(rows.rows.len() + 1);
    output.push(
        rows.columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>()
            .join("\t"),
    );

    output.extend(rows.rows.iter().map(|row| {
        row.iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\t")
    }));

    output
}

fn rendered_lines(result: &QueryResult) -> Vec<String> {
    match result {
        QueryResult::Rows(rows) => rendered_row_batch_lines(rows),
        QueryResult::Command(command) => vec![format!(
            "{} {}",
            command_tag_name(command.tag),
            command.affected
        )],
        QueryResult::Explain(plan) => vec!["Query Plan:".to_string(), plan.to_string()],
        QueryResult::ExplainAnalyze(result) => vec![
            "Query Plan:".to_string(),
            result.plan.to_string(),
            format!("Planning Time: {:.3} ms", result.planning_ms),
            format!("Execution Time: {:.3} ms", result.execution_ms),
            format!("Actual Rows: {}", result.actual_rows),
        ],
    }
}

fn rendered_row_batch_lines(rows: &RowBatch) -> Vec<String> {
    let mut lines = Vec::with_capacity(rows.rows.len() + 2);
    lines.push(
        rows.columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>()
            .join("\t"),
    );
    lines.push("-".repeat(40));
    lines.extend(rows.rows.iter().map(|row| {
        row.iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\t")
    }));
    lines
}

fn command_tag_name(tag: CommandTag) -> &'static str {
    match tag {
        CommandTag::CreateTable => "CreateTable",
        CommandTag::DropTable => "DropTable",
        CommandTag::AlterTable => "AlterTable",
        CommandTag::CreateIndex => "CreateIndex",
        CommandTag::DropIndex => "DropIndex",
        CommandTag::Insert => "Insert",
        CommandTag::Update => "Update",
        CommandTag::Delete => "Delete",
        CommandTag::BeginTransaction => "BeginTransaction",
        CommandTag::CommitTransaction => "CommitTransaction",
        CommandTag::RollbackTransaction => "RollbackTransaction",
        CommandTag::Savepoint => "Savepoint",
        CommandTag::ReleaseSavepoint => "ReleaseSavepoint",
        CommandTag::RollbackToSavepoint => "RollbackToSavepoint",
        CommandTag::Analyze => "Analyze",
        CommandTag::TruncateTable => "TruncateTable",
        CommandTag::CreateView => "CreateView",
        CommandTag::DropView => "DropView",
        CommandTag::Merge => "Merge",
        CommandTag::Do => "Do",
    }
}
