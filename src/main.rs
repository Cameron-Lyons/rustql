use rustql::Value;
use rustql::{CommandTag, Engine, EngineOptions, ExplainAnalyzeResult, QueryResult, RowBatch};
use std::io::{self, IsTerminal, Read, Write};
use std::process::ExitCode;

fn main() -> ExitCode {
    let options = match EngineOptions::from_env() {
        Ok(options) => options,
        Err(e) => {
            eprintln!("Error: {}", e);
            return ExitCode::FAILURE;
        }
    };

    let engine = match Engine::open(options) {
        Ok(engine) => engine,
        Err(e) => {
            eprintln!("Error: {}", e);
            return ExitCode::FAILURE;
        }
    };
    let mut session = engine.session();

    if std::io::stdin().is_terminal() {
        println!("RustQL v1");
        println!("Type 'exit' to quit\n");

        loop {
            print!("rustql> ");
            if let Err(e) = io::stdout().flush() {
                eprintln!("Error: {}", e);
                return ExitCode::FAILURE;
            }

            let mut input = String::new();
            if let Err(e) = io::stdin().read_line(&mut input) {
                eprintln!("Error: {}", e);
                return ExitCode::FAILURE;
            }

            let query = input.trim();

            if query.eq_ignore_ascii_case("exit") {
                println!("Goodbye!");
                break;
            }

            if query.is_empty() {
                continue;
            }

            match session.execute_one(query) {
                Ok(result) => println!("{}", render_result(&result)),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
    } else {
        let mut input = String::new();
        if let Err(e) = io::stdin().read_to_string(&mut input) {
            eprintln!("Error: {}", e);
            return ExitCode::FAILURE;
        }

        if !input.trim().is_empty() {
            match session.execute_script(&input) {
                Ok(results) => println!("{}", render_results(&results)),
                Err(e) => {
                    eprintln!("Error: {}", e);
                    return ExitCode::FAILURE;
                }
            }
        }
    }

    ExitCode::SUCCESS
}

fn render_results(results: &[QueryResult]) -> String {
    results
        .iter()
        .map(render_result)
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_result(result: &QueryResult) -> String {
    match result {
        QueryResult::Rows(rows) => render_rows(rows),
        QueryResult::Command(command) => render_command(command.tag, command.affected),
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

fn render_command(tag: CommandTag, affected: u64) -> String {
    match tag {
        CommandTag::CreateTable => "CREATE TABLE".to_string(),
        CommandTag::DropTable => "DROP TABLE".to_string(),
        CommandTag::AlterTable => "ALTER TABLE".to_string(),
        CommandTag::CreateIndex => "CREATE INDEX".to_string(),
        CommandTag::DropIndex => "DROP INDEX".to_string(),
        CommandTag::Insert => format!("INSERT {}", affected),
        CommandTag::Update => format!("UPDATE {}", affected),
        CommandTag::Delete => format!("DELETE {}", affected),
        CommandTag::BeginTransaction => "BEGIN".to_string(),
        CommandTag::CommitTransaction => "COMMIT".to_string(),
        CommandTag::RollbackTransaction => "ROLLBACK".to_string(),
        CommandTag::Savepoint => "SAVEPOINT".to_string(),
        CommandTag::ReleaseSavepoint => "RELEASE SAVEPOINT".to_string(),
        CommandTag::RollbackToSavepoint => "ROLLBACK TO SAVEPOINT".to_string(),
        CommandTag::Analyze => format!("ANALYZE {}", affected),
        CommandTag::TruncateTable => "TRUNCATE TABLE".to_string(),
        CommandTag::CreateView => "CREATE VIEW".to_string(),
        CommandTag::DropView => "DROP VIEW".to_string(),
        CommandTag::Merge => format!("MERGE {}", affected),
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
    value.to_string()
}
