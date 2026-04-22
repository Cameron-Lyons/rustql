mod common;
use common::{process_query, reset_database};
use std::fs;
use std::path::Path;
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

#[derive(Debug)]
enum Block {
    StatementOk {
        sql: String,
    },
    StatementError {
        sql: String,
        expected_error: Option<String>,
    },
    QueryContains {
        sql: String,
        expected: Vec<String>,
    },
    QueryExact {
        sql: String,
        expected: Vec<String>,
    },
    QueryError {
        sql: String,
        expected_error: Option<String>,
    },
}

#[test]
fn sql_logic_corpus() {
    let _guard = TEST_MUTEX.lock().unwrap();

    let corpus_dir = Path::new("tests/sql_logic");
    let mut files: Vec<_> = fs::read_dir(corpus_dir)
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "slt"))
        .collect();
    files.sort();

    for file in files {
        reset_database();
        let content = fs::read_to_string(&file).unwrap();
        let blocks = parse_blocks(&content);
        for block in blocks {
            match block {
                Block::StatementOk { sql } => {
                    let result = process_query(sql.trim());
                    assert!(
                        result.is_ok(),
                        "statement failed in {}:\n{}\nerror: {:?}",
                        file.display(),
                        sql,
                        result.err()
                    );
                }
                Block::StatementError {
                    sql,
                    expected_error,
                } => {
                    let error = process_query(sql.trim()).expect_err(&format!(
                        "expected statement error in {}:\n{}",
                        file.display(),
                        sql
                    ));
                    assert_expected_error(&file, &sql, &error, expected_error.as_deref());
                }
                Block::QueryContains { sql, expected } => {
                    let output = process_query(sql.trim()).unwrap_or_else(|e| {
                        panic!("query failed in {}:\n{}\nerror: {}", file.display(), sql, e)
                    });
                    for needle in expected {
                        let needle = decode_expected_line(&needle);
                        assert!(
                            output.contains(&needle),
                            "expected '{}' in output from {}:\nquery:\n{}\noutput:\n{}",
                            needle,
                            file.display(),
                            sql,
                            output
                        );
                    }
                }
                Block::QueryExact { sql, expected } => {
                    let output = process_query(sql.trim()).unwrap_or_else(|e| {
                        panic!("query failed in {}:\n{}\nerror: {}", file.display(), sql, e)
                    });
                    let actual = normalize_query_output(&output);
                    let expected = expected
                        .iter()
                        .map(|line| decode_expected_line(line))
                        .collect::<Vec<_>>();
                    assert_eq!(
                        actual,
                        expected,
                        "exact output mismatch in {}:\nquery:\n{}\nraw output:\n{}",
                        file.display(),
                        sql,
                        output
                    );
                }
                Block::QueryError {
                    sql,
                    expected_error,
                } => {
                    let error = process_query(sql.trim()).expect_err(&format!(
                        "expected query error in {}:\n{}",
                        file.display(),
                        sql
                    ));
                    assert_expected_error(&file, &sql, &error, expected_error.as_deref());
                }
            }
        }
    }
}

fn parse_blocks(content: &str) -> Vec<Block> {
    let lines: Vec<&str> = content.lines().collect();
    let mut blocks = Vec::new();
    let mut i = 0usize;

    while i < lines.len() {
        let line = lines[i].trim();
        if line.is_empty() || line.starts_with('#') {
            i += 1;
            continue;
        }

        if line == "statement ok" {
            i += 1;
            let mut sql_lines = Vec::new();
            while i < lines.len() {
                let current = lines[i];
                if current.trim().is_empty() {
                    break;
                }
                sql_lines.push(current);
                i += 1;
            }
            blocks.push(Block::StatementOk {
                sql: sql_lines.join("\n"),
            });
            continue;
        }

        if let Some(expected_error) = line.strip_prefix("statement error") {
            let (sql, next_index) = parse_statement_block(&lines, i + 1);
            blocks.push(Block::StatementError {
                sql,
                expected_error: parse_expected_error(expected_error),
            });
            i = next_index;
            continue;
        }

        if line == "query ok" || line == "query contains" {
            let (sql, expected, next_index) = parse_query_block(&lines, i + 1);
            blocks.push(Block::QueryContains { sql, expected });
            i = next_index;
            continue;
        }

        if line == "query exact" {
            let (sql, expected, next_index) = parse_query_block(&lines, i + 1);
            blocks.push(Block::QueryExact { sql, expected });
            i = next_index;
            continue;
        }

        if let Some(expected_error) = line.strip_prefix("query error") {
            let (sql, next_index) = parse_statement_block(&lines, i + 1);
            blocks.push(Block::QueryError {
                sql,
                expected_error: parse_expected_error(expected_error),
            });
            i = next_index;
            continue;
        }

        panic!("unknown sql logic directive: {}", line);
    }

    blocks
}

fn parse_statement_block(lines: &[&str], mut i: usize) -> (String, usize) {
    let mut sql_lines = Vec::new();
    while i < lines.len() {
        let current = lines[i];
        if current.trim().is_empty() {
            break;
        }
        sql_lines.push(current);
        i += 1;
    }
    (sql_lines.join("\n"), i)
}

fn parse_query_block(lines: &[&str], mut i: usize) -> (String, Vec<String>, usize) {
    let mut sql_lines = Vec::new();
    while i < lines.len() && lines[i].trim() != "----" {
        sql_lines.push(lines[i]);
        i += 1;
    }
    assert!(i < lines.len(), "query block missing ---- separator");
    i += 1;

    let mut expected = Vec::new();
    while i < lines.len() {
        let current = lines[i].trim();
        if current.is_empty() {
            break;
        }
        expected.push(current.to_string());
        i += 1;
    }

    (sql_lines.join("\n"), expected, i)
}

fn normalize_query_output(output: &str) -> Vec<String> {
    output
        .lines()
        .map(str::trim_end)
        .filter(|line| !line.trim().is_empty())
        .filter(|line| !is_separator_line(line.trim()))
        .map(ToString::to_string)
        .collect()
}

fn is_separator_line(line: &str) -> bool {
    !line.is_empty() && line.chars().all(|ch| ch == '-')
}

fn decode_expected_line(line: &str) -> String {
    let mut decoded = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.next() {
                Some('t') => decoded.push('\t'),
                Some('n') => decoded.push('\n'),
                Some('\\') => decoded.push('\\'),
                Some(other) => {
                    decoded.push('\\');
                    decoded.push(other);
                }
                None => decoded.push('\\'),
            }
        } else {
            decoded.push(ch);
        }
    }
    decoded
}

fn parse_expected_error(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn assert_expected_error(file: &Path, sql: &str, error: &str, expected_error: Option<&str>) {
    if let Some(expected_error) = expected_error {
        assert!(
            error.contains(expected_error),
            "expected error containing '{}' in {}:\nquery:\n{}\nerror:\n{}",
            expected_error,
            file.display(),
            sql,
            error
        );
    }
}
