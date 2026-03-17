use rustql::testing::{process_query, reset_database};
use std::fs;
use std::path::Path;
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

#[derive(Debug)]
enum Block {
    StatementOk { sql: String },
    QueryOk { sql: String, expected: Vec<String> },
}

#[test]
fn sql_logic_corpus() {
    let _guard = TEST_MUTEX.lock().unwrap();
    reset_database();

    let corpus_dir = Path::new("tests/sql_logic");
    let mut files: Vec<_> = fs::read_dir(corpus_dir)
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "slt"))
        .collect();
    files.sort();

    for file in files {
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
                Block::QueryOk { sql, expected } => {
                    let output = process_query(sql.trim()).unwrap_or_else(|e| {
                        panic!("query failed in {}:\n{}\nerror: {}", file.display(), sql, e)
                    });
                    for needle in expected {
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

        if line == "query ok" {
            i += 1;
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
            blocks.push(Block::QueryOk {
                sql: sql_lines.join("\n"),
                expected,
            });
            continue;
        }

        panic!("unknown sql logic directive: {}", line);
    }

    blocks
}
