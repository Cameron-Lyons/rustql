use rustql::{
    CommandTag, ConstraintKind, Engine, EngineOptions, QueryResult, RustqlError, StorageMode, ast,
    lexer, parser, planner,
};
use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::sync::{Mutex, OnceLock};

struct EnvVarGuard {
    key: &'static str,
    original: Option<OsString>,
}

impl EnvVarGuard {
    fn unset(key: &'static str) -> Self {
        let original = std::env::var_os(key);
        unsafe {
            std::env::remove_var(key);
        }
        Self { key, original }
    }

    fn set(key: &'static str, value: impl AsRef<OsStr>) -> Self {
        let original = std::env::var_os(key);
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, original }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        unsafe {
            match &self.original {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }
}

fn test_guard() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
}

fn unique_temp_path(label: &str, extension: &str) -> std::path::PathBuf {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "rustql_{}_{}_{}.{}",
        label,
        std::process::id(),
        timestamp,
        extension
    ))
}

fn cleanup_storage_path(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let mut journal_path = path.as_os_str().to_os_string();
    journal_path.push(".wal");
    let _ = std::fs::remove_file(std::path::PathBuf::from(journal_path));
}

#[test]
fn default_engine_options_use_json_storage() {
    let options = EngineOptions::default();

    match options.storage {
        StorageMode::Json { path } => {
            assert_eq!(path, std::path::PathBuf::from("rustql_data.json"))
        }
        other => panic!("expected JSON storage by default, got: {other:?}"),
    }
}

#[test]
fn engine_options_constructors_set_storage_modes() {
    match EngineOptions::memory().storage {
        StorageMode::Memory => {}
        other => panic!("expected memory storage, got: {other:?}"),
    }

    let json_path = std::path::PathBuf::from("custom.json");
    match EngineOptions::json(&json_path).storage {
        StorageMode::Json { path } => assert_eq!(path, json_path),
        other => panic!("expected JSON storage, got: {other:?}"),
    }

    let btree_path = std::path::PathBuf::from("custom.dat");
    match EngineOptions::btree(&btree_path).storage {
        StorageMode::BTree { path } => assert_eq!(path, btree_path),
        other => panic!("expected BTree storage, got: {other:?}"),
    }
}

#[test]
fn in_memory_engine_constructor_opens_engine() {
    let engine = Engine::in_memory().unwrap();
    let mut session = engine.session();

    let result = session.execute_one("SELECT 1 AS value").unwrap();

    match result {
        QueryResult::Rows(rows) => assert_eq!(rows.rows, vec![vec![ast::Value::Integer(1)]]),
        other => panic!("expected rows, got: {other:?}"),
    }
}

#[test]
fn engine_from_env_opens_configured_storage() {
    let _guard = test_guard();
    let path = unique_temp_path("engine_from_env", "json");
    cleanup_storage_path(&path);
    let _storage = EnvVarGuard::set("RUSTQL_STORAGE", "json");
    let _path = EnvVarGuard::set("RUSTQL_STORAGE_PATH", &path);

    {
        let engine = Engine::from_env().unwrap();
        let mut session = engine.session();
        session
            .execute_one("CREATE TABLE env_open (id INTEGER)")
            .unwrap();
    }

    assert!(path.exists());

    cleanup_storage_path(&path);
}

#[test]
fn engine_options_from_env_default_uses_json_storage() {
    let _guard = test_guard();
    let _storage = EnvVarGuard::unset("RUSTQL_STORAGE");
    let _path = EnvVarGuard::unset("RUSTQL_STORAGE_PATH");

    let options = EngineOptions::from_env().unwrap();

    match options.storage {
        StorageMode::Json { path } => {
            assert_eq!(path, std::path::PathBuf::from("rustql_data.json"))
        }
        other => panic!("expected JSON storage by default, got: {other:?}"),
    }
}

#[test]
fn engine_options_from_env_uses_path_override() {
    let _guard = test_guard();
    let path = unique_temp_path("env_json", "json");
    let _storage = EnvVarGuard::set("RUSTQL_STORAGE", "json");
    let _path = EnvVarGuard::set("RUSTQL_STORAGE_PATH", &path);

    let options = EngineOptions::from_env().unwrap();

    match options.storage {
        StorageMode::Json { path: actual } => assert_eq!(actual, path),
        other => panic!("expected JSON storage, got: {other:?}"),
    }
}

#[test]
fn engine_options_from_env_btree_uses_path_override() {
    let _guard = test_guard();
    let path = unique_temp_path("env_btree", "dat");
    let _storage = EnvVarGuard::set("RUSTQL_STORAGE", "btree");
    let _path = EnvVarGuard::set("RUSTQL_STORAGE_PATH", &path);

    let options = EngineOptions::from_env().unwrap();

    match options.storage {
        StorageMode::BTree { path: actual } => assert_eq!(actual, path),
        other => panic!("expected BTree storage, got: {other:?}"),
    }
}

#[test]
fn engine_options_from_env_rejects_empty_path_override() {
    let _guard = test_guard();
    let _storage = EnvVarGuard::set("RUSTQL_STORAGE", "json");
    let _path = EnvVarGuard::set("RUSTQL_STORAGE_PATH", "");

    let error = EngineOptions::from_env().unwrap_err();

    assert!(
        error
            .to_string()
            .contains("RUSTQL_STORAGE_PATH cannot be empty")
    );
}

#[test]
fn engine_options_from_env_rejects_unknown_storage() {
    let _guard = test_guard();
    let _storage = EnvVarGuard::set("RUSTQL_STORAGE", "sqlite");
    let _path = EnvVarGuard::unset("RUSTQL_STORAGE_PATH");

    let error = EngineOptions::from_env().unwrap_err();

    assert!(
        error
            .to_string()
            .contains("Unsupported RUSTQL_STORAGE value")
    );
}

#[test]
fn execute_one_parse_errors_include_line_and_column() {
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    let error = session.execute_one("SELECT FROM users").unwrap_err();

    assert!(error.to_string().contains("line 1, column"));
}

#[test]
fn execute_one_accepts_trailing_semicolon() {
    let engine = Engine::in_memory().unwrap();
    let mut session = engine.session();

    let result = session.execute_one("SELECT 1 AS value;").unwrap();

    match result {
        QueryResult::Rows(rows) => assert_eq!(rows.rows, vec![vec![ast::Value::Integer(1)]]),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn execute_one_rejects_multiple_statements() {
    let engine = Engine::in_memory().unwrap();
    let mut session = engine.session();

    let error = session.execute_one("SELECT 1;\nSELECT 2").unwrap_err();

    assert!(error.to_string().contains("Unexpected trailing token"));
    assert!(error.to_string().contains("line 2, column 1"));
}

#[test]
fn execute_script_requires_statement_separator() {
    let engine = Engine::in_memory().unwrap();
    let mut session = engine.session();

    let error = session.execute_script("SELECT 1\nSELECT 2").unwrap_err();

    assert!(
        error
            .to_string()
            .contains("Expected semicolon or end of input")
    );
    assert!(error.to_string().contains("line 2, column 1"));
}

#[test]
fn parse_script_spanned_retains_error_span_data() {
    let tokens = lexer::tokenize_spanned("SELECT 1\nSELECT 2").unwrap();
    let error = parser::parse_script_spanned(tokens).unwrap_err();

    let span = error.span().expect("expected parse span");
    assert_eq!(span.start.line, 2);
    assert_eq!(span.start.column, 1);
    assert!(error.to_string().contains("line 2, column 1"));
}

#[test]
fn delete_where_invalid_column_returns_error() {
    let engine = Engine::in_memory().unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE delete_errors (id INTEGER);
            INSERT INTO delete_errors VALUES (1);
            ",
        )
        .unwrap();
    let error = session
        .execute_one("DELETE FROM delete_errors WHERE missing = 1")
        .unwrap_err();

    assert!(matches!(error, RustqlError::ColumnNotFound(_)));
}

#[test]
fn delete_using_invalid_column_returns_error() {
    let engine = Engine::in_memory().unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE delete_using_errors (id INTEGER);
            CREATE TABLE delete_using_filter (id INTEGER);
            INSERT INTO delete_using_errors VALUES (1);
            INSERT INTO delete_using_filter VALUES (1);
            ",
        )
        .unwrap();
    let error = session
        .execute_one("DELETE FROM delete_using_errors USING delete_using_filter WHERE missing = 1")
        .unwrap_err();

    assert!(matches!(error, RustqlError::ColumnNotFound(_)));
}

#[test]
fn partial_index_invalid_filter_column_returns_error() {
    let engine = Engine::in_memory().unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE partial_index_errors (id INTEGER);
            INSERT INTO partial_index_errors VALUES (1);
            ",
        )
        .unwrap();
    let error = session
        .execute_one("CREATE INDEX idx_bad_partial ON partial_index_errors(id) WHERE missing = 1")
        .unwrap_err();

    assert!(matches!(error, RustqlError::ColumnNotFound(_)));
}

#[test]
fn check_constraint_errors_report_check_kind() {
    let engine = Engine::in_memory().unwrap();
    let mut session = engine.session();

    session
        .execute_one("CREATE TABLE checked_values (value INTEGER CHECK (value > 0))")
        .unwrap();
    let error = session
        .execute_one("INSERT INTO checked_values VALUES (-1)")
        .unwrap_err();

    match error {
        RustqlError::ConstraintViolation { kind, message } => {
            assert_eq!(kind, ConstraintKind::Check);
            assert!(message.contains("CHECK constraint violation"));
        }
        other => panic!("expected CHECK constraint violation, got {other:?}"),
    }
}

#[test]
fn aggregate_filter_invalid_column_returns_error() {
    let engine = Engine::in_memory().unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE aggregate_filter_errors (id INTEGER);
            INSERT INTO aggregate_filter_errors VALUES (1);
            ",
        )
        .unwrap();
    let error = session
        .execute_one("SELECT COUNT(*) FILTER (WHERE missing = 1) FROM aggregate_filter_errors")
        .unwrap_err();

    assert!(matches!(error, RustqlError::ColumnNotFound(_)));
}

#[test]
fn any_all_type_mismatch_returns_error() {
    let engine = Engine::in_memory().unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE any_all_values (id INTEGER, val INTEGER);
            CREATE TABLE any_all_text_values (val TEXT);
            INSERT INTO any_all_values VALUES (1, 10);
            INSERT INTO any_all_text_values VALUES ('x');
            ",
        )
        .unwrap();

    let any_error = session
        .execute_one(
            "SELECT id FROM any_all_values WHERE val = ANY (SELECT val FROM any_all_text_values)",
        )
        .unwrap_err();
    let all_error = session
        .execute_one(
            "SELECT id FROM any_all_values WHERE val = ALL (SELECT val FROM any_all_text_values)",
        )
        .unwrap_err();

    assert!(matches!(any_error, RustqlError::TypeMismatch(_)));
    assert!(matches!(all_error, RustqlError::TypeMismatch(_)));
}

#[test]
fn arithmetic_with_null_returns_null() {
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    let result = session.execute_one("SELECT NULL + 5 AS value").unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(rows.rows, vec![vec![ast::Value::Null]]);
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn planned_arithmetic_with_null_returns_null() {
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE null_math (a INTEGER, b INTEGER);
            INSERT INTO null_math VALUES (10, NULL);
            ",
        )
        .unwrap();

    let result = session
        .execute_one("SELECT a + b AS value FROM null_math")
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(rows.rows, vec![vec![ast::Value::Null]]);
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn explain_expression_projection_uses_plan() {
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE sales (price INTEGER, quantity INTEGER);
            INSERT INTO sales VALUES (10, 2), (7, 3);
            ",
        )
        .unwrap();

    let result = session
        .execute_one("EXPLAIN SELECT price * quantity AS total FROM sales")
        .unwrap();

    match result {
        QueryResult::Explain(plan) => {
            assert!(matches!(plan, planner::PlanNode::SeqScan { .. }));
        }
        other => panic!("expected explain result, got: {other:?}"),
    }

    let result = session
        .execute_one("SELECT price * quantity AS total FROM sales")
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(rows.columns[0].name, "total");
            assert_eq!(
                rows.rows,
                vec![vec![ast::Value::Integer(20)], vec![ast::Value::Integer(21)]]
            );
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn explain_scalar_and_cast_projection_uses_plan() {
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE people (name TEXT, age INTEGER);
            INSERT INTO people VALUES ('Alice', 30), ('Bob', 41);
            ",
        )
        .unwrap();

    let result = session
        .execute_one(
            "EXPLAIN SELECT UPPER(name) AS upper_name, CAST(age AS TEXT) AS age_text FROM people",
        )
        .unwrap();

    match result {
        QueryResult::Explain(plan) => {
            assert!(matches!(plan, planner::PlanNode::SeqScan { .. }));
        }
        other => panic!("expected explain result, got: {other:?}"),
    }

    let result = session
        .execute_one("SELECT UPPER(name) AS upper_name, CAST(age AS TEXT) AS age_text FROM people")
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(rows.columns[0].name, "upper_name");
            assert_eq!(rows.columns[1].name, "age_text");
            assert_eq!(
                rows.rows,
                vec![
                    vec![
                        ast::Value::Text("ALICE".to_string()),
                        ast::Value::Text("30".to_string())
                    ],
                    vec![
                        ast::Value::Text("BOB".to_string()),
                        ast::Value::Text("41".to_string())
                    ]
                ]
            );
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn explain_case_projection_uses_plan() {
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE employees (name TEXT, salary INTEGER);
            INSERT INTO employees VALUES ('Alice', 80000), ('Bob', 40000), ('Charlie', 120000);
            ",
        )
        .unwrap();

    let result = session
        .execute_one(
            "EXPLAIN SELECT CASE
                 WHEN salary > 100000 THEN 'high'
                 WHEN salary > 50000 THEN 'medium'
                 ELSE 'low'
             END AS level FROM employees",
        )
        .unwrap();

    match result {
        QueryResult::Explain(plan) => {
            assert!(matches!(plan, planner::PlanNode::SeqScan { .. }));
        }
        other => panic!("expected explain result, got: {other:?}"),
    }

    let result = session
        .execute_one(
            "SELECT CASE
                 WHEN salary > 100000 THEN 'high'
                 WHEN salary > 50000 THEN 'medium'
                 ELSE 'low'
             END AS level FROM employees",
        )
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(
                rows.rows,
                vec![
                    vec![ast::Value::Text("medium".to_string())],
                    vec![ast::Value::Text("low".to_string())],
                    vec![ast::Value::Text("high".to_string())]
                ]
            );
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn explain_scalar_and_cast_filter_uses_plan() {
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE people (id INTEGER, name TEXT, age INTEGER);
            INSERT INTO people VALUES (1, 'Alice', 30), (2, 'Bo', 30), (3, 'Carla', 41);
            ",
        )
        .unwrap();

    let result = session
        .execute_one(
            "EXPLAIN SELECT id FROM people
             WHERE LENGTH(name) > 3 AND CAST(age AS TEXT) = '30'",
        )
        .unwrap();

    match result {
        QueryResult::Explain(plan) => {
            assert!(matches!(
                plan,
                planner::PlanNode::SeqScan {
                    filter: Some(_),
                    ..
                }
            ));
        }
        other => panic!("expected explain result, got: {other:?}"),
    }

    let result = session
        .execute_one("SELECT id FROM people WHERE LENGTH(name) > 3 AND CAST(age AS TEXT) = '30'")
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(rows.rows, vec![vec![ast::Value::Integer(1)]]);
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn explain_between_and_case_filter_uses_plan() {
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE sales (id INTEGER, price INTEGER, quantity INTEGER, active INTEGER);
            INSERT INTO sales VALUES (1, 10, 2, 1), (2, 5, 5, 1), (3, 7, 1, 0);
            ",
        )
        .unwrap();

    let result = session
        .execute_one(
            "EXPLAIN SELECT id FROM sales
             WHERE price * quantity BETWEEN 20 AND 25
               AND CASE WHEN active = 1 THEN quantity ELSE 0 END >= 2",
        )
        .unwrap();

    match result {
        QueryResult::Explain(plan) => {
            assert!(matches!(
                plan,
                planner::PlanNode::SeqScan {
                    filter: Some(_),
                    ..
                }
            ));
        }
        other => panic!("expected explain result, got: {other:?}"),
    }

    let result = session
        .execute_one(
            "SELECT id FROM sales
             WHERE price * quantity BETWEEN 20 AND 25
               AND CASE WHEN active = 1 THEN quantity ELSE 0 END >= 2",
        )
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(
                rows.rows,
                vec![vec![ast::Value::Integer(1)], vec![ast::Value::Integer(2)]]
            );
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn explain_order_by_expression_uses_sort_plan() {
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE sales (id INTEGER, price INTEGER, quantity INTEGER);
            INSERT INTO sales VALUES (1, 10, 2), (2, 5, 5), (3, 7, 1);
            ",
        )
        .unwrap();

    let result = session
        .execute_one("EXPLAIN SELECT id, price, quantity FROM sales ORDER BY price * quantity DESC")
        .unwrap();

    match result {
        QueryResult::Explain(plan) => {
            assert!(matches!(plan, planner::PlanNode::Sort { .. }));
        }
        other => panic!("expected explain result, got: {other:?}"),
    }

    let result = session
        .execute_one("SELECT id, price, quantity FROM sales ORDER BY price * quantity DESC")
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(
                rows.rows,
                vec![
                    vec![
                        ast::Value::Integer(2),
                        ast::Value::Integer(5),
                        ast::Value::Integer(5)
                    ],
                    vec![
                        ast::Value::Integer(1),
                        ast::Value::Integer(10),
                        ast::Value::Integer(2)
                    ],
                    vec![
                        ast::Value::Integer(3),
                        ast::Value::Integer(7),
                        ast::Value::Integer(1)
                    ]
                ]
            );
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn explain_order_by_scalar_expression_uses_sort_plan() {
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE people (name TEXT);
            INSERT INTO people VALUES ('Bo'), ('Eleanor'), ('Mia');
            ",
        )
        .unwrap();

    let result = session
        .execute_one("EXPLAIN SELECT name FROM people ORDER BY LENGTH(name) DESC")
        .unwrap();

    match result {
        QueryResult::Explain(plan) => {
            assert!(matches!(plan, planner::PlanNode::Sort { .. }));
        }
        other => panic!("expected explain result, got: {other:?}"),
    }

    let result = session
        .execute_one("SELECT name FROM people ORDER BY LENGTH(name) DESC")
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(
                rows.rows,
                vec![
                    vec![ast::Value::Text("Eleanor".to_string())],
                    vec![ast::Value::Text("Mia".to_string())],
                    vec![ast::Value::Text("Bo".to_string())]
                ]
            );
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn order_by_projection_alias_remains_correct() {
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE sales (price INTEGER, quantity INTEGER);
            INSERT INTO sales VALUES (10, 2), (5, 5), (7, 1);
            ",
        )
        .unwrap();

    let result = session
        .execute_one("SELECT price * quantity AS total FROM sales ORDER BY total DESC")
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(
                rows.rows,
                vec![
                    vec![ast::Value::Integer(25)],
                    vec![ast::Value::Integer(20)],
                    vec![ast::Value::Integer(7)]
                ]
            );
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn explicit_json_storage_persists_between_engines() {
    let _guard = test_guard();
    let path = unique_temp_path("engine_json", "json");
    cleanup_storage_path(&path);

    {
        let engine = Engine::open(EngineOptions {
            storage: StorageMode::Json { path: path.clone() },
        })
        .unwrap();
        let mut session = engine.session();
        session
            .execute_script(
                "
                CREATE TABLE users (id INTEGER, name TEXT);
                INSERT INTO users VALUES (1, 'Alice');
                ",
            )
            .unwrap();
    }

    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Json { path: path.clone() },
    })
    .unwrap();
    let mut session = engine.session();
    let result = session
        .execute_one("SELECT name FROM users WHERE id = 1")
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(rows.rows, vec![vec![ast::Value::Text("Alice".to_string())]]);
        }
        other => panic!("expected rows result, got: {other:?}"),
    }

    cleanup_storage_path(&path);
}

#[test]
fn explicit_btree_storage_persists_between_engines() {
    let _guard = test_guard();
    let path = unique_temp_path("engine_btree", "dat");
    cleanup_storage_path(&path);

    {
        let engine = Engine::open(EngineOptions {
            storage: StorageMode::BTree { path: path.clone() },
        })
        .unwrap();
        let mut session = engine.session();
        session
            .execute_script(
                "
                CREATE TABLE users (id INTEGER, name TEXT);
                INSERT INTO users VALUES (1, 'Alice');
                ",
            )
            .unwrap();
    }

    let engine = Engine::open(EngineOptions {
        storage: StorageMode::BTree { path: path.clone() },
    })
    .unwrap();
    let mut session = engine.session();
    let result = session
        .execute_one("SELECT name FROM users WHERE id = 1")
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(rows.rows, vec![vec![ast::Value::Text("Alice".to_string())]]);
        }
        other => panic!("expected rows result, got: {other:?}"),
    }

    cleanup_storage_path(&path);
}

#[test]
fn execute_select_returns_typed_rows() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_one("CREATE TABLE users (id INTEGER, name TEXT)")
        .unwrap();
    session
        .execute_one("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    let result = session
        .execute_one("SELECT id, name FROM users ORDER BY id")
        .unwrap();

    match &result {
        QueryResult::Rows(rows) => {
            assert_eq!(rows.columns.len(), 2);
            assert_eq!(rows.columns[0].name, "id");
            assert_eq!(rows.columns[1].name, "name");
            assert_eq!(rows.rows.len(), 2);
        }
        other => panic!("expected rows result, got: {other:?}"),
    }
}

#[test]
fn explain_constant_select_returns_one_row_plan() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    let result = session.execute_one("EXPLAIN SELECT 1 AS value").unwrap();

    match result {
        QueryResult::Explain(plan) => {
            assert!(matches!(plan, planner::PlanNode::OneRow { .. }));
        }
        other => panic!("expected explain result, got: {other:?}"),
    }
}

#[test]
fn execute_joined_select_returns_typed_rows() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE users (id INTEGER, name TEXT);
            CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER);
            INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
            INSERT INTO orders VALUES (10, 1, 75), (11, 1, 25), (12, 2, 50);
            ",
        )
        .unwrap();

    let result = session
        .execute_one(
            "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id ORDER BY orders.amount",
        )
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(rows.rows.len(), 3);
            assert_eq!(
                rows.rows[0],
                vec![
                    ast::Value::Text("Alice".to_string()),
                    ast::Value::Integer(25)
                ]
            );
            assert_eq!(
                rows.rows[2],
                vec![
                    ast::Value::Text("Alice".to_string()),
                    ast::Value::Integer(75)
                ]
            );
        }
        other => panic!("expected rows result, got: {other:?}"),
    }
}

#[test]
fn execute_multi_join_returns_typed_rows() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE customers (id INTEGER, name TEXT);
            CREATE TABLE orders (id INTEGER, customer_id INTEGER, product_id INTEGER);
            CREATE TABLE products (id INTEGER, pname TEXT);
            INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob');
            INSERT INTO orders VALUES (10, 1, 100), (11, 2, 101);
            INSERT INTO products VALUES (100, 'Widget'), (101, 'Gadget');
            ",
        )
        .unwrap();

    let result = session
        .execute_one(
            "SELECT customers.name, products.pname
             FROM customers
             JOIN orders ON customers.id = orders.customer_id
             JOIN products ON orders.product_id = products.id
             ORDER BY customers.name",
        )
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(
                rows.rows,
                vec![
                    vec![
                        ast::Value::Text("Alice".to_string()),
                        ast::Value::Text("Widget".to_string())
                    ],
                    vec![
                        ast::Value::Text("Bob".to_string()),
                        ast::Value::Text("Gadget".to_string())
                    ],
                ]
            );
        }
        other => panic!("expected rows result, got: {other:?}"),
    }
}

#[test]
fn execute_join_subquery_returns_typed_rows() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE users (id INTEGER, name TEXT);
            CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER);
            INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');
            INSERT INTO orders VALUES (10, 1, 75), (11, 1, 25), (12, 2, 60), (13, 4, 90);
            ",
        )
        .unwrap();

    let result = session
        .execute_one(
            "SELECT users.name, recent.amount
             FROM users
             JOIN (
                 SELECT user_id, amount
                 FROM orders
                 WHERE amount >= 60
             ) AS recent ON users.id = recent.user_id
             ORDER BY recent.amount",
        )
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(
                rows.rows,
                vec![
                    vec![ast::Value::Text("Bob".to_string()), ast::Value::Integer(60)],
                    vec![
                        ast::Value::Text("Alice".to_string()),
                        ast::Value::Integer(75)
                    ],
                ]
            );
        }
        other => panic!("expected rows result, got: {other:?}"),
    }
}

#[test]
fn execute_lateral_join_returns_typed_rows() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE users (id INTEGER, name TEXT);
            CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER);
            INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');
            INSERT INTO orders VALUES (10, 1, 75), (11, 1, 25), (12, 2, 60);
            ",
        )
        .unwrap();

    let result = session
        .execute_one(
            "SELECT users.name, recent.amount
             FROM users
             LEFT JOIN LATERAL (
                 SELECT amount
                 FROM orders
                 WHERE orders.user_id = users.id
                 ORDER BY amount DESC
                 FETCH FIRST 1 ROW ONLY
             ) AS recent
             ORDER BY users.name",
        )
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(
                rows.rows,
                vec![
                    vec![
                        ast::Value::Text("Alice".to_string()),
                        ast::Value::Integer(75),
                    ],
                    vec![ast::Value::Text("Bob".to_string()), ast::Value::Integer(60),],
                    vec![ast::Value::Text("Carol".to_string()), ast::Value::Null],
                ]
            );
        }
        other => panic!("expected rows result, got: {other:?}"),
    }
}

#[test]
fn execute_left_join_using_returns_typed_rows() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE users (id INTEGER, name TEXT);
            CREATE TABLE scores (id INTEGER, score INTEGER);
            INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');
            INSERT INTO scores VALUES (1, 90), (2, 85);
            ",
        )
        .unwrap();

    let result = session
        .execute_one(
            "SELECT users.name, scores.score
             FROM users LEFT JOIN scores USING (id)
             ORDER BY users.name",
        )
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(
                rows.rows,
                vec![
                    vec![
                        ast::Value::Text("Alice".to_string()),
                        ast::Value::Integer(90)
                    ],
                    vec![ast::Value::Text("Bob".to_string()), ast::Value::Integer(85)],
                    vec![ast::Value::Text("Carol".to_string()), ast::Value::Null],
                ]
            );
        }
        other => panic!("expected rows result, got: {other:?}"),
    }
}

#[test]
fn execute_join_using_multi_column_returns_typed_rows() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE lhs (x INTEGER, y INTEGER, val TEXT);
            CREATE TABLE rhs (x INTEGER, y INTEGER, info TEXT);
            INSERT INTO lhs VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c');
            INSERT INTO rhs VALUES (1, 10, 'foo'), (2, 20, 'bar');
            ",
        )
        .unwrap();

    let result = session
        .execute_one(
            "SELECT lhs.val, rhs.info
             FROM lhs JOIN rhs USING (x, y)
             ORDER BY lhs.val",
        )
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(
                rows.rows,
                vec![
                    vec![
                        ast::Value::Text("a".to_string()),
                        ast::Value::Text("foo".to_string())
                    ],
                    vec![
                        ast::Value::Text("b".to_string()),
                        ast::Value::Text("bar".to_string())
                    ],
                ]
            );
        }
        other => panic!("expected rows result, got: {other:?}"),
    }
}

#[test]
fn execute_right_join_returns_typed_rows() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE customers (id INTEGER, name TEXT);
            CREATE TABLE orders (id INTEGER, customer_id INTEGER, product TEXT);
            INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob');
            INSERT INTO orders VALUES (101, 1, 'Laptop'), (102, 2, 'Keyboard'), (103, 999, 'Mouse');
            ",
        )
        .unwrap();

    let result = session
        .execute_one(
            "SELECT customers.name, orders.product
             FROM customers RIGHT JOIN orders ON customers.id = orders.customer_id
             ORDER BY orders.product",
        )
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(
                rows.rows,
                vec![
                    vec![
                        ast::Value::Text("Bob".to_string()),
                        ast::Value::Text("Keyboard".to_string())
                    ],
                    vec![
                        ast::Value::Text("Alice".to_string()),
                        ast::Value::Text("Laptop".to_string())
                    ],
                    vec![ast::Value::Null, ast::Value::Text("Mouse".to_string())],
                ]
            );
        }
        other => panic!("expected rows result, got: {other:?}"),
    }
}

#[test]
fn execute_full_join_returns_typed_rows() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE left_table (id INTEGER, name TEXT);
            CREATE TABLE right_table (id INTEGER, value TEXT);
            INSERT INTO left_table VALUES (1, 'A'), (2, 'B');
            INSERT INTO right_table VALUES (2, 'Y'), (3, 'Z');
            ",
        )
        .unwrap();

    let result = session
        .execute_one(
            "SELECT left_table.id, left_table.name, right_table.value
             FROM left_table FULL JOIN right_table ON left_table.id = right_table.id
             ORDER BY left_table.id, right_table.value",
        )
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(
                rows.rows,
                vec![
                    vec![
                        ast::Value::Integer(1),
                        ast::Value::Text("A".to_string()),
                        ast::Value::Null
                    ],
                    vec![
                        ast::Value::Integer(2),
                        ast::Value::Text("B".to_string()),
                        ast::Value::Text("Y".to_string())
                    ],
                    vec![
                        ast::Value::Null,
                        ast::Value::Null,
                        ast::Value::Text("Z".to_string())
                    ],
                ]
            );
        }
        other => panic!("expected rows result, got: {other:?}"),
    }
}

#[test]
fn execute_aggregate_on_empty_table_returns_single_row() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_one("CREATE TABLE users (id INTEGER, age INTEGER)")
        .unwrap();

    let result = session
        .execute_one("SELECT COUNT(*) AS total_users FROM users")
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(rows.columns[0].name, "total_users");
            assert_eq!(rows.rows, vec![vec![ast::Value::Integer(0)]]);
        }
        other => panic!("expected rows result, got: {other:?}"),
    }
}

#[test]
fn execute_filtered_grouped_aggregate_returns_typed_rows() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE employees (id INTEGER, dept TEXT, status TEXT);
            INSERT INTO employees VALUES
                (1, 'eng', 'active'),
                (2, 'eng', 'inactive'),
                (3, 'eng', 'active'),
                (4, 'sales', 'active'),
                (5, 'sales', 'inactive');
            ",
        )
        .unwrap();

    let result = session
        .execute_one(
            "SELECT dept, COUNT(*) FILTER (WHERE status = 'active') AS active_count
             FROM employees
             GROUP BY dept
             ORDER BY dept",
        )
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(rows.columns[0].name, "dept");
            assert_eq!(rows.columns[1].name, "active_count");
            assert_eq!(
                rows.rows,
                vec![
                    vec![ast::Value::Text("eng".to_string()), ast::Value::Integer(2)],
                    vec![
                        ast::Value::Text("sales".to_string()),
                        ast::Value::Integer(1)
                    ],
                ]
            );
        }
        other => panic!("expected rows result, got: {other:?}"),
    }
}

#[test]
fn execute_rollup_returns_typed_rows() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE sales (region TEXT, product TEXT, amount INTEGER);
            INSERT INTO sales VALUES
                ('East', 'Widget', 100),
                ('East', 'Gadget', 150),
                ('West', 'Widget', 200),
                ('West', 'Gadget', 250);
            ",
        )
        .unwrap();

    let result = session
        .execute_one(
            "SELECT region, product, COUNT(*) AS total
             FROM sales
             GROUP BY ROLLUP(region, product)",
        )
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            let actual: BTreeSet<Vec<ast::Value>> = rows.rows.into_iter().collect();
            let expected: BTreeSet<Vec<ast::Value>> = [
                vec![
                    ast::Value::Text("East".to_string()),
                    ast::Value::Text("Widget".to_string()),
                    ast::Value::Integer(1),
                ],
                vec![
                    ast::Value::Text("East".to_string()),
                    ast::Value::Text("Gadget".to_string()),
                    ast::Value::Integer(1),
                ],
                vec![
                    ast::Value::Text("East".to_string()),
                    ast::Value::Null,
                    ast::Value::Integer(2),
                ],
                vec![
                    ast::Value::Text("West".to_string()),
                    ast::Value::Text("Widget".to_string()),
                    ast::Value::Integer(1),
                ],
                vec![
                    ast::Value::Text("West".to_string()),
                    ast::Value::Text("Gadget".to_string()),
                    ast::Value::Integer(1),
                ],
                vec![
                    ast::Value::Text("West".to_string()),
                    ast::Value::Null,
                    ast::Value::Integer(2),
                ],
                vec![ast::Value::Null, ast::Value::Null, ast::Value::Integer(4)],
            ]
            .into_iter()
            .collect();

            assert_eq!(actual, expected);
        }
        other => panic!("expected rows result, got: {other:?}"),
    }
}

#[test]
fn execute_window_row_number_returns_typed_rows() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE scores (id INTEGER, name TEXT, score INTEGER);
            INSERT INTO scores VALUES
                (1, 'Alice', 90),
                (2, 'Bob', 85),
                (3, 'Charlie', 95);
            ",
        )
        .unwrap();

    let result = session
        .execute_one(
            "SELECT name, score, ROW_NUMBER() OVER (ORDER BY score DESC) AS rnum
             FROM scores
             ORDER BY score DESC",
        )
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(
                rows.rows,
                vec![
                    vec![
                        ast::Value::Text("Charlie".to_string()),
                        ast::Value::Integer(95),
                        ast::Value::Integer(1),
                    ],
                    vec![
                        ast::Value::Text("Alice".to_string()),
                        ast::Value::Integer(90),
                        ast::Value::Integer(2),
                    ],
                    vec![
                        ast::Value::Text("Bob".to_string()),
                        ast::Value::Integer(85),
                        ast::Value::Integer(3),
                    ],
                ]
            );
        }
        other => panic!("expected rows result, got: {other:?}"),
    }
}

#[test]
fn execute_fetch_first_returns_typed_rows() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE users (id INTEGER, name TEXT);
            INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');
            ",
        )
        .unwrap();

    let result = session
        .execute_one("SELECT id, name FROM users ORDER BY id FETCH FIRST 2 ROWS ONLY")
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(rows.rows.len(), 2);
            assert_eq!(
                rows.rows,
                vec![
                    vec![
                        ast::Value::Integer(1),
                        ast::Value::Text("Alice".to_string())
                    ],
                    vec![ast::Value::Integer(2), ast::Value::Text("Bob".to_string())],
                ]
            );
        }
        other => panic!("expected rows result, got: {other:?}"),
    }
}

#[test]
fn execute_fetch_with_ties_returns_typed_rows() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE scores (id INTEGER, score INTEGER);
            INSERT INTO scores VALUES (1, 100), (2, 90), (3, 90), (4, 80);
            ",
        )
        .unwrap();

    let result = session
        .execute_one(
            "SELECT id, score FROM scores ORDER BY score DESC FETCH FIRST 2 ROWS WITH TIES",
        )
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(
                rows.rows,
                vec![
                    vec![ast::Value::Integer(1), ast::Value::Integer(100)],
                    vec![ast::Value::Integer(2), ast::Value::Integer(90)],
                    vec![ast::Value::Integer(3), ast::Value::Integer(90)],
                ]
            );
        }
        other => panic!("expected rows result, got: {other:?}"),
    }
}

#[test]
fn execute_distinct_on_returns_typed_rows() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE items (category TEXT, item TEXT, price INTEGER);
            INSERT INTO items VALUES
                ('fruit', 'apple', 1),
                ('fruit', 'banana', 2),
                ('veggie', 'carrot', 3),
                ('veggie', 'broccoli', 4);
            ",
        )
        .unwrap();

    let result = session
        .execute_one(
            "SELECT DISTINCT ON (category) category, item, price
             FROM items
             ORDER BY category, price",
        )
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(
                rows.rows,
                vec![
                    vec![
                        ast::Value::Text("fruit".to_string()),
                        ast::Value::Text("apple".to_string()),
                        ast::Value::Integer(1),
                    ],
                    vec![
                        ast::Value::Text("veggie".to_string()),
                        ast::Value::Text("carrot".to_string()),
                        ast::Value::Integer(3),
                    ],
                ]
            );
        }
        other => panic!("expected rows result, got: {other:?}"),
    }
}

#[test]
fn execute_from_values_returns_typed_rows() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    let result = session
        .execute_one(
            "SELECT id, name
             FROM (VALUES (3, 'Carol'), (1, 'Alice'), (2, 'Bob')) AS t(id, name)
             WHERE id > 1
             ORDER BY id",
        )
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(
                rows.rows,
                vec![
                    vec![ast::Value::Integer(2), ast::Value::Text("Bob".to_string())],
                    vec![
                        ast::Value::Integer(3),
                        ast::Value::Text("Carol".to_string())
                    ],
                ]
            );
        }
        other => panic!("expected rows result, got: {other:?}"),
    }
}

#[test]
fn explain_from_values_returns_plan() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    let result = session
        .execute_one(
            "EXPLAIN SELECT id
             FROM (VALUES (3), (1), (2)) AS t(id)
             ORDER BY id
             FETCH FIRST 2 ROWS ONLY",
        )
        .unwrap();

    match result {
        QueryResult::Explain(plan) => {
            assert!(matches!(plan, planner::PlanNode::Limit { .. }));
            assert!(format!("{plan}").contains("Values Scan"));
        }
        other => panic!("expected explain result, got: {other:?}"),
    }
}

#[test]
fn explain_from_subquery_returns_plan() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE users (id INTEGER, name TEXT);
            INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');
            ",
        )
        .unwrap();

    let result = session
        .execute_one(
            "EXPLAIN SELECT name
             FROM (SELECT id, name FROM users WHERE id > 1) AS filtered
             ORDER BY name",
        )
        .unwrap();

    match result {
        QueryResult::Explain(plan) => {
            assert!(matches!(plan, planner::PlanNode::Sort { .. }));
            assert!(format!("{plan}").contains("Subquery Scan on filtered"));
        }
        other => panic!("expected explain result, got: {other:?}"),
    }
}

#[test]
fn explain_join_subquery_returns_plan() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE users (id INTEGER, name TEXT);
            CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER);
            INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
            INSERT INTO orders VALUES (10, 1, 75), (11, 2, 60);
            ",
        )
        .unwrap();

    let result = session
        .execute_one(
            "EXPLAIN SELECT users.name, recent.amount
             FROM users
             JOIN (
                 SELECT user_id, amount
                 FROM orders
                 WHERE amount >= 60
             ) AS recent ON users.id = recent.user_id",
        )
        .unwrap();

    match result {
        QueryResult::Explain(plan) => {
            assert!(matches!(
                plan,
                planner::PlanNode::HashJoin { .. } | planner::PlanNode::NestedLoopJoin { .. }
            ));
            assert!(format!("{plan}").contains("Subquery Scan on recent"));
        }
        other => panic!("expected explain result, got: {other:?}"),
    }
}

#[test]
fn explain_lateral_join_returns_plan() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE users (id INTEGER, name TEXT);
            CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER);
            INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
            INSERT INTO orders VALUES (10, 1, 75), (11, 2, 60);
            ",
        )
        .unwrap();

    let result = session
        .execute_one(
            "EXPLAIN SELECT users.name, recent.amount
             FROM users
             LEFT JOIN LATERAL (
                 SELECT amount
                 FROM orders
                 WHERE orders.user_id = users.id
                 ORDER BY amount DESC
                 FETCH FIRST 1 ROW ONLY
             ) AS recent",
        )
        .unwrap();

    match result {
        QueryResult::Explain(plan) => {
            assert!(matches!(plan, planner::PlanNode::LateralJoin { .. }));
        }
        other => panic!("expected explain result, got: {other:?}"),
    }
}

#[test]
fn explain_cte_returns_plan() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_one("CREATE TABLE users (id INTEGER, age INTEGER)")
        .unwrap();
    session
        .execute_one("INSERT INTO users VALUES (1, 25), (2, 40), (3, 50)")
        .unwrap();

    let result = session
        .execute_one(
            "EXPLAIN WITH adults AS (
                 SELECT id, age FROM users WHERE age >= 40
             )
             SELECT id FROM adults ORDER BY id",
        )
        .unwrap();

    match result {
        QueryResult::Explain(plan) => {
            assert!(matches!(plan, planner::PlanNode::Sort { .. }));
            assert!(format!("{plan}").contains("CTE Scan on adults"));
        }
        other => panic!("expected explain result, got: {other:?}"),
    }
}

#[test]
fn explain_view_returns_view_scan_plan() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE users (id INTEGER, age INTEGER);
            INSERT INTO users VALUES (1, 25), (2, 40);
            CREATE VIEW adults AS SELECT id, age FROM users WHERE age >= 40;
            ",
        )
        .unwrap();

    let result = session
        .execute_one("EXPLAIN SELECT id FROM adults")
        .unwrap();

    match result {
        QueryResult::Explain(plan) => {
            assert!(matches!(plan, planner::PlanNode::ViewScan { .. }));
        }
        other => panic!("expected explain result, got: {other:?}"),
    }
}

#[test]
fn explain_recursive_cte_returns_recursive_cte_scan_plan() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    let result = session
        .execute_one(
            "EXPLAIN WITH RECURSIVE nums AS (
                 SELECT 1 AS n
                 UNION ALL
                 SELECT n + 1 FROM nums WHERE n < 3
             )
             SELECT n FROM nums",
        )
        .unwrap();

    match result {
        QueryResult::Explain(plan) => {
            assert!(matches!(plan, planner::PlanNode::RecursiveCteScan { .. }));
        }
        other => panic!("expected explain result, got: {other:?}"),
    }
}

#[test]
fn explain_rollup_returns_plan() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE sales (region TEXT, product TEXT, amount INTEGER);
            INSERT INTO sales VALUES ('East', 'Widget', 100), ('West', 'Gadget', 200);
            ",
        )
        .unwrap();

    let result = session
        .execute_one(
            "EXPLAIN SELECT region, product, COUNT(*) AS total
             FROM sales
             GROUP BY ROLLUP(region, product)",
        )
        .unwrap();

    match result {
        QueryResult::Explain(plan) => {
            assert!(matches!(plan, planner::PlanNode::Aggregate { .. }));
        }
        other => panic!("expected explain result, got: {other:?}"),
    }
}

#[test]
fn execute_generate_series_returns_typed_rows() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    let result = session
        .execute_one(
            "SELECT generate_series
             FROM GENERATE_SERIES(1, 10)
             WHERE generate_series > 7
             ORDER BY generate_series DESC
             FETCH FIRST 2 ROWS ONLY",
        )
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(
                rows.rows,
                vec![vec![ast::Value::Integer(10)], vec![ast::Value::Integer(9)],]
            );
        }
        other => panic!("expected rows result, got: {other:?}"),
    }
}

#[test]
fn explain_generate_series_returns_function_scan_plan() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    let result = session
        .execute_one("EXPLAIN SELECT * FROM GENERATE_SERIES(1, 5)")
        .unwrap();

    match result {
        QueryResult::Explain(plan) => {
            assert!(matches!(plan, planner::PlanNode::FunctionScan { .. }));
        }
        other => panic!("expected explain result, got: {other:?}"),
    }
}

#[test]
fn explain_distinct_on_returns_plan() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE items (category TEXT, item TEXT, price INTEGER);
            INSERT INTO items VALUES
                ('fruit', 'apple', 1),
                ('fruit', 'banana', 2),
                ('veggie', 'carrot', 3);
            ",
        )
        .unwrap();

    let result = session
        .execute_one(
            "EXPLAIN SELECT DISTINCT ON (category) category, item, price
             FROM items
             ORDER BY category, price",
        )
        .unwrap();

    match result {
        QueryResult::Explain(plan) => {
            assert!(matches!(plan, planner::PlanNode::DistinctOn { .. }));
        }
        other => panic!("expected explain result, got: {other:?}"),
    }
}

#[test]
fn explain_natural_join_returns_plan() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE departments (id INTEGER, dept_name TEXT);
            CREATE TABLE staff (id INTEGER, staff_name TEXT);
            INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales');
            INSERT INTO staff VALUES (1, 'Alice'), (2, 'Bob');
            ",
        )
        .unwrap();

    let result = session
        .execute_one(
            "EXPLAIN SELECT dept_name, staff_name
             FROM departments NATURAL JOIN staff",
        )
        .unwrap();

    match result {
        QueryResult::Explain(plan) => {
            assert!(matches!(
                plan,
                planner::PlanNode::NestedLoopJoin {
                    join_type: ast::JoinType::Natural,
                    ..
                }
            ));
        }
        other => panic!("expected explain result, got: {other:?}"),
    }
}

#[test]
fn explain_full_join_returns_plan() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE left_table (id INTEGER, name TEXT);
            CREATE TABLE right_table (id INTEGER, value TEXT);
            INSERT INTO left_table VALUES (1, 'A'), (2, 'B');
            INSERT INTO right_table VALUES (2, 'Y'), (3, 'Z');
            ",
        )
        .unwrap();

    let result = session
        .execute_one(
            "EXPLAIN SELECT *
             FROM left_table FULL JOIN right_table ON left_table.id = right_table.id",
        )
        .unwrap();

    match result {
        QueryResult::Explain(plan) => {
            assert!(matches!(
                plan,
                planner::PlanNode::NestedLoopJoin {
                    join_type: ast::JoinType::Full,
                    ..
                }
            ));
        }
        other => panic!("expected explain result, got: {other:?}"),
    }
}

#[test]
fn explain_multi_join_returns_plan() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE customers (id INTEGER, name TEXT);
            CREATE TABLE orders (id INTEGER, customer_id INTEGER, product_id INTEGER);
            CREATE TABLE products (id INTEGER, pname TEXT);
            INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob');
            INSERT INTO orders VALUES (10, 1, 100), (11, 2, 101);
            INSERT INTO products VALUES (100, 'Widget'), (101, 'Gadget');
            ",
        )
        .unwrap();

    let result = session
        .execute_one(
            "EXPLAIN SELECT customers.name, products.pname
             FROM customers
             JOIN orders ON customers.id = orders.customer_id
             JOIN products ON orders.product_id = products.id",
        )
        .unwrap();

    match result {
        QueryResult::Explain(plan) => {
            assert!(matches!(plan, planner::PlanNode::NestedLoopJoin { .. }));
        }
        other => panic!("expected explain result, got: {other:?}"),
    }
}

#[test]
fn execute_update_returns_command_tag_and_affected_rows() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_one("CREATE TABLE users (id INTEGER, name TEXT)")
        .unwrap();
    session
        .execute_one("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    let result = session
        .execute_one("UPDATE users SET name = 'Updated' WHERE id = 1")
        .unwrap();

    match &result {
        QueryResult::Command(command) => {
            assert_eq!(command.tag, CommandTag::Update);
            assert_eq!(command.affected, 1);
        }
        other => panic!("expected command result, got: {other:?}"),
    }
}

#[test]
fn execute_script_returns_typed_results() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    let results = session
        .execute_script(
            "
            CREATE TABLE users (id INTEGER, name TEXT);
            INSERT INTO users VALUES (1, 'Alice');
            SELECT id, name FROM users;
            ",
        )
        .unwrap();

    assert_eq!(results.len(), 3);

    match &results[0] {
        QueryResult::Command(command) => {
            assert_eq!(command.tag, CommandTag::CreateTable);
            assert_eq!(command.affected, 0);
        }
        other => panic!("expected create-table command result, got: {other:?}"),
    }

    match &results[1] {
        QueryResult::Command(command) => {
            assert_eq!(command.tag, CommandTag::Insert);
            assert_eq!(command.affected, 1);
        }
        other => panic!("expected insert command result, got: {other:?}"),
    }

    match &results[2] {
        QueryResult::Rows(rows) => {
            assert_eq!(rows.columns.len(), 2);
            assert_eq!(rows.rows.len(), 1);
            assert_eq!(rows.rows[0][0], ast::Value::Integer(1));
            assert_eq!(rows.rows[0][1], ast::Value::Text("Alice".to_string()));
        }
        other => panic!("expected row result, got: {other:?}"),
    }
}

#[test]
fn explain_returns_same_plan_as_planner() {
    let _guard = test_guard();
    let engine = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let mut session = engine.session();

    session
        .execute_script(
            "
            CREATE TABLE users (id INTEGER, age INTEGER);
            INSERT INTO users VALUES (1, 25), (2, 40);
            CREATE INDEX idx_age ON users (age);
            ",
        )
        .unwrap();

    let select_sql = "SELECT * FROM users WHERE age > 30";
    let statement = parser::parse(lexer::tokenize(select_sql).unwrap()).unwrap();
    let expected_plan = match statement {
        ast::Statement::Select(stmt) => {
            planner::plan_query(&engine.snapshot_database(), &stmt).unwrap()
        }
        other => panic!("expected select statement, got: {other:?}"),
    };

    let result = session
        .execute_one("EXPLAIN SELECT * FROM users WHERE age > 30")
        .unwrap();

    match result {
        QueryResult::Explain(plan) => assert_eq!(plan, expected_plan),
        other => panic!("expected explain result, got: {other:?}"),
    }
}

#[test]
fn separate_engines_do_not_share_state() {
    let engine_a = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();
    let engine_b = Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap();

    std::thread::scope(|scope| {
        let thread_a = scope.spawn(|| {
            let mut session = engine_a.session();
            session
                .execute_script(
                    "
                    CREATE TABLE items (id INTEGER, name TEXT);
                    INSERT INTO items VALUES (1, 'left');
                    ",
                )
                .unwrap();
            session.execute_one("SELECT name FROM items").unwrap()
        });

        let thread_b = scope.spawn(|| {
            let mut session = engine_b.session();
            session
                .execute_script(
                    "
                    CREATE TABLE items (id INTEGER, name TEXT);
                    INSERT INTO items VALUES (1, 'right');
                    ",
                )
                .unwrap();
            session.execute_one("SELECT name FROM items").unwrap()
        });

        let result_a = thread_a.join().unwrap();
        let result_b = thread_b.join().unwrap();

        match result_a {
            QueryResult::Rows(rows) => {
                assert_eq!(rows.rows, vec![vec![ast::Value::Text("left".to_string())]]);
            }
            other => panic!("expected rows for engine A, got: {other:?}"),
        }

        match result_b {
            QueryResult::Rows(rows) => {
                assert_eq!(rows.rows, vec![vec![ast::Value::Text("right".to_string())]]);
            }
            other => panic!("expected rows for engine B, got: {other:?}"),
        }
    });
}
