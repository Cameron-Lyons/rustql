use rustql::{process_query, reset_database};
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup_test<'a>() -> std::sync::MutexGuard<'a, ()> {
    let guard = TEST_MUTEX.lock().unwrap();
    reset_database();
    guard
}

#[test]
fn test_truncate_table() {
    let _guard = setup_test();
    process_query("CREATE TABLE t1 (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO t1 VALUES (1, 'Alice')").unwrap();
    process_query("INSERT INTO t1 VALUES (2, 'Bob')").unwrap();

    let result = process_query("TRUNCATE TABLE t1");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Table 't1' truncated");

    let select = process_query("SELECT * FROM t1").unwrap();
    let lines: Vec<&str> = select.lines().collect();
    assert_eq!(lines.len(), 2);
}

#[test]
fn test_truncate_without_table_keyword() {
    let _guard = setup_test();
    process_query("CREATE TABLE t2 (id INTEGER)").unwrap();
    process_query("INSERT INTO t2 VALUES (1)").unwrap();

    let result = process_query("TRUNCATE t2");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Table 't2' truncated");
}

#[test]
fn test_truncate_nonexistent_table() {
    let _guard = setup_test();
    let result = process_query("TRUNCATE TABLE nonexistent");
    assert!(result.is_err());
}

#[test]
fn test_create_view_and_select() {
    let _guard = setup_test();
    process_query("CREATE TABLE employees (id INTEGER, name TEXT, dept TEXT)").unwrap();
    process_query("INSERT INTO employees VALUES (1, 'Alice', 'Engineering')").unwrap();
    process_query("INSERT INTO employees VALUES (2, 'Bob', 'Sales')").unwrap();
    process_query("INSERT INTO employees VALUES (3, 'Carol', 'Engineering')").unwrap();

    let result = process_query(
        "CREATE VIEW eng_employees AS SELECT id, name FROM employees WHERE dept = 'Engineering'",
    );
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "View 'eng_employees' created");

    let select = process_query("SELECT * FROM eng_employees").unwrap();
    assert!(select.contains("Alice"));
    assert!(select.contains("Carol"));
    assert!(!select.contains("Bob"));
}

#[test]
fn test_drop_view() {
    let _guard = setup_test();
    process_query("CREATE TABLE v_table (id INTEGER)").unwrap();
    process_query("CREATE VIEW v1 AS SELECT id FROM v_table").unwrap();

    let result = process_query("DROP VIEW v1");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "View 'v1' dropped");
}

#[test]
fn test_drop_view_if_exists() {
    let _guard = setup_test();
    let result = process_query("DROP VIEW IF EXISTS nonexistent_view");
    assert!(result.is_ok(), "Expected Ok but got: {:?}", result);
}

#[test]
fn test_drop_view_nonexistent_error() {
    let _guard = setup_test();
    let result = process_query("DROP VIEW nonexistent_view");
    assert!(result.is_err());
}

#[test]
fn test_create_view_duplicate_error() {
    let _guard = setup_test();
    process_query("CREATE TABLE vd_table (id INTEGER)").unwrap();
    process_query("CREATE VIEW vdup AS SELECT id FROM vd_table").unwrap();
    let result = process_query("CREATE VIEW vdup AS SELECT id FROM vd_table");
    assert!(result.is_err());
}

#[test]
fn test_create_table_as_select() {
    let _guard = setup_test();
    process_query("CREATE TABLE source (id INTEGER, name TEXT, score INTEGER)").unwrap();
    process_query("INSERT INTO source VALUES (1, 'Alice', 90)").unwrap();
    process_query("INSERT INTO source VALUES (2, 'Bob', 80)").unwrap();
    process_query("INSERT INTO source VALUES (3, 'Carol', 95)").unwrap();

    let result =
        process_query("CREATE TABLE top_scorers AS SELECT id, name FROM source WHERE score > 85");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Table 'top_scorers' created");

    let select = process_query("SELECT * FROM top_scorers").unwrap();
    assert!(select.contains("Alice"));
    assert!(select.contains("Carol"));
    assert!(!select.contains("Bob"));
}

#[test]
fn test_ctas_preserves_types() {
    let _guard = setup_test();
    process_query("CREATE TABLE typed_src (id INTEGER, price FLOAT, name TEXT)").unwrap();
    process_query("INSERT INTO typed_src VALUES (1, 9.99, 'Alice')").unwrap();

    process_query("CREATE TABLE typed_copy AS SELECT * FROM typed_src").unwrap();

    let desc = process_query("DESCRIBE typed_copy").unwrap();
    assert!(desc.contains("id"));
    assert!(desc.contains("price"));
    assert!(desc.contains("name"));

    let select = process_query("SELECT * FROM typed_copy").unwrap();
    assert!(select.contains("9.99"));
    assert!(select.contains("Alice"));
}

#[test]
fn test_insert_on_conflict_do_nothing() {
    let _guard = setup_test();
    process_query("CREATE TABLE upsert1 (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    process_query("INSERT INTO upsert1 VALUES (1, 'Alice')").unwrap();

    let result = process_query("INSERT INTO upsert1 VALUES (1, 'Bob') ON CONFLICT (id) DO NOTHING");
    assert!(result.is_ok());
    assert!(result.unwrap().contains("0 row(s) inserted"));

    let select = process_query("SELECT name FROM upsert1 WHERE id = 1").unwrap();
    assert!(select.contains("Alice"));
    assert!(!select.contains("Bob"));
}

#[test]
fn test_insert_on_conflict_do_update() {
    let _guard = setup_test();
    process_query("CREATE TABLE upsert2 (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
        .unwrap();
    process_query("INSERT INTO upsert2 VALUES (1, 'Alice', 80)").unwrap();

    let result = process_query(
        "INSERT INTO upsert2 VALUES (1, 'Alice', 95) ON CONFLICT (id) DO UPDATE SET score = 95",
    );
    assert!(result.is_ok());

    let select = process_query("SELECT score FROM upsert2 WHERE id = 1").unwrap();
    assert!(select.contains("95"));
}

#[test]
fn test_insert_on_conflict_no_conflict() {
    let _guard = setup_test();
    process_query("CREATE TABLE upsert3 (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    process_query("INSERT INTO upsert3 VALUES (1, 'Alice')").unwrap();

    let result = process_query("INSERT INTO upsert3 VALUES (2, 'Bob') ON CONFLICT (id) DO NOTHING");
    assert!(result.is_ok());
    assert!(result.unwrap().contains("1 row(s) inserted"));

    let select = process_query("SELECT * FROM upsert3").unwrap();
    assert!(select.contains("Alice"));
    assert!(select.contains("Bob"));
}

#[test]
fn test_insert_on_conflict_unique() {
    let _guard = setup_test();
    process_query("CREATE TABLE upsert4 (id INTEGER, email TEXT UNIQUE)").unwrap();
    process_query("INSERT INTO upsert4 VALUES (1, 'alice@test.com')").unwrap();

    let result = process_query(
        "INSERT INTO upsert4 VALUES (2, 'alice@test.com') ON CONFLICT (email) DO NOTHING",
    );
    assert!(result.is_ok());
    assert!(result.unwrap().contains("0 row(s) inserted"));
}

#[test]
fn test_update_with_expression() {
    let _guard = setup_test();
    process_query("CREATE TABLE scores (id INTEGER, score INTEGER)").unwrap();
    process_query("INSERT INTO scores VALUES (1, 80)").unwrap();
    process_query("INSERT INTO scores VALUES (2, 90)").unwrap();

    process_query("UPDATE scores SET score = score + 10 WHERE id = 1").unwrap();

    let select = process_query("SELECT score FROM scores WHERE id = 1").unwrap();
    assert!(select.contains("90"));
}

#[test]
fn test_view_with_aggregate() {
    let _guard = setup_test();
    process_query("CREATE TABLE sales (id INTEGER, dept TEXT, amount INTEGER)").unwrap();
    process_query("INSERT INTO sales VALUES (1, 'A', 100)").unwrap();
    process_query("INSERT INTO sales VALUES (2, 'A', 200)").unwrap();
    process_query("INSERT INTO sales VALUES (3, 'B', 150)").unwrap();

    process_query(
        "CREATE VIEW dept_totals AS SELECT dept, SUM(amount) AS total FROM sales GROUP BY dept",
    )
    .unwrap();

    let select = process_query("SELECT * FROM dept_totals").unwrap();
    assert!(select.contains("A"));
    assert!(select.contains("300"));
    assert!(select.contains("B"));
    assert!(select.contains("150"));
}
