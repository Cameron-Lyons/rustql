mod common;
use common::*;
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
    execute_sql("CREATE TABLE t1 (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO t1 VALUES (1, 'Alice')").unwrap();
    execute_sql("INSERT INTO t1 VALUES (2, 'Bob')").unwrap();

    let result = execute_sql("TRUNCATE TABLE t1");
    assert!(result.is_ok());
    assert_command(result.unwrap(), CommandTag::TruncateTable, 0);

    let select = execute_sql("SELECT * FROM t1").unwrap();
    let lines: Vec<String> = select.lines().collect();
    assert_eq!(lines.len(), 2);
}

#[test]
fn test_truncate_without_table_keyword() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE t2 (id INTEGER)").unwrap();
    execute_sql("INSERT INTO t2 VALUES (1)").unwrap();

    let result = execute_sql("TRUNCATE t2");
    assert!(result.is_ok());
    assert_command(result.unwrap(), CommandTag::TruncateTable, 0);
}

#[test]
fn test_truncate_nonexistent_table() {
    let _guard = setup_test();
    let result = execute_sql("TRUNCATE TABLE nonexistent");
    assert!(result.is_err());
}

#[test]
fn test_create_view_and_select() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE employees (id INTEGER, name TEXT, dept TEXT)").unwrap();
    execute_sql("INSERT INTO employees VALUES (1, 'Alice', 'Engineering')").unwrap();
    execute_sql("INSERT INTO employees VALUES (2, 'Bob', 'Sales')").unwrap();
    execute_sql("INSERT INTO employees VALUES (3, 'Carol', 'Engineering')").unwrap();

    let result = execute_sql(
        "CREATE VIEW eng_employees AS SELECT id, name FROM employees WHERE dept = 'Engineering'",
    );
    assert!(result.is_ok());
    assert_command(result.unwrap(), CommandTag::CreateView, 0);

    let select = execute_sql("SELECT * FROM eng_employees").unwrap();
    assert!(select.contains("Alice"));
    assert!(select.contains("Carol"));
    assert!(!select.contains("Bob"));
}

#[test]
fn test_drop_view() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE v_table (id INTEGER)").unwrap();
    execute_sql("CREATE VIEW v1 AS SELECT id FROM v_table").unwrap();

    let result = execute_sql("DROP VIEW v1");
    assert!(result.is_ok());
    assert_command(result.unwrap(), CommandTag::DropView, 0);
}

#[test]
fn test_drop_view_if_exists() {
    let _guard = setup_test();
    let result = execute_sql("DROP VIEW IF EXISTS nonexistent_view");
    assert!(result.is_ok(), "Expected Ok but got: {:?}", result);
}

#[test]
fn test_drop_view_nonexistent_error() {
    let _guard = setup_test();
    let result = execute_sql("DROP VIEW nonexistent_view");
    assert!(result.is_err());
}

#[test]
fn test_create_view_duplicate_error() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE vd_table (id INTEGER)").unwrap();
    execute_sql("CREATE VIEW vdup AS SELECT id FROM vd_table").unwrap();
    let result = execute_sql("CREATE VIEW vdup AS SELECT id FROM vd_table");
    assert!(result.is_err());
}

#[test]
fn test_create_table_as_select() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE source (id INTEGER, name TEXT, score INTEGER)").unwrap();
    execute_sql("INSERT INTO source VALUES (1, 'Alice', 90)").unwrap();
    execute_sql("INSERT INTO source VALUES (2, 'Bob', 80)").unwrap();
    execute_sql("INSERT INTO source VALUES (3, 'Carol', 95)").unwrap();

    let result =
        execute_sql("CREATE TABLE top_scorers AS SELECT id, name FROM source WHERE score > 85");
    assert!(result.is_ok());
    assert_command(result.unwrap(), CommandTag::CreateTable, 2);

    let select = execute_sql("SELECT * FROM top_scorers").unwrap();
    assert!(select.contains("Alice"));
    assert!(select.contains("Carol"));
    assert!(!select.contains("Bob"));
}

#[test]
fn test_ctas_preserves_types() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE typed_src (id INTEGER, price FLOAT, name TEXT)").unwrap();
    execute_sql("INSERT INTO typed_src VALUES (1, 9.99, 'Alice')").unwrap();

    execute_sql("CREATE TABLE typed_copy AS SELECT * FROM typed_src").unwrap();

    let desc = execute_sql("DESCRIBE typed_copy").unwrap();
    assert!(desc.contains("id"));
    assert!(desc.contains("price"));
    assert!(desc.contains("name"));

    let select = execute_sql("SELECT * FROM typed_copy").unwrap();
    assert!(select.contains("9.99"));
    assert!(select.contains("Alice"));
}

#[test]
fn test_insert_on_conflict_do_nothing() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE upsert1 (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    execute_sql("INSERT INTO upsert1 VALUES (1, 'Alice')").unwrap();

    let result = execute_sql("INSERT INTO upsert1 VALUES (1, 'Bob') ON CONFLICT (id) DO NOTHING");
    assert!(result.is_ok());
    assert_command(result.unwrap(), CommandTag::Insert, 0);

    let select = execute_sql("SELECT name FROM upsert1 WHERE id = 1").unwrap();
    assert!(select.contains("Alice"));
    assert!(!select.contains("Bob"));
}

#[test]
fn test_insert_on_conflict_do_update() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE upsert2 (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)").unwrap();
    execute_sql("INSERT INTO upsert2 VALUES (1, 'Alice', 80)").unwrap();

    let result = execute_sql(
        "INSERT INTO upsert2 VALUES (1, 'Alice', 95) ON CONFLICT (id) DO UPDATE SET score = 95",
    );
    assert!(result.is_ok());

    let select = execute_sql("SELECT score FROM upsert2 WHERE id = 1").unwrap();
    assert!(select.contains("95"));
}

#[test]
fn test_insert_on_conflict_no_conflict() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE upsert3 (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    execute_sql("INSERT INTO upsert3 VALUES (1, 'Alice')").unwrap();

    let result = execute_sql("INSERT INTO upsert3 VALUES (2, 'Bob') ON CONFLICT (id) DO NOTHING");
    assert!(result.is_ok());
    assert_command(result.unwrap(), CommandTag::Insert, 1);

    let select = execute_sql("SELECT * FROM upsert3").unwrap();
    assert!(select.contains("Alice"));
    assert!(select.contains("Bob"));
}

#[test]
fn test_insert_on_conflict_unique() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE upsert4 (id INTEGER, email TEXT UNIQUE)").unwrap();
    execute_sql("INSERT INTO upsert4 VALUES (1, 'alice@test.com')").unwrap();

    let result = execute_sql(
        "INSERT INTO upsert4 VALUES (2, 'alice@test.com') ON CONFLICT (email) DO NOTHING",
    );
    assert!(result.is_ok());
    assert_command(result.unwrap(), CommandTag::Insert, 0);
}

#[test]
fn test_update_with_expression() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE scores (id INTEGER, score INTEGER)").unwrap();
    execute_sql("INSERT INTO scores VALUES (1, 80)").unwrap();
    execute_sql("INSERT INTO scores VALUES (2, 90)").unwrap();

    execute_sql("UPDATE scores SET score = score + 10 WHERE id = 1").unwrap();

    let select = execute_sql("SELECT score FROM scores WHERE id = 1").unwrap();
    assert!(select.contains("90"));
}

#[test]
fn test_view_with_aggregate() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE sales (id INTEGER, dept TEXT, amount INTEGER)").unwrap();
    execute_sql("INSERT INTO sales VALUES (1, 'A', 100)").unwrap();
    execute_sql("INSERT INTO sales VALUES (2, 'A', 200)").unwrap();
    execute_sql("INSERT INTO sales VALUES (3, 'B', 150)").unwrap();

    execute_sql(
        "CREATE VIEW dept_totals AS SELECT dept, SUM(amount) AS total FROM sales GROUP BY dept",
    )
    .unwrap();

    let select = execute_sql("SELECT * FROM dept_totals").unwrap();
    assert!(select.contains("A"));
    assert!(select.contains("300"));
    assert!(select.contains("B"));
    assert!(select.contains("150"));
}
