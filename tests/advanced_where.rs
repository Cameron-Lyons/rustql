mod common;
use common::*;
use rustql::ast::Value;
use std::sync::Mutex;

static GLOBAL_TEST_LOCK: Mutex<()> = Mutex::new(());

fn setup_test() -> std::sync::MutexGuard<'static, ()> {
    let guard = GLOBAL_TEST_LOCK.lock().unwrap();
    reset_database();
    guard
}

#[test]
fn test_in_operator() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    execute_sql("INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 35), (4, 'David', 40)").unwrap();

    let result = execute_sql("SELECT name FROM users WHERE age IN (25, 35, 40)").unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("Charlie"));
    assert!(result.contains("David"));
    assert!(!result.contains("Bob"));

    let result = execute_sql("SELECT name FROM users WHERE name IN ('Alice', 'Charlie')").unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("Charlie"));
    assert!(!result.contains("Bob"));
}

#[test]
fn test_like_operator() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE products (id INTEGER, name TEXT, category TEXT)").unwrap();
    execute_sql("INSERT INTO products VALUES (1, 'Laptop Computer', 'Electronics'), (2, 'Desktop PC', 'Electronics'), (3, 'Mouse Pad', 'Accessories'), (4, 'Keyboard', 'Accessories')").unwrap();

    let result = execute_sql("SELECT name FROM products WHERE name LIKE '%Computer%'").unwrap();
    assert!(result.contains("Laptop Computer"));
    assert!(!result.contains("Desktop PC"));
    assert!(!result.contains("Mouse Pad"));

    let result = execute_sql("SELECT name FROM products WHERE category LIKE '%Electron%'").unwrap();
    assert!(result.contains("Laptop Computer"));
    assert!(result.contains("Desktop PC"));
    assert!(!result.contains("Mouse Pad"));

    let result = execute_sql("SELECT name FROM products WHERE name LIKE 'Laptop%'").unwrap();
    assert!(result.contains("Laptop Computer"));
    assert!(!result.contains("Desktop PC"));
}

#[test]
fn test_between_operator() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE employees (id INTEGER, name TEXT, salary FLOAT)").unwrap();
    execute_sql("INSERT INTO employees VALUES (1, 'Alice', 50000.0), (2, 'Bob', 75000.0), (3, 'Charlie', 100000.0), (4, 'David', 125000.0)").unwrap();

    let result =
        execute_sql("SELECT name FROM employees WHERE salary BETWEEN 50000 AND 75000").unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("Bob"));
    assert!(!result.contains("Charlie"));
    assert!(!result.contains("David"));

    let result =
        execute_sql("SELECT name FROM employees WHERE salary BETWEEN 100000 AND 125000").unwrap();
    assert!(!result.contains("Alice"));
    assert!(!result.contains("Bob"));
    assert!(result.contains("Charlie"));
    assert!(result.contains("David"));
}

#[test]
fn test_in_with_mixed_types() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE items (id INTEGER, price FLOAT, name TEXT)").unwrap();
    execute_sql(
        "INSERT INTO items VALUES (1, 10.5, 'Apple'), (2, 20.0, 'Banana'), (3, 15.75, 'Orange')",
    )
    .unwrap();

    let result = execute_sql("SELECT name FROM items WHERE price IN (10.5, 20.0)").unwrap();
    assert!(result.contains("Apple"));
    assert!(result.contains("Banana"));
    assert!(!result.contains("Orange"));
}

#[test]
fn test_in_list_uses_numeric_equality_semantics() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE numbers (id INTEGER, value INTEGER)").unwrap();
    execute_sql("INSERT INTO numbers VALUES (1, 1), (2, 2), (3, NULL)").unwrap();

    let rows = query_rows(
        "SELECT id \
         FROM numbers \
         WHERE value IN (1.0, 3.0, NULL) \
         ORDER BY id",
    )
    .unwrap();

    rows.assert_columns(&["id"]);
    assert_eq!(rows.rows, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_in_list_accepts_expressions() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE numbers (id INTEGER, value INTEGER, delta INTEGER)").unwrap();
    execute_sql("INSERT INTO numbers VALUES (1, 2, 1), (2, 3, 1), (3, 5, 2), (4, NULL, 0)")
        .unwrap();

    let rows = query_rows(
        "SELECT id \
         FROM numbers \
         WHERE value IN (1 + 1, value + 1, CAST('5' AS INTEGER), delta + 1) \
         ORDER BY id",
    )
    .unwrap();

    rows.assert_columns(&["id"]);
    assert_eq!(
        rows.rows,
        vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]
    );
}

#[test]
fn test_in_subquery_uses_numeric_equality_semantics() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE left_numbers (id INTEGER, value INTEGER)").unwrap();
    execute_sql("CREATE TABLE right_numbers (value FLOAT)").unwrap();
    execute_sql("INSERT INTO left_numbers VALUES (1, 1), (2, 2), (3, NULL)").unwrap();
    execute_sql("INSERT INTO right_numbers VALUES (1.0), (3.0), (NULL)").unwrap();

    let rows = query_rows(
        "SELECT id \
         FROM left_numbers \
         WHERE value IN (SELECT value FROM right_numbers) \
         ORDER BY id",
    )
    .unwrap();

    rows.assert_columns(&["id"]);
    assert_eq!(rows.rows, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_unknown_predicates_do_not_pass_where() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE unknown_filter (id INTEGER, value INTEGER)").unwrap();
    execute_sql("INSERT INTO unknown_filter VALUES (1, NULL), (2, 10)").unwrap();

    let rows = query_rows(
        "SELECT id \
         FROM unknown_filter \
         WHERE value = NULL OR (value = 10 AND NULL) \
         ORDER BY id",
    )
    .unwrap();

    rows.assert_columns(&["id"]);
    assert!(rows.rows.is_empty());
}

#[test]
fn test_like_with_wildcards() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE files (id INTEGER, filename TEXT)").unwrap();
    execute_sql(
        "INSERT INTO files VALUES (1, 'document.pdf'), (2, 'image.jpg'), (3, 'report.docx')",
    )
    .unwrap();

    let result = execute_sql("SELECT filename FROM files WHERE filename LIKE '%.pdf'").unwrap();
    assert!(result.contains("document.pdf"));
    assert!(!result.contains("image.jpg"));
    assert!(!result.contains("report.docx"));

    let result = execute_sql("SELECT filename FROM files WHERE filename LIKE 'i%'").unwrap();
    assert!(result.contains("image.jpg"));
    assert!(!result.contains("document.pdf"));
}

#[test]
fn test_between_with_text() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE items (name TEXT, category TEXT)").unwrap();
    execute_sql(
        "INSERT INTO items VALUES ('Apple', 'Fruit'), ('Banana', 'Fruit'), ('Carrot', 'Vegetable')",
    )
    .unwrap();

    let result =
        execute_sql("SELECT name FROM items WHERE name BETWEEN 'Apple' AND 'Banana'").unwrap();
    assert!(result.contains("Apple"));
    assert!(result.contains("Banana"));
    assert!(!result.contains("Carrot"));
}

#[test]
fn test_combined_operators() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE students (id INTEGER, name TEXT, age INTEGER, grade TEXT)").unwrap();
    execute_sql("INSERT INTO students VALUES (1, 'Alice', 20, 'A'), (2, 'Bob', 21, 'B'), (3, 'Charlie', 22, 'A'), (4, 'David', 20, 'C')").unwrap();

    let result = execute_sql(
        "SELECT name FROM students WHERE age BETWEEN 20 AND 21 AND grade IN ('A', 'B')",
    )
    .unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("Bob"));
    assert!(!result.contains("Charlie"));
    assert!(!result.contains("David"));
}

#[test]
fn test_not_in() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE users (id INTEGER, name TEXT, role TEXT)").unwrap();
    execute_sql("INSERT INTO users VALUES (1, 'Alice', 'Admin'), (2, 'Bob', 'User'), (3, 'Charlie', 'Guest')").unwrap();

    let rows = query_rows("SELECT name FROM users WHERE role NOT IN ('Guest', 'User')").unwrap();
    rows.assert_columns(&["name"]);
    assert_eq!(rows.rows, vec![vec![Value::Text("Alice".to_string())]]);
}

#[test]
fn test_like_not_like() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE products (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO products VALUES (1, 'laptop'), (2, 'desktop'), (3, 'mouse')").unwrap();

    let rows = query_rows("SELECT name FROM products WHERE name NOT LIKE '%top%'").unwrap();
    rows.assert_columns(&["name"]);
    assert_eq!(rows.rows, vec![vec![Value::Text("mouse".to_string())]]);
}

#[test]
fn test_like_escape_literal_wildcards() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE files (id INTEGER, name TEXT)").unwrap();
    execute_sql(
        "INSERT INTO files VALUES \
         (1, '100%'), \
         (2, '1000'), \
         (3, 'under_score'), \
         (4, 'underXscore'), \
         (5, 'bang!value')",
    )
    .unwrap();

    let rows = query_rows("SELECT name FROM files WHERE name LIKE '100!%' ESCAPE '!' ORDER BY id")
        .unwrap();
    rows.assert_columns(&["name"]);
    assert_eq!(rows.rows, vec![vec![Value::Text("100%".to_string())]]);

    let rows =
        query_rows("SELECT name FROM files WHERE name LIKE 'under!_score' ESCAPE '!' ORDER BY id")
            .unwrap();
    rows.assert_columns(&["name"]);
    assert_eq!(
        rows.rows,
        vec![vec![Value::Text("under_score".to_string())]]
    );

    let rows =
        query_rows("SELECT name FROM files WHERE name LIKE 'bang!!value' ESCAPE '!'").unwrap();
    rows.assert_columns(&["name"]);
    assert_eq!(rows.rows, vec![vec![Value::Text("bang!value".to_string())]]);
}

#[test]
fn test_not_like_escape_literal_wildcards() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE files (id INTEGER, name TEXT)").unwrap();
    execute_sql(
        "INSERT INTO files VALUES \
         (1, 'under_score'), \
         (2, 'underXscore'), \
         (3, 'other')",
    )
    .unwrap();

    let rows = query_rows(
        "SELECT name FROM files WHERE name NOT LIKE 'under!_score' ESCAPE '!' ORDER BY id",
    )
    .unwrap();
    rows.assert_columns(&["name"]);
    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Text("underXscore".to_string())],
            vec![Value::Text("other".to_string())],
        ]
    );
}

#[test]
fn test_infix_not_between_and_not_ilike() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE items (id INTEGER, name TEXT, score INTEGER)").unwrap();
    execute_sql(
        "INSERT INTO items VALUES \
         (1, 'alpha', 10), \
         (2, 'BETA', 20), \
         (3, 'gamma', 30)",
    )
    .unwrap();

    let rows =
        query_rows("SELECT id FROM items WHERE score NOT BETWEEN 15 AND 25 ORDER BY id").unwrap();
    rows.assert_columns(&["id"]);
    assert_eq!(
        rows.rows,
        vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]
    );

    let rows = query_rows("SELECT id FROM items WHERE name NOT ILIKE 'a%' ORDER BY id").unwrap();
    rows.assert_columns(&["id"]);
    assert_eq!(
        rows.rows,
        vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]
    );
}
