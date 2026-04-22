mod common;
use common::*;
use std::sync::Mutex;

static GLOBAL_TEST_LOCK: Mutex<()> = Mutex::new(());

fn setup() -> std::sync::MutexGuard<'static, ()> {
    let guard = GLOBAL_TEST_LOCK.lock().unwrap();
    reset_database();
    execute_sql(
        "CREATE TABLE people (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, age INTEGER)",
    )
    .unwrap();
    execute_sql(
        "INSERT INTO people (id, first_name, last_name, age) VALUES (1, 'John', 'Doe', 30)",
    )
    .unwrap();
    execute_sql(
        "INSERT INTO people (id, first_name, last_name, age) VALUES (2, 'Jane', 'Smith', 25)",
    )
    .unwrap();
    guard
}

#[test]
fn test_concat_two_columns() {
    let _g = setup();
    let result = execute_sql("SELECT first_name || last_name FROM people WHERE id = 1").unwrap();
    assert!(
        result.contains("JohnDoe"),
        "Expected JohnDoe, got: {:?}",
        result
    );
}

#[test]
fn test_concat_with_literal() {
    let _g = setup();
    let result =
        execute_sql("SELECT first_name || ' ' || last_name FROM people WHERE id = 1").unwrap();
    assert!(
        result.contains("John Doe"),
        "Expected 'John Doe', got: {:?}",
        result
    );
}

#[test]
fn test_concat_with_number() {
    let _g = setup();
    let result = execute_sql("SELECT first_name || age FROM people WHERE id = 1").unwrap();
    assert!(
        result.contains("John30"),
        "Expected John30, got: {:?}",
        result
    );
}

#[test]
fn test_concat_in_where_clause() {
    let _g = setup();
    let result =
        execute_sql("SELECT id FROM people WHERE first_name || ' ' || last_name = 'John Doe'")
            .unwrap();
    assert!(result.contains("1"), "Expected id 1, got: {:?}", result);
}

#[test]
fn test_concat_with_alias() {
    let _g = setup();
    let result =
        execute_sql("SELECT first_name || ' ' || last_name AS full_name FROM people WHERE id = 2")
            .unwrap();
    assert!(
        result.contains("full_name"),
        "Expected header full_name, got: {:?}",
        result
    );
    assert!(
        result.contains("Jane Smith"),
        "Expected 'Jane Smith', got: {:?}",
        result
    );
}

#[test]
fn test_concat_function() {
    let _g = setup();
    let result =
        execute_sql("SELECT CONCAT(first_name, ' ', last_name) FROM people WHERE id = 1").unwrap();
    assert!(
        result.contains("John Doe"),
        "Expected 'John Doe', got: {:?}",
        result
    );
}

#[test]
fn test_concat_function_variadic() {
    let _g = setup();
    let result = execute_sql(
        "SELECT CONCAT(first_name, ' ', last_name, ' age:', age) FROM people WHERE id = 1",
    )
    .unwrap();
    assert!(
        result.contains("John Doe age:30"),
        "Expected 'John Doe age:30', got: {:?}",
        result
    );
}
