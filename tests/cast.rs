use rustql::testing::{process_query, reset_database};
use std::sync::Mutex;

static GLOBAL_TEST_LOCK: Mutex<()> = Mutex::new(());

fn setup() -> std::sync::MutexGuard<'static, ()> {
    let guard = GLOBAL_TEST_LOCK.lock().unwrap();
    reset_database();
    process_query("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price FLOAT, active BOOLEAN, created DATE)").unwrap();
    process_query("INSERT INTO items (id, name, price, active, created) VALUES (1, 'Widget', 19.99, 'true', '2024-01-15')").unwrap();
    process_query("INSERT INTO items (id, name, price, active, created) VALUES (2, '42', 9.5, 'false', '2024-06-20')").unwrap();
    process_query("INSERT INTO items (id, name, price, active, created) VALUES (3, 'Gadget', 100.0, 'true', '2024-12-01')").unwrap();
    guard
}

#[test]
fn test_cast_float_to_integer() {
    let _g = setup();
    let result = process_query("SELECT CAST(price AS INTEGER) FROM items WHERE id = 1").unwrap();
    assert!(result.contains("19"), "Expected 19, got: {}", result);
}

#[test]
fn test_cast_integer_to_float() {
    let _g = setup();
    let result = process_query("SELECT CAST(id AS FLOAT) FROM items WHERE id = 1").unwrap();
    assert!(result.contains("1"), "Expected 1 as float, got: {}", result);
}

#[test]
fn test_cast_text_to_integer() {
    let _g = setup();
    let result = process_query("SELECT CAST(name AS INTEGER) FROM items WHERE id = 2").unwrap();
    assert!(result.contains("42"), "Expected 42, got: {}", result);
}

#[test]
fn test_cast_integer_to_text() {
    let _g = setup();
    let result = process_query("SELECT CAST(id AS TEXT) FROM items WHERE id = 1").unwrap();
    assert!(result.contains("1"), "Expected '1', got: {}", result);
}

#[test]
fn test_cast_text_to_float() {
    let _g = setup();
    let result = process_query("SELECT CAST(name AS FLOAT) FROM items WHERE id = 2").unwrap();
    assert!(result.contains("42"), "Expected 42.0, got: {}", result);
}

#[test]
fn test_cast_null_returns_null() {
    let _g = setup();
    let result = process_query("SELECT CAST(NULL AS INTEGER) FROM items WHERE id = 1").unwrap();
    assert!(result.contains("NULL"), "Expected NULL, got: {}", result);
}

#[test]
fn test_cast_to_boolean() {
    let _g = setup();
    let result = process_query("SELECT CAST(1 AS BOOLEAN) FROM items WHERE id = 1").unwrap();
    assert!(result.contains("true"), "Expected true, got: {}", result);
}

#[test]
fn test_cast_datetime_to_date() {
    let _g = setup();
    process_query("CREATE TABLE events (id INTEGER PRIMARY KEY, ts DATETIME)").unwrap();
    process_query("INSERT INTO events (id, ts) VALUES (1, '2024-03-15 10:30:00')").unwrap();
    let result = process_query("SELECT CAST(ts AS DATE) FROM events WHERE id = 1").unwrap();
    assert!(
        result.contains("2024-03-15"),
        "Expected 2024-03-15, got: {}",
        result
    );
}

#[test]
fn test_cast_date_to_datetime() {
    let _g = setup();
    let result = process_query("SELECT CAST(created AS DATETIME) FROM items WHERE id = 1").unwrap();
    assert!(
        result.contains("2024-01-15 00:00:00"),
        "Expected 2024-01-15 00:00:00, got: {}",
        result
    );
}

#[test]
fn test_cast_in_where_clause() {
    let _g = setup();
    let result = process_query("SELECT name FROM items WHERE CAST(price AS INTEGER) = 19").unwrap();
    assert!(
        result.contains("Widget"),
        "Expected Widget, got: {}",
        result
    );
}

#[test]
fn test_cast_with_alias() {
    let _g = setup();
    let result =
        process_query("SELECT CAST(price AS INTEGER) AS int_price FROM items WHERE id = 1")
            .unwrap();
    assert!(
        result.contains("int_price"),
        "Expected header int_price, got: {}",
        result
    );
    assert!(result.contains("19"), "Expected 19, got: {}", result);
}
