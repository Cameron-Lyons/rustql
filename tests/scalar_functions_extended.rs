mod common;
use common::{process_query, reset_database};
use std::sync::Mutex;

static GLOBAL_TEST_LOCK: Mutex<()> = Mutex::new(());

fn setup() -> std::sync::MutexGuard<'static, ()> {
    let guard = GLOBAL_TEST_LOCK.lock().unwrap();
    reset_database();
    process_query(
        "CREATE TABLE data (id INTEGER PRIMARY KEY, name TEXT, val FLOAT, dt DATE, ts DATETIME)",
    )
    .unwrap();
    process_query(
        "INSERT INTO data (id, name, val, dt, ts) VALUES (1, '  hello world  ', 16.7, '2024-03-15', '2024-03-15 10:30:00')",
    )
    .unwrap();
    process_query(
        "INSERT INTO data (id, name, val, dt, ts) VALUES (2, 'foo bar baz', -9.0, '2024-12-25', '2024-12-25 23:59:59')",
    )
    .unwrap();
    process_query(
        "INSERT INTO data (id, name, val, dt, ts) VALUES (3, 'abcdef', 0.0, '2025-01-01', '2025-01-01 00:00:00')",
    )
    .unwrap();
    guard
}

#[test]
fn test_trim() {
    let _g = setup();
    let result = process_query("SELECT TRIM(name) FROM data WHERE id = 1").unwrap();
    assert!(
        result.contains("hello world"),
        "Expected 'hello world', got: {}",
        result
    );
}

#[test]
fn test_replace() {
    let _g = setup();
    let result =
        process_query("SELECT REPLACE(name, 'bar', 'BAR') FROM data WHERE id = 2").unwrap();
    assert!(
        result.contains("foo BAR baz"),
        "Expected 'foo BAR baz', got: {}",
        result
    );
}

#[test]
fn test_position() {
    let _g = setup();
    let result = process_query("SELECT POSITION('world', name) FROM data WHERE id = 1").unwrap();
    assert!(result.contains("9"), "Expected 9, got: {}", result);
}

#[test]
fn test_position_not_found() {
    let _g = setup();
    let result = process_query("SELECT POSITION('xyz', name) FROM data WHERE id = 1").unwrap();
    assert!(result.contains("0"), "Expected 0, got: {}", result);
}

#[test]
fn test_instr() {
    let _g = setup();
    let result = process_query("SELECT INSTR(name, 'bar') FROM data WHERE id = 2").unwrap();
    assert!(result.contains("5"), "Expected 5, got: {}", result);
}

#[test]
fn test_ceil() {
    let _g = setup();
    let result = process_query("SELECT CEIL(val) FROM data WHERE id = 1").unwrap();
    assert!(result.contains("17"), "Expected 17, got: {}", result);
}

#[test]
fn test_ceiling_alias() {
    let _g = setup();
    let result = process_query("SELECT CEILING(val) FROM data WHERE id = 1").unwrap();
    assert!(result.contains("17"), "Expected 17, got: {}", result);
}

#[test]
fn test_floor() {
    let _g = setup();
    let result = process_query("SELECT FLOOR(val) FROM data WHERE id = 1").unwrap();
    assert!(result.contains("16"), "Expected 16, got: {}", result);
}

#[test]
fn test_floor_negative() {
    let _g = setup();
    let result = process_query("SELECT FLOOR(val) FROM data WHERE id = 2").unwrap();
    assert!(result.contains("-9"), "Expected -9, got: {}", result);
}

#[test]
fn test_sqrt() {
    let _g = setup();
    let result = process_query("SELECT SQRT(val) FROM data WHERE id = 1").unwrap();
    assert!(result.contains("4.0"), "Expected ~4.08..., got: {}", result);
}

#[test]
fn test_sqrt_integer() {
    let _g = setup();
    let result = process_query("SELECT SQRT(id) FROM data WHERE id = 1").unwrap();
    assert!(result.contains("1"), "Expected 1.0, got: {}", result);
}

#[test]
fn test_power() {
    let _g = setup();
    let result = process_query("SELECT POWER(id, 3) FROM data WHERE id = 2").unwrap();
    assert!(result.contains("8"), "Expected 8.0, got: {}", result);
}

#[test]
fn test_mod() {
    let _g = setup();
    let result = process_query("SELECT MOD(id, 2) FROM data WHERE id = 3").unwrap();
    assert!(result.contains("1"), "Expected 1, got: {}", result);
}

#[test]
fn test_mod_division_by_zero() {
    let _g = setup();
    let result = process_query("SELECT MOD(id, 0) FROM data WHERE id = 1");
    assert!(result.is_err(), "Expected error for MOD by zero");
}

#[test]
fn test_now() {
    let _g = setup();
    let result = process_query("SELECT NOW() FROM data WHERE id = 1").unwrap();
    assert!(
        result.contains("20"),
        "Expected a datetime with year starting with 20, got: {}",
        result
    );
}

#[test]
fn test_year() {
    let _g = setup();
    let result = process_query("SELECT YEAR(dt) FROM data WHERE id = 1").unwrap();
    assert!(result.contains("2024"), "Expected 2024, got: {}", result);
}

#[test]
fn test_month() {
    let _g = setup();
    let result = process_query("SELECT MONTH(dt) FROM data WHERE id = 1").unwrap();
    assert!(result.contains("3"), "Expected 3, got: {}", result);
}

#[test]
fn test_day() {
    let _g = setup();
    let result = process_query("SELECT DAY(dt) FROM data WHERE id = 1").unwrap();
    assert!(result.contains("15"), "Expected 15, got: {}", result);
}

#[test]
fn test_year_from_datetime() {
    let _g = setup();
    let result = process_query("SELECT YEAR(ts) FROM data WHERE id = 2").unwrap();
    assert!(result.contains("2024"), "Expected 2024, got: {}", result);
}

#[test]
fn test_date_add() {
    let _g = setup();
    let result = process_query("SELECT DATE_ADD(dt, 10) FROM data WHERE id = 1").unwrap();
    assert!(
        result.contains("2024-03-25"),
        "Expected 2024-03-25, got: {}",
        result
    );
}

#[test]
fn test_date_add_cross_month() {
    let _g = setup();
    let result = process_query("SELECT DATE_ADD(dt, 20) FROM data WHERE id = 2").unwrap();
    assert!(
        result.contains("2025-01-14"),
        "Expected 2025-01-14, got: {}",
        result
    );
}

#[test]
fn test_date_add_negative() {
    let _g = setup();
    let result = process_query("SELECT DATE_ADD(dt, -15) FROM data WHERE id = 1").unwrap();
    assert!(
        result.contains("2024-02-29"),
        "Expected 2024-02-29 (leap year), got: {}",
        result
    );
}

#[test]
fn test_datediff() {
    let _g = setup();
    let result = process_query("SELECT DATEDIFF(dt, '2024-03-01') FROM data WHERE id = 1").unwrap();
    assert!(result.contains("14"), "Expected 14, got: {}", result);
}

#[test]
fn test_datediff_negative() {
    let _g = setup();
    let result = process_query("SELECT DATEDIFF(dt, '2024-12-31') FROM data WHERE id = 2").unwrap();
    assert!(result.contains("-6"), "Expected -6, got: {}", result);
}

#[test]
fn test_ceil_integer_passthrough() {
    let _g = setup();
    let result = process_query("SELECT CEIL(id) FROM data WHERE id = 2").unwrap();
    assert!(result.contains("2"), "Expected 2, got: {}", result);
}

#[test]
fn test_null_propagation() {
    let _g = setup();
    let result = process_query("SELECT TRIM(NULL) FROM data WHERE id = 1").unwrap();
    assert!(result.contains("NULL"), "Expected NULL, got: {}", result);
}

#[test]
fn test_chained_functions() {
    let _g = setup();
    let result = process_query("SELECT UPPER(TRIM(name)) FROM data WHERE id = 1").unwrap();
    assert!(
        result.contains("HELLO WORLD"),
        "Expected 'HELLO WORLD', got: {}",
        result
    );
}
