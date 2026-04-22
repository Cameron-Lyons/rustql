mod common;
use common::{process_query, reset_database};
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup_test() -> std::sync::MutexGuard<'static, ()> {
    let guard = TEST_MUTEX.lock().unwrap();
    reset_database();
    guard
}

#[test]
fn test_division_by_zero_integer() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (id INTEGER)").unwrap();
    process_query("INSERT INTO t VALUES (1)").unwrap();

    let result = process_query("SELECT 10 / 0 FROM t");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Division by zero"));
}

#[test]
fn test_division_by_zero_column() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (a INTEGER, b INTEGER)").unwrap();
    process_query("INSERT INTO t VALUES (10, 0)").unwrap();

    let result = process_query("SELECT a / b FROM t");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Division by zero"));
}

#[test]
fn test_integer_division_exact() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (id INTEGER)").unwrap();
    process_query("INSERT INTO t VALUES (1)").unwrap();

    let result = process_query("SELECT 10 / 2 FROM t").unwrap();
    assert!(result.contains("5"));
}

#[test]
fn test_integer_division_with_remainder() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (id INTEGER)").unwrap();
    process_query("INSERT INTO t VALUES (1)").unwrap();

    let result = process_query("SELECT 10 / 3 FROM t").unwrap();
    assert!(result.contains("3.3333"));
}

#[test]
fn test_float_division() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (id INTEGER)").unwrap();
    process_query("INSERT INTO t VALUES (1)").unwrap();

    let result = process_query("SELECT 10.0 / 4.0 FROM t").unwrap();
    assert!(result.contains("2.5"));
}

#[test]
fn test_column_division() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (a INTEGER, b INTEGER)").unwrap();
    process_query("INSERT INTO t VALUES (20, 4)").unwrap();

    let result = process_query("SELECT a / b FROM t").unwrap();
    assert!(result.contains("5"));
}
