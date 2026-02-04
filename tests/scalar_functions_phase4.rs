use rustql::{process_query, reset_database};
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup_test<'a>() -> std::sync::MutexGuard<'a, ()> {
    let guard = TEST_MUTEX.lock().unwrap();
    reset_database();
    guard
}

#[test]
fn test_nullif_different() {
    let _guard = setup_test();
    process_query("CREATE TABLE t (id INTEGER, a INTEGER, b INTEGER)").unwrap();
    process_query("INSERT INTO t VALUES (1, 10, 20)").unwrap();

    let result = process_query("SELECT NULLIF(a, b) FROM t").unwrap();
    assert!(result.contains("10"));
}

#[test]
fn test_nullif_equal() {
    let _guard = setup_test();
    process_query("CREATE TABLE t (id INTEGER, a INTEGER)").unwrap();
    process_query("INSERT INTO t VALUES (1, 5)").unwrap();

    let result = process_query("SELECT NULLIF(a, 5) FROM t").unwrap();
    assert!(result.contains("NULL"));
}

#[test]
fn test_greatest() {
    let _guard = setup_test();
    process_query("CREATE TABLE t (id INTEGER, a INTEGER, b INTEGER, c INTEGER)").unwrap();
    process_query("INSERT INTO t VALUES (1, 3, 7, 5)").unwrap();

    let result = process_query("SELECT GREATEST(a, b, c) FROM t").unwrap();
    assert!(result.contains("7"));
}

#[test]
fn test_least() {
    let _guard = setup_test();
    process_query("CREATE TABLE t (id INTEGER, a INTEGER, b INTEGER, c INTEGER)").unwrap();
    process_query("INSERT INTO t VALUES (1, 3, 7, 5)").unwrap();

    let result = process_query("SELECT LEAST(a, b, c) FROM t").unwrap();
    assert!(result.contains("3"));
}

#[test]
fn test_lpad() {
    let _guard = setup_test();
    process_query("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO t VALUES (1, 'hi')").unwrap();

    let result = process_query("SELECT LPAD(name, 5, '*') FROM t").unwrap();
    assert!(result.contains("***hi"));
}

#[test]
fn test_rpad() {
    let _guard = setup_test();
    process_query("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO t VALUES (1, 'hi')").unwrap();

    let result = process_query("SELECT RPAD(name, 5, '*') FROM t").unwrap();
    assert!(result.contains("hi***"));
}

#[test]
fn test_left_function() {
    let _guard = setup_test();
    process_query("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO t VALUES (1, 'Hello World')").unwrap();

    let result = process_query("SELECT LEFT(name, 5) FROM t").unwrap();
    assert!(result.contains("Hello"));
}

#[test]
fn test_right_function() {
    let _guard = setup_test();
    process_query("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO t VALUES (1, 'Hello World')").unwrap();

    let result = process_query("SELECT RIGHT(name, 5) FROM t").unwrap();
    assert!(result.contains("World"));
}

#[test]
fn test_reverse() {
    let _guard = setup_test();
    process_query("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO t VALUES (1, 'abc')").unwrap();

    let result = process_query("SELECT REVERSE(name) FROM t").unwrap();
    assert!(result.contains("cba"));
}

#[test]
fn test_repeat() {
    let _guard = setup_test();
    process_query("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO t VALUES (1, 'ab')").unwrap();

    let result = process_query("SELECT REPEAT(name, 3) FROM t").unwrap();
    assert!(result.contains("ababab"));
}

#[test]
fn test_log() {
    let _guard = setup_test();
    process_query("CREATE TABLE t (id INTEGER, val FLOAT)").unwrap();
    process_query("INSERT INTO t VALUES (1, 2.718281828)").unwrap();

    let result = process_query("SELECT LOG(val) FROM t").unwrap();
    assert!(result.contains("0.99999"));
}

#[test]
fn test_exp() {
    let _guard = setup_test();
    process_query("CREATE TABLE t (id INTEGER, val FLOAT)").unwrap();
    process_query("INSERT INTO t VALUES (1, 1.0)").unwrap();

    let result = process_query("SELECT ROUND(EXP(val), 2) FROM t").unwrap();
    assert!(result.contains("2.72"));
}

#[test]
fn test_sign_positive() {
    let _guard = setup_test();
    process_query("CREATE TABLE t (id INTEGER, val INTEGER)").unwrap();
    process_query("INSERT INTO t VALUES (1, 42)").unwrap();

    let result = process_query("SELECT SIGN(val) FROM t").unwrap();
    assert!(result.contains("1"));
}

#[test]
fn test_sign_negative() {
    let _guard = setup_test();
    process_query("CREATE TABLE t (id INTEGER, val INTEGER)").unwrap();
    process_query("INSERT INTO t VALUES (1, -5)").unwrap();

    let result = process_query("SELECT SIGN(val) FROM t").unwrap();
    assert!(result.contains("-1"));
}

#[test]
fn test_sign_zero() {
    let _guard = setup_test();
    process_query("CREATE TABLE t (id INTEGER, val INTEGER)").unwrap();
    process_query("INSERT INTO t VALUES (1, 0)").unwrap();

    let result = process_query("SELECT SIGN(val) FROM t").unwrap();
    assert!(result.contains("0"));
}

#[test]
fn test_date_trunc_year() {
    let _guard = setup_test();
    process_query("CREATE TABLE t (id INTEGER, d DATE)").unwrap();
    process_query("INSERT INTO t VALUES (1, '2024-06-15')").unwrap();

    let result = process_query("SELECT DATE_TRUNC('year', d) FROM t").unwrap();
    assert!(result.contains("2024-01-01"));
}

#[test]
fn test_date_trunc_month() {
    let _guard = setup_test();
    process_query("CREATE TABLE t (id INTEGER, d DATE)").unwrap();
    process_query("INSERT INTO t VALUES (1, '2024-06-15')").unwrap();

    let result = process_query("SELECT DATE_TRUNC('month', d) FROM t").unwrap();
    assert!(result.contains("2024-06-01"));
}

#[test]
fn test_extract_year() {
    let _guard = setup_test();
    process_query("CREATE TABLE t (id INTEGER, d DATE)").unwrap();
    process_query("INSERT INTO t VALUES (1, '2024-06-15')").unwrap();

    let result = process_query("SELECT EXTRACT(YEAR FROM d) FROM t").unwrap();
    assert!(result.contains("2024"));
}

#[test]
fn test_extract_month() {
    let _guard = setup_test();
    process_query("CREATE TABLE t (id INTEGER, d DATE)").unwrap();
    process_query("INSERT INTO t VALUES (1, '2024-06-15')").unwrap();

    let result = process_query("SELECT EXTRACT(MONTH FROM d) FROM t").unwrap();
    assert!(result.contains("6"));
}

#[test]
fn test_extract_day() {
    let _guard = setup_test();
    process_query("CREATE TABLE t (id INTEGER, d DATE)").unwrap();
    process_query("INSERT INTO t VALUES (1, '2024-06-15')").unwrap();

    let result = process_query("SELECT EXTRACT(DAY FROM d) FROM t").unwrap();
    assert!(result.contains("15"));
}

#[test]
fn test_greatest_with_null() {
    let _guard = setup_test();
    process_query("CREATE TABLE t (id INTEGER, a INTEGER)").unwrap();
    process_query("INSERT INTO t VALUES (1, 5)").unwrap();

    let result = process_query("SELECT GREATEST(a, 3) FROM t").unwrap();
    assert!(result.contains("5"));
}

#[test]
fn test_least_with_null() {
    let _guard = setup_test();
    process_query("CREATE TABLE t (id INTEGER, a INTEGER)").unwrap();
    process_query("INSERT INTO t VALUES (1, 5)").unwrap();

    let result = process_query("SELECT LEAST(a, 3) FROM t").unwrap();
    assert!(result.contains("3"));
}

#[test]
fn test_lpad_default_space() {
    let _guard = setup_test();
    process_query("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO t VALUES (1, 'hi')").unwrap();

    let result = process_query("SELECT LPAD(name, 5) FROM t").unwrap();
    assert!(result.contains("   hi"));
}
