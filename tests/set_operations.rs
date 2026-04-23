mod common;
use common::*;
use rustql::ast::Value;
use std::{collections::BTreeSet, sync::Mutex};

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup_test<'a>() -> std::sync::MutexGuard<'a, ()> {
    let guard = TEST_MUTEX.lock().unwrap();
    reset_database();
    guard
}

#[test]
fn test_value_ordering_distinguishes_unrelated_types() {
    let mut values = BTreeSet::new();

    assert!(values.insert(Value::Integer(1)));
    assert!(values.insert(Value::Float(1.0)));
    assert!(values.insert(Value::Text("1".to_string())));
    assert!(values.insert(Value::Boolean(true)));

    assert_eq!(
        values.into_iter().collect::<Vec<_>>(),
        vec![
            Value::Integer(1),
            Value::Float(1.0),
            Value::Text("1".to_string()),
            Value::Boolean(true),
        ]
    );
}

#[test]
fn test_value_ordering_handles_nan_deterministically() {
    let mut values = BTreeSet::new();

    assert!(values.insert(Value::Float(f64::INFINITY)));
    assert!(values.insert(Value::Float(f64::NAN)));
    assert!(!values.insert(Value::Float(f64::NAN)));

    assert_eq!(
        values.into_iter().collect::<Vec<_>>(),
        vec![Value::Float(f64::INFINITY), Value::Float(f64::NAN)]
    );
}

#[test]
fn test_intersect_basic() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE t1 (id INTEGER, name TEXT)").unwrap();
    execute_sql("CREATE TABLE t2 (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO t1 VALUES (1, 'Alice')").unwrap();
    execute_sql("INSERT INTO t1 VALUES (2, 'Bob')").unwrap();
    execute_sql("INSERT INTO t1 VALUES (3, 'Charlie')").unwrap();
    execute_sql("INSERT INTO t2 VALUES (2, 'Bob')").unwrap();
    execute_sql("INSERT INTO t2 VALUES (3, 'Charlie')").unwrap();
    execute_sql("INSERT INTO t2 VALUES (4, 'Diana')").unwrap();

    let result = execute_sql("SELECT id, name FROM t1 INTERSECT SELECT id, name FROM t2").unwrap();
    assert!(result.contains("Bob"));
    assert!(result.contains("Charlie"));
    assert!(!result.contains("Alice"));
    assert!(!result.contains("Diana"));
}

#[test]
fn test_intersect_no_overlap() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE t1 (id INTEGER)").unwrap();
    execute_sql("CREATE TABLE t2 (id INTEGER)").unwrap();
    execute_sql("INSERT INTO t1 VALUES (1)").unwrap();
    execute_sql("INSERT INTO t1 VALUES (2)").unwrap();
    execute_sql("INSERT INTO t2 VALUES (3)").unwrap();
    execute_sql("INSERT INTO t2 VALUES (4)").unwrap();

    let result = execute_sql("SELECT id FROM t1 INTERSECT SELECT id FROM t2").unwrap();
    let lines: Vec<String> = result.lines().collect();
    assert_eq!(lines.len(), 2);
}

#[test]
fn test_intersect_all() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE t1 (val INTEGER)").unwrap();
    execute_sql("CREATE TABLE t2 (val INTEGER)").unwrap();
    execute_sql("INSERT INTO t1 VALUES (1)").unwrap();
    execute_sql("INSERT INTO t1 VALUES (1)").unwrap();
    execute_sql("INSERT INTO t1 VALUES (2)").unwrap();
    execute_sql("INSERT INTO t2 VALUES (1)").unwrap();
    execute_sql("INSERT INTO t2 VALUES (1)").unwrap();
    execute_sql("INSERT INTO t2 VALUES (1)").unwrap();

    let result = execute_sql("SELECT val FROM t1 INTERSECT ALL SELECT val FROM t2").unwrap();
    let data_lines: Vec<String> = result.lines().skip(2).collect();
    let ones = data_lines.iter().filter(|l| l.contains("1")).count();
    assert_eq!(ones, 2);
}

#[test]
fn test_except_basic() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE t1 (id INTEGER, name TEXT)").unwrap();
    execute_sql("CREATE TABLE t2 (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO t1 VALUES (1, 'Alice')").unwrap();
    execute_sql("INSERT INTO t1 VALUES (2, 'Bob')").unwrap();
    execute_sql("INSERT INTO t1 VALUES (3, 'Charlie')").unwrap();
    execute_sql("INSERT INTO t2 VALUES (2, 'Bob')").unwrap();
    execute_sql("INSERT INTO t2 VALUES (3, 'Charlie')").unwrap();

    let result = execute_sql("SELECT id, name FROM t1 EXCEPT SELECT id, name FROM t2").unwrap();
    assert!(result.contains("Alice"));
    assert!(!result.contains("Bob"));
    assert!(!result.contains("Charlie"));
}

#[test]
fn test_except_no_overlap() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE t1 (id INTEGER)").unwrap();
    execute_sql("CREATE TABLE t2 (id INTEGER)").unwrap();
    execute_sql("INSERT INTO t1 VALUES (1)").unwrap();
    execute_sql("INSERT INTO t1 VALUES (2)").unwrap();
    execute_sql("INSERT INTO t2 VALUES (3)").unwrap();

    let result = execute_sql("SELECT id FROM t1 EXCEPT SELECT id FROM t2").unwrap();
    assert!(result.contains("1"));
    assert!(result.contains("2"));
}

#[test]
fn test_except_all() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE t1 (val INTEGER)").unwrap();
    execute_sql("CREATE TABLE t2 (val INTEGER)").unwrap();
    execute_sql("INSERT INTO t1 VALUES (1)").unwrap();
    execute_sql("INSERT INTO t1 VALUES (1)").unwrap();
    execute_sql("INSERT INTO t1 VALUES (1)").unwrap();
    execute_sql("INSERT INTO t2 VALUES (1)").unwrap();

    let result = execute_sql("SELECT val FROM t1 EXCEPT ALL SELECT val FROM t2").unwrap();
    let data_lines: Vec<String> = result.lines().skip(2).collect();
    let ones = data_lines.iter().filter(|l| l.contains("1")).count();
    assert_eq!(ones, 2);
}

#[test]
fn test_set_ops_with_order_by() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE t1 (id INTEGER)").unwrap();
    execute_sql("CREATE TABLE t2 (id INTEGER)").unwrap();
    execute_sql("INSERT INTO t1 VALUES (3)").unwrap();
    execute_sql("INSERT INTO t1 VALUES (1)").unwrap();
    execute_sql("INSERT INTO t2 VALUES (2)").unwrap();
    execute_sql("INSERT INTO t2 VALUES (1)").unwrap();

    let result = execute_sql("SELECT id FROM t1 UNION SELECT id FROM t2 ORDER BY id ASC").unwrap();
    let data_lines: Vec<String> = result.lines().skip(2).collect();
    assert!(data_lines.len() >= 3);
}

#[test]
fn test_union_distinguishes_unrelated_literal_types() {
    let _guard = setup_test();

    let rows = query_rows("SELECT 1 AS x UNION SELECT 'a' AS x").unwrap();

    rows.assert_columns(&["x"]);
    assert_eq!(
        rows.rows,
        vec![vec![Value::Integer(1)], vec![Value::Text("a".to_string())],]
    );
}
