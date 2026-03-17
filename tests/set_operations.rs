use rustql::testing::{process_query, reset_database};
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup_test<'a>() -> std::sync::MutexGuard<'a, ()> {
    let guard = TEST_MUTEX.lock().unwrap();
    reset_database();
    guard
}

#[test]
fn test_intersect_basic() {
    let _guard = setup_test();
    process_query("CREATE TABLE t1 (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE t2 (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO t1 VALUES (1, 'Alice')").unwrap();
    process_query("INSERT INTO t1 VALUES (2, 'Bob')").unwrap();
    process_query("INSERT INTO t1 VALUES (3, 'Charlie')").unwrap();
    process_query("INSERT INTO t2 VALUES (2, 'Bob')").unwrap();
    process_query("INSERT INTO t2 VALUES (3, 'Charlie')").unwrap();
    process_query("INSERT INTO t2 VALUES (4, 'Diana')").unwrap();

    let result =
        process_query("SELECT id, name FROM t1 INTERSECT SELECT id, name FROM t2").unwrap();
    assert!(result.contains("Bob"));
    assert!(result.contains("Charlie"));
    assert!(!result.contains("Alice"));
    assert!(!result.contains("Diana"));
}

#[test]
fn test_intersect_no_overlap() {
    let _guard = setup_test();
    process_query("CREATE TABLE t1 (id INTEGER)").unwrap();
    process_query("CREATE TABLE t2 (id INTEGER)").unwrap();
    process_query("INSERT INTO t1 VALUES (1)").unwrap();
    process_query("INSERT INTO t1 VALUES (2)").unwrap();
    process_query("INSERT INTO t2 VALUES (3)").unwrap();
    process_query("INSERT INTO t2 VALUES (4)").unwrap();

    let result = process_query("SELECT id FROM t1 INTERSECT SELECT id FROM t2").unwrap();
    let lines: Vec<&str> = result.lines().collect();
    assert_eq!(lines.len(), 2);
}

#[test]
fn test_intersect_all() {
    let _guard = setup_test();
    process_query("CREATE TABLE t1 (val INTEGER)").unwrap();
    process_query("CREATE TABLE t2 (val INTEGER)").unwrap();
    process_query("INSERT INTO t1 VALUES (1)").unwrap();
    process_query("INSERT INTO t1 VALUES (1)").unwrap();
    process_query("INSERT INTO t1 VALUES (2)").unwrap();
    process_query("INSERT INTO t2 VALUES (1)").unwrap();
    process_query("INSERT INTO t2 VALUES (1)").unwrap();
    process_query("INSERT INTO t2 VALUES (1)").unwrap();

    let result = process_query("SELECT val FROM t1 INTERSECT ALL SELECT val FROM t2").unwrap();
    let data_lines: Vec<&str> = result.lines().skip(2).collect();
    let ones = data_lines.iter().filter(|l| l.contains("1")).count();
    assert_eq!(ones, 2);
}

#[test]
fn test_except_basic() {
    let _guard = setup_test();
    process_query("CREATE TABLE t1 (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE t2 (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO t1 VALUES (1, 'Alice')").unwrap();
    process_query("INSERT INTO t1 VALUES (2, 'Bob')").unwrap();
    process_query("INSERT INTO t1 VALUES (3, 'Charlie')").unwrap();
    process_query("INSERT INTO t2 VALUES (2, 'Bob')").unwrap();
    process_query("INSERT INTO t2 VALUES (3, 'Charlie')").unwrap();

    let result = process_query("SELECT id, name FROM t1 EXCEPT SELECT id, name FROM t2").unwrap();
    assert!(result.contains("Alice"));
    assert!(!result.contains("Bob"));
    assert!(!result.contains("Charlie"));
}

#[test]
fn test_except_no_overlap() {
    let _guard = setup_test();
    process_query("CREATE TABLE t1 (id INTEGER)").unwrap();
    process_query("CREATE TABLE t2 (id INTEGER)").unwrap();
    process_query("INSERT INTO t1 VALUES (1)").unwrap();
    process_query("INSERT INTO t1 VALUES (2)").unwrap();
    process_query("INSERT INTO t2 VALUES (3)").unwrap();

    let result = process_query("SELECT id FROM t1 EXCEPT SELECT id FROM t2").unwrap();
    assert!(result.contains("1"));
    assert!(result.contains("2"));
}

#[test]
fn test_except_all() {
    let _guard = setup_test();
    process_query("CREATE TABLE t1 (val INTEGER)").unwrap();
    process_query("CREATE TABLE t2 (val INTEGER)").unwrap();
    process_query("INSERT INTO t1 VALUES (1)").unwrap();
    process_query("INSERT INTO t1 VALUES (1)").unwrap();
    process_query("INSERT INTO t1 VALUES (1)").unwrap();
    process_query("INSERT INTO t2 VALUES (1)").unwrap();

    let result = process_query("SELECT val FROM t1 EXCEPT ALL SELECT val FROM t2").unwrap();
    let data_lines: Vec<&str> = result.lines().skip(2).collect();
    let ones = data_lines.iter().filter(|l| l.contains("1")).count();
    assert_eq!(ones, 2);
}

#[test]
fn test_set_ops_with_order_by() {
    let _guard = setup_test();
    process_query("CREATE TABLE t1 (id INTEGER)").unwrap();
    process_query("CREATE TABLE t2 (id INTEGER)").unwrap();
    process_query("INSERT INTO t1 VALUES (3)").unwrap();
    process_query("INSERT INTO t1 VALUES (1)").unwrap();
    process_query("INSERT INTO t2 VALUES (2)").unwrap();
    process_query("INSERT INTO t2 VALUES (1)").unwrap();

    let result =
        process_query("SELECT id FROM t1 UNION SELECT id FROM t2 ORDER BY id ASC").unwrap();
    let data_lines: Vec<&str> = result.lines().skip(2).collect();
    assert!(data_lines.len() >= 3);
}
