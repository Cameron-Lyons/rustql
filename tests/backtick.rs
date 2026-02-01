use rustql::{process_query, reset_database};
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup_test() -> std::sync::MutexGuard<'static, ()> {
    let guard = TEST_MUTEX.lock().unwrap();
    reset_database();
    guard
}

#[test]
fn test_backtick_column_name() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (id INTEGER, `first name` TEXT)").unwrap();
    process_query("INSERT INTO t VALUES (1, 'Alice')").unwrap();

    let result = process_query("SELECT `first name` FROM t").unwrap();
    assert!(result.contains("Alice"));
}

#[test]
fn test_backtick_reserved_word_as_identifier() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (id INTEGER, `select` TEXT, `from` TEXT)").unwrap();
    process_query("INSERT INTO t VALUES (1, 'val1', 'val2')").unwrap();

    let result = process_query("SELECT `select`, `from` FROM t").unwrap();
    assert!(result.contains("val1"));
    assert!(result.contains("val2"));
}

#[test]
fn test_backtick_in_where_clause() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (id INTEGER, `my col` TEXT)").unwrap();
    process_query("INSERT INTO t VALUES (1, 'hello'), (2, 'world')").unwrap();

    let result = process_query("SELECT * FROM t WHERE `my col` = 'hello'").unwrap();
    assert!(result.contains("hello"));
    assert!(!result.contains("world"));
}

#[test]
fn test_unterminated_backtick_error() {
    let _guard = setup_test();

    let result = process_query("SELECT `unterminated FROM t");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unterminated"));
}

#[test]
fn test_empty_backtick_error() {
    let _guard = setup_test();

    let result = process_query("SELECT `` FROM t");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Empty"));
}
