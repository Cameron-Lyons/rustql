mod common;
use common::*;
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

    execute_sql("CREATE TABLE t (id INTEGER, `first name` TEXT)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();

    let result = execute_sql("SELECT `first name` FROM t").unwrap();
    assert!(result.contains("Alice"));
}

#[test]
fn test_backtick_reserved_word_as_identifier() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, `select` TEXT, `from` TEXT)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'val1', 'val2')").unwrap();

    let result = execute_sql("SELECT `select`, `from` FROM t").unwrap();
    assert!(result.contains("val1"));
    assert!(result.contains("val2"));
}

#[test]
fn test_backtick_in_where_clause() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, `my col` TEXT)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'hello'), (2, 'world')").unwrap();

    let result = execute_sql("SELECT * FROM t WHERE `my col` = 'hello'").unwrap();
    assert!(result.contains("hello"));
    assert!(!result.contains("world"));
}

#[test]
fn test_double_quoted_column_name() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, \"first name\" TEXT)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();

    let result = execute_sql("SELECT \"first name\" FROM t").unwrap();
    assert!(result.contains("Alice"));
}

#[test]
fn test_double_quoted_reserved_word_as_identifier() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, \"select\" TEXT, \"from\" TEXT)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'val1', 'val2')").unwrap();

    let result = execute_sql("SELECT \"select\", \"from\" FROM t").unwrap();
    assert!(result.contains("val1"));
    assert!(result.contains("val2"));
}

#[test]
fn test_double_quoted_identifier_escapes_quote() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, \"a\"\"b\" TEXT)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'quoted')").unwrap();

    let result = execute_sql("SELECT \"a\"\"b\" FROM t").unwrap();
    assert!(result.contains("quoted"));
}

#[test]
fn test_double_quoted_identifier_in_view_round_trips() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE people (id INTEGER, \"first name\" TEXT)").unwrap();
    execute_sql("INSERT INTO people VALUES (1, 'Alice')").unwrap();
    execute_sql("CREATE VIEW people_names AS SELECT \"first name\" FROM people").unwrap();

    let result = execute_sql("SELECT * FROM people_names").unwrap();
    assert!(result.contains("Alice"));
}

#[test]
fn test_double_quoted_identifier_in_check_round_trips() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE items (\"item name\" TEXT CHECK (\"item name\" <> 'bad'))").unwrap();

    assert!(execute_sql("INSERT INTO items VALUES ('good')").is_ok());
    assert!(execute_sql("INSERT INTO items VALUES ('bad')").is_err());
}

#[test]
fn test_unterminated_backtick_error() {
    let _guard = setup_test();

    let result = execute_sql("SELECT `unterminated FROM t");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unterminated"));
}

#[test]
fn test_empty_backtick_error() {
    let _guard = setup_test();

    let result = execute_sql("SELECT `` FROM t");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Empty"));
}

#[test]
fn test_unterminated_double_quote_error() {
    let _guard = setup_test();

    let result = execute_sql("SELECT \"unterminated FROM t");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unterminated"));
}

#[test]
fn test_empty_double_quote_error() {
    let _guard = setup_test();

    let result = execute_sql("SELECT \"\" FROM t");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Empty"));
}
