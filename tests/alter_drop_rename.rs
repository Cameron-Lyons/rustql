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
fn test_alter_drop_column_basic() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT, age INTEGER)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'Alice', 25)").unwrap();

    execute_sql("ALTER TABLE t DROP COLUMN age").unwrap();

    let result = execute_sql("SELECT * FROM t").unwrap();
    assert!(result.contains("id"));
    assert!(result.contains("name"));
    assert!(!result.contains("age"));
    assert!(result.contains("Alice"));
    assert!(!result.contains("25"));
}

#[test]
fn test_alter_drop_column_data_preserved() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (a INTEGER, b TEXT, c INTEGER)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'hello', 100)").unwrap();
    execute_sql("INSERT INTO t VALUES (2, 'world', 200)").unwrap();

    execute_sql("ALTER TABLE t DROP COLUMN b").unwrap();

    let result = execute_sql("SELECT * FROM t").unwrap();
    assert!(result.contains("1"));
    assert!(result.contains("100"));
    assert!(result.contains("2"));
    assert!(result.contains("200"));
    assert!(!result.contains("hello"));
    assert!(!result.contains("world"));
}

#[test]
fn test_alter_drop_column_select_dropped_errors() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT, age INTEGER)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'Alice', 25)").unwrap();

    execute_sql("ALTER TABLE t DROP COLUMN age").unwrap();

    let result = execute_sql("SELECT age FROM t");
    assert!(result.is_err());
}

#[test]
fn test_alter_drop_column_nonexistent() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();

    let result = execute_sql("ALTER TABLE t DROP COLUMN nonexistent");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("does not exist"));
}

#[test]
fn test_alter_rename_column_basic() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();

    execute_sql("ALTER TABLE t RENAME COLUMN name TO username").unwrap();

    let result = execute_sql("SELECT username FROM t").unwrap();
    assert!(result.contains("Alice"));
}

#[test]
fn test_alter_rename_column_old_name_fails() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();

    execute_sql("ALTER TABLE t RENAME COLUMN name TO username").unwrap();

    let result = execute_sql("SELECT name FROM t");
    assert!(result.is_err());
}

#[test]
fn test_alter_rename_column_data_preserved() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    execute_sql("ALTER TABLE t RENAME COLUMN name TO username").unwrap();

    let result = execute_sql("SELECT * FROM t").unwrap();
    assert!(result.contains("username"));
    assert!(!result.contains("\tname"));
    assert!(result.contains("Alice"));
    assert!(result.contains("Bob"));
}

#[test]
fn test_alter_rename_to_existing_name_errors() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();

    let result = execute_sql("ALTER TABLE t RENAME COLUMN name TO id");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("already exists"));
}

#[test]
fn test_alter_rename_nonexistent_column_errors() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();

    let result = execute_sql("ALTER TABLE t RENAME COLUMN nonexistent TO something");
    assert!(result.is_err());
}
