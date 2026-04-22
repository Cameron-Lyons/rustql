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
fn test_rollback_insert() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();

    execute_sql("BEGIN TRANSACTION").unwrap();
    execute_sql("INSERT INTO t VALUES (2, 'Bob')").unwrap();
    execute_sql("ROLLBACK").unwrap();

    let result = execute_sql("SELECT * FROM t").unwrap();
    assert!(result.contains("Alice"));
    assert!(!result.contains("Bob"));
}

#[test]
fn test_rollback_update() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();

    execute_sql("BEGIN TRANSACTION").unwrap();
    execute_sql("UPDATE t SET name = 'Updated' WHERE id = 1").unwrap();
    execute_sql("ROLLBACK").unwrap();

    let result = execute_sql("SELECT * FROM t").unwrap();
    assert!(result.contains("Alice"));
    assert!(!result.contains("Updated"));
}

#[test]
fn test_rollback_delete() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    execute_sql("BEGIN TRANSACTION").unwrap();
    execute_sql("DELETE FROM t WHERE id = 1").unwrap();
    execute_sql("ROLLBACK").unwrap();

    let result = execute_sql("SELECT * FROM t").unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("Bob"));
}

#[test]
fn test_rollback_create_table() {
    let _guard = setup_test();

    execute_sql("BEGIN TRANSACTION").unwrap();
    execute_sql("CREATE TABLE new_table (id INTEGER)").unwrap();
    execute_sql("ROLLBACK").unwrap();

    let result = execute_sql("SELECT * FROM new_table");
    assert!(result.is_err());
}

#[test]
fn test_rollback_drop_table() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();

    execute_sql("BEGIN TRANSACTION").unwrap();
    execute_sql("DROP TABLE t").unwrap();
    execute_sql("ROLLBACK").unwrap();

    let result = execute_sql("SELECT * FROM t").unwrap();
    assert!(result.contains("Alice"));
}

#[test]
fn test_rollback_alter_add_column() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();

    execute_sql("BEGIN TRANSACTION").unwrap();
    execute_sql("ALTER TABLE t ADD COLUMN age INTEGER").unwrap();
    execute_sql("ROLLBACK").unwrap();

    let result = execute_sql("SELECT * FROM t").unwrap();
    assert!(!result.contains("age"));
}

#[test]
fn test_rollback_alter_drop_column() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT, age INTEGER)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'Alice', 25)").unwrap();

    execute_sql("BEGIN TRANSACTION").unwrap();
    execute_sql("ALTER TABLE t DROP COLUMN age").unwrap();
    execute_sql("ROLLBACK").unwrap();

    let result = execute_sql("SELECT * FROM t").unwrap();
    assert!(result.contains("age"));
    assert!(result.contains("25"));
}

#[test]
fn test_rollback_alter_rename_column() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();

    execute_sql("BEGIN TRANSACTION").unwrap();
    execute_sql("ALTER TABLE t RENAME COLUMN name TO username").unwrap();
    execute_sql("ROLLBACK").unwrap();

    let result = execute_sql("SELECT name FROM t").unwrap();
    assert!(result.contains("Alice"));
}

#[test]
fn test_rollback_create_index() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();

    execute_sql("BEGIN TRANSACTION").unwrap();
    execute_sql("CREATE INDEX idx_name ON t (name)").unwrap();
    execute_sql("ROLLBACK").unwrap();

    let result = execute_sql("CREATE INDEX idx_name ON t (name)");
    assert!(result.is_ok());
}

#[test]
fn test_rollback_drop_index() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    execute_sql("CREATE INDEX idx_name ON t (name)").unwrap();

    execute_sql("BEGIN TRANSACTION").unwrap();
    execute_sql("DROP INDEX idx_name").unwrap();
    execute_sql("ROLLBACK").unwrap();

    let result = execute_sql("CREATE INDEX idx_name ON t (name)");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("already exists"));
}

#[test]
fn test_rollback_multiple_operations() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    execute_sql("BEGIN TRANSACTION").unwrap();
    execute_sql("INSERT INTO t VALUES (3, 'Charlie')").unwrap();
    execute_sql("UPDATE t SET name = 'Updated' WHERE id = 1").unwrap();
    execute_sql("DELETE FROM t WHERE id = 2").unwrap();
    execute_sql("ROLLBACK").unwrap();

    let result = execute_sql("SELECT * FROM t").unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("Bob"));
    assert!(!result.contains("Charlie"));
    assert!(!result.contains("Updated"));
}
