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
fn test_rollback_insert() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO t VALUES (1, 'Alice')").unwrap();

    process_query("BEGIN TRANSACTION").unwrap();
    process_query("INSERT INTO t VALUES (2, 'Bob')").unwrap();
    process_query("ROLLBACK").unwrap();

    let result = process_query("SELECT * FROM t").unwrap();
    assert!(result.contains("Alice"));
    assert!(!result.contains("Bob"));
}

#[test]
fn test_rollback_update() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO t VALUES (1, 'Alice')").unwrap();

    process_query("BEGIN TRANSACTION").unwrap();
    process_query("UPDATE t SET name = 'Updated' WHERE id = 1").unwrap();
    process_query("ROLLBACK").unwrap();

    let result = process_query("SELECT * FROM t").unwrap();
    assert!(result.contains("Alice"));
    assert!(!result.contains("Updated"));
}

#[test]
fn test_rollback_delete() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    process_query("BEGIN TRANSACTION").unwrap();
    process_query("DELETE FROM t WHERE id = 1").unwrap();
    process_query("ROLLBACK").unwrap();

    let result = process_query("SELECT * FROM t").unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("Bob"));
}

#[test]
fn test_rollback_create_table() {
    let _guard = setup_test();

    process_query("BEGIN TRANSACTION").unwrap();
    process_query("CREATE TABLE new_table (id INTEGER)").unwrap();
    process_query("ROLLBACK").unwrap();

    let result = process_query("SELECT * FROM new_table");
    assert!(result.is_err());
}

#[test]
fn test_rollback_drop_table() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO t VALUES (1, 'Alice')").unwrap();

    process_query("BEGIN TRANSACTION").unwrap();
    process_query("DROP TABLE t").unwrap();
    process_query("ROLLBACK").unwrap();

    let result = process_query("SELECT * FROM t").unwrap();
    assert!(result.contains("Alice"));
}

#[test]
fn test_rollback_alter_add_column() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO t VALUES (1, 'Alice')").unwrap();

    process_query("BEGIN TRANSACTION").unwrap();
    process_query("ALTER TABLE t ADD COLUMN age INTEGER").unwrap();
    process_query("ROLLBACK").unwrap();

    let result = process_query("SELECT * FROM t").unwrap();
    assert!(!result.contains("age"));
}

#[test]
fn test_rollback_alter_drop_column() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (id INTEGER, name TEXT, age INTEGER)").unwrap();
    process_query("INSERT INTO t VALUES (1, 'Alice', 25)").unwrap();

    process_query("BEGIN TRANSACTION").unwrap();
    process_query("ALTER TABLE t DROP COLUMN age").unwrap();
    process_query("ROLLBACK").unwrap();

    let result = process_query("SELECT * FROM t").unwrap();
    assert!(result.contains("age"));
    assert!(result.contains("25"));
}

#[test]
fn test_rollback_alter_rename_column() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO t VALUES (1, 'Alice')").unwrap();

    process_query("BEGIN TRANSACTION").unwrap();
    process_query("ALTER TABLE t RENAME COLUMN name TO username").unwrap();
    process_query("ROLLBACK").unwrap();

    let result = process_query("SELECT name FROM t").unwrap();
    assert!(result.contains("Alice"));
}

#[test]
fn test_rollback_create_index() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO t VALUES (1, 'Alice')").unwrap();

    process_query("BEGIN TRANSACTION").unwrap();
    process_query("CREATE INDEX idx_name ON t (name)").unwrap();
    process_query("ROLLBACK").unwrap();

    let result = process_query("CREATE INDEX idx_name ON t (name)");
    assert!(result.is_ok());
}

#[test]
fn test_rollback_drop_index() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE INDEX idx_name ON t (name)").unwrap();

    process_query("BEGIN TRANSACTION").unwrap();
    process_query("DROP INDEX idx_name").unwrap();
    process_query("ROLLBACK").unwrap();

    let result = process_query("CREATE INDEX idx_name ON t (name)");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("already exists"));
}

#[test]
fn test_rollback_multiple_operations() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    process_query("BEGIN TRANSACTION").unwrap();
    process_query("INSERT INTO t VALUES (3, 'Charlie')").unwrap();
    process_query("UPDATE t SET name = 'Updated' WHERE id = 1").unwrap();
    process_query("DELETE FROM t WHERE id = 2").unwrap();
    process_query("ROLLBACK").unwrap();

    let result = process_query("SELECT * FROM t").unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("Bob"));
    assert!(!result.contains("Charlie"));
    assert!(!result.contains("Updated"));
}
