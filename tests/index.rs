mod common;
use common::*;
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup_test<'a>() -> std::sync::MutexGuard<'a, ()> {
    let guard = TEST_MUTEX.lock().unwrap();
    reset_database();
    guard
}

#[test]
fn test_create_index() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    execute_sql("INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 35)")
        .unwrap();

    let result = execute_sql("CREATE INDEX idx_age ON users (age)");
    assert!(result.is_ok());
    assert_command(result.unwrap(), CommandTag::CreateIndex, 0);

    let result = execute_sql("CREATE INDEX idx_age ON users (age)");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("already exists"));
}

#[test]
fn test_drop_index() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    execute_sql("CREATE INDEX idx_name ON users (name)").unwrap();

    let result = execute_sql("DROP INDEX idx_name");
    assert!(result.is_ok());
    assert_command(result.unwrap(), CommandTag::DropIndex, 0);

    let result = execute_sql("DROP INDEX idx_name");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("does not exist"));
}

#[test]
fn test_index_maintenance_on_insert() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    execute_sql("CREATE INDEX idx_age ON users (age)").unwrap();

    let result = execute_sql("INSERT INTO users VALUES (1, 'Alice', 25)");
    assert!(result.is_ok());

    let result = execute_sql("SELECT * FROM users WHERE age = 25");
    assert!(result.is_ok());
    assert!(result.unwrap().contains("Alice"));
}

#[test]
fn test_index_maintenance_on_update() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    execute_sql("INSERT INTO users VALUES (1, 'Alice', 25)").unwrap();
    execute_sql("CREATE INDEX idx_age ON users (age)").unwrap();

    let result = execute_sql("UPDATE users SET age = 26 WHERE name = 'Alice'");
    assert!(result.is_ok());

    let result = execute_sql("SELECT * FROM users WHERE age = 25");
    assert!(result.is_ok());
    assert!(!result.unwrap().contains("Alice"));

    let result = execute_sql("SELECT * FROM users WHERE age = 26");
    assert!(result.is_ok());
    assert!(result.unwrap().contains("Alice"));
}

#[test]
fn test_index_maintenance_on_delete() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    execute_sql("INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30)").unwrap();
    execute_sql("CREATE INDEX idx_age ON users (age)").unwrap();

    let result = execute_sql("DELETE FROM users WHERE name = 'Alice'");
    assert!(result.is_ok());

    let result = execute_sql("SELECT * FROM users WHERE age = 25");
    assert!(result.is_ok());
    assert!(!result.unwrap().contains("Alice"));

    let result = execute_sql("SELECT * FROM users WHERE age = 30");
    assert!(result.is_ok());
    assert!(result.unwrap().contains("Bob"));
}
