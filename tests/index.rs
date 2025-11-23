use rustql::{process_query, reset_database};
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
    process_query("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    process_query("INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 35)")
        .unwrap();

    let result = process_query("CREATE INDEX idx_age ON users (age)");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Index 'idx_age' created on users.age");

    // Try to create duplicate index
    let result = process_query("CREATE INDEX idx_age ON users (age)");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("already exists"));
}

#[test]
fn test_drop_index() {
    let _guard = setup_test();
    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE INDEX idx_name ON users (name)").unwrap();

    let result = process_query("DROP INDEX idx_name");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Index 'idx_name' dropped");

    // Try to drop non-existent index
    let result = process_query("DROP INDEX idx_name");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("does not exist"));
}

#[test]
fn test_index_maintenance_on_insert() {
    let _guard = setup_test();
    process_query("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    process_query("CREATE INDEX idx_age ON users (age)").unwrap();

    // Insert should maintain index
    let result = process_query("INSERT INTO users VALUES (1, 'Alice', 25)");
    assert!(result.is_ok());

    // Query should still work
    let result = process_query("SELECT * FROM users WHERE age = 25");
    assert!(result.is_ok());
    assert!(result.unwrap().contains("Alice"));
}

#[test]
fn test_index_maintenance_on_update() {
    let _guard = setup_test();
    process_query("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    process_query("INSERT INTO users VALUES (1, 'Alice', 25)").unwrap();
    process_query("CREATE INDEX idx_age ON users (age)").unwrap();

    // Update should maintain index
    let result = process_query("UPDATE users SET age = 26 WHERE name = 'Alice'");
    assert!(result.is_ok());

    // Query with old value should not find
    let result = process_query("SELECT * FROM users WHERE age = 25");
    assert!(result.is_ok());
    assert!(!result.unwrap().contains("Alice"));

    // Query with new value should find
    let result = process_query("SELECT * FROM users WHERE age = 26");
    assert!(result.is_ok());
    assert!(result.unwrap().contains("Alice"));
}

#[test]
fn test_index_maintenance_on_delete() {
    let _guard = setup_test();
    process_query("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    process_query("INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30)").unwrap();
    process_query("CREATE INDEX idx_age ON users (age)").unwrap();

    // Delete should maintain index
    let result = process_query("DELETE FROM users WHERE name = 'Alice'");
    assert!(result.is_ok());

    // Query should not find deleted row
    let result = process_query("SELECT * FROM users WHERE age = 25");
    assert!(result.is_ok());
    assert!(!result.unwrap().contains("Alice"));

    // Other rows should still be findable
    let result = process_query("SELECT * FROM users WHERE age = 30");
    assert!(result.is_ok());
    assert!(result.unwrap().contains("Bob"));
}
