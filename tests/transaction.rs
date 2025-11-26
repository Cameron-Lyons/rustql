use rustql::{process_query, reset_database};
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup_test<'a>() -> std::sync::MutexGuard<'a, ()> {
    let guard = TEST_MUTEX.lock().unwrap();
    reset_database();
    guard
}

#[test]
fn test_begin_commit_transaction() {
    let _guard = setup_test();
    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();

    let result = process_query("BEGIN TRANSACTION");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Transaction begun");

    process_query("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    process_query("INSERT INTO users VALUES (2, 'Bob')").unwrap();

    let result = process_query("SELECT * FROM users");
    assert!(result.is_ok());
    let result_str = result.unwrap();
    assert!(result_str.contains("Alice"));
    assert!(result_str.contains("Bob"));

    let result = process_query("COMMIT TRANSACTION");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Transaction committed");

    let result = process_query("SELECT * FROM users");
    assert!(result.is_ok());
    let result_str = result.unwrap();
    assert!(result_str.contains("Alice"));
    assert!(result_str.contains("Bob"));
}

#[test]
fn test_begin_rollback_transaction() {
    let _guard = setup_test();
    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO users VALUES (1, 'Alice')").unwrap();

    let result = process_query("BEGIN TRANSACTION");
    assert!(result.is_ok());

    process_query("INSERT INTO users VALUES (2, 'Bob')").unwrap();
    process_query("INSERT INTO users VALUES (3, 'Charlie')").unwrap();

    let result = process_query("SELECT * FROM users");
    assert!(result.is_ok());
    let result_str = result.unwrap();
    assert!(result_str.contains("Alice"));
    assert!(result_str.contains("Bob"));
    assert!(result_str.contains("Charlie"));

    let result = process_query("ROLLBACK TRANSACTION");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Transaction rolled back");

    let result = process_query("SELECT * FROM users");
    assert!(result.is_ok());
    let result_str = result.unwrap();
    assert!(result_str.contains("Alice"));
    assert!(!result_str.contains("Bob"));
    assert!(!result_str.contains("Charlie"));
}

#[test]
fn test_nested_transaction_error() {
    let _guard = setup_test();
    process_query("BEGIN TRANSACTION").unwrap();

    let result = process_query("BEGIN TRANSACTION");
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .contains("Transaction already in progress")
    );

    process_query("ROLLBACK TRANSACTION").unwrap();
}

#[test]
fn test_commit_without_transaction_error() {
    let _guard = setup_test();

    let result = process_query("COMMIT TRANSACTION");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("No transaction in progress"));
}

#[test]
fn test_rollback_without_transaction_error() {
    let _guard = setup_test();

    let result = process_query("ROLLBACK TRANSACTION");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("No transaction in progress"));
}
