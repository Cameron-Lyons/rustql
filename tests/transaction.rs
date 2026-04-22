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
fn test_begin_commit_transaction() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();

    let result = execute_sql("BEGIN TRANSACTION");
    assert!(result.is_ok());
    assert_command(result.unwrap(), CommandTag::BeginTransaction, 0);

    execute_sql("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    execute_sql("INSERT INTO users VALUES (2, 'Bob')").unwrap();

    let result = execute_sql("SELECT * FROM users");
    assert!(result.is_ok());
    let result_str = result.unwrap();
    assert!(result_str.contains("Alice"));
    assert!(result_str.contains("Bob"));

    let result = execute_sql("COMMIT TRANSACTION");
    assert!(result.is_ok());
    assert_command(result.unwrap(), CommandTag::CommitTransaction, 0);

    let result = execute_sql("SELECT * FROM users");
    assert!(result.is_ok());
    let result_str = result.unwrap();
    assert!(result_str.contains("Alice"));
    assert!(result_str.contains("Bob"));
}

#[test]
fn test_begin_rollback_transaction() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO users VALUES (1, 'Alice')").unwrap();

    let result = execute_sql("BEGIN TRANSACTION");
    assert!(result.is_ok());

    execute_sql("INSERT INTO users VALUES (2, 'Bob')").unwrap();
    execute_sql("INSERT INTO users VALUES (3, 'Charlie')").unwrap();

    let result = execute_sql("SELECT * FROM users");
    assert!(result.is_ok());
    let result_str = result.unwrap();
    assert!(result_str.contains("Alice"));
    assert!(result_str.contains("Bob"));
    assert!(result_str.contains("Charlie"));

    let result = execute_sql("ROLLBACK TRANSACTION");
    assert!(result.is_ok());
    assert_command(result.unwrap(), CommandTag::RollbackTransaction, 0);

    let result = execute_sql("SELECT * FROM users");
    assert!(result.is_ok());
    let result_str = result.unwrap();
    assert!(result_str.contains("Alice"));
    assert!(!result_str.contains("Bob"));
    assert!(!result_str.contains("Charlie"));
}

#[test]
fn test_nested_transaction_error() {
    let _guard = setup_test();
    execute_sql("BEGIN TRANSACTION").unwrap();

    let result = execute_sql("BEGIN TRANSACTION");
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .contains("Transaction already in progress")
    );

    execute_sql("ROLLBACK TRANSACTION").unwrap();
}

#[test]
fn test_commit_without_transaction_error() {
    let _guard = setup_test();

    let result = execute_sql("COMMIT TRANSACTION");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("No transaction in progress"));
}

#[test]
fn test_rollback_without_transaction_error() {
    let _guard = setup_test();

    let result = execute_sql("ROLLBACK TRANSACTION");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("No transaction in progress"));
}
