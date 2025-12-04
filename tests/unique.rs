use rustql::process_query;
use rustql::reset_database;
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup_test<'a>() -> std::sync::MutexGuard<'a, ()> {
    let guard = TEST_MUTEX.lock().unwrap();
    reset_database();
    guard
}

#[test]
fn test_unique_constraint() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, email TEXT UNIQUE, name TEXT)").unwrap();

    // First insert should succeed
    let result = process_query("INSERT INTO users VALUES (1, 'alice@example.com', 'Alice')");
    assert!(result.is_ok());

    // Duplicate email should fail
    let result = process_query("INSERT INTO users VALUES (2, 'alice@example.com', 'Bob')");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unique constraint violation"));

    // Different email should succeed
    let result = process_query("INSERT INTO users VALUES (3, 'bob@example.com', 'Bob')");
    assert!(result.is_ok());
}

#[test]
fn test_unique_allows_null() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, email TEXT UNIQUE, name TEXT)").unwrap();

    // Multiple NULL values should be allowed (UNIQUE allows NULL)
    let result = process_query("INSERT INTO users VALUES (1, NULL, 'Alice')");
    assert!(result.is_ok());

    let result = process_query("INSERT INTO users VALUES (2, NULL, 'Bob')");
    assert!(result.is_ok());

    // But duplicate non-NULL values should fail
    let result = process_query("INSERT INTO users VALUES (3, 'test@example.com', 'Charlie')");
    assert!(result.is_ok());

    let result = process_query("INSERT INTO users VALUES (4, 'test@example.com', 'David')");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unique constraint violation"));
}

#[test]
fn test_unique_with_update() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, email TEXT UNIQUE, name TEXT)").unwrap();
    process_query("INSERT INTO users VALUES (1, 'alice@example.com', 'Alice')").unwrap();
    process_query("INSERT INTO users VALUES (2, 'bob@example.com', 'Bob')").unwrap();

    // Update to duplicate email should fail
    let result = process_query("UPDATE users SET email = 'alice@example.com' WHERE id = 2");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unique constraint violation"));

    // Update to unique email should succeed
    let result = process_query("UPDATE users SET email = 'charlie@example.com' WHERE id = 2");
    assert!(result.is_ok());

    // Update to NULL should succeed (multiple NULLs allowed)
    let result = process_query("UPDATE users SET email = NULL WHERE id = 2");
    assert!(result.is_ok());
}

#[test]
fn test_unique_with_primary_key() {
    let _guard = setup_test();

    // A column can be both PRIMARY KEY and UNIQUE (though PRIMARY KEY already implies uniqueness)
    process_query("CREATE TABLE users (id INTEGER PRIMARY KEY UNIQUE, name TEXT)").unwrap();

    let result = process_query("INSERT INTO users VALUES (1, 'Alice')");
    assert!(result.is_ok());

    // Duplicate primary key should fail
    let result = process_query("INSERT INTO users VALUES (1, 'Bob')");
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .contains("Primary key constraint violation")
    );
}

#[test]
fn test_describe_shows_unique() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, email TEXT UNIQUE, name TEXT)").unwrap();

    let result = process_query("DESCRIBE users").unwrap();
    assert!(result.contains("Unique"));
    assert!(result.contains("email"));
    // Check that email shows as unique
    let lines: Vec<&str> = result.lines().collect();
    let email_line = lines.iter().find(|line| line.contains("email"));
    assert!(email_line.is_some());
    assert!(email_line.unwrap().contains("YES")); // Should show YES in Unique column
}
