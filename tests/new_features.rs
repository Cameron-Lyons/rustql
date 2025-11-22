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
fn test_insert_with_column_names() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();

    let result = process_query("INSERT INTO users (name, age, id) VALUES ('Alice', 25, 1)");
    assert!(result.is_ok(), "Insert with column names should succeed");
    assert_eq!(result.unwrap(), "1 row(s) inserted");

    let result = process_query("SELECT id, name, age FROM users");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("1"));
    assert!(output.contains("Alice"));
    assert!(output.contains("25"));
}

#[test]
fn test_arithmetic_expressions_in_select() {
    let _guard = setup_test();

    process_query("CREATE TABLE products (id INTEGER, price FLOAT, quantity INTEGER)").unwrap();
    process_query("INSERT INTO products VALUES (1, 10.5, 3), (2, 20.0, 2)").unwrap();

    let result = process_query("SELECT id, price * quantity AS total FROM products");
    match &result {
        Ok(output) => {
            println!("Success! Output: {}", output);
            assert!(output.contains("total"));
            assert!(output.contains("31.5")); // 10.5 * 3
        }
        Err(e) => {
            panic!("Arithmetic expression failed with error: {}", e);
        }
    }

    let result = process_query("SELECT id, price - quantity AS diff FROM products");
    assert!(result.is_ok());

    let result = process_query("SELECT id, price / quantity AS unit_price FROM products");
    assert!(result.is_ok());
}

#[test]
fn test_union() {
    let _guard = setup_test();

    process_query("CREATE TABLE users1 (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE users2 (id INTEGER, name TEXT)").unwrap();

    process_query("INSERT INTO users1 VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
    process_query("INSERT INTO users2 VALUES (3, 'Charlie'), (1, 'Alice')").unwrap();

    let result = process_query("SELECT name FROM users1 UNION SELECT name FROM users2");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("Alice"));
    assert!(output.contains("Bob"));
    assert!(output.contains("Charlie"));
    let alice_count = output.matches("Alice").count();
    assert_eq!(alice_count, 1, "UNION should remove duplicate 'Alice'");
}

#[test]
fn test_primary_key() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").unwrap();

    let result = process_query("INSERT INTO users VALUES (1, 'Alice')");
    assert!(result.is_ok());

    let result = process_query("INSERT INTO users VALUES (1, 'Bob')");
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .contains("Primary key constraint violation")
    );

    let result = process_query("INSERT INTO users VALUES (NULL, 'Charlie')");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("cannot be NULL"));

    let result = process_query("INSERT INTO users VALUES (2, 'David')");
    assert!(result.is_ok());
}

#[test]
fn test_default_values() {
    let _guard = setup_test();

    process_query(
        "CREATE TABLE users (id INTEGER, name TEXT DEFAULT 'Unknown', age INTEGER DEFAULT 0)",
    )
    .unwrap();

    let result = process_query("INSERT INTO users (id) VALUES (1)");
    assert!(result.is_ok());

    let result = process_query("SELECT * FROM users WHERE id = 1").unwrap();
    assert!(result.contains("Unknown"));
    assert!(result.contains("0"));

    let result = process_query("INSERT INTO users VALUES (2, 'Alice', 25)");
    assert!(result.is_ok());

    let result = process_query("SELECT * FROM users WHERE id = 2").unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("25"));
}

#[test]
fn test_primary_key_with_default() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT DEFAULT 'User')").unwrap();

    let result = process_query("INSERT INTO users (id) VALUES (1)");
    assert!(result.is_ok());

    let result = process_query("SELECT * FROM users WHERE id = 1").unwrap();
    assert!(result.contains("User"));
}
