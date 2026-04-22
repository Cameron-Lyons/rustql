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
fn test_ilike_basic() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    execute_sql("INSERT INTO users VALUES (2, 'BOB')").unwrap();
    execute_sql("INSERT INTO users VALUES (3, 'Charlie')").unwrap();

    let result = execute_sql("SELECT name FROM users WHERE name ILIKE 'alice'").unwrap();
    assert!(result.contains("Alice"));
    assert!(!result.contains("BOB"));
}

#[test]
fn test_ilike_with_wildcards() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE items (id INTEGER, label TEXT)").unwrap();
    execute_sql("INSERT INTO items VALUES (1, 'Apple')").unwrap();
    execute_sql("INSERT INTO items VALUES (2, 'APRICOT')").unwrap();
    execute_sql("INSERT INTO items VALUES (3, 'Banana')").unwrap();

    let result = execute_sql("SELECT label FROM items WHERE label ILIKE 'ap%'").unwrap();
    assert!(result.contains("Apple"));
    assert!(result.contains("APRICOT"));
    assert!(!result.contains("Banana"));
}

#[test]
fn test_ilike_underscore_wildcard() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE words (id INTEGER, word TEXT)").unwrap();
    execute_sql("INSERT INTO words VALUES (1, 'Cat')").unwrap();
    execute_sql("INSERT INTO words VALUES (2, 'CAR')").unwrap();
    execute_sql("INSERT INTO words VALUES (3, 'Dog')").unwrap();

    let result = execute_sql("SELECT word FROM words WHERE word ILIKE 'ca_'").unwrap();
    assert!(result.contains("Cat"));
    assert!(result.contains("CAR"));
    assert!(!result.contains("Dog"));
}

#[test]
fn test_ilike_mixed_case_pattern() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE products (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO products VALUES (1, 'iPhone')").unwrap();
    execute_sql("INSERT INTO products VALUES (2, 'IPHONE')").unwrap();
    execute_sql("INSERT INTO products VALUES (3, 'iphone')").unwrap();

    let result = execute_sql("SELECT name FROM products WHERE name ILIKE 'IpHoNe'").unwrap();
    assert!(result.contains("iPhone"));
    assert!(result.contains("IPHONE"));
    assert!(result.contains("iphone"));
}

#[test]
fn test_ilike_no_match() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE colors (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO colors VALUES (1, 'Red')").unwrap();
    execute_sql("INSERT INTO colors VALUES (2, 'BLUE')").unwrap();
    execute_sql("INSERT INTO colors VALUES (3, 'Green')").unwrap();

    let result = execute_sql("SELECT name FROM colors WHERE name ILIKE 'yellow'").unwrap();
    assert!(!result.contains("Red"));
    assert!(!result.contains("BLUE"));
    assert!(!result.contains("Green"));
}
