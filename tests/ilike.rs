use rustql::{process_query, reset_database};
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
    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    process_query("INSERT INTO users VALUES (2, 'BOB')").unwrap();
    process_query("INSERT INTO users VALUES (3, 'Charlie')").unwrap();

    let result = process_query("SELECT name FROM users WHERE name ILIKE 'alice'").unwrap();
    assert!(result.contains("Alice"));
    assert!(!result.contains("BOB"));
}

#[test]
fn test_ilike_with_wildcards() {
    let _guard = setup_test();
    process_query("CREATE TABLE items (id INTEGER, label TEXT)").unwrap();
    process_query("INSERT INTO items VALUES (1, 'Apple')").unwrap();
    process_query("INSERT INTO items VALUES (2, 'APRICOT')").unwrap();
    process_query("INSERT INTO items VALUES (3, 'Banana')").unwrap();

    let result = process_query("SELECT label FROM items WHERE label ILIKE 'ap%'").unwrap();
    assert!(result.contains("Apple"));
    assert!(result.contains("APRICOT"));
    assert!(!result.contains("Banana"));
}

#[test]
fn test_ilike_underscore_wildcard() {
    let _guard = setup_test();
    process_query("CREATE TABLE words (id INTEGER, word TEXT)").unwrap();
    process_query("INSERT INTO words VALUES (1, 'Cat')").unwrap();
    process_query("INSERT INTO words VALUES (2, 'CAR')").unwrap();
    process_query("INSERT INTO words VALUES (3, 'Dog')").unwrap();

    let result = process_query("SELECT word FROM words WHERE word ILIKE 'ca_'").unwrap();
    assert!(result.contains("Cat"));
    assert!(result.contains("CAR"));
    assert!(!result.contains("Dog"));
}

#[test]
fn test_ilike_mixed_case_pattern() {
    let _guard = setup_test();
    process_query("CREATE TABLE products (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO products VALUES (1, 'iPhone')").unwrap();
    process_query("INSERT INTO products VALUES (2, 'IPHONE')").unwrap();
    process_query("INSERT INTO products VALUES (3, 'iphone')").unwrap();

    let result = process_query("SELECT name FROM products WHERE name ILIKE 'IpHoNe'").unwrap();
    assert!(result.contains("iPhone"));
    assert!(result.contains("IPHONE"));
    assert!(result.contains("iphone"));
}

#[test]
fn test_ilike_no_match() {
    let _guard = setup_test();
    process_query("CREATE TABLE colors (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO colors VALUES (1, 'Red')").unwrap();
    process_query("INSERT INTO colors VALUES (2, 'BLUE')").unwrap();
    process_query("INSERT INTO colors VALUES (3, 'Green')").unwrap();

    let result = process_query("SELECT name FROM colors WHERE name ILIKE 'yellow'").unwrap();
    assert!(!result.contains("Red"));
    assert!(!result.contains("BLUE"));
    assert!(!result.contains("Green"));
}
