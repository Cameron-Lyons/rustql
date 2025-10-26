use rustql::{process_query, reset_database};
use std::sync::{Once, Mutex};

static INIT: Once = Once::new();
static GLOBAL_TEST_LOCK: Mutex<()> = Mutex::new(());

fn setup_test() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap();
    INIT.call_once(|| {});
    reset_database();
}

#[test]
fn test_inner_join() {
    setup_test();
    
    process_query("CREATE TABLE users (id INTEGER, name TEXT, email TEXT)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT, price FLOAT)").unwrap();
    
    process_query("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com'), (3, 'Charlie', 'charlie@example.com')").unwrap();
    process_query("INSERT INTO orders VALUES (101, 1, 'Laptop', 999.99), (102, 1, 'Mouse', 29.99), (103, 2, 'Keyboard', 79.99)").unwrap();
    
    let result = process_query("SELECT name, product FROM users JOIN orders ON users.id = orders.user_id").unwrap();
    
    assert!(result.contains("Alice"));
    assert!(result.contains("Laptop"));
    assert!(result.contains("Mouse"));
    assert!(result.contains("Bob"));
    assert!(result.contains("Keyboard"));
    assert!(!result.contains("Charlie"));
}

#[test]
fn test_left_join() {
    setup_test();
    
    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT)").unwrap();
    
    process_query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')").unwrap();
    process_query("INSERT INTO orders VALUES (101, 1, 'Laptop'), (102, 2, 'Keyboard')").unwrap();
    
    let result = process_query("SELECT users.name, orders.product FROM users LEFT JOIN orders ON users.id = orders.user_id").unwrap();
    
    assert!(result.contains("Alice"));
    assert!(result.contains("Bob"));
    assert!(result.contains("Charlie"));
    
    assert!(result.contains("Laptop"));
    assert!(result.contains("Keyboard"));
}

#[test]
fn test_join_with_where_clause() {
    setup_test();
    
    process_query("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT, price FLOAT)").unwrap();
    
    process_query("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)").unwrap();
    process_query("INSERT INTO orders VALUES (101, 1, 'Laptop', 999.99), (102, 2, 'Mouse', 29.99), (103, 3, 'Keyboard', 79.99)").unwrap();
    
    let result = process_query("SELECT users.name, orders.price FROM users JOIN orders ON users.id = orders.user_id WHERE orders.price > 50").unwrap();
    
    assert!(result.contains("Alice"));
    assert!(result.contains("999.99"));
    assert!(result.contains("Charlie"));
    assert!(result.contains("79.99"));
    assert!(!result.contains("Bob")); 
    assert!(!result.contains("29.99"));
}

#[test]
fn test_join_with_specific_columns() {
    setup_test();
    
    process_query("CREATE TABLE users (id INTEGER, name TEXT, email TEXT)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT, quantity INTEGER)").unwrap();
    
    process_query("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')").unwrap();
    process_query("INSERT INTO orders VALUES (101, 1, 'Laptop', 2), (102, 1, 'Mouse', 5)").unwrap();
    
    let result = process_query("SELECT users.name, orders.product, orders.quantity FROM users JOIN orders ON users.id = orders.user_id").unwrap();
    
    assert!(result.contains("Alice"));
    assert!(result.contains("Laptop"));
    assert!(result.contains("2"));
    assert!(result.contains("Mouse"));
    assert!(result.contains("5"));
}

#[test]
fn test_join_all_columns() {
    setup_test();
    
    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT)").unwrap();
    
    process_query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
    process_query("INSERT INTO orders VALUES (101, 1, 'Laptop'), (102, 2, 'Keyboard')").unwrap();
    
    let result = process_query("SELECT * FROM users JOIN orders ON users.id = orders.user_id").unwrap();
    
    assert!(result.contains("id"));
    assert!(result.contains("name"));
    assert!(result.contains("user_id"));
    assert!(result.contains("product"));
    assert!(result.contains("Alice"));
    assert!(result.contains("Laptop"));
}

#[test]
fn test_join_missing_table() {
    setup_test();
    
    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    
    let result = process_query("SELECT * FROM users JOIN nonexistent ON users.id = nonexistent.user_id");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("does not exist"));
}

#[test]
fn test_join_invalid_column() {
    setup_test();
    
    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT)").unwrap();
    
    let result = process_query("SELECT users.nonexistent, orders.product FROM users JOIN orders ON users.id = orders.user_id");
    assert!(result.is_err());
}

#[test]
fn test_join_multiple_conditions() {
    setup_test();
    
    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT, price FLOAT)").unwrap();
    
    process_query("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    process_query("INSERT INTO orders VALUES (101, 1, 'Laptop', 999.99), (102, 1, 'Mouse', 29.99)").unwrap();
    
    let result = process_query("SELECT users.name, orders.product, orders.price FROM users JOIN orders ON users.id = orders.user_id WHERE orders.price < 100").unwrap();
    
    assert!(result.contains("Alice"));
    assert!(result.contains("Mouse"));
    assert!(result.contains("29.99"));
    assert!(!result.contains("Laptop"));
    assert!(!result.contains("999.99"));
}

#[test]
fn test_join_empty_tables() {
    setup_test();
    
    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT)").unwrap();
    
    let result = process_query("SELECT * FROM users JOIN orders ON users.id = orders.user_id").unwrap();
    
    assert!(result.contains("id"));
    assert!(result.contains("name"));
}

#[test]
fn test_join_no_matching_rows() {
    setup_test();
    
    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT)").unwrap();
    
    process_query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
    process_query("INSERT INTO orders VALUES (101, 999, 'Laptop')").unwrap(); // user_id 999 doesn't exist
    
    let result = process_query("SELECT * FROM users JOIN orders ON users.id = orders.user_id").unwrap();
    
    let lines: Vec<&str> = result.lines().collect();
    assert!(lines.len() <= 2); 
}

