use rustql::{process_query, reset_database};
use std::sync::Mutex;

static GLOBAL_TEST_LOCK: Mutex<()> = Mutex::new(());

fn setup_test() -> std::sync::MutexGuard<'static, ()> {
    let guard = GLOBAL_TEST_LOCK.lock().unwrap();
    reset_database();
    guard
}

#[test]
fn test_in_operator() {
    let _guard = setup_test();
    
    process_query("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    process_query("INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 35), (4, 'David', 40)").unwrap();
    
    let result = process_query("SELECT name FROM users WHERE age IN (25, 35, 40)").unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("Charlie"));
    assert!(result.contains("David"));
    assert!(!result.contains("Bob"));
    
    let result = process_query("SELECT name FROM users WHERE name IN ('Alice', 'Charlie')").unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("Charlie"));
    assert!(!result.contains("Bob"));
}

#[test]
fn test_like_operator() {
    let _guard = setup_test();
    
    process_query("CREATE TABLE products (id INTEGER, name TEXT, category TEXT)").unwrap();
    process_query("INSERT INTO products VALUES (1, 'Laptop Computer', 'Electronics'), (2, 'Desktop PC', 'Electronics'), (3, 'Mouse Pad', 'Accessories'), (4, 'Keyboard', 'Accessories')").unwrap();
    
    let result = process_query("SELECT name FROM products WHERE name LIKE '%Computer%'").unwrap();
    assert!(result.contains("Laptop Computer"));
    assert!(!result.contains("Desktop PC"));
    assert!(!result.contains("Mouse Pad"));
    
    let result = process_query("SELECT name FROM products WHERE category LIKE '%Electron%'").unwrap();
    assert!(result.contains("Laptop Computer"));
    assert!(result.contains("Desktop PC"));
    assert!(!result.contains("Mouse Pad"));
    
    let result = process_query("SELECT name FROM products WHERE name LIKE 'Laptop%'").unwrap();
    assert!(result.contains("Laptop Computer"));
    assert!(!result.contains("Desktop PC"));
}

#[test]
fn test_between_operator() {
    let _guard = setup_test();
    
    process_query("CREATE TABLE employees (id INTEGER, name TEXT, salary FLOAT)").unwrap();
    process_query("INSERT INTO employees VALUES (1, 'Alice', 50000.0), (2, 'Bob', 75000.0), (3, 'Charlie', 100000.0), (4, 'David', 125000.0)").unwrap();
    
    let result = process_query("SELECT name FROM employees WHERE salary BETWEEN 50000 AND 75000").unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("Bob"));
    assert!(!result.contains("Charlie"));
    assert!(!result.contains("David"));
    
    let result = process_query("SELECT name FROM employees WHERE salary BETWEEN 100000 AND 125000").unwrap();
    assert!(!result.contains("Alice"));
    assert!(!result.contains("Bob"));
    assert!(result.contains("Charlie"));
    assert!(result.contains("David"));
}

#[test]
fn test_in_with_mixed_types() {
    let _guard = setup_test();
    
    process_query("CREATE TABLE items (id INTEGER, price FLOAT, name TEXT)").unwrap();
    process_query("INSERT INTO items VALUES (1, 10.5, 'Apple'), (2, 20.0, 'Banana'), (3, 15.75, 'Orange')").unwrap();
    
    let result = process_query("SELECT name FROM items WHERE price IN (10.5, 20.0)").unwrap();
    assert!(result.contains("Apple"));
    assert!(result.contains("Banana"));
    assert!(!result.contains("Orange"));
}

#[test]
fn test_like_with_wildcards() {
    let _guard = setup_test();
    
    process_query("CREATE TABLE files (id INTEGER, filename TEXT)").unwrap();
    process_query("INSERT INTO files VALUES (1, 'document.pdf'), (2, 'image.jpg'), (3, 'report.docx')").unwrap();
    
    let result = process_query("SELECT filename FROM files WHERE filename LIKE '%.pdf'").unwrap();
    assert!(result.contains("document.pdf"));
    assert!(!result.contains("image.jpg"));
    assert!(!result.contains("report.docx"));
    
    let result = process_query("SELECT filename FROM files WHERE filename LIKE 'i%'").unwrap();
    assert!(result.contains("image.jpg"));
    assert!(!result.contains("document.pdf"));
}

#[test]
fn test_between_with_text() {
    let _guard = setup_test();
    
    process_query("CREATE TABLE items (name TEXT, category TEXT)").unwrap();
    process_query("INSERT INTO items VALUES ('Apple', 'Fruit'), ('Banana', 'Fruit'), ('Carrot', 'Vegetable')").unwrap();
    
    let result = process_query("SELECT name FROM items WHERE name BETWEEN 'Apple' AND 'Banana'").unwrap();
    assert!(result.contains("Apple"));
    assert!(result.contains("Banana"));
    assert!(!result.contains("Carrot"));
}

#[test]
fn test_combined_operators() {
    let _guard = setup_test();
    
    process_query("CREATE TABLE students (id INTEGER, name TEXT, age INTEGER, grade TEXT)").unwrap();
    process_query("INSERT INTO students VALUES (1, 'Alice', 20, 'A'), (2, 'Bob', 21, 'B'), (3, 'Charlie', 22, 'A'), (4, 'David', 20, 'C')").unwrap();
    
    let result = process_query("SELECT name FROM students WHERE age BETWEEN 20 AND 21 AND grade IN ('A', 'B')").unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("Bob"));
    assert!(!result.contains("Charlie"));
    assert!(!result.contains("David"));
}

#[test]
fn test_not_in() {
    let _guard = setup_test();
    
    process_query("CREATE TABLE users (id INTEGER, name TEXT, role TEXT)").unwrap();
    process_query("INSERT INTO users VALUES (1, 'Alice', 'Admin'), (2, 'Bob', 'User'), (3, 'Charlie', 'Guest')").unwrap();
    
    let result = process_query("SELECT name FROM users WHERE NOT (role IN ('Guest', 'User'))").unwrap();
    assert!(result.contains("Alice"));
    assert!(!result.contains("Bob"));
    assert!(!result.contains("Charlie"));
}

#[test]
fn test_like_not_like() {
    let _guard = setup_test();
    
    process_query("CREATE TABLE products (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO products VALUES (1, 'laptop'), (2, 'desktop'), (3, 'mouse')").unwrap();
    
    let result = process_query("SELECT name FROM products WHERE NOT (name LIKE '%top%')").unwrap();
    assert!(result.contains("mouse"));
    assert!(!result.contains("laptop"));
    assert!(!result.contains("desktop"));
}

