use rustql::{process_query, reset_database};
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup_test<'a>() -> std::sync::MutexGuard<'a, ()> {
    let guard = TEST_MUTEX.lock().unwrap();
    reset_database();
    guard
}

#[test]
fn test_create_table() {
    let _guard = setup_test();
    let result = process_query("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Table 'users' created");

    let result = process_query("CREATE TABLE users (id INTEGER, name TEXT)");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("already exists"));
}

#[test]
fn test_insert_and_select() {
    let _guard = setup_test();
    process_query("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    let result = process_query("INSERT INTO users VALUES (1, 'Alice', 25)");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "1 row(s) inserted");

    let result = process_query("INSERT INTO users VALUES (2, 'Bob', 30), (3, 'Charlie', 35)");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "2 row(s) inserted");

    let result = process_query("SELECT * FROM users").unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("Bob"));
    assert!(result.contains("Charlie"));

    let result = process_query("SELECT name, age FROM users").unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("25"));
    assert!(!result.contains("id"));
}

#[test]
fn test_where_clause() {
    let _guard = setup_test();
    process_query("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    process_query("INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 35)")
        .unwrap();

    let result = process_query("SELECT name FROM users WHERE age > 30").unwrap();
    assert!(result.contains("Charlie"));
    assert!(!result.contains("Alice"));
    assert!(!result.contains("Bob"));

    let result = process_query("SELECT name FROM users WHERE age > 20 AND age < 35").unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("Bob"));
    assert!(!result.contains("Charlie"));

    let result = process_query("SELECT name FROM users WHERE age = 25 OR age = 35").unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("Charlie"));
    assert!(!result.contains("Bob"));
}

#[test]
fn test_update() {
    let _guard = setup_test();
    process_query("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    process_query("INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30)").unwrap();
    let result = process_query("UPDATE users SET age = 26 WHERE name = 'Alice'");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "1 row(s) updated");

    let result = process_query("SELECT age FROM users WHERE name = 'Alice'").unwrap();
    assert!(result.contains("26"));
}

#[test]
fn test_delete() {
    let _guard = setup_test();
    process_query("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    process_query(
        "INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 35)",
    )
    .unwrap();

    let result = process_query("DELETE FROM users WHERE name = 'Bob'");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "1 row(s) deleted");

    let result = process_query("SELECT * FROM users").unwrap();
    assert!(result.contains("Alice"));
    assert!(!result.contains("Bob"));
    assert!(result.contains("Charlie"));
}

#[test]
fn test_order_by() {
    let _guard = setup_test();
    process_query("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    process_query(
        "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)",
    )
    .unwrap();

    let result = process_query("SELECT name FROM users ORDER BY age ASC").unwrap();
    let lines: Vec<&str> = result.lines().collect();
    let bob_pos = lines.iter().position(|&line| line.contains("Bob")).unwrap();
    let alice_pos = lines.iter().position(|&line| line.contains("Alice")).unwrap();
    assert!(bob_pos < alice_pos);

    let result = process_query("SELECT name FROM users ORDER BY age DESC").unwrap();
    let lines: Vec<&str> = result.lines().collect();
    let charlie_pos = lines.iter().position(|&line| line.contains("Charlie")).unwrap();
    let alice_pos = lines.iter().position(|&line| line.contains("Alice")).unwrap();
    assert!(charlie_pos < alice_pos);
}

#[test]
fn test_limit_offset() {
    let _guard = setup_test();
    process_query("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    process_query(
        "INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 35)",
    )
    .unwrap();

    let result = process_query("SELECT name FROM users LIMIT 2").unwrap();
    let name_count = result.matches("Alice").count()
        + result.matches("Bob").count()
        + result.matches("Charlie").count();
    assert_eq!(name_count, 2);

    let result = process_query("SELECT name FROM users OFFSET 1").unwrap();
    let name_count = result.matches("Alice").count()
        + result.matches("Bob").count()
        + result.matches("Charlie").count();
    assert_eq!(name_count, 2);
}

#[test]
fn test_aggregate_functions() {
    let _guard = setup_test();
    process_query("CREATE TABLE sales (id INTEGER, amount FLOAT, quantity INTEGER)").unwrap();
    process_query("INSERT INTO sales VALUES (1, 100.0, 5), (2, 200.0, 3), (3, 150.0, 7)").unwrap();

    let result = process_query("SELECT COUNT(*) FROM sales").unwrap();
    assert!(result.contains("3"));
    let result = process_query("SELECT SUM(quantity) FROM sales").unwrap();
    assert!(result.contains("15"));
    let result = process_query("SELECT AVG(amount) FROM sales").unwrap();
    assert!(result.contains("150"));
    let result = process_query("SELECT MIN(amount) FROM sales").unwrap();
    assert!(result.contains("100"));
    let result = process_query("SELECT MAX(amount) FROM sales").unwrap();
    assert!(result.contains("200"));
}

#[test]
fn test_group_by() {
    let _guard = setup_test();
    process_query("CREATE TABLE orders (id INTEGER, category TEXT, amount FLOAT)").unwrap();
    process_query("INSERT INTO orders VALUES (1, 'Electronics', 100.0), (2, 'Electronics', 200.0), (3, 'Books', 50.0), (4, 'Books', 75.0)").unwrap();

    let result = process_query("SELECT category, COUNT(*) FROM orders GROUP BY category").unwrap();
    assert!(result.contains("Electronics"));
    assert!(result.contains("2"));
    assert!(result.contains("Books"));

    let result = process_query("SELECT category, SUM(amount) FROM orders GROUP BY category").unwrap();
    assert!(result.contains("300"));
    assert!(result.contains("125"));

    let result = process_query("SELECT category, AVG(amount) FROM orders GROUP BY category").unwrap();
    assert!(result.contains("150"));
    assert!(result.contains("62.5"));
}

#[test]
fn test_data_types() {
    let _guard = setup_test();
    process_query("CREATE TABLE test_types (int_col INTEGER, float_col FLOAT, text_col TEXT, bool_col BOOLEAN)").unwrap();

    let result = process_query("INSERT INTO test_types VALUES (42, 3.14, 'hello', 1)");
    assert!(result.is_ok());
    let result = process_query("INSERT INTO test_types VALUES (-10, -2.5, 'world', 0)");
    assert!(result.is_ok());

    let result = process_query("SELECT float_col FROM test_types WHERE float_col > 0").unwrap();
    assert!(result.contains("3.14"));
    assert!(!result.contains("-2.5"));
}

#[test]
fn test_drop_table() {
    let _guard = setup_test();
    process_query("CREATE TABLE temp_table (id INTEGER)").unwrap();

    let result = process_query("DROP TABLE temp_table");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Table 'temp_table' dropped");

    let result = process_query("DROP TABLE nonexistent");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("does not exist"));
}

#[test]
fn test_string_operations() {
    let _guard = setup_test();
    process_query("CREATE TABLE messages (id INTEGER, content TEXT, author TEXT)").unwrap();
    process_query("INSERT INTO messages VALUES (1, 'Hello World', 'Alice'), (2, 'Goodbye', 'Bob')")
        .unwrap();

    let result = process_query("SELECT content FROM messages WHERE author = 'Alice'").unwrap();
    assert!(result.contains("Hello World"));
    assert!(!result.contains("Goodbye"));

    let result = process_query("SELECT author FROM messages WHERE content != 'Goodbye'").unwrap();
    assert!(result.contains("Alice"));
    assert!(!result.contains("Bob"));
}

#[test]
fn test_complex_queries() {
    let _guard = setup_test();
    process_query(
        "CREATE TABLE employees (id INTEGER, name TEXT, department TEXT, salary FLOAT)",
    )
    .unwrap();
    process_query("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 80000.0), (2, 'Bob', 'Engineering', 75000.0), (3, 'Charlie', 'Sales', 60000.0), (4, 'David', 'Sales', 65000.0)").unwrap();

    let result = process_query("SELECT department, AVG(salary) FROM employees GROUP BY department")
        .unwrap();
    assert!(result.contains("Engineering"));
    assert!(result.contains("77500"));
    assert!(result.contains("Sales"));
    assert!(result.contains("62500"));
}

#[test]
fn test_error_handling() {
    let _guard = setup_test();
    let result = process_query("SELCT * FROM users");
    assert!(result.is_err());

    let result = process_query("SELECT * FROM nonexistent");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("does not exist"));

    process_query("CREATE TABLE test (a INTEGER, b INTEGER)").unwrap();
    let result = process_query("INSERT INTO test VALUES (1)");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Column count mismatch"));
}

#[test]
fn test_subquery_in_where() {
    let _guard = setup_test();
    process_query("CREATE TABLE orders (id INTEGER, amount FLOAT)").unwrap();
    process_query("CREATE TABLE customers (id INTEGER, name TEXT)").unwrap();
    
    process_query("INSERT INTO orders VALUES (1, 100.0), (2, 200.0), (3, 150.0)").unwrap();
    process_query("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
    
    let result = process_query("SELECT * FROM orders WHERE id IN (SELECT id FROM customers)");
    assert!(result.is_ok());
    let result_str = result.unwrap();
    
    assert!(result_str.contains("100.0") || result_str.contains("100"));
    assert!(result_str.contains("200.0") || result_str.contains("200"));
    assert!(!result_str.contains("150.0") && !result_str.contains("150"));
    
    let result = process_query("SELECT * FROM orders WHERE id IN (SELECT id FROM customers WHERE name = 'Alice')");
    assert!(result.is_ok());
    let result_str = result.unwrap();
    
    assert!(result_str.contains("100.0") || result_str.contains("100"));
    assert!(!result_str.contains("200.0") && !result_str.contains("200"));
}

#[test]
fn test_is_null() {
    let _guard = setup_test();
    process_query("CREATE TABLE users (id INTEGER, name TEXT, email TEXT)").unwrap();
    
    process_query("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com'), (2, NULL, 'bob@example.com'), (3, 'Charlie', NULL)").unwrap();
    
    let result = process_query("SELECT name FROM users WHERE name IS NULL").unwrap();
    assert!(!result.contains("Alice"));
    assert!(!result.contains("Charlie"));
    assert!(result.contains("NULL"));
    
    let result = process_query("SELECT name FROM users WHERE name IS NOT NULL").unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("Charlie"));
    assert!(!result.contains("NULL") || result.matches("NULL").count() <= 1); // Only column header or NULL value
    
    let result = process_query("SELECT id FROM users WHERE email IS NULL").unwrap();
    assert!(result.contains("3"));
    assert!(!result.contains("1"));
    assert!(!result.contains("2"));
    
    let result = process_query("SELECT id FROM users WHERE name IS NULL AND email IS NOT NULL").unwrap();
    assert!(result.contains("2"));
    assert!(!result.contains("1"));
    assert!(!result.contains("3"));
}

#[test]
fn test_in_subquery_with_aggregate() {
	let _guard = setup_test();
	process_query("CREATE TABLE orders (id INTEGER, user_id INTEGER, amount FLOAT)").unwrap();
	process_query("INSERT INTO orders VALUES (1, 1, 100.0), (2, 1, 50.0), (3, 2, 200.0)").unwrap();

	let result = process_query("SELECT amount FROM orders WHERE amount IN (SELECT MAX(amount) FROM orders)").unwrap();
	assert!(result.contains("200") || result.contains("200.0"));
	assert!(!result.contains("100.0") && !result.contains("50.0") && !result.contains("100\t"));
}

#[test]
fn test_in_subquery_with_group_by_and_aggregate() {
	let _guard = setup_test();
	process_query("CREATE TABLE orders2 (id INTEGER, user_id INTEGER, amount FLOAT)").unwrap();
	process_query("INSERT INTO orders2 VALUES (1, 1, 100.0), (2, 1, 50.0), (3, 2, 200.0), (4, 2, 120.0)").unwrap();

	let result = process_query(
		"SELECT id, amount FROM orders2 WHERE amount IN (SELECT MAX(amount) FROM orders2 GROUP BY user_id) ORDER BY id"
	).unwrap();
	assert!(result.contains("id\tamount"));
	assert!(result.contains("100") || result.contains("100.0"));
	assert!(result.contains("200") || result.contains("200.0"));
	assert!(!result.contains("50.0") && !result.contains("120.0"));
}
