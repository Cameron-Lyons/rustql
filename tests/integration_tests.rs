mod common;
use common::*;
use rustql::CommandTag;
use rustql::ast::Value;
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
    let result = command_result("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)");
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result.tag, CommandTag::CreateTable);
    assert_eq!(result.affected, 0);

    let result = execute_sql("CREATE TABLE users (id INTEGER, name TEXT)");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("already exists"));
}

#[test]
fn test_insert_and_select() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    let result = command_result("INSERT INTO users VALUES (1, 'Alice', 25)");
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result.tag, CommandTag::Insert);
    assert_eq!(result.affected, 1);

    let result = command_result("INSERT INTO users VALUES (2, 'Bob', 30), (3, 'Charlie', 35)");
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result.tag, CommandTag::Insert);
    assert_eq!(result.affected, 2);

    let result = query_rows("SELECT * FROM users ORDER BY id").unwrap();
    assert_eq!(
        result
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "name", "age"]
    );
    assert_eq!(
        result.rows,
        vec![
            vec![
                Value::Integer(1),
                Value::Text("Alice".to_string()),
                Value::Integer(25)
            ],
            vec![
                Value::Integer(2),
                Value::Text("Bob".to_string()),
                Value::Integer(30)
            ],
            vec![
                Value::Integer(3),
                Value::Text("Charlie".to_string()),
                Value::Integer(35)
            ],
        ]
    );

    let result = query_rows("SELECT name, age FROM users ORDER BY id").unwrap();
    assert_eq!(
        result
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["name", "age"]
    );
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Text("Alice".to_string()), Value::Integer(25)],
            vec![Value::Text("Bob".to_string()), Value::Integer(30)],
            vec![Value::Text("Charlie".to_string()), Value::Integer(35)],
        ]
    );
}

#[test]
fn test_select_aliases() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    execute_sql("INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30)").unwrap();

    let result = query_rows("SELECT name AS username, age FROM users ORDER BY id").unwrap();
    assert_eq!(
        result
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["username", "age"]
    );
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Text("Alice".to_string()), Value::Integer(25)],
            vec![Value::Text("Bob".to_string()), Value::Integer(30)],
        ]
    );
}

#[test]
fn test_where_clause() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    execute_sql("INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 35)")
        .unwrap();

    let result = execute_sql("SELECT name FROM users WHERE age > 30").unwrap();
    assert!(result.contains("Charlie"));
    assert!(!result.contains("Alice"));
    assert!(!result.contains("Bob"));

    let result = execute_sql("SELECT name FROM users WHERE age > 20 AND age < 35").unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("Bob"));
    assert!(!result.contains("Charlie"));

    let result = execute_sql("SELECT name FROM users WHERE age = 25 OR age = 35").unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("Charlie"));
    assert!(!result.contains("Bob"));
}

#[test]
fn test_update() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    execute_sql("INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30)").unwrap();
    let result = execute_sql("UPDATE users SET age = 26 WHERE name = 'Alice'");
    assert!(result.is_ok());
    assert_command(result.unwrap(), CommandTag::Update, 1);

    let result = execute_sql("SELECT age FROM users WHERE name = 'Alice'").unwrap();
    assert!(result.contains("26"));
}

#[test]
fn test_delete() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    execute_sql("INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 35)")
        .unwrap();

    let result = execute_sql("DELETE FROM users WHERE name = 'Bob'");
    assert!(result.is_ok());
    assert_command(result.unwrap(), CommandTag::Delete, 1);

    let result = execute_sql("SELECT * FROM users").unwrap();
    assert!(result.contains("Alice"));
    assert!(!result.contains("Bob"));
    assert!(result.contains("Charlie"));
}

#[test]
fn test_order_by() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    execute_sql("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)")
        .unwrap();

    let result = execute_sql("SELECT name FROM users ORDER BY age ASC").unwrap();
    let lines: Vec<String> = result.lines().collect();
    let bob_pos = lines.iter().position(|line| line.contains("Bob")).unwrap();
    let alice_pos = lines
        .iter()
        .position(|line| line.contains("Alice"))
        .unwrap();
    assert!(bob_pos < alice_pos);

    let result = execute_sql("SELECT name FROM users ORDER BY age DESC").unwrap();
    let lines: Vec<String> = result.lines().collect();
    let charlie_pos = lines
        .iter()
        .position(|line| line.contains("Charlie"))
        .unwrap();
    let alice_pos = lines
        .iter()
        .position(|line| line.contains("Alice"))
        .unwrap();
    assert!(charlie_pos < alice_pos);
}

#[test]
fn test_order_by_ordinal() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    execute_sql("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)")
        .unwrap();

    let result = query_rows("SELECT name, age FROM users ORDER BY 2 DESC").unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Text("Charlie".to_string()), Value::Integer(35)],
            vec![Value::Text("Alice".to_string()), Value::Integer(30)],
            vec![Value::Text("Bob".to_string()), Value::Integer(25)],
        ]
    );
}

#[test]
fn test_order_by_expression() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE sales (id INTEGER, price INTEGER, quantity INTEGER)").unwrap();
    execute_sql("INSERT INTO sales VALUES (1, 10, 2), (2, 5, 5), (3, 7, 1)").unwrap();

    let result =
        query_rows("SELECT id, price, quantity FROM sales ORDER BY price * quantity DESC").unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Integer(2), Value::Integer(5), Value::Integer(5)],
            vec![Value::Integer(1), Value::Integer(10), Value::Integer(2)],
            vec![Value::Integer(3), Value::Integer(7), Value::Integer(1)],
        ]
    );
}

#[test]
fn test_limit_offset() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    execute_sql("INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 35)")
        .unwrap();

    let result = execute_sql("SELECT name FROM users LIMIT 2").unwrap();
    let name_count = result.matches("Alice").count()
        + result.matches("Bob").count()
        + result.matches("Charlie").count();
    assert_eq!(name_count, 2);

    let result = execute_sql("SELECT name FROM users OFFSET 1").unwrap();
    let name_count = result.matches("Alice").count()
        + result.matches("Bob").count()
        + result.matches("Charlie").count();
    assert_eq!(name_count, 2);
}

#[test]
fn test_aggregate_functions() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE sales (id INTEGER, amount FLOAT, quantity INTEGER)").unwrap();
    execute_sql("INSERT INTO sales VALUES (1, 100.0, 5), (2, 200.0, 3), (3, 150.0, 7)").unwrap();

    let result = execute_sql("SELECT COUNT(*) FROM sales").unwrap();
    assert!(result.contains("3"));
    let result = execute_sql("SELECT SUM(quantity) FROM sales").unwrap();
    assert!(result.contains("15"));
    let result = execute_sql("SELECT AVG(amount) FROM sales").unwrap();
    assert!(result.contains("150"));
    let result = execute_sql("SELECT MIN(amount) FROM sales").unwrap();
    assert!(result.contains("100"));
    let result = execute_sql("SELECT MAX(amount) FROM sales").unwrap();
    assert!(result.contains("200"));
}

#[test]
fn test_group_by() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE orders (id INTEGER, category TEXT, amount FLOAT)").unwrap();
    execute_sql("INSERT INTO orders VALUES (1, 'Electronics', 100.0), (2, 'Electronics', 200.0), (3, 'Books', 50.0), (4, 'Books', 75.0)").unwrap();

    let result = execute_sql("SELECT category, COUNT(*) FROM orders GROUP BY category").unwrap();
    assert!(result.contains("Electronics"));
    assert!(result.contains("2"));
    assert!(result.contains("Books"));

    let result = execute_sql("SELECT category, SUM(amount) FROM orders GROUP BY category").unwrap();
    assert!(result.contains("300"));
    assert!(result.contains("125"));

    let result = execute_sql("SELECT category, AVG(amount) FROM orders GROUP BY category").unwrap();
    assert!(result.contains("150"));
    assert!(result.contains("62.5"));
}

#[test]
fn test_data_types() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE test_types (int_col INTEGER, float_col FLOAT, text_col TEXT, bool_col BOOLEAN)").unwrap();

    let result = execute_sql("INSERT INTO test_types VALUES (42, 3.14, 'hello', 1)");
    assert!(result.is_ok());
    let result = execute_sql("INSERT INTO test_types VALUES (-10, -2.5, 'world', 0)");
    assert!(result.is_ok());

    let result = execute_sql("SELECT float_col FROM test_types WHERE float_col > 0").unwrap();
    assert!(result.contains("3.14"));
    assert!(!result.contains("-2.5"));
}

#[test]
fn test_drop_table() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE temp_table (id INTEGER)").unwrap();

    let result = execute_sql("DROP TABLE temp_table");
    assert!(result.is_ok());
    assert_command(result.unwrap(), CommandTag::DropTable, 0);

    let result = execute_sql("DROP TABLE nonexistent");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("does not exist"));
}

#[test]
fn test_string_operations() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE messages (id INTEGER, content TEXT, author TEXT)").unwrap();
    execute_sql("INSERT INTO messages VALUES (1, 'Hello World', 'Alice'), (2, 'Goodbye', 'Bob')")
        .unwrap();

    let result = execute_sql("SELECT content FROM messages WHERE author = 'Alice'").unwrap();
    assert!(result.contains("Hello World"));
    assert!(!result.contains("Goodbye"));

    let result = execute_sql("SELECT author FROM messages WHERE content != 'Goodbye'").unwrap();
    assert!(result.contains("Alice"));
    assert!(!result.contains("Bob"));
}

#[test]
fn test_complex_queries() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE employees (id INTEGER, name TEXT, department TEXT, salary FLOAT)")
        .unwrap();
    execute_sql("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 80000.0), (2, 'Bob', 'Engineering', 75000.0), (3, 'Charlie', 'Sales', 60000.0), (4, 'David', 'Sales', 65000.0)").unwrap();

    let result =
        execute_sql("SELECT department, AVG(salary) FROM employees GROUP BY department").unwrap();
    assert!(result.contains("Engineering"));
    assert!(result.contains("77500"));
    assert!(result.contains("Sales"));
    assert!(result.contains("62500"));
}

#[test]
fn test_error_handling() {
    let _guard = setup_test();
    let result = execute_sql("SELCT * FROM users");
    assert!(result.is_err());

    let result = execute_sql("SELECT * FROM nonexistent");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("does not exist"));

    execute_sql("CREATE TABLE test (a INTEGER, b INTEGER)").unwrap();
    let result = execute_sql("INSERT INTO test VALUES (1)");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Column count mismatch"));
}

#[test]
fn test_subquery_in_where() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE orders (id INTEGER, amount FLOAT)").unwrap();
    execute_sql("CREATE TABLE customers (id INTEGER, name TEXT)").unwrap();

    execute_sql("INSERT INTO orders VALUES (1, 100.0), (2, 200.0), (3, 150.0)").unwrap();
    execute_sql("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    let result = execute_sql("SELECT * FROM orders WHERE id IN (SELECT id FROM customers)");
    assert!(result.is_ok());
    let result_str = result.unwrap();

    assert!(result_str.contains("100.0") || result_str.contains("100"));
    assert!(result_str.contains("200.0") || result_str.contains("200"));
    assert!(!result_str.contains("150.0") && !result_str.contains("150"));

    let result = execute_sql(
        "SELECT * FROM orders WHERE id IN (SELECT id FROM customers WHERE name = 'Alice')",
    );
    assert!(result.is_ok());
    let result_str = result.unwrap();

    assert!(result_str.contains("100.0") || result_str.contains("100"));
    assert!(!result_str.contains("200.0") && !result_str.contains("200"));
}

#[test]
fn test_is_null() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE users (id INTEGER, name TEXT, email TEXT)").unwrap();

    execute_sql("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com'), (2, NULL, 'bob@example.com'), (3, 'Charlie', NULL)").unwrap();

    let result = execute_sql("SELECT name FROM users WHERE name IS NULL").unwrap();
    assert!(!result.contains("Alice"));
    assert!(!result.contains("Charlie"));
    assert!(result.contains("NULL"));

    let result = execute_sql("SELECT name FROM users WHERE name IS NOT NULL").unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("Charlie"));
    assert!(!result.contains("NULL") || result.matches("NULL").count() <= 1); // Only column header or NULL value

    let result = execute_sql("SELECT id FROM users WHERE email IS NULL").unwrap();
    assert!(result.contains("3"));
    assert!(!result.contains("1"));
    assert!(!result.contains("2"));

    let result =
        execute_sql("SELECT id FROM users WHERE name IS NULL AND email IS NOT NULL").unwrap();
    assert!(result.contains("2"));
    assert!(!result.contains("1"));
    assert!(!result.contains("3"));
}

#[test]
fn test_in_subquery_with_aggregate() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE orders (id INTEGER, user_id INTEGER, amount FLOAT)").unwrap();
    execute_sql("INSERT INTO orders VALUES (1, 1, 100.0), (2, 1, 50.0), (3, 2, 200.0)").unwrap();

    let result =
        execute_sql("SELECT amount FROM orders WHERE amount IN (SELECT MAX(amount) FROM orders)")
            .unwrap();
    assert!(result.contains("200") || result.contains("200.0"));
    assert!(!result.contains("100.0") && !result.contains("50.0") && !result.contains("100\t"));
}

#[test]
fn test_in_subquery_with_group_by_and_aggregate() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE orders2 (id INTEGER, user_id INTEGER, amount FLOAT)").unwrap();
    execute_sql(
        "INSERT INTO orders2 VALUES (1, 1, 100.0), (2, 1, 50.0), (3, 2, 200.0), (4, 2, 120.0)",
    )
    .unwrap();

    let rows = query_rows(
        "SELECT id, amount FROM orders2 WHERE amount IN (SELECT MAX(amount) FROM orders2 GROUP BY user_id) ORDER BY id",
    )
    .unwrap();
    rows.assert_columns(&["id", "amount"]);
    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Integer(1), Value::Float(100.0)],
            vec![Value::Integer(3), Value::Float(200.0)],
        ]
    );
}

#[test]
fn test_in_subquery_with_nested_scalar_subquery() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE numbers (value INTEGER)").unwrap();
    execute_sql("INSERT INTO numbers VALUES (1), (2), (3)").unwrap();

    execute_sql("CREATE TABLE wrapper (inner_id INTEGER)").unwrap();
    execute_sql("INSERT INTO wrapper VALUES (1), (2)").unwrap();

    execute_sql("CREATE TABLE inner_values (id INTEGER, wrapped INTEGER)").unwrap();
    execute_sql("INSERT INTO inner_values VALUES (1, 2), (2, 3)").unwrap();

    let result = execute_sql(
		"SELECT value FROM numbers WHERE value IN (SELECT (SELECT wrapped FROM inner_values WHERE inner_values.id = wrapper.inner_id) FROM wrapper) ORDER BY value"
	).unwrap();
    assert!(result.contains("value"));
    assert!(result.contains("2"));
    assert!(result.contains("3"));
    assert!(!result.contains("\n1\t"));
}
