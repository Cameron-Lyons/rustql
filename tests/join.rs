use rustql::{process_query, reset_database};
use std::sync::Mutex;

static GLOBAL_TEST_LOCK: Mutex<()> = Mutex::new(());

fn setup_test() -> std::sync::MutexGuard<'static, ()> {
    let guard = GLOBAL_TEST_LOCK.lock().unwrap();
    reset_database();
    guard
}

#[test]
fn test_inner_join() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT, email TEXT)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT, price FLOAT)")
        .unwrap();
    process_query("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com'), (3, 'Charlie', 'charlie@example.com')").unwrap();
    process_query("INSERT INTO orders VALUES (101, 1, 'Laptop', 999.99), (102, 1, 'Mouse', 29.99), (103, 2, 'Keyboard', 79.99)").unwrap();

    let result =
        process_query("SELECT name, product FROM users JOIN orders ON users.id = orders.user_id")
            .unwrap();

    assert!(result.contains("Alice"));
    assert!(result.contains("Laptop"));
    assert!(result.contains("Mouse"));
    assert!(result.contains("Bob"));
    assert!(result.contains("Keyboard"));
    assert!(!result.contains("Charlie"));
}

#[test]
fn test_left_join() {
    let _guard = setup_test();

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
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT, price FLOAT)")
        .unwrap();

    process_query("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)")
        .unwrap();
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
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT, email TEXT)").unwrap();
    process_query(
        "CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT, quantity INTEGER)",
    )
    .unwrap();

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
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT)").unwrap();

    process_query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
    process_query("INSERT INTO orders VALUES (101, 1, 'Laptop'), (102, 2, 'Keyboard')").unwrap();

    let result =
        process_query("SELECT * FROM users JOIN orders ON users.id = orders.user_id").unwrap();

    assert!(result.contains("id"));
    assert!(result.contains("name"));
    assert!(result.contains("user_id"));
    assert!(result.contains("product"));
    assert!(result.contains("Alice"));
    assert!(result.contains("Laptop"));
}

#[test]
fn test_join_column_aliases() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT)").unwrap();

    process_query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
    process_query("INSERT INTO orders VALUES (101, 1, 'Laptop'), (102, 2, 'Keyboard')").unwrap();

    let result = process_query("SELECT users.name AS user_name, orders.product AS product_name FROM users JOIN orders ON users.id = orders.user_id ORDER BY user_name").unwrap();

    let mut lines = result.lines();
    let header = lines.next().unwrap();
    let header_cols: Vec<&str> = header.split('\t').filter(|s| !s.is_empty()).collect();
    assert_eq!(header_cols, vec!["user_name", "product_name"]);

    let separator = lines.next().unwrap();
    assert!(separator.chars().all(|c| c == '-'));

    let rows: Vec<&str> = lines.collect();
    assert_eq!(rows.len(), 2);
    assert!(rows[0].contains("Alice") && rows[0].contains("Laptop"));
    assert!(rows[1].contains("Bob") && rows[1].contains("Keyboard"));
}

#[test]
fn test_join_missing_table() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();

    let result =
        process_query("SELECT * FROM users JOIN nonexistent ON users.id = nonexistent.user_id");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("does not exist"));
}

#[test]
fn test_join_invalid_column() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT)").unwrap();

    let result = process_query(
        "SELECT users.nonexistent, orders.product FROM users JOIN orders ON users.id = orders.user_id",
    );
    assert!(result.is_err());
}

#[test]
fn test_join_multiple_conditions() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT, price FLOAT)")
        .unwrap();

    process_query("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    process_query("INSERT INTO orders VALUES (101, 1, 'Laptop', 999.99), (102, 1, 'Mouse', 29.99)")
        .unwrap();

    let result = process_query("SELECT users.name, orders.product, orders.price FROM users JOIN orders ON users.id = orders.user_id WHERE orders.price < 100").unwrap();

    assert!(result.contains("Alice"));
    assert!(result.contains("Mouse"));
    assert!(result.contains("29.99"));
    assert!(!result.contains("Laptop"));
    assert!(!result.contains("999.99"));
}

#[test]
fn test_join_empty_tables() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT)").unwrap();

    let result =
        process_query("SELECT * FROM users JOIN orders ON users.id = orders.user_id").unwrap();

    assert!(result.contains("id"));
    assert!(result.contains("name"));
}

#[test]
fn test_join_no_matching_rows() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT)").unwrap();

    process_query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
    process_query("INSERT INTO orders VALUES (101, 999, 'Laptop')").unwrap(); // user_id 999 doesn't exist

    let result =
        process_query("SELECT * FROM users JOIN orders ON users.id = orders.user_id").unwrap();

    let lines: Vec<&str> = result.lines().collect();
    assert!(lines.len() <= 2);
}

#[test]
fn test_right_join() {
    let _guard = setup_test();

    process_query("CREATE TABLE customers (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, customer_id INTEGER, product TEXT)").unwrap();

    process_query("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
    process_query(
        "INSERT INTO orders VALUES (101, 1, 'Laptop'), (102, 2, 'Keyboard'), (103, 999, 'Mouse')",
    )
    .unwrap(); // customer_id 999 doesn't exist

    let result = process_query("SELECT customers.name, orders.product FROM customers RIGHT JOIN orders ON customers.id = orders.customer_id").unwrap();

    // Should include all orders
    assert!(result.contains("Alice"));
    assert!(result.contains("Bob"));
    assert!(result.contains("Laptop"));
    assert!(result.contains("Keyboard"));
    assert!(result.contains("Mouse"));
}

#[test]
fn test_full_join() {
    let _guard = setup_test();

    process_query("CREATE TABLE left_table (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE right_table (id INTEGER, value TEXT)").unwrap();

    process_query("INSERT INTO left_table VALUES (1, 'A'), (2, 'B')").unwrap();
    process_query("INSERT INTO right_table VALUES (2, 'Y'), (3, 'Z')").unwrap();

    let result = process_query(
        "SELECT * FROM left_table FULL JOIN right_table ON left_table.id = right_table.id",
    )
    .unwrap();

    assert!(result.contains("1"));
    assert!(result.contains("2"));
    assert!(result.contains("3"));
}

#[test]
fn test_right_join_no_matches() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE products (id INTEGER, name TEXT)").unwrap();

    process_query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
    process_query("INSERT INTO products VALUES (101, 'Laptop'), (102, 'Mouse')").unwrap(); // No matching IDs

    let result =
        process_query("SELECT * FROM users RIGHT JOIN products ON users.id = products.id").unwrap();

    assert!(result.contains("Laptop"));
    assert!(result.contains("Mouse"));
}

#[test]
fn test_full_join_multiple_unmatches() {
    let _guard = setup_test();

    process_query("CREATE TABLE a (id INTEGER, val TEXT)").unwrap();
    process_query("CREATE TABLE b (id INTEGER, val TEXT)").unwrap();

    process_query("INSERT INTO a VALUES (1, 'A1'), (2, 'A2')").unwrap();
    process_query("INSERT INTO b VALUES (3, 'B1'), (4, 'B2')").unwrap(); // No matching IDs

    let result = process_query("SELECT * FROM a FULL JOIN b ON a.id = b.id").unwrap();

    assert!(result.contains("A1"));
    assert!(result.contains("A2"));
    assert!(result.contains("B1"));
    assert!(result.contains("B2"));
}

#[test]
fn test_join_with_aggregate_function() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT)").unwrap();

    process_query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')").unwrap();
    process_query(
        "INSERT INTO orders VALUES (101, 1, 'Laptop'), (102, 1, 'Mouse'), (103, 2, 'Keyboard')",
    )
    .unwrap();

    let result =
        process_query("SELECT COUNT(*) FROM users JOIN orders ON users.id = orders.user_id")
            .unwrap();

    assert!(result.contains("Count(*)"));
    assert!(result.contains("3"));
}

#[test]
fn test_join_group_by_with_aggregate() {
    use std::collections::HashMap;

    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query(
        "CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT, quantity INTEGER)",
    )
    .unwrap();

    process_query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')").unwrap();
    process_query(
        "INSERT INTO orders VALUES (101, 1, 'Laptop', 2), (102, 1, 'Mouse', 5), (103, 2, 'Keyboard', 1)",
    )
    .unwrap();

    let result = process_query("SELECT users.name, COUNT(orders.product) AS order_count FROM users LEFT JOIN orders ON users.id = orders.user_id GROUP BY users.name").unwrap();

    let mut lines = result.lines();
    let header = lines.next().unwrap();
    let headers: Vec<&str> = header.split('\t').filter(|s| !s.is_empty()).collect();
    assert_eq!(headers, vec!["users.name", "order_count"]);

    let separator = lines.next().unwrap();
    assert!(separator.chars().all(|c| c == '-'));

    let mut counts: HashMap<String, String> = HashMap::new();
    for row in lines {
        let cols: Vec<&str> = row.split('\t').filter(|s| !s.is_empty()).collect();
        if cols.len() == 2 {
            counts.insert(cols[0].to_string(), cols[1].to_string());
        }
    }

    assert_eq!(counts.get("Alice").map(String::as_str), Some("2"));
    assert_eq!(counts.get("Bob").map(String::as_str), Some("1"));
    assert_eq!(counts.get("Charlie").map(String::as_str), Some("0"));
}

#[test]
fn test_join_group_by_order_by_limit() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query(
        "CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT, quantity INTEGER)",
    )
    .unwrap();

    process_query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')").unwrap();
    process_query(
        "INSERT INTO orders VALUES (101, 1, 'Laptop', 2), (102, 1, 'Mouse', 5), (103, 2, 'Keyboard', 1)",
    )
    .unwrap();

    let result = process_query("SELECT users.name, COUNT(orders.product) AS order_count FROM users LEFT JOIN orders ON users.id = orders.user_id GROUP BY users.name ORDER BY order_count DESC LIMIT 1").unwrap();

    let rows: Vec<&str> = result
        .lines()
        .skip(2)
        .filter(|line| !line.trim().is_empty())
        .collect();

    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("Alice"));
    assert!(rows[0].contains("2"));
}

#[test]
fn test_join_group_by_order_by_ordinal() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query(
        "CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT, quantity INTEGER)",
    )
    .unwrap();

    process_query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')").unwrap();
    process_query(
        "INSERT INTO orders VALUES (101, 1, 'Laptop', 2), (102, 1, 'Mouse', 5), (103, 2, 'Keyboard', 1)",
    )
    .unwrap();

    let result = process_query("SELECT users.name, COUNT(orders.product) AS order_count FROM users LEFT JOIN orders ON users.id = orders.user_id GROUP BY users.name ORDER BY 2 DESC").unwrap();

    let rows: Vec<&str> = result
        .lines()
        .skip(2)
        .filter(|line| !line.trim().is_empty())
        .collect();

    assert_eq!(rows.len(), 3);
    assert!(rows[0].contains("Alice") && rows[0].contains("2"));
    assert!(rows[1].contains("Bob") && rows[1].contains("1"));
    assert!(rows[2].contains("Charlie") && rows[2].contains("0"));
}

#[test]
fn test_join_group_by_order_by_expression() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query(
        "CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT, quantity INTEGER)",
    )
    .unwrap();

    process_query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')").unwrap();
    process_query(
        "INSERT INTO orders VALUES (101, 1, 'Laptop', 2), (102, 1, 'Mouse', 5), (103, 2, 'Keyboard', 1)",
    )
    .unwrap();

    let result = process_query("SELECT users.name, COUNT(orders.product) AS order_count FROM users LEFT JOIN orders ON users.id = orders.user_id GROUP BY users.name ORDER BY order_count + 1 DESC LIMIT 1").unwrap();

    let rows: Vec<&str> = result
        .lines()
        .skip(2)
        .filter(|line| !line.trim().is_empty())
        .collect();

    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("Alice"));
}
