mod common;
use common::{process_query, reset_database, snapshot_database};
use rustql::*;
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup_test() -> std::sync::MutexGuard<'static, ()> {
    let guard = TEST_MUTEX.lock().unwrap();
    reset_database();
    guard
}

#[test]
fn test_query_planner_basic() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();

    process_query("INSERT INTO users VALUES (1, 'Alice', 25)").unwrap();
    process_query("INSERT INTO users VALUES (2, 'Bob', 30)").unwrap();
    process_query("INSERT INTO users VALUES (3, 'Charlie', 35)").unwrap();

    process_query("CREATE INDEX idx_age ON users (age)").unwrap();

    let db = snapshot_database().unwrap();

    let tokens = lexer::tokenize("SELECT * FROM users WHERE age > 25").unwrap();
    let statement = parser::parse(tokens).unwrap();

    if let ast::Statement::Select(stmt) = statement {
        let plan_result = planner::explain_query(&db, &stmt);
        assert!(plan_result.is_ok());
        let plan_str = plan_result.unwrap();

        assert!(plan_str.contains("Query Plan"));

        assert!(plan_str.contains("Index Scan") || plan_str.contains("Seq Scan"));
    }
}

#[test]
fn test_query_planner_join() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, user_id INTEGER, amount FLOAT)").unwrap();

    process_query("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    process_query("INSERT INTO orders VALUES (1, 1, 100.0)").unwrap();

    let db = snapshot_database().unwrap();

    let tokens = lexer::tokenize(
        "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id",
    )
    .unwrap();
    let statement = parser::parse(tokens).unwrap();

    if let ast::Statement::Select(stmt) = statement {
        let plan_result = planner::explain_query(&db, &stmt);
        assert!(plan_result.is_ok());
        let plan_str = plan_result.unwrap();

        assert!(
            plan_str.contains("Join")
                || plan_str.contains("Hash Join")
                || plan_str.contains("Nested Loop")
        );
    }
}

#[test]
fn test_explain_command() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();

    process_query("INSERT INTO users VALUES (1, 'Alice', 25)").unwrap();
    process_query("INSERT INTO users VALUES (2, 'Bob', 30)").unwrap();

    process_query("CREATE INDEX idx_age ON users (age)").unwrap();

    let result = process_query("EXPLAIN SELECT * FROM users WHERE age > 25");
    assert!(result.is_ok());
    let plan_str = result.unwrap();

    assert!(plan_str.contains("Query Plan"));
    assert!(plan_str.contains("users"));

    assert!(plan_str.contains("Index Scan") || plan_str.contains("Seq Scan"));
}

#[test]
fn test_explain_with_join() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, user_id INTEGER, amount FLOAT)").unwrap();

    process_query("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    process_query("INSERT INTO orders VALUES (1, 1, 100.0)").unwrap();

    let result = process_query(
        "EXPLAIN SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id",
    );
    assert!(result.is_ok());
    let plan_str = result.unwrap();

    assert!(plan_str.contains("Query Plan"));
    assert!(
        plan_str.contains("Join")
            || plan_str.contains("Hash Join")
            || plan_str.contains("Nested Loop")
    );
}

#[test]
fn test_explain_fetch_with_ties() {
    let _guard = setup_test();

    process_query("CREATE TABLE scores (id INTEGER, score INTEGER)").unwrap();
    process_query("INSERT INTO scores VALUES (1, 100)").unwrap();
    process_query("INSERT INTO scores VALUES (2, 90)").unwrap();
    process_query("INSERT INTO scores VALUES (3, 90)").unwrap();

    let result = process_query(
        "EXPLAIN SELECT id FROM scores ORDER BY score DESC FETCH FIRST 2 ROWS WITH TIES",
    );
    assert!(result.is_ok());
    let plan_str = result.unwrap();

    assert!(plan_str.contains("Query Plan"));
    assert!(plan_str.contains("Limit: 2 Offset: 0 With Ties"));
}

#[test]
fn test_explain_generate_series() {
    let _guard = setup_test();

    let result = process_query("EXPLAIN SELECT * FROM GENERATE_SERIES(1, 5)");
    assert!(result.is_ok());
    let plan_str = result.unwrap();

    assert!(plan_str.contains("Query Plan"));
    assert!(plan_str.contains("Function Scan on generate_series"));
}

#[test]
fn test_explain_distinct_on() {
    let _guard = setup_test();

    process_query("CREATE TABLE items (category TEXT, item TEXT, price INTEGER)").unwrap();
    process_query("INSERT INTO items VALUES ('fruit', 'apple', 1)").unwrap();
    process_query("INSERT INTO items VALUES ('fruit', 'banana', 2)").unwrap();
    process_query("INSERT INTO items VALUES ('veggie', 'carrot', 3)").unwrap();

    let result = process_query(
        "EXPLAIN SELECT DISTINCT ON (category) category, item, price FROM items ORDER BY category, price",
    );
    assert!(result.is_ok());
    let plan_str = result.unwrap();

    assert!(plan_str.contains("Query Plan"));
    assert!(plan_str.contains("Distinct On (Column(\"category\"))"));
}

#[test]
fn test_explain_natural_join() {
    let _guard = setup_test();

    process_query("CREATE TABLE departments (id INTEGER, dept_name TEXT)").unwrap();
    process_query("CREATE TABLE staff (id INTEGER, staff_name TEXT)").unwrap();
    process_query("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales')").unwrap();
    process_query("INSERT INTO staff VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    let result =
        process_query("EXPLAIN SELECT dept_name, staff_name FROM departments NATURAL JOIN staff");
    assert!(result.is_ok());
    let plan_str = result.unwrap();

    assert!(plan_str.contains("Query Plan"));
    assert!(plan_str.contains("Nested Loop Natural Join"));
}

#[test]
fn test_explain_full_join() {
    let _guard = setup_test();

    process_query("CREATE TABLE left_table (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE right_table (id INTEGER, value TEXT)").unwrap();
    process_query("INSERT INTO left_table VALUES (1, 'A'), (2, 'B')").unwrap();
    process_query("INSERT INTO right_table VALUES (2, 'Y'), (3, 'Z')").unwrap();

    let result = process_query(
        "EXPLAIN SELECT * FROM left_table FULL JOIN right_table ON left_table.id = right_table.id",
    );
    assert!(result.is_ok());
    let plan_str = result.unwrap();

    assert!(plan_str.contains("Query Plan"));
    assert!(plan_str.contains("Nested Loop Full Join"));
}

#[test]
fn test_explain_multi_join() {
    let _guard = setup_test();

    process_query("CREATE TABLE customers (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, customer_id INTEGER, product_id INTEGER)")
        .unwrap();
    process_query("CREATE TABLE products (id INTEGER, pname TEXT)").unwrap();
    process_query("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
    process_query("INSERT INTO orders VALUES (10, 1, 100), (11, 2, 101)").unwrap();
    process_query("INSERT INTO products VALUES (100, 'Widget'), (101, 'Gadget')").unwrap();

    let result = process_query(
        "EXPLAIN SELECT customers.name, products.pname
         FROM customers
         JOIN orders ON customers.id = orders.customer_id
         JOIN products ON orders.product_id = products.id",
    );
    assert!(result.is_ok());
    let plan_str = result.unwrap();

    assert!(plan_str.contains("Query Plan"));
    assert!(plan_str.matches("Join").count() >= 2);
}

#[test]
fn test_explain_join_subquery() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER)").unwrap();
    process_query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
    process_query("INSERT INTO orders VALUES (10, 1, 75), (11, 2, 60)").unwrap();

    let result = process_query(
        "EXPLAIN SELECT users.name, recent.amount
         FROM users
         JOIN (
             SELECT user_id, amount
             FROM orders
             WHERE amount >= 60
         ) AS recent ON users.id = recent.user_id",
    );
    assert!(result.is_ok());
    let plan_str = result.unwrap();

    assert!(plan_str.contains("Query Plan"));
    assert!(plan_str.contains("Join"));
}

#[test]
fn test_explain_rollup() {
    let _guard = setup_test();

    process_query("CREATE TABLE sales (region TEXT, product TEXT, amount INTEGER)").unwrap();
    process_query("INSERT INTO sales VALUES ('East', 'Widget', 100), ('West', 'Gadget', 200)")
        .unwrap();

    let result = process_query(
        "EXPLAIN SELECT region, product, COUNT(*) AS total
         FROM sales
         GROUP BY ROLLUP(region, product)",
    );
    assert!(result.is_ok());
    let plan_str = result.unwrap();

    assert!(plan_str.contains("Query Plan"));
    assert!(plan_str.contains("Grouping Sets"));
}

#[test]
fn test_explain_lateral_join() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER)").unwrap();
    process_query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
    process_query("INSERT INTO orders VALUES (10, 1, 75), (11, 2, 60)").unwrap();

    let result = process_query(
        "EXPLAIN SELECT users.name, recent.amount
         FROM users
         LEFT JOIN LATERAL (
             SELECT amount
             FROM orders
             WHERE orders.user_id = users.id
             ORDER BY amount DESC
             FETCH FIRST 1 ROW ONLY
         ) AS recent",
    );
    assert!(result.is_ok());
    let plan_str = result.unwrap();

    assert!(plan_str.contains("Query Plan"));
    assert!(plan_str.contains("Lateral"));
}
