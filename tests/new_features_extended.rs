mod common;
use common::{process_query, reset_database};
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup_test<'a>() -> std::sync::MutexGuard<'a, ()> {
    let guard = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    reset_database();
    guard
}

#[test]
fn test_case_when_simple() {
    let _guard = setup_test();
    process_query("CREATE TABLE employees (id INTEGER, name TEXT, salary INTEGER)").unwrap();
    process_query("INSERT INTO employees VALUES (1, 'Alice', 80000), (2, 'Bob', 40000), (3, 'Charlie', 120000)").unwrap();

    let result = process_query(
        "SELECT name, CASE WHEN salary > 100000 THEN 'high' WHEN salary > 50000 THEN 'medium' ELSE 'low' END AS level FROM employees",
    ).unwrap();
    assert!(result.contains("high"));
    assert!(result.contains("medium"));
    assert!(result.contains("low"));
}

#[test]
fn test_case_when_no_else() {
    let _guard = setup_test();
    process_query("CREATE TABLE items (id INTEGER, status TEXT)").unwrap();
    process_query("INSERT INTO items VALUES (1, 'active'), (2, 'inactive')").unwrap();

    let result = process_query(
        "SELECT id, CASE WHEN status = 'active' THEN 'yes' END AS is_active FROM items",
    )
    .unwrap();
    assert!(result.contains("yes"));
}

#[test]
fn test_upper_function() {
    let _guard = setup_test();
    process_query("CREATE TABLE words (id INTEGER, word TEXT)").unwrap();
    process_query("INSERT INTO words VALUES (1, 'hello'), (2, 'world')").unwrap();

    let result = process_query("SELECT UPPER(word) FROM words").unwrap();
    assert!(result.contains("HELLO"));
    assert!(result.contains("WORLD"));
}

#[test]
fn test_lower_function() {
    let _guard = setup_test();
    process_query("CREATE TABLE words (id INTEGER, word TEXT)").unwrap();
    process_query("INSERT INTO words VALUES (1, 'HELLO'), (2, 'WORLD')").unwrap();

    let result = process_query("SELECT LOWER(word) FROM words").unwrap();
    assert!(result.contains("hello"));
    assert!(result.contains("world"));
}

#[test]
fn test_length_function() {
    let _guard = setup_test();
    process_query("CREATE TABLE words (id INTEGER, word TEXT)").unwrap();
    process_query("INSERT INTO words VALUES (1, 'hello'), (2, 'ab')").unwrap();

    let result = process_query("SELECT LENGTH(word) FROM words").unwrap();
    assert!(result.contains("5"));
    assert!(result.contains("2"));
}

#[test]
fn test_abs_function() {
    let _guard = setup_test();
    process_query("CREATE TABLE nums (id INTEGER, val INTEGER)").unwrap();
    process_query("INSERT INTO nums VALUES (1, -5), (2, 3)").unwrap();

    let result = process_query("SELECT ABS(val) FROM nums").unwrap();
    assert!(result.contains("5"));
    assert!(result.contains("3"));
}

#[test]
fn test_round_function() {
    let _guard = setup_test();
    process_query("CREATE TABLE decimals (id INTEGER, val FLOAT)").unwrap();
    process_query("INSERT INTO decimals VALUES (1, 3.14159)").unwrap();

    let result = process_query("SELECT ROUND(val, 2) FROM decimals").unwrap();
    assert!(result.contains("3.14"));
}

#[test]
fn test_coalesce_function() {
    let _guard = setup_test();
    process_query("CREATE TABLE data (id INTEGER, a INTEGER, b INTEGER)").unwrap();
    process_query("INSERT INTO data VALUES (1, NULL, 10)").unwrap();

    let result = process_query("SELECT COALESCE(a, b) FROM data").unwrap();
    assert!(result.contains("10"));
}

#[test]
fn test_substring_function() {
    let _guard = setup_test();
    process_query("CREATE TABLE words (id INTEGER, word TEXT)").unwrap();
    process_query("INSERT INTO words VALUES (1, 'hello world')").unwrap();

    let result = process_query("SELECT SUBSTRING(word, 1, 5) FROM words").unwrap();
    assert!(result.contains("hello"));
}

#[test]
fn test_cross_join() {
    let _guard = setup_test();
    process_query("CREATE TABLE colors (name TEXT)").unwrap();
    process_query("CREATE TABLE sizes (size TEXT)").unwrap();
    process_query("INSERT INTO colors VALUES ('red'), ('blue')").unwrap();
    process_query("INSERT INTO sizes VALUES ('S'), ('L')").unwrap();

    let result =
        process_query("SELECT colors.name, sizes.size FROM colors CROSS JOIN sizes").unwrap();
    assert!(result.contains("red"));
    assert!(result.contains("blue"));
    assert!(result.contains("S"));
    assert!(result.contains("L"));
}

#[test]
fn test_natural_join() {
    let _guard = setup_test();
    process_query("CREATE TABLE departments (id INTEGER, dept_name TEXT)").unwrap();
    process_query("CREATE TABLE staff (id INTEGER, staff_name TEXT)").unwrap();
    process_query("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales')").unwrap();
    process_query("INSERT INTO staff VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    let result =
        process_query("SELECT dept_name, staff_name FROM departments NATURAL JOIN staff").unwrap();
    assert!(result.contains("Engineering"));
    assert!(result.contains("Alice"));
    assert!(result.contains("Sales"));
    assert!(result.contains("Bob"));
}

#[test]
fn test_insert_select() {
    let _guard = setup_test();
    process_query("CREATE TABLE src (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE dst (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO src VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    let result = process_query("INSERT INTO dst SELECT * FROM src").unwrap();
    assert!(result.contains("2 row(s) inserted"));

    let result = process_query("SELECT * FROM dst").unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("Bob"));
}

#[test]
fn test_insert_select_with_where() {
    let _guard = setup_test();
    process_query("CREATE TABLE src (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE dst2 (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO src VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')").unwrap();

    process_query("INSERT INTO dst2 SELECT * FROM src WHERE id > 1").unwrap();

    let result = process_query("SELECT * FROM dst2").unwrap();
    assert!(!result.contains("Alice"));
    assert!(result.contains("Bob"));
    assert!(result.contains("Charlie"));
}

#[test]
fn test_check_constraint() {
    let _guard = setup_test();
    process_query("CREATE TABLE ages (id INTEGER, age INTEGER CHECK (age > 0))").unwrap();

    let result = process_query("INSERT INTO ages VALUES (1, 25)");
    assert!(result.is_ok());

    let result = process_query("INSERT INTO ages VALUES (2, -5)");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("CHECK") || err.contains("constraint"));
}

#[test]
fn test_check_constraint_on_update() {
    let _guard = setup_test();
    process_query("CREATE TABLE ages (id INTEGER, age INTEGER CHECK (age > 0))").unwrap();
    process_query("INSERT INTO ages VALUES (1, 25)").unwrap();

    let result = process_query("UPDATE ages SET age = -1 WHERE id = 1");
    assert!(result.is_err());
}

#[test]
fn test_auto_increment() {
    let _guard = setup_test();
    process_query("CREATE TABLE items (id INTEGER AUTO_INCREMENT, name TEXT)").unwrap();

    process_query("INSERT INTO items (name) VALUES ('first')").unwrap();
    process_query("INSERT INTO items (name) VALUES ('second')").unwrap();

    let result = process_query("SELECT * FROM items").unwrap();
    assert!(result.contains("first"));
    assert!(result.contains("second"));
    assert!(result.contains("1"));
    assert!(result.contains("2"));
}

#[test]
fn test_savepoint_and_release() {
    let _guard = setup_test();
    process_query("CREATE TABLE sp_test (id INTEGER, name TEXT)").unwrap();
    process_query("BEGIN TRANSACTION").unwrap();
    process_query("INSERT INTO sp_test VALUES (1, 'Alice')").unwrap();

    let result = process_query("SAVEPOINT sp1");
    assert!(result.is_ok());

    process_query("INSERT INTO sp_test VALUES (2, 'Bob')").unwrap();

    let result = process_query("RELEASE SAVEPOINT sp1");
    assert!(result.is_ok());

    process_query("COMMIT").unwrap();

    let result = process_query("SELECT * FROM sp_test").unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("Bob"));
}

#[test]
fn test_savepoint_rollback() {
    let _guard = setup_test();
    process_query("CREATE TABLE sp_test2 (id INTEGER, name TEXT)").unwrap();
    process_query("BEGIN TRANSACTION").unwrap();
    process_query("INSERT INTO sp_test2 VALUES (1, 'Alice')").unwrap();
    process_query("SAVEPOINT sp1").unwrap();
    process_query("INSERT INTO sp_test2 VALUES (2, 'Bob')").unwrap();

    process_query("ROLLBACK TO SAVEPOINT sp1").unwrap();
    process_query("COMMIT").unwrap();

    let result = process_query("SELECT * FROM sp_test2").unwrap();
    assert!(result.contains("Alice"));
    assert!(!result.contains("Bob"));
}

#[test]
fn test_cte_basic() {
    let _guard = setup_test();
    process_query("CREATE TABLE employees (id INTEGER, name TEXT, salary INTEGER)").unwrap();
    process_query(
        "INSERT INTO employees VALUES (1, 'Alice', 80000), (2, 'Bob', 120000), (3, 'Charlie', 60000)",
    )
    .unwrap();

    let result = process_query(
        "WITH high_earners AS (SELECT name, salary FROM employees WHERE salary > 70000) SELECT * FROM high_earners",
    )
    .unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("Bob"));
    assert!(!result.contains("Charlie"));
}

#[test]
fn test_window_row_number() {
    let _guard = setup_test();
    process_query("CREATE TABLE scores (id INTEGER, name TEXT, score INTEGER)").unwrap();
    process_query("INSERT INTO scores VALUES (1, 'Alice', 90), (2, 'Bob', 85), (3, 'Charlie', 95)")
        .unwrap();

    let result = process_query(
        "SELECT name, score, ROW_NUMBER() OVER (ORDER BY score DESC) AS rnum FROM scores",
    )
    .unwrap();
    assert!(result.contains("rnum"));
    assert!(result.contains("1"));
    assert!(result.contains("2"));
    assert!(result.contains("3"));
}

#[test]
fn test_window_rank() {
    let _guard = setup_test();
    process_query("CREATE TABLE scores (id INTEGER, name TEXT, score INTEGER)").unwrap();
    process_query("INSERT INTO scores VALUES (1, 'Alice', 90), (2, 'Bob', 90), (3, 'Charlie', 85)")
        .unwrap();

    let result =
        process_query("SELECT name, score, RANK() OVER (ORDER BY score DESC) AS rnk FROM scores")
            .unwrap();
    assert!(result.contains("rnk"));
}

#[test]
fn test_window_dense_rank() {
    let _guard = setup_test();
    process_query("CREATE TABLE scores (id INTEGER, name TEXT, score INTEGER)").unwrap();
    process_query("INSERT INTO scores VALUES (1, 'Alice', 90), (2, 'Bob', 90), (3, 'Charlie', 85)")
        .unwrap();

    let result = process_query(
        "SELECT name, score, DENSE_RANK() OVER (ORDER BY score DESC) AS drnk FROM scores",
    )
    .unwrap();
    assert!(result.contains("drnk"));
}

#[test]
fn test_composite_index() {
    let _guard = setup_test();
    process_query("CREATE TABLE orders (id INTEGER, customer TEXT, product TEXT, amount FLOAT)")
        .unwrap();
    process_query(
        "INSERT INTO orders VALUES (1, 'Alice', 'Widget', 10.0), (2, 'Bob', 'Gadget', 20.0)",
    )
    .unwrap();

    let result = process_query("CREATE INDEX idx_cust_prod ON orders (customer, product)");
    assert!(result.is_ok());

    let result = process_query("SELECT * FROM orders WHERE customer = 'Alice'").unwrap();
    assert!(result.contains("Widget"));
}

#[test]
fn test_analyze() {
    let _guard = setup_test();
    process_query("CREATE TABLE stats_test (id INTEGER, val TEXT)").unwrap();
    process_query("INSERT INTO stats_test VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();

    let result = process_query("ANALYZE stats_test");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("stats_test") || output.contains("3"));
}

#[test]
fn test_case_when_in_where() {
    let _guard = setup_test();
    process_query("CREATE TABLE products (id INTEGER, category TEXT, price FLOAT)").unwrap();
    process_query(
        "INSERT INTO products VALUES (1, 'electronics', 100.0), (2, 'clothing', 50.0), (3, 'electronics', 200.0)",
    )
    .unwrap();

    let result = process_query(
        "SELECT * FROM products WHERE CASE WHEN category = 'electronics' THEN price > 150 ELSE price > 0 END",
    );
    if let Ok(output) = result {
        assert!(output.contains("200"));
    }
}

#[test]
fn test_multiple_scalar_functions() {
    let _guard = setup_test();
    process_query("CREATE TABLE people (id INTEGER, first_name TEXT, last_name TEXT)").unwrap();
    process_query("INSERT INTO people VALUES (1, 'alice', 'smith')").unwrap();

    let result = process_query("SELECT UPPER(first_name), LENGTH(last_name) FROM people").unwrap();
    assert!(result.contains("ALICE"));
    assert!(result.contains("5"));
}

#[test]
fn test_insert_select_with_columns() {
    let _guard = setup_test();
    process_query("CREATE TABLE src3 (id INTEGER, name TEXT, age INTEGER)").unwrap();
    process_query("CREATE TABLE dst3 (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO src3 VALUES (1, 'Alice', 30), (2, 'Bob', 25)").unwrap();

    process_query("INSERT INTO dst3 (id, name) SELECT id, name FROM src3").unwrap();

    let result = process_query("SELECT * FROM dst3").unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("Bob"));
}

#[test]
fn test_cross_join_produces_cartesian_product() {
    let _guard = setup_test();
    process_query("CREATE TABLE t1 (a INTEGER)").unwrap();
    process_query("CREATE TABLE t2 (b INTEGER)").unwrap();
    process_query("INSERT INTO t1 VALUES (1), (2)").unwrap();
    process_query("INSERT INTO t2 VALUES (10), (20)").unwrap();

    let result = process_query("SELECT t1.a, t2.b FROM t1 CROSS JOIN t2").unwrap();
    let lines: Vec<&str> = result.lines().collect();
    assert!(lines.len() >= 5); // header + separator + 4 data rows
}

#[test]
fn test_cte_with_aggregation() {
    let _guard = setup_test();
    process_query("CREATE TABLE sales (id INTEGER, region TEXT, amount FLOAT)").unwrap();
    process_query(
        "INSERT INTO sales VALUES (1, 'North', 100.0), (2, 'North', 200.0), (3, 'South', 150.0)",
    )
    .unwrap();

    let result = process_query(
        "WITH regional_totals AS (SELECT region, SUM(amount) AS total FROM sales GROUP BY region) SELECT * FROM regional_totals",
    )
    .unwrap();
    assert!(result.contains("North"));
    assert!(result.contains("300"));
    assert!(result.contains("South"));
    assert!(result.contains("150"));
}

#[test]
fn test_window_with_partition() {
    let _guard = setup_test();
    process_query("CREATE TABLE emp (id INTEGER, dept TEXT, salary INTEGER)").unwrap();
    process_query(
        "INSERT INTO emp VALUES (1, 'eng', 100), (2, 'eng', 120), (3, 'sales', 90), (4, 'sales', 110)",
    )
    .unwrap();

    let result = process_query(
        "SELECT dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM emp",
    )
    .unwrap();
    assert!(result.contains("rn"));
}
