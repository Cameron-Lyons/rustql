mod common;
use common::{process_query, reset_database};
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup_test<'a>() -> std::sync::MutexGuard<'a, ()> {
    let guard = TEST_MUTEX.lock().unwrap();
    reset_database();
    guard
}

#[test]
fn test_derived_table_basic() {
    let _guard = setup_test();
    process_query("CREATE TABLE employees (id INTEGER, name TEXT, salary INTEGER)").unwrap();
    process_query("INSERT INTO employees VALUES (1, 'Alice', 50000)").unwrap();
    process_query("INSERT INTO employees VALUES (2, 'Bob', 60000)").unwrap();
    process_query("INSERT INTO employees VALUES (3, 'Charlie', 70000)").unwrap();

    let result = process_query(
        "SELECT sub.name FROM (SELECT name, salary FROM employees WHERE salary > 55000) AS sub",
    )
    .unwrap();
    assert!(!result.contains("Alice"));
    assert!(result.contains("Bob"));
    assert!(result.contains("Charlie"));
}

#[test]
fn test_derived_table_with_where() {
    let _guard = setup_test();
    process_query("CREATE TABLE items (id INTEGER, price FLOAT, category TEXT)").unwrap();
    process_query("INSERT INTO items VALUES (1, 10.0, 'A')").unwrap();
    process_query("INSERT INTO items VALUES (2, 20.0, 'B')").unwrap();
    process_query("INSERT INTO items VALUES (3, 30.0, 'A')").unwrap();

    let result = process_query(
        "SELECT sub.price FROM (SELECT price, category FROM items WHERE category = 'A') AS sub WHERE sub.price > 15",
    )
    .unwrap();
    assert!(!result.contains("10"));
    assert!(result.contains("30"));
}

#[test]
fn test_derived_table_with_aggregation() {
    let _guard = setup_test();
    process_query("CREATE TABLE sales (id INTEGER, product TEXT, amount INTEGER)").unwrap();
    process_query("INSERT INTO sales VALUES (1, 'Widget', 100)").unwrap();
    process_query("INSERT INTO sales VALUES (2, 'Widget', 200)").unwrap();
    process_query("INSERT INTO sales VALUES (3, 'Gadget', 150)").unwrap();

    let result = process_query(
        "SELECT sub.product, sub.total FROM (SELECT product, SUM(amount) AS total FROM sales GROUP BY product) AS sub ORDER BY sub.total DESC",
    )
    .unwrap();
    assert!(result.contains("Widget"));
    assert!(result.contains("300"));
    assert!(result.contains("Gadget"));
    assert!(result.contains("150"));
}

#[test]
fn test_derived_table_with_alias_reference() {
    let _guard = setup_test();
    process_query("CREATE TABLE nums (val INTEGER)").unwrap();
    process_query("INSERT INTO nums VALUES (1)").unwrap();
    process_query("INSERT INTO nums VALUES (2)").unwrap();
    process_query("INSERT INTO nums VALUES (3)").unwrap();

    let result =
        process_query("SELECT d.val FROM (SELECT val FROM nums) AS d WHERE d.val >= 2").unwrap();
    assert!(!result.contains(" 1"));
    assert!(result.contains("2"));
    assert!(result.contains("3"));
}
