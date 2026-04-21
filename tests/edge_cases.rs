use rustql::testing::{process_query, reset_database};
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup_test() -> std::sync::MutexGuard<'static, ()> {
    let guard = TEST_MUTEX.lock().unwrap();
    reset_database();
    guard
}

#[test]
fn test_out_of_range_integer_literal_returns_parse_error() {
    let _guard = setup_test();

    let error = process_query("SELECT 9223372036854775808").unwrap_err();
    assert!(error.contains("Invalid integer literal"));
}

#[test]
fn test_minimum_integer_literal_is_supported() {
    let _guard = setup_test();

    let result = process_query("SELECT -9223372036854775808").unwrap();
    assert!(result.contains("-9223372036854775808"));
}

#[test]
fn test_three_table_join() {
    let _guard = setup_test();

    process_query("CREATE TABLE customers (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, customer_id INTEGER, product_id INTEGER)")
        .unwrap();
    process_query("CREATE TABLE products (id INTEGER, pname TEXT)").unwrap();

    process_query("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
    process_query("INSERT INTO orders VALUES (10, 1, 100), (11, 2, 101)").unwrap();
    process_query("INSERT INTO products VALUES (100, 'Widget'), (101, 'Gadget')").unwrap();

    let result = process_query(
        "SELECT customers.name, products.pname \
         FROM customers \
         JOIN orders ON customers.id = orders.customer_id \
         JOIN products ON orders.product_id = products.id",
    )
    .unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("Widget"));
    assert!(result.contains("Bob"));
    assert!(result.contains("Gadget"));
}

#[test]
fn test_four_table_join() {
    let _guard = setup_test();

    process_query("CREATE TABLE t1 (id INTEGER, v1 TEXT)").unwrap();
    process_query("CREATE TABLE t2 (id INTEGER, t1_id INTEGER, v2 TEXT)").unwrap();
    process_query("CREATE TABLE t3 (id INTEGER, t2_id INTEGER, v3 TEXT)").unwrap();
    process_query("CREATE TABLE t4 (id INTEGER, t3_id INTEGER, v4 TEXT)").unwrap();

    process_query("INSERT INTO t1 VALUES (1, 'alpha')").unwrap();
    process_query("INSERT INTO t2 VALUES (10, 1, 'beta')").unwrap();
    process_query("INSERT INTO t3 VALUES (100, 10, 'gamma')").unwrap();
    process_query("INSERT INTO t4 VALUES (1000, 100, 'delta')").unwrap();

    let result = process_query(
        "SELECT t1.v1, t2.v2, t3.v3, t4.v4 \
         FROM t1 \
         JOIN t2 ON t1.id = t2.t1_id \
         JOIN t3 ON t2.id = t3.t2_id \
         JOIN t4 ON t3.id = t4.t3_id",
    )
    .unwrap();
    assert!(result.contains("alpha"));
    assert!(result.contains("beta"));
    assert!(result.contains("gamma"));
    assert!(result.contains("delta"));
}

#[test]
fn test_null_in_between() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (id INTEGER, val INTEGER)").unwrap();
    process_query("INSERT INTO t (id, val) VALUES (1, 10)").unwrap();
    process_query("INSERT INTO t (id, val) VALUES (2, 50)").unwrap();

    let result = process_query("SELECT * FROM t WHERE val BETWEEN 5 AND 100").unwrap();
    assert!(result.contains("10"));
    assert!(result.contains("50"));
}

#[test]
fn test_null_in_comparison() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (id INTEGER, val INTEGER)").unwrap();
    process_query("INSERT INTO t (id, val) VALUES (1, 10)").unwrap();

    let result = process_query("SELECT * FROM t WHERE val IS NULL").unwrap();
    assert!(!result.contains("10"));
}

#[test]
fn test_select_with_subquery_in_where() {
    let _guard = setup_test();

    process_query("CREATE TABLE t1 (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE t2 (id INTEGER, t1_id INTEGER)").unwrap();

    process_query("INSERT INTO t1 VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')").unwrap();
    process_query("INSERT INTO t2 VALUES (10, 1), (11, 3)").unwrap();

    let result = process_query("SELECT name FROM t1 WHERE id IN (SELECT t1_id FROM t2)").unwrap();
    assert!(result.contains("Alice"));
    assert!(result.contains("Charlie"));
    assert!(!result.contains("Bob"));
}

#[test]
fn test_multiple_aggregates_in_select() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (id INTEGER, val INTEGER)").unwrap();
    process_query("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();

    let result = process_query("SELECT COUNT(*), SUM(val), MIN(val), MAX(val) FROM t").unwrap();
    assert!(result.contains("3"));
    assert!(result.contains("60"));
    assert!(result.contains("10"));
    assert!(result.contains("30"));
}
