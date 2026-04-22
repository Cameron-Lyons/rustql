mod common;
use common::*;
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup_test<'a>() -> std::sync::MutexGuard<'a, ()> {
    let guard = TEST_MUTEX.lock().unwrap();
    reset_database();
    guard
}

#[test]
fn test_create_composite_index() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE orders (customer_id INTEGER, order_date TEXT, total FLOAT)").unwrap();
    execute_sql("INSERT INTO orders VALUES (1, '2024-01-01', 100.0)").unwrap();
    execute_sql("INSERT INTO orders VALUES (2, '2024-01-02', 200.0)").unwrap();

    let result = execute_sql("CREATE INDEX idx_cust_date ON orders (customer_id, order_date)");
    assert!(result.is_ok());
}

#[test]
fn test_composite_index_query() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE orders (customer_id INTEGER, order_date TEXT, total FLOAT)").unwrap();
    execute_sql("INSERT INTO orders VALUES (1, '2024-01-01', 100.0)").unwrap();
    execute_sql("INSERT INTO orders VALUES (1, '2024-01-02', 150.0)").unwrap();
    execute_sql("INSERT INTO orders VALUES (2, '2024-01-01', 200.0)").unwrap();
    execute_sql("CREATE INDEX idx_cust_date ON orders (customer_id, order_date)").unwrap();

    let result =
        execute_sql("SELECT total FROM orders WHERE customer_id = 1 AND order_date = '2024-01-02'")
            .unwrap();
    assert!(result.contains("150"));
}

#[test]
fn test_composite_index_with_explain() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE products (category TEXT, brand TEXT, price FLOAT)").unwrap();
    execute_sql("INSERT INTO products VALUES ('Electronics', 'Acme', 99.99)").unwrap();
    execute_sql("CREATE INDEX idx_cat_brand ON products (category, brand)").unwrap();

    let result =
        execute_sql("EXPLAIN SELECT * FROM products WHERE category = 'Electronics'").unwrap();
    assert!(result.contains("Scan") || result.contains("scan"));
}

#[test]
fn test_composite_index_exact_match_uses_named_index() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE orders (customer_id INTEGER, order_date TEXT, total FLOAT)").unwrap();
    execute_sql("INSERT INTO orders VALUES (1, '2024-01-01', 100.0)").unwrap();
    execute_sql("INSERT INTO orders VALUES (1, '2024-01-02', 150.0)").unwrap();
    execute_sql("INSERT INTO orders VALUES (2, '2024-01-01', 200.0)").unwrap();
    execute_sql("CREATE INDEX idx_cust_date ON orders (customer_id, order_date)").unwrap();

    let result = execute_sql(
        "EXPLAIN SELECT total FROM orders WHERE customer_id = 1 AND order_date = '2024-01-02'",
    )
    .unwrap();

    assert!(
        result.contains("Index Scan using idx_cust_date"),
        "{result:?}"
    );
}

#[test]
fn test_drop_composite_index() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE t (a INTEGER, b INTEGER)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 2)").unwrap();
    execute_sql("CREATE INDEX idx_ab ON t (a, b)").unwrap();

    execute_sql("DROP INDEX idx_ab").unwrap();

    let result = execute_sql("SELECT * FROM t WHERE a = 1").unwrap();
    assert!(result.contains("1"));
}

#[test]
fn test_multi_column_index_does_not_reserve_helper_names() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE helper_idx (a INTEGER, b INTEGER, c INTEGER)").unwrap();

    execute_sql("CREATE INDEX idx_main ON helper_idx (a, b)").unwrap();

    let result = execute_sql("CREATE INDEX idx_main_2 ON helper_idx (c)");
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn test_drop_table_removes_associated_indexes() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE drop_idx (a INTEGER, b INTEGER)").unwrap();
    execute_sql("CREATE INDEX idx_drop_idx ON drop_idx (a, b)").unwrap();
    execute_sql("DROP TABLE drop_idx").unwrap();

    execute_sql("CREATE TABLE drop_idx (a INTEGER, b INTEGER)").unwrap();

    let result = execute_sql("CREATE INDEX idx_drop_idx ON drop_idx (a, b)");
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn test_composite_index_tracks_insert_update_and_delete() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE lifecycle_idx (a INTEGER, b INTEGER, payload TEXT)").unwrap();
    execute_sql("INSERT INTO lifecycle_idx VALUES (1, 1, 'old')").unwrap();
    execute_sql("CREATE INDEX idx_lifecycle ON lifecycle_idx (a, b)").unwrap();

    execute_sql("INSERT INTO lifecycle_idx VALUES (2, 2, 'new')").unwrap();
    let inserted = execute_sql("SELECT payload FROM lifecycle_idx WHERE a = 2 AND b = 2").unwrap();
    assert!(inserted.contains("new"), "{inserted:?}");

    execute_sql("UPDATE lifecycle_idx SET a = 3, b = 4 WHERE payload = 'new'").unwrap();
    let updated = execute_sql("SELECT payload FROM lifecycle_idx WHERE a = 3 AND b = 4").unwrap();
    assert!(updated.contains("new"), "{updated:?}");

    execute_sql("DELETE FROM lifecycle_idx WHERE a = 3 AND b = 4").unwrap();
    let deleted = execute_sql("SELECT payload FROM lifecycle_idx WHERE a = 3 AND b = 4").unwrap();
    assert!(!deleted.contains("new"), "{deleted:?}");
}
