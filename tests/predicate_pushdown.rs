use rustql::{process_query, reset_database};
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup_test() -> std::sync::MutexGuard<'static, ()> {
    let guard = TEST_MUTEX.lock().unwrap();
    reset_database();
    guard
}

#[test]
fn test_explain_seq_scan_with_filter() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    let result = process_query("EXPLAIN SELECT * FROM t WHERE name = 'Alice'").unwrap();
    assert!(result.contains("Seq Scan"));
    assert!(result.contains("Filter"));
}

#[test]
fn test_explain_index_scan() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
    process_query("CREATE INDEX idx_id ON t (id)").unwrap();

    let result = process_query("EXPLAIN SELECT * FROM t WHERE id = 1").unwrap();
    assert!(result.contains("Index Scan"));
}

#[test]
fn test_explain_join_shows_join_node() {
    let _guard = setup_test();

    process_query("CREATE TABLE a (id INTEGER, val TEXT)").unwrap();
    process_query("CREATE TABLE b (id INTEGER, a_id INTEGER)").unwrap();
    process_query("INSERT INTO a VALUES (1, 'x')").unwrap();
    process_query("INSERT INTO b VALUES (1, 1)").unwrap();

    let result = process_query("EXPLAIN SELECT * FROM a JOIN b ON a.id = b.a_id").unwrap();
    assert!(result.contains("Hash Join") || result.contains("Nested Loop"));
}

#[test]
fn test_explain_aggregate() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (id INTEGER, category TEXT, amount INTEGER)").unwrap();
    process_query("INSERT INTO t VALUES (1, 'A', 100)").unwrap();

    let result =
        process_query("EXPLAIN SELECT category, SUM(amount) FROM t GROUP BY category").unwrap();
    assert!(result.contains("Aggregate"));
}

#[test]
fn test_explain_sort() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    let result = process_query("EXPLAIN SELECT * FROM t ORDER BY name").unwrap();
    assert!(result.contains("Sort"));
}

#[test]
fn test_explain_limit() {
    let _guard = setup_test();

    process_query("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    let result = process_query("EXPLAIN SELECT * FROM t LIMIT 1").unwrap();
    assert!(result.contains("Limit"));
}
