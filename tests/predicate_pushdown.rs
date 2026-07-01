mod common;
use common::*;
use rustql::ast::Value;
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

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    let result = execute_sql("EXPLAIN SELECT * FROM t WHERE name = 'Alice'").unwrap();
    assert!(result.contains("Seq Scan"));
    assert!(result.contains("Filter"));
}

#[test]
fn test_explain_index_scan() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
    execute_sql("CREATE INDEX idx_id ON t (id)").unwrap();

    let result = execute_sql("EXPLAIN SELECT * FROM t WHERE id = 1").unwrap();
    assert!(result.contains("Index Scan"));
}

#[test]
fn test_index_scan_numeric_equality_uses_comparison_semantics() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, value INTEGER)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();
    execute_sql("CREATE INDEX idx_value ON t (value)").unwrap();

    let plan = execute_sql("EXPLAIN SELECT id FROM t WHERE value = 10.0").unwrap();
    assert!(plan.contains("Index Scan using idx_value"));
    assert!(plan.contains("Rows: 1"), "{plan:?}");

    let rows = query_rows("SELECT id FROM t WHERE value = 10.0").unwrap();
    rows.assert_columns(&["id"]);
    assert_eq!(rows.rows, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_index_scan_numeric_in_uses_comparison_semantics() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, value INTEGER)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();
    execute_sql("CREATE INDEX idx_value ON t (value)").unwrap();

    let plan = execute_sql("EXPLAIN SELECT id FROM t WHERE value IN (10.0, 30.0)").unwrap();
    assert!(plan.contains("Index Scan using idx_value"));
    assert!(plan.contains("Rows: 2"), "{plan:?}");

    let rows = query_rows("SELECT id FROM t WHERE value IN (10.0, 30.0) ORDER BY id").unwrap();
    rows.assert_columns(&["id"]);
    assert_eq!(
        rows.rows,
        vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]
    );
}

#[test]
fn test_expression_in_list_does_not_use_literal_index_lookup() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, value INTEGER)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();
    execute_sql("CREATE INDEX idx_value ON t (value)").unwrap();

    let plan = execute_sql("EXPLAIN SELECT id FROM t WHERE value IN (10 + 0, 30)").unwrap();
    assert!(plan.contains("Seq Scan"), "{plan:?}");
    assert!(!plan.contains("Index Scan using idx_value"), "{plan:?}");

    let rows = query_rows("SELECT id FROM t WHERE value IN (10 + 0, 30) ORDER BY id").unwrap();
    rows.assert_columns(&["id"]);
    assert_eq!(
        rows.rows,
        vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]
    );
}

#[test]
fn test_index_scan_numeric_range_uses_comparison_semantics() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, value INTEGER)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 10), (2, 11), (3, 20)").unwrap();
    execute_sql("CREATE INDEX idx_value ON t (value)").unwrap();

    let plan = execute_sql("EXPLAIN SELECT id FROM t WHERE value > 10.5").unwrap();
    assert!(plan.contains("Index Scan using idx_value"));

    let rows = query_rows("SELECT id FROM t WHERE value > 10.5 ORDER BY id").unwrap();
    rows.assert_columns(&["id"]);
    assert_eq!(
        rows.rows,
        vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]
    );
}

#[test]
fn test_index_scan_commuted_range_predicates() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, value INTEGER)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 10), (2, 11), (3, 20), (4, 21)").unwrap();
    execute_sql("CREATE INDEX idx_value ON t (value)").unwrap();

    let greater_plan = execute_sql("EXPLAIN SELECT id FROM t WHERE 10.5 < value").unwrap();
    assert!(greater_plan.contains("Index Scan using idx_value"));

    let greater_rows = query_rows("SELECT id FROM t WHERE 10.5 < value ORDER BY id").unwrap();
    greater_rows.assert_columns(&["id"]);
    assert_eq!(
        greater_rows.rows,
        vec![
            vec![Value::Integer(2)],
            vec![Value::Integer(3)],
            vec![Value::Integer(4)]
        ]
    );

    let less_plan = execute_sql("EXPLAIN SELECT id FROM t WHERE 20.0 >= value").unwrap();
    assert!(less_plan.contains("Index Scan using idx_value"));

    let less_rows = query_rows("SELECT id FROM t WHERE 20.0 >= value ORDER BY id").unwrap();
    less_rows.assert_columns(&["id"]);
    assert_eq!(
        less_rows.rows,
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(3)]
        ]
    );
}

#[test]
fn test_not_predicate_does_not_use_positive_index_candidates() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, value TEXT)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'one'), (2, 'two'), (3, 'three')").unwrap();
    execute_sql("CREATE INDEX idx_id ON t (id)").unwrap();

    let plan = execute_sql("EXPLAIN SELECT id FROM t WHERE NOT (id = 1)").unwrap();
    assert!(!plan.contains("Index Scan using idx_id"));

    let rows = query_rows("SELECT id FROM t WHERE NOT (id = 1) ORDER BY id").unwrap();
    rows.assert_columns(&["id"]);
    assert_eq!(
        rows.rows,
        vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]
    );
}

#[test]
fn test_explain_join_shows_join_node() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE a (id INTEGER, val TEXT)").unwrap();
    execute_sql("CREATE TABLE b (id INTEGER, a_id INTEGER)").unwrap();
    execute_sql("INSERT INTO a VALUES (1, 'x')").unwrap();
    execute_sql("INSERT INTO b VALUES (1, 1)").unwrap();

    let result = execute_sql("EXPLAIN SELECT * FROM a JOIN b ON a.id = b.a_id").unwrap();
    assert!(result.contains("Hash Join") || result.contains("Nested Loop"));
}

#[test]
fn test_explain_aggregate() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, category TEXT, amount INTEGER)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'A', 100)").unwrap();

    let result =
        execute_sql("EXPLAIN SELECT category, SUM(amount) FROM t GROUP BY category").unwrap();
    assert!(result.contains("Aggregate"));
}

#[test]
fn test_explain_sort() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    let result = execute_sql("EXPLAIN SELECT * FROM t ORDER BY name").unwrap();
    assert!(result.contains("Sort"));
}

#[test]
fn test_explain_limit() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    let result = execute_sql("EXPLAIN SELECT * FROM t LIMIT 1").unwrap();
    assert!(result.contains("Limit"));
}
