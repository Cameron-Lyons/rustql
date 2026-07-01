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

fn setup_sales_table() {
    execute_sql("CREATE TABLE sales (id INTEGER, category TEXT, amount INTEGER)").unwrap();
    execute_sql(
        "INSERT INTO sales VALUES \
         (1, 'A', 50), \
         (2, 'A', 150), \
         (3, 'B', 200), \
         (4, 'B', 300), \
         (5, 'B', 100), \
         (6, 'C', 25)",
    )
    .unwrap();
}

#[test]
fn test_having_with_count() {
    let _guard = setup_test();
    setup_sales_table();

    let result =
        execute_sql("SELECT category, COUNT(*) FROM sales GROUP BY category HAVING COUNT(*) > 1")
            .unwrap();
    assert!(result.contains("A"));
    assert!(result.contains("B"));
    assert!(!result.contains("\tC\t") && !result.contains("\nC\t"));
}

#[test]
fn test_having_with_sum() {
    let _guard = setup_test();
    setup_sales_table();

    let result = execute_sql(
        "SELECT category, SUM(amount) FROM sales GROUP BY category HAVING SUM(amount) > 100",
    )
    .unwrap();
    assert!(result.contains("A"));
    assert!(result.contains("B"));
    assert!(!result.contains("\tC\t") && !result.contains("\nC\t"));
}

#[test]
fn test_having_filters_all_groups() {
    let _guard = setup_test();
    setup_sales_table();

    let result =
        execute_sql("SELECT category, COUNT(*) FROM sales GROUP BY category HAVING COUNT(*) > 100")
            .unwrap();
    assert!(!result.contains("A\t"));
    assert!(!result.contains("B\t"));
    assert!(!result.contains("C\t"));
}

#[test]
fn test_having_with_and() {
    let _guard = setup_test();
    setup_sales_table();

    let result = execute_sql(
        "SELECT category, COUNT(*) AS cnt, SUM(amount) AS total \
         FROM sales GROUP BY category \
         HAVING COUNT(*) > 1 AND SUM(amount) > 300",
    )
    .unwrap();
    assert!(result.contains("B"));
    assert!(!result.contains("\tA\t") && !result.contains("\nA\t"));
}

#[test]
fn test_having_with_or() {
    let _guard = setup_test();
    setup_sales_table();

    let result = execute_sql(
        "SELECT category, COUNT(*) AS cnt, SUM(amount) AS total \
         FROM sales GROUP BY category \
         HAVING COUNT(*) > 2 OR SUM(amount) > 100",
    )
    .unwrap();
    assert!(result.contains("A"));
    assert!(result.contains("B"));
}

#[test]
fn test_having_with_min() {
    let _guard = setup_test();
    setup_sales_table();

    let result = execute_sql(
        "SELECT category, MIN(amount) FROM sales GROUP BY category HAVING MIN(amount) > 30",
    )
    .unwrap();
    assert!(result.contains("A"));
    assert!(result.contains("B"));
    assert!(!result.contains("\tC\t") && !result.contains("\nC\t"));
}

#[test]
fn test_having_with_max() {
    let _guard = setup_test();
    setup_sales_table();

    let result = execute_sql(
        "SELECT category, MAX(amount) FROM sales GROUP BY category HAVING MAX(amount) < 200",
    )
    .unwrap();
    assert!(result.contains("A"));
    assert!(!result.contains("\tB\t") && !result.contains("\nB\t"));
}

#[test]
fn test_having_with_between_and_in() {
    let _guard = setup_test();
    setup_sales_table();

    let rows = query_rows(
        "SELECT category, SUM(amount) FROM sales GROUP BY category \
         HAVING SUM(amount) BETWEEN 175 AND 650 AND category IN ('A', 'B') \
         ORDER BY category",
    )
    .unwrap();

    rows.assert_columns(&["category", "Sum(amount)"]);
    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Text("A".to_string()), Value::Float(200.0)],
            vec![Value::Text("B".to_string()), Value::Float(600.0)],
        ]
    );
}

#[test]
fn test_having_with_scalar_cast_and_case_expressions() {
    let _guard = setup_test();
    setup_sales_table();

    let rows = query_rows(
        "SELECT category, COUNT(*) FROM sales GROUP BY category \
         HAVING CASE WHEN LOWER(CAST(category AS TEXT)) = 'b' THEN COUNT(*) ELSE 0 END = 3",
    )
    .unwrap();

    rows.assert_columns(&["category", "Count(*)"]);
    assert_eq!(
        rows.rows,
        vec![vec![Value::Text("B".to_string()), Value::Integer(3)]]
    );
}

#[test]
fn test_having_with_is_distinct_from_numeric_semantics() {
    let _guard = setup_test();
    setup_sales_table();

    let rows = query_rows(
        "SELECT category, SUM(amount) FROM sales GROUP BY category \
         HAVING SUM(amount) IS NOT DISTINCT FROM 200",
    )
    .unwrap();

    rows.assert_columns(&["category", "Sum(amount)"]);
    assert_eq!(
        rows.rows,
        vec![vec![Value::Text("A".to_string()), Value::Float(200.0)]]
    );
}
