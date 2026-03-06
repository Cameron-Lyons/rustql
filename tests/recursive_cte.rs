use rustql::testing::process_query;
use rustql::testing::reset_database;
use std::sync::Once;

static INIT: Once = Once::new();

fn setup() {
    INIT.call_once(|| {
        reset_database();
    });
}

#[test]
fn test_recursive_cte_numbers() {
    setup();
    let result = process_query(
        "WITH RECURSIVE nums AS (
            SELECT 1 AS n
            UNION ALL
            SELECT n + 1 FROM nums WHERE n < 5
        )
        SELECT n FROM nums",
    )
    .unwrap();
    assert!(result.contains("1"));
    assert!(result.contains("2"));
    assert!(result.contains("3"));
    assert!(result.contains("4"));
    assert!(result.contains("5"));
}

#[test]
fn test_recursive_cte_sequence() {
    setup();
    let result = process_query(
        "WITH RECURSIVE seq AS (
            SELECT 10 AS val
            UNION ALL
            SELECT val - 2 FROM seq WHERE val > 0
        )
        SELECT val FROM seq",
    )
    .unwrap();
    assert!(result.contains("10"));
    assert!(result.contains("8"));
    assert!(result.contains("6"));
    assert!(result.contains("4"));
    assert!(result.contains("2"));
    assert!(result.contains("0"));
}

#[test]
fn test_recursive_cte_fibonacci() {
    setup();
    let result = process_query(
        "WITH RECURSIVE fib AS (
            SELECT 1 AS n, 0 AS a, 1 AS b
            UNION ALL
            SELECT n + 1, b, a + b FROM fib WHERE n < 10
        )
        SELECT a FROM fib",
    )
    .unwrap();
    assert!(result.contains("0"));
    assert!(result.contains("1"));
    assert!(result.contains("2"));
    assert!(result.contains("3"));
    assert!(result.contains("5"));
    assert!(result.contains("8"));
}

#[test]
fn test_recursive_cte_with_limit() {
    setup();
    let result = process_query(
        "WITH RECURSIVE nums AS (
            SELECT 1 AS n
            UNION ALL
            SELECT n + 1 FROM nums WHERE n < 100
        )
        SELECT n FROM nums LIMIT 5",
    )
    .unwrap();
    let lines: Vec<&str> = result.lines().filter(|l| !l.starts_with('-')).collect();
    assert!(lines.len() <= 7);
}
