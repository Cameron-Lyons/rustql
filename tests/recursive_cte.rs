use rustql::executor::reset_database_state;
use rustql::process_query;

#[test]
fn test_recursive_cte_numbers() {
    reset_database_state();
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
fn test_recursive_cte_hierarchy() {
    reset_database_state();
    let result = process_query(
        "WITH RECURSIVE seq AS (
            SELECT 10 AS val
            UNION ALL
            SELECT val + 10 FROM seq WHERE val < 40
        )
        SELECT val FROM seq",
    )
    .unwrap();
    assert!(result.contains("10"));
    assert!(result.contains("20"));
    assert!(result.contains("30"));
    assert!(result.contains("40"));
}

#[test]
fn test_recursive_cte_with_limit() {
    reset_database_state();
    let result = process_query(
        "WITH RECURSIVE counter AS (
            SELECT 1 AS i
            UNION ALL
            SELECT i + 1 FROM counter WHERE i < 100
        )
        SELECT i FROM counter LIMIT 3",
    )
    .unwrap();
    assert!(result.contains("1"));
    assert!(result.contains("2"));
    assert!(result.contains("3"));
}

#[test]
fn test_recursive_cte_dedupe() {
    reset_database_state();
    let result = process_query(
        "WITH RECURSIVE seq AS (
            SELECT 1 AS v
            UNION
            SELECT 1 FROM seq WHERE v < 3
        )
        SELECT v FROM seq",
    )
    .unwrap();
    let count = result.matches('1').count();
    assert!(count <= 2);
}
