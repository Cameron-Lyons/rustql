use rustql::{process_query, reset_database};
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup_test() -> std::sync::MutexGuard<'static, ()> {
    let guard = TEST_MUTEX.lock().unwrap();
    reset_database();
    guard
}

fn setup_sales_table() {
    process_query("CREATE TABLE sales (id INTEGER, category TEXT, amount INTEGER)").unwrap();
    process_query(
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
        process_query("SELECT category, COUNT(*) FROM sales GROUP BY category HAVING COUNT(*) > 1")
            .unwrap();
    assert!(result.contains("A"));
    assert!(result.contains("B"));
    assert!(!result.contains("\tC\t") && !result.contains("\nC\t"));
}

#[test]
fn test_having_with_sum() {
    let _guard = setup_test();
    setup_sales_table();

    let result = process_query(
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

    let result = process_query(
        "SELECT category, COUNT(*) FROM sales GROUP BY category HAVING COUNT(*) > 100",
    )
    .unwrap();
    assert!(!result.contains("A\t"));
    assert!(!result.contains("B\t"));
    assert!(!result.contains("C\t"));
}

#[test]
fn test_having_with_and() {
    let _guard = setup_test();
    setup_sales_table();

    let result = process_query(
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

    let result = process_query(
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

    let result = process_query(
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

    let result = process_query(
        "SELECT category, MAX(amount) FROM sales GROUP BY category HAVING MAX(amount) < 200",
    )
    .unwrap();
    assert!(result.contains("A"));
    assert!(!result.contains("\tB\t") && !result.contains("\nB\t"));
}
