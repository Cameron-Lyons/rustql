mod common;
use common::reset_database;
use common::*;
use std::sync::Once;

static INIT: Once = Once::new();

fn setup() {
    INIT.call_once(|| {
        reset_database();
    });
}

#[test]
fn test_group_by_single_column() {
    setup();
    execute_sql("CREATE TABLE IF NOT EXISTS gbe_sales (region TEXT, amount INTEGER)").unwrap();
    let _ = execute_sql("DELETE FROM gbe_sales");
    execute_sql(
        "INSERT INTO gbe_sales VALUES ('North', 100), ('South', 200), ('North', 150), ('South', 50)",
    )
    .unwrap();

    let result = execute_sql("SELECT region, SUM(amount) FROM gbe_sales GROUP BY region").unwrap();
    assert!(result.contains("North"));
    assert!(result.contains("South"));
    assert!(result.contains("250")); // North total or South total
}

#[test]
fn test_group_by_with_count() {
    setup();
    execute_sql("CREATE TABLE IF NOT EXISTS gbe_items (category TEXT, name TEXT)").unwrap();
    let _ = execute_sql("DELETE FROM gbe_items");
    execute_sql(
        "INSERT INTO gbe_items VALUES ('A', 'Item1'), ('A', 'Item2'), ('B', 'Item3'), ('A', 'Item4')",
    )
    .unwrap();

    let result = execute_sql("SELECT category, COUNT(*) FROM gbe_items GROUP BY category").unwrap();
    assert!(result.contains("A"));
    assert!(result.contains("B"));
    assert!(result.contains("3")); // A has 3 items
    assert!(result.contains("1")); // B has 1 item
}

#[test]
fn test_group_by_with_having() {
    setup();
    execute_sql("CREATE TABLE IF NOT EXISTS gbe_products (category TEXT, price INTEGER)").unwrap();
    let _ = execute_sql("DELETE FROM gbe_products");
    execute_sql(
        "INSERT INTO gbe_products VALUES ('A', 10), ('A', 20), ('B', 5), ('B', 8), ('C', 100)",
    )
    .unwrap();

    let result = execute_sql(
        "SELECT category, SUM(price) FROM gbe_products GROUP BY category HAVING SUM(price) > 15",
    )
    .unwrap();
    assert!(result.contains("A")); // sum = 30
    assert!(result.contains("C")); // sum = 100
}

#[test]
fn test_group_by_with_avg() {
    setup();
    execute_sql("CREATE TABLE IF NOT EXISTS gbe_scores (team TEXT, score INTEGER)").unwrap();
    let _ = execute_sql("DELETE FROM gbe_scores");
    execute_sql(
        "INSERT INTO gbe_scores VALUES ('Red', 10), ('Red', 20), ('Blue', 15), ('Blue', 25)",
    )
    .unwrap();

    let result = execute_sql("SELECT team, AVG(score) FROM gbe_scores GROUP BY team").unwrap();
    assert!(result.contains("Red"));
    assert!(result.contains("Blue"));
    assert!(result.contains("15")); // Red avg
    assert!(result.contains("20")); // Blue avg
}

#[test]
fn test_group_by_with_min_max() {
    setup();
    execute_sql("CREATE TABLE IF NOT EXISTS gbe_temps (city TEXT, temp INTEGER)").unwrap();
    let _ = execute_sql("DELETE FROM gbe_temps");
    execute_sql("INSERT INTO gbe_temps VALUES ('NYC', 30), ('NYC', 40), ('LA', 60), ('LA', 80)")
        .unwrap();

    let result =
        execute_sql("SELECT city, MIN(temp), MAX(temp) FROM gbe_temps GROUP BY city").unwrap();
    assert!(result.contains("NYC"));
    assert!(result.contains("LA"));
    assert!(result.contains("30")); // NYC min
    assert!(result.contains("40")); // NYC max
    assert!(result.contains("60")); // LA min
    assert!(result.contains("80")); // LA max
}
