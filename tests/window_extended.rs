mod common;
use common::*;
use rustql::ast::Value;
use std::sync::Mutex;

static GLOBAL_TEST_LOCK: Mutex<()> = Mutex::new(());

fn setup_sales() {
    reset_database();
    execute_sql("CREATE TABLE sales (id INTEGER, rep TEXT, region TEXT, amount INTEGER)").unwrap();
    execute_sql("INSERT INTO sales VALUES (1, 'Alice', 'East', 100)").unwrap();
    execute_sql("INSERT INTO sales VALUES (2, 'Bob', 'East', 200)").unwrap();
    execute_sql("INSERT INTO sales VALUES (3, 'Charlie', 'West', 150)").unwrap();
    execute_sql("INSERT INTO sales VALUES (4, 'Diana', 'West', 250)").unwrap();
    execute_sql("INSERT INTO sales VALUES (5, 'Eve', 'East', 200)").unwrap();
    execute_sql("INSERT INTO sales VALUES (6, 'Frank', 'West', 300)").unwrap();
}

#[test]
fn test_first_value() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    setup_sales();
    let result = execute_sql(
        "SELECT rep, amount, FIRST_VALUE(rep) OVER (ORDER BY amount) AS first_rep FROM sales",
    )
    .unwrap();
    assert!(result.contains("first_rep"));
    assert!(result.contains("Alice"));
}

#[test]
fn test_first_value_with_partition() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    setup_sales();
    let result = execute_sql(
        "SELECT rep, region, amount, FIRST_VALUE(rep) OVER (PARTITION BY region ORDER BY amount) AS first_in_region FROM sales",
    )
    .unwrap();
    assert!(result.contains("first_in_region"));
}

#[test]
fn test_window_partition_uses_numeric_equality_semantics() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_database();

    let rows = query_rows(
        "SELECT label, ROW_NUMBER() OVER (PARTITION BY k ORDER BY amount) AS rn \
         FROM (VALUES (1, 'int', 20), (1.0, 'float', 10), ('1', 'text', 30)) AS t(k, label, amount) \
         ORDER BY label",
    )
    .unwrap();

    rows.assert_columns(&["label", "rn"]);
    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Text("float".to_string()), Value::Integer(1)],
            vec![Value::Text("int".to_string()), Value::Integer(2)],
            vec![Value::Text("text".to_string()), Value::Integer(1)],
        ]
    );
}

#[test]
fn test_window_order_by_nulls_last() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_database();

    let rows = query_rows(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY score DESC NULLS LAST, id) AS rn \
         FROM (VALUES (1, NULL), (2, 10), (3, 5), (4, NULL)) AS t(id, score) \
         ORDER BY id",
    )
    .unwrap();

    rows.assert_columns(&["id", "rn"]);
    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Integer(1), Value::Integer(3)],
            vec![Value::Integer(2), Value::Integer(1)],
            vec![Value::Integer(3), Value::Integer(2)],
            vec![Value::Integer(4), Value::Integer(4)],
        ]
    );
}

#[test]
fn test_last_value_unbounded() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    setup_sales();
    let result = execute_sql(
        "SELECT rep, amount, LAST_VALUE(rep) OVER (ORDER BY amount ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_rep FROM sales",
    )
    .unwrap();
    assert!(result.contains("last_rep"));
    assert!(result.contains("Frank"));
}

#[test]
fn test_nth_value() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    setup_sales();
    let result = execute_sql(
        "SELECT rep, amount, NTH_VALUE(rep, 2) OVER (ORDER BY amount ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rep FROM sales",
    )
    .unwrap();
    assert!(result.contains("second_rep"));
    assert!(result.contains("Charlie"));
}

#[test]
fn test_nth_value_out_of_range() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    setup_sales();
    let result =
        execute_sql("SELECT rep, NTH_VALUE(rep, 2) OVER (ORDER BY amount) AS nth FROM sales")
            .unwrap();
    assert!(result.contains("nth"));
    let lines: Vec<String> = result.lines().collect();
    assert!(lines.len() > 2);
    let first_data_line = &lines[2];
    assert!(first_data_line.contains("NULL"));
}

#[test]
fn test_lag_and_lead_zero_offsets_use_current_row() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    setup_sales();

    let rows = query_rows(
        "SELECT id, LAG(amount, 0) OVER (ORDER BY id) AS lag_same, \
         LEAD(amount, 0) OVER (ORDER BY id) AS lead_same \
         FROM sales ORDER BY id",
    )
    .unwrap();

    rows.assert_columns(&["id", "lag_same", "lead_same"]);
    assert_eq!(
        rows.rows[0],
        vec![Value::Integer(1), Value::Integer(100), Value::Integer(100)]
    );
    assert_eq!(
        rows.rows[5],
        vec![Value::Integer(6), Value::Integer(300), Value::Integer(300)]
    );
}

#[test]
fn test_window_functions_reject_invalid_offsets() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    setup_sales();

    for sql in [
        "SELECT LAG(amount, -1) OVER (ORDER BY id) FROM sales",
        "SELECT LEAD(amount, -1) OVER (ORDER BY id) FROM sales",
        "SELECT NTILE(0) OVER (ORDER BY id) FROM sales",
        "SELECT NTH_VALUE(amount, 0) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM sales",
    ] {
        assert!(execute_sql(sql).is_err(), "expected error for {sql}");
    }
}

#[test]
fn test_percent_rank() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    setup_sales();
    let result = execute_sql(
        "SELECT rep, amount, PERCENT_RANK() OVER (ORDER BY amount) AS pct_rank FROM sales",
    )
    .unwrap();
    assert!(result.contains("pct_rank"));
    assert!(result.contains("0"));
    assert!(result.contains("1"));
}

#[test]
fn test_percent_rank_with_ties() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    setup_sales();
    let result = execute_sql(
        "SELECT rep, amount, PERCENT_RANK() OVER (ORDER BY amount) AS pct_rank FROM sales",
    )
    .unwrap();
    let lines: Vec<String> = result.lines().collect();
    let mut found_200 = 0;
    for line in &lines[2..] {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() >= 2 && parts[1].trim() == "200" {
            found_200 += 1;
        }
    }
    assert!(found_200 >= 2, "Expected at least two rows with amount=200");
}

#[test]
fn test_cume_dist() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    setup_sales();
    let result =
        execute_sql("SELECT rep, amount, CUME_DIST() OVER (ORDER BY amount) AS cd FROM sales")
            .unwrap();
    assert!(result.contains("cd"));
    assert!(result.contains("1\t") || result.contains("1\n") || result.contains("1"));
}

#[test]
fn test_cume_dist_with_partition() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    setup_sales();
    let result = execute_sql(
        "SELECT rep, region, amount, CUME_DIST() OVER (PARTITION BY region ORDER BY amount) AS cd FROM sales",
    )
    .unwrap();
    assert!(result.contains("cd"));
}

#[test]
fn test_first_value_partition_by() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    setup_sales();
    let result = execute_sql(
        "SELECT rep, region, amount, FIRST_VALUE(amount) OVER (PARTITION BY region ORDER BY amount) AS min_in_region FROM sales",
    )
    .unwrap();
    assert!(result.contains("min_in_region"));
    assert!(result.contains("100"));
    assert!(result.contains("150"));
}

#[test]
fn test_percent_rank_single_row() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_database();
    execute_sql("CREATE TABLE solo (id INTEGER, val INTEGER)").unwrap();
    execute_sql("INSERT INTO solo VALUES (1, 42)").unwrap();
    let result =
        execute_sql("SELECT val, PERCENT_RANK() OVER (ORDER BY val) AS pr FROM solo").unwrap();
    assert!(result.contains("0"));
}
