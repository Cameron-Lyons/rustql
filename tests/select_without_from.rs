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
fn test_select_constant_integer() {
    setup();
    let result = execute_sql("SELECT 42 AS answer").unwrap();
    assert!(result.contains("42"));
    assert!(result.contains("answer"));
}

#[test]
fn test_select_constant_string() {
    setup();
    let result = execute_sql("SELECT 'hello' AS greeting").unwrap();
    assert!(result.contains("hello"));
    assert!(result.contains("greeting"));
}

#[test]
fn test_select_arithmetic_expression() {
    setup();
    let result = execute_sql("SELECT 2 + 3 * 4 AS calc").unwrap();
    assert!(result.contains("14")); // 2 + 12 = 14
}

#[test]
fn test_select_multiple_constants() {
    setup();
    let result = execute_sql("SELECT 1 AS a, 2 AS b, 3 AS c").unwrap();
    assert!(result.contains("1"));
    assert!(result.contains("2"));
    assert!(result.contains("3"));
}

#[test]
fn test_select_scalar_function_no_table() {
    setup();
    let result = execute_sql("SELECT UPPER('hello') AS upper_hello").unwrap();
    assert!(result.contains("HELLO"));
}

#[test]
fn test_select_coalesce_constants() {
    setup();
    let result = execute_sql("SELECT COALESCE(NULL, 'default') AS val").unwrap();
    assert!(result.contains("default"));
}

#[test]
fn test_select_case_no_table() {
    setup();
    let result = execute_sql("SELECT CASE WHEN 1 = 1 THEN 'yes' ELSE 'no' END AS result").unwrap();
    assert!(result.contains("yes"));
}

#[test]
fn test_cte_with_constant_base() {
    setup();
    let result = execute_sql("WITH nums AS (SELECT 100 AS n) SELECT n FROM nums").unwrap();
    assert!(result.contains("100"));
}
