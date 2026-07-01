mod common;
use common::reset_database;
use common::*;
use rustql::ast::Value;
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
fn test_select_escaped_string_literals() {
    setup();

    let rows = query_rows("SELECT 'it''s ready' AS phrase, 'a\\\\b' AS slash").unwrap();

    rows.assert_columns(&["phrase", "slash"]);
    assert_eq!(
        rows.rows,
        vec![vec![
            Value::Text("it's ready".to_string()),
            Value::Text("a\\b".to_string()),
        ]]
    );
}

#[test]
fn test_select_boolean_literals() {
    setup();

    let rows = query_rows(
        "SELECT TRUE AS yes_value, \
                FALSE AS no_value, \
                NOT false AS not_false, \
                true IS NOT DISTINCT FROM TRUE AS same_value",
    )
    .unwrap();

    rows.assert_columns(&["yes_value", "no_value", "not_false", "same_value"]);
    assert_eq!(
        rows.rows,
        vec![vec![
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Boolean(true),
            Value::Boolean(true),
        ]]
    );
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
fn test_select_predicates_without_from_return_booleans() {
    setup();

    let rows = query_rows(
        "SELECT 1 = 1 AS eq, \
                NOT (1 = 2) AS not_false, \
                2 IN (1, 2) AS listed, \
                NULL IS NULL AS missing, \
                1.0 IS NOT DISTINCT FROM 1 AS numeric_same",
    )
    .unwrap();

    rows.assert_columns(&["eq", "not_false", "listed", "missing", "numeric_same"]);
    assert_eq!(
        rows.rows,
        vec![vec![
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
        ]]
    );
}

#[test]
fn test_null_predicates_return_unknown_in_projection() {
    setup();

    let rows = query_rows(
        "SELECT NULL = 1 AS eq_unknown, \
                1 <> NULL AS ne_unknown, \
                NULL LIKE 'a%' AS like_unknown, \
                2 BETWEEN NULL AND 3 AS between_unknown, \
                NULL IN (1, 2) AS in_unknown",
    )
    .unwrap();

    rows.assert_columns(&[
        "eq_unknown",
        "ne_unknown",
        "like_unknown",
        "between_unknown",
        "in_unknown",
    ]);
    assert_eq!(
        rows.rows,
        vec![vec![
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ]]
    );
}

#[test]
fn test_boolean_operators_use_sql_unknown_semantics_in_projection() {
    setup();

    let rows = query_rows(
        "SELECT TRUE AND NULL AS true_and_null, \
                FALSE AND NULL AS false_and_null, \
                TRUE OR NULL AS true_or_null, \
                FALSE OR NULL AS false_or_null, \
                NOT NULL AS not_null",
    )
    .unwrap();

    rows.assert_columns(&[
        "true_and_null",
        "false_and_null",
        "true_or_null",
        "false_or_null",
        "not_null",
    ]);
    assert_eq!(
        rows.rows,
        vec![vec![
            Value::Null,
            Value::Boolean(false),
            Value::Boolean(true),
            Value::Null,
            Value::Null,
        ]]
    );
}

#[test]
fn test_truth_tests_in_projection() {
    setup();

    let rows = query_rows(
        "SELECT TRUE IS TRUE AS true_is_true, \
                FALSE IS FALSE AS false_is_false, \
                NULL IS UNKNOWN AS null_is_unknown, \
                (NULL = 1) IS UNKNOWN AS predicate_is_unknown, \
                TRUE IS NOT FALSE AS true_is_not_false, \
                FALSE IS NOT TRUE AS false_is_not_true, \
                NULL IS NOT TRUE AS null_is_not_true, \
                NULL IS TRUE AS null_is_true, \
                TRUE IS UNKNOWN AS true_is_unknown",
    )
    .unwrap();

    rows.assert_columns(&[
        "true_is_true",
        "false_is_false",
        "null_is_unknown",
        "predicate_is_unknown",
        "true_is_not_false",
        "false_is_not_true",
        "null_is_not_true",
        "null_is_true",
        "true_is_unknown",
    ]);
    assert_eq!(
        rows.rows,
        vec![vec![
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Boolean(false),
        ]]
    );
}

#[test]
fn test_any_all_preserve_unknown_in_projection() {
    setup();

    let rows = query_rows(
        "SELECT 1 = ANY (SELECT CAST(NULL AS INTEGER) AS v) AS any_unknown, \
                1 = ALL (SELECT CAST(NULL AS INTEGER) AS v) AS all_unknown",
    )
    .unwrap();

    rows.assert_columns(&["any_unknown", "all_unknown"]);
    assert_eq!(rows.rows, vec![vec![Value::Null, Value::Null]]);
}

#[test]
fn test_infix_negated_predicates_in_projection() {
    setup();

    let rows = query_rows(
        "SELECT 1 NOT IN (2, 3) AS not_in_true, \
                1 NOT IN (1, NULL) AS not_in_false, \
                1 NOT IN (2, NULL) AS not_in_unknown, \
                'mouse' NOT LIKE '%top%' AS not_like_true, \
                'Alpha' NOT ILIKE 'a%' AS not_ilike_false, \
                2 NOT BETWEEN NULL AND 3 AS not_between_unknown",
    )
    .unwrap();

    rows.assert_columns(&[
        "not_in_true",
        "not_in_false",
        "not_in_unknown",
        "not_like_true",
        "not_ilike_false",
        "not_between_unknown",
    ]);
    assert_eq!(
        rows.rows,
        vec![vec![
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Null,
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Null,
        ]]
    );
}

#[test]
fn test_like_escape_in_projection() {
    setup();

    let rows = query_rows(
        "SELECT '100%' LIKE '100!%' ESCAPE '!' AS escaped_percent, \
                'under_score' LIKE 'under!_score' ESCAPE '!' AS escaped_underscore, \
                'underXscore' LIKE 'under!_score' ESCAPE '!' AS escaped_miss, \
                'A_B' ILIKE 'a!_b' ESCAPE '!' AS escaped_ilike, \
                NULL LIKE 'x' ESCAPE '!' AS null_like",
    )
    .unwrap();

    rows.assert_columns(&[
        "escaped_percent",
        "escaped_underscore",
        "escaped_miss",
        "escaped_ilike",
        "null_like",
    ]);
    assert_eq!(
        rows.rows,
        vec![vec![
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Boolean(true),
            Value::Null,
        ]]
    );
}

#[test]
fn test_like_escape_rejects_invalid_escape_patterns() {
    setup();

    let err = execute_sql("SELECT 'a' LIKE 'a' ESCAPE 'xx'").unwrap_err();
    assert!(err.contains("single-character"));

    let err = execute_sql("SELECT 'a' LIKE 'a!' ESCAPE '!'").unwrap_err();
    assert!(err.contains("cannot end with the ESCAPE character"));
}

#[test]
fn test_simple_case_uses_numeric_equality_semantics() {
    setup();

    let rows = query_rows("SELECT CASE 1.0 WHEN 1 THEN 'match' ELSE 'miss' END AS result").unwrap();

    rows.assert_columns(&["result"]);
    assert_eq!(rows.rows, vec![vec![Value::Text("match".to_string())]]);
}

#[test]
fn test_cte_with_constant_base() {
    setup();
    let result = execute_sql("WITH nums AS (SELECT 100 AS n) SELECT n FROM nums").unwrap();
    assert!(result.contains("100"));
}
