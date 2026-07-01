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
fn test_delete_using_basic() {
    setup();
    execute_sql("CREATE TABLE IF NOT EXISTS du_items (item_id INTEGER, name TEXT)").unwrap();
    execute_sql("CREATE TABLE IF NOT EXISTS du_expired (expired_id INTEGER)").unwrap();
    let _ = execute_sql("DELETE FROM du_items");
    let _ = execute_sql("DELETE FROM du_expired");
    execute_sql("INSERT INTO du_items VALUES (1, 'Apple'), (2, 'Banana'), (3, 'Cherry')").unwrap();
    execute_sql("INSERT INTO du_expired VALUES (1), (3)").unwrap();

    let result =
        execute_sql("DELETE FROM du_items USING du_expired WHERE item_id = expired_id").unwrap();
    assert_command(result, CommandTag::Delete, 2);

    let remaining = execute_sql("SELECT name FROM du_items").unwrap();
    assert!(remaining.contains("Banana"));
    assert!(!remaining.contains("Apple"));
    assert!(!remaining.contains("Cherry"));
}

#[test]
fn test_delete_using_no_matches() {
    setup();
    execute_sql("CREATE TABLE IF NOT EXISTS du_data1 (data_id INTEGER, val TEXT)").unwrap();
    execute_sql("CREATE TABLE IF NOT EXISTS du_filter1 (filter_id INTEGER)").unwrap();
    let _ = execute_sql("DELETE FROM du_data1");
    let _ = execute_sql("DELETE FROM du_filter1");
    execute_sql("INSERT INTO du_data1 VALUES (1, 'A'), (2, 'B')").unwrap();
    execute_sql("INSERT INTO du_filter1 VALUES (99)").unwrap();

    let result =
        execute_sql("DELETE FROM du_data1 USING du_filter1 WHERE data_id = filter_id").unwrap();
    assert_command(result, CommandTag::Delete, 0);

    let remaining = execute_sql("SELECT COUNT(*) FROM du_data1").unwrap();
    assert!(remaining.contains("2"));
}

#[test]
fn test_delete_using_all_match() {
    setup();
    execute_sql("CREATE TABLE IF NOT EXISTS du_all (all_id INTEGER)").unwrap();
    execute_sql("CREATE TABLE IF NOT EXISTS du_all_filter (filter_id INTEGER)").unwrap();
    let _ = execute_sql("DELETE FROM du_all");
    let _ = execute_sql("DELETE FROM du_all_filter");
    execute_sql("INSERT INTO du_all VALUES (1), (2), (3)").unwrap();
    execute_sql("INSERT INTO du_all_filter VALUES (1), (2), (3)").unwrap();

    let result =
        execute_sql("DELETE FROM du_all USING du_all_filter WHERE all_id = filter_id").unwrap();
    assert_command(result, CommandTag::Delete, 3);

    let remaining = execute_sql("SELECT COUNT(*) FROM du_all").unwrap();
    assert!(remaining.contains("0"));
}

#[test]
fn test_delete_using_with_returning() {
    setup();
    execute_sql("CREATE TABLE IF NOT EXISTS du_ret (ret_id INTEGER, name TEXT)").unwrap();
    execute_sql("CREATE TABLE IF NOT EXISTS du_ret_filter (filter_id INTEGER)").unwrap();
    let _ = execute_sql("DELETE FROM du_ret");
    let _ = execute_sql("DELETE FROM du_ret_filter");
    execute_sql("INSERT INTO du_ret VALUES (1, 'Delete1'), (2, 'Keep'), (3, 'Delete3')").unwrap();
    execute_sql("INSERT INTO du_ret_filter VALUES (1), (3)").unwrap();

    let result =
        execute_sql("DELETE FROM du_ret USING du_ret_filter WHERE ret_id = filter_id RETURNING *")
            .unwrap();
    assert!(result.contains("Delete1"));
    assert!(result.contains("Delete3"));
    assert!(!result.contains("Keep"));
}

#[test]
fn test_delete_using_join_uses_joined_source_rows() {
    setup();
    execute_sql("CREATE TABLE IF NOT EXISTS du_join_items (id INTEGER, name TEXT)").unwrap();
    execute_sql("CREATE TABLE IF NOT EXISTS du_join_source (item_id INTEGER, flag_id INTEGER)")
        .unwrap();
    execute_sql("CREATE TABLE IF NOT EXISTS du_join_flags (flag_id INTEGER, active BOOLEAN)")
        .unwrap();
    let _ = execute_sql("DELETE FROM du_join_items");
    let _ = execute_sql("DELETE FROM du_join_source");
    let _ = execute_sql("DELETE FROM du_join_flags");
    execute_sql("INSERT INTO du_join_items VALUES (1, 'delete'), (2, 'keep'), (3, 'delete')")
        .unwrap();
    execute_sql("INSERT INTO du_join_source VALUES (1, 10), (2, 20), (3, 30)").unwrap();
    execute_sql("INSERT INTO du_join_flags VALUES (10, TRUE), (20, FALSE), (30, TRUE)").unwrap();

    assert_command_sql(
        "DELETE FROM du_join_items
         USING du_join_source AS src
         JOIN du_join_flags AS flag ON src.flag_id = flag.flag_id
         WHERE du_join_items.id = src.item_id AND flag.active IS TRUE",
        CommandTag::Delete,
        2,
    );

    assert_rows(
        "SELECT id, name FROM du_join_items ORDER BY id",
        &["id", "name"],
        vec![vec![Value::Integer(2), Value::Text("keep".to_string())]],
    );
}

#[test]
fn test_delete_using_left_join_null_extends_source_rows() {
    setup();
    execute_sql("CREATE TABLE IF NOT EXISTS du_left_items (id INTEGER, name TEXT)").unwrap();
    execute_sql("CREATE TABLE IF NOT EXISTS du_left_source (item_id INTEGER)").unwrap();
    execute_sql("CREATE TABLE IF NOT EXISTS du_left_flags (item_id INTEGER, marker TEXT)").unwrap();
    let _ = execute_sql("DELETE FROM du_left_items");
    let _ = execute_sql("DELETE FROM du_left_source");
    let _ = execute_sql("DELETE FROM du_left_flags");
    execute_sql("INSERT INTO du_left_items VALUES (1, 'keep'), (2, 'delete'), (3, 'ignore')")
        .unwrap();
    execute_sql("INSERT INTO du_left_source VALUES (1), (2)").unwrap();
    execute_sql("INSERT INTO du_left_flags VALUES (1, 'present')").unwrap();

    assert_command_sql(
        "DELETE FROM du_left_items
         USING du_left_source AS src
         LEFT JOIN du_left_flags AS flag ON src.item_id = flag.item_id
         WHERE du_left_items.id = src.item_id AND flag.marker IS NULL",
        CommandTag::Delete,
        1,
    );

    assert_rows(
        "SELECT id, name FROM du_left_items ORDER BY id",
        &["id", "name"],
        vec![
            vec![Value::Integer(1), Value::Text("keep".to_string())],
            vec![Value::Integer(3), Value::Text("ignore".to_string())],
        ],
    );
}
