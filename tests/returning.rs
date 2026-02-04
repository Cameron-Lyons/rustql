use rustql::executor::reset_database_state;
use rustql::process_query;
use std::sync::Once;

static INIT: Once = Once::new();

fn setup() {
    INIT.call_once(|| {
        reset_database_state();
    });
}

#[test]
fn test_insert_returning_star() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS ret_star_tbl (id INTEGER, name TEXT)").unwrap();
    let result =
        process_query("INSERT INTO ret_star_tbl (id, name) VALUES (100, 'Alice') RETURNING *")
            .unwrap();
    assert!(result.contains("id"));
    assert!(result.contains("name"));
    assert!(result.contains("100"));
    assert!(result.contains("Alice"));
}

#[test]
fn test_insert_returning_specific_columns() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS ret_spec_tbl (id INTEGER, name TEXT, age INTEGER)")
        .unwrap();
    let result = process_query(
        "INSERT INTO ret_spec_tbl (id, name, age) VALUES (200, 'Bob', 30) RETURNING id, name",
    )
    .unwrap();
    assert!(result.contains("id"));
    assert!(result.contains("name"));
    assert!(result.contains("200"));
    assert!(result.contains("Bob"));
}

#[test]
fn test_update_returning() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS ret_upd_tbl (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO ret_upd_tbl VALUES (300, 'Original')").unwrap();
    let result =
        process_query("UPDATE ret_upd_tbl SET name = 'Updated300' WHERE id = 300 RETURNING *")
            .unwrap();
    assert!(result.contains("300"));
    assert!(result.contains("Updated300"));
}

#[test]
fn test_delete_returning() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS ret_del_tbl (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO ret_del_tbl VALUES (400, 'ToDelete400')").unwrap();
    let result = process_query("DELETE FROM ret_del_tbl WHERE id = 400 RETURNING *").unwrap();
    assert!(result.contains("400"));
    assert!(result.contains("ToDelete400"));
}

#[test]
fn test_insert_multiple_returning() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS ret_multi_tbl (id INTEGER, val TEXT)").unwrap();
    let result = process_query(
        "INSERT INTO ret_multi_tbl VALUES (501, 'A'), (502, 'B'), (503, 'C') RETURNING id",
    )
    .unwrap();
    assert!(result.contains("501"));
    assert!(result.contains("502"));
    assert!(result.contains("503"));
}
