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
fn test_insert_returning_star() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS ret_ins (ret_id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    process_query("DELETE FROM ret_ins").unwrap_or_default();

    let result = process_query("INSERT INTO ret_ins VALUES (1, 'Alice') RETURNING *").unwrap();
    assert!(result.contains("1"));
    assert!(result.contains("Alice"));
}

#[test]
fn test_insert_returning_specific_columns() {
    setup();
    process_query(
        "CREATE TABLE IF NOT EXISTS ret_ins2 (id2 INTEGER PRIMARY KEY, name2 TEXT, age2 INTEGER)",
    )
    .unwrap();
    process_query("DELETE FROM ret_ins2").unwrap_or_default();

    let result =
        process_query("INSERT INTO ret_ins2 VALUES (1, 'Bob', 30) RETURNING name2, age2").unwrap();
    assert!(result.contains("Bob"));
    assert!(result.contains("30"));
}

#[test]
fn test_update_returning_star() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS ret_upd (upd_id INTEGER PRIMARY KEY, value INTEGER)")
        .unwrap();
    process_query("DELETE FROM ret_upd").unwrap_or_default();
    process_query("INSERT INTO ret_upd VALUES (1, 100), (2, 200)").unwrap();

    let result =
        process_query("UPDATE ret_upd SET value = 999 WHERE upd_id = 1 RETURNING *").unwrap();
    assert!(result.contains("1"));
    assert!(result.contains("999"));
    assert!(!result.contains("200"));
}

#[test]
fn test_delete_returning_star() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS ret_del (del_id INTEGER, item TEXT)").unwrap();
    process_query("DELETE FROM ret_del").unwrap_or_default();
    process_query("INSERT INTO ret_del VALUES (1, 'Delete Me'), (2, 'Keep Me')").unwrap();

    let result = process_query("DELETE FROM ret_del WHERE del_id = 1 RETURNING *").unwrap();
    assert!(result.contains("Delete Me"));
    assert!(!result.contains("Keep Me"));
}

#[test]
fn test_insert_multiple_returning() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS ret_multi (multi_id INTEGER, val INTEGER)").unwrap();
    process_query("DELETE FROM ret_multi").unwrap_or_default();

    let result =
        process_query("INSERT INTO ret_multi VALUES (1, 10), (2, 20), (3, 30) RETURNING *")
            .unwrap();
    assert!(result.contains("1"));
    assert!(result.contains("2"));
    assert!(result.contains("3"));
    assert!(result.contains("10"));
    assert!(result.contains("20"));
    assert!(result.contains("30"));
}
