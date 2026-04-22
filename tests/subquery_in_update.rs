mod common;
use common::process_query;
use common::reset_database;
use std::sync::Once;

static INIT: Once = Once::new();

fn setup() {
    INIT.call_once(|| {
        reset_database();
    });
}

#[test]
fn test_update_with_scalar_subquery() {
    setup();
    process_query(
        "CREATE TABLE IF NOT EXISTS siu_targets (target_id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .unwrap();
    process_query("CREATE TABLE IF NOT EXISTS siu_sources (src_id INTEGER, max_val INTEGER)")
        .unwrap();
    process_query("DELETE FROM siu_targets").unwrap_or_default();
    process_query("DELETE FROM siu_sources").unwrap_or_default();
    process_query("INSERT INTO siu_targets VALUES (1, 0), (2, 0)").unwrap();
    process_query("INSERT INTO siu_sources VALUES (1, 100), (2, 200)").unwrap();

    let result = process_query(
        "UPDATE siu_targets SET val = (SELECT MAX(max_val) FROM siu_sources) WHERE target_id = 1",
    )
    .unwrap();
    assert!(result.contains("1 row(s) updated"));

    let select_result = process_query("SELECT val FROM siu_targets WHERE target_id = 1").unwrap();
    assert!(select_result.contains("200"));
}

#[test]
fn test_update_with_subquery_returning_null() {
    setup();
    process_query(
        "CREATE TABLE IF NOT EXISTS siu_data (data_id INTEGER PRIMARY KEY, value INTEGER)",
    )
    .unwrap();
    process_query("CREATE TABLE IF NOT EXISTS siu_empty (empty_val INTEGER)").unwrap();
    process_query("DELETE FROM siu_data").unwrap_or_default();
    process_query("DELETE FROM siu_empty").unwrap_or_default();
    process_query("INSERT INTO siu_data VALUES (1, 100)").unwrap();

    let result = process_query(
        "UPDATE siu_data SET value = (SELECT MAX(empty_val) FROM siu_empty) WHERE data_id = 1",
    )
    .unwrap();
    assert!(result.contains("1 row(s) updated"));

    let select_result = process_query("SELECT value FROM siu_data WHERE data_id = 1").unwrap();
    assert!(select_result.contains("NULL"));
}

#[test]
fn test_update_with_count_subquery() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS siu_counter (counter_id INTEGER PRIMARY KEY, count_val INTEGER)")
        .unwrap();
    process_query("CREATE TABLE IF NOT EXISTS siu_items (item_id INTEGER)").unwrap();
    process_query("DELETE FROM siu_counter").unwrap_or_default();
    process_query("DELETE FROM siu_items").unwrap_or_default();
    process_query("INSERT INTO siu_counter VALUES (1, 0)").unwrap();
    process_query("INSERT INTO siu_items VALUES (1), (2), (3), (4), (5)").unwrap();

    let result = process_query(
        "UPDATE siu_counter SET count_val = (SELECT COUNT(*) FROM siu_items) WHERE counter_id = 1",
    )
    .unwrap();
    assert!(result.contains("1 row(s) updated"));

    let select_result =
        process_query("SELECT count_val FROM siu_counter WHERE counter_id = 1").unwrap();
    assert!(select_result.contains("5"));
}

#[test]
fn test_update_with_returning_and_subquery() {
    setup();
    process_query(
        "CREATE TABLE IF NOT EXISTS siu_ret (ret_id INTEGER PRIMARY KEY, amount INTEGER)",
    )
    .unwrap();
    process_query("CREATE TABLE IF NOT EXISTS siu_ref (ref_val INTEGER)").unwrap();
    process_query("DELETE FROM siu_ret").unwrap_or_default();
    process_query("DELETE FROM siu_ref").unwrap_or_default();
    process_query("INSERT INTO siu_ret VALUES (1, 0)").unwrap();
    process_query("INSERT INTO siu_ref VALUES (42)").unwrap();

    let result = process_query(
        "UPDATE siu_ret SET amount = (SELECT MAX(ref_val) FROM siu_ref) WHERE ret_id = 1 RETURNING *",
    )
    .unwrap();
    assert!(result.contains("42"));
    assert!(result.contains("1"));
}
