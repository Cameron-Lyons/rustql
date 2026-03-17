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
fn test_delete_using_basic() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS du_items (item_id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE IF NOT EXISTS du_expired (expired_id INTEGER)").unwrap();
    process_query("DELETE FROM du_items").unwrap_or_default();
    process_query("DELETE FROM du_expired").unwrap_or_default();
    process_query("INSERT INTO du_items VALUES (1, 'Apple'), (2, 'Banana'), (3, 'Cherry')")
        .unwrap();
    process_query("INSERT INTO du_expired VALUES (1), (3)").unwrap();

    let result =
        process_query("DELETE FROM du_items USING du_expired WHERE item_id = expired_id").unwrap();
    assert!(result.contains("2 row(s) deleted"));

    let remaining = process_query("SELECT name FROM du_items").unwrap();
    assert!(remaining.contains("Banana"));
    assert!(!remaining.contains("Apple"));
    assert!(!remaining.contains("Cherry"));
}

#[test]
fn test_delete_using_no_matches() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS du_data1 (data_id INTEGER, val TEXT)").unwrap();
    process_query("CREATE TABLE IF NOT EXISTS du_filter1 (filter_id INTEGER)").unwrap();
    process_query("DELETE FROM du_data1").unwrap_or_default();
    process_query("DELETE FROM du_filter1").unwrap_or_default();
    process_query("INSERT INTO du_data1 VALUES (1, 'A'), (2, 'B')").unwrap();
    process_query("INSERT INTO du_filter1 VALUES (99)").unwrap();

    let result =
        process_query("DELETE FROM du_data1 USING du_filter1 WHERE data_id = filter_id").unwrap();
    assert!(result.contains("0 row(s) deleted"));

    let remaining = process_query("SELECT COUNT(*) FROM du_data1").unwrap();
    assert!(remaining.contains("2"));
}

#[test]
fn test_delete_using_all_match() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS du_all (all_id INTEGER)").unwrap();
    process_query("CREATE TABLE IF NOT EXISTS du_all_filter (filter_id INTEGER)").unwrap();
    process_query("DELETE FROM du_all").unwrap_or_default();
    process_query("DELETE FROM du_all_filter").unwrap_or_default();
    process_query("INSERT INTO du_all VALUES (1), (2), (3)").unwrap();
    process_query("INSERT INTO du_all_filter VALUES (1), (2), (3)").unwrap();

    let result =
        process_query("DELETE FROM du_all USING du_all_filter WHERE all_id = filter_id").unwrap();
    assert!(result.contains("3 row(s) deleted"));

    let remaining = process_query("SELECT COUNT(*) FROM du_all").unwrap();
    assert!(remaining.contains("0"));
}

#[test]
fn test_delete_using_with_returning() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS du_ret (ret_id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE IF NOT EXISTS du_ret_filter (filter_id INTEGER)").unwrap();
    process_query("DELETE FROM du_ret").unwrap_or_default();
    process_query("DELETE FROM du_ret_filter").unwrap_or_default();
    process_query("INSERT INTO du_ret VALUES (1, 'Delete1'), (2, 'Keep'), (3, 'Delete3')").unwrap();
    process_query("INSERT INTO du_ret_filter VALUES (1), (3)").unwrap();

    let result = process_query(
        "DELETE FROM du_ret USING du_ret_filter WHERE ret_id = filter_id RETURNING *",
    )
    .unwrap();
    assert!(result.contains("Delete1"));
    assert!(result.contains("Delete3"));
    assert!(!result.contains("Keep"));
}
