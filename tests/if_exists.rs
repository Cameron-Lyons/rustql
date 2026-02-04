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
fn test_create_table_if_not_exists_new_table() {
    setup();
    let result =
        process_query("CREATE TABLE IF NOT EXISTS ifne_new_t1 (id INTEGER, name TEXT)").unwrap();
    assert_eq!(result, "Table 'ifne_new_t1' created");
}

#[test]
fn test_create_table_if_not_exists_existing_table() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS ifne_exist_t2 (id INTEGER)").unwrap();
    let result =
        process_query("CREATE TABLE IF NOT EXISTS ifne_exist_t2 (id INTEGER, name TEXT)").unwrap();
    assert_eq!(result, "Table 'ifne_exist_t2' already exists, skipping");
}

#[test]
fn test_drop_table_if_exists_existing() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS ifne_drop_t3 (id INTEGER)").unwrap();
    let result = process_query("DROP TABLE IF EXISTS ifne_drop_t3").unwrap();
    assert!(
        result == "Table 'ifne_drop_t3' dropped"
            || result == "Table 'ifne_drop_t3' does not exist, skipping"
    );
}

#[test]
fn test_drop_table_if_exists_not_existing() {
    setup();
    let result = process_query("DROP TABLE IF EXISTS ifne_never_existed_t4").unwrap();
    assert_eq!(
        result,
        "Table 'ifne_never_existed_t4' does not exist, skipping"
    );
}

#[test]
fn test_create_index_if_not_exists_new() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS ifne_idx_t5 (id INTEGER, name TEXT)").unwrap();
    let result =
        process_query("CREATE INDEX IF NOT EXISTS ifne_idx_i5 ON ifne_idx_t5 (name)").unwrap();
    assert!(
        result.contains("Index 'ifne_idx_i5' created")
            || result.contains("Index 'ifne_idx_i5' already exists")
    );
}

#[test]
fn test_create_index_if_not_exists_existing() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS ifne_idx_t6 (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE INDEX IF NOT EXISTS ifne_idx_i6 ON ifne_idx_t6 (name)").unwrap();
    let result =
        process_query("CREATE INDEX IF NOT EXISTS ifne_idx_i6 ON ifne_idx_t6 (name)").unwrap();
    assert_eq!(result, "Index 'ifne_idx_i6' already exists, skipping");
}

#[test]
fn test_drop_index_if_exists_existing() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS ifne_idx_t7 (id INTEGER)").unwrap();
    process_query("CREATE INDEX IF NOT EXISTS ifne_idx_i7 ON ifne_idx_t7 (id)").unwrap();
    let result = process_query("DROP INDEX IF EXISTS ifne_idx_i7").unwrap();
    assert!(
        result == "Index 'ifne_idx_i7' dropped"
            || result == "Index 'ifne_idx_i7' does not exist, skipping"
    );
}

#[test]
fn test_drop_index_if_exists_not_existing() {
    setup();
    let result = process_query("DROP INDEX IF EXISTS ifne_never_existed_i8").unwrap();
    assert_eq!(
        result,
        "Index 'ifne_never_existed_i8' does not exist, skipping"
    );
}
