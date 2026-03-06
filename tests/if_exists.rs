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
fn test_create_table_if_not_exists_new() {
    setup();
    process_query("DROP TABLE IF EXISTS ife_new").unwrap_or_default();
    let result = process_query("CREATE TABLE IF NOT EXISTS ife_new (id INTEGER)").unwrap();
    assert!(result.contains("created") || result.contains("Table"));
}

#[test]
fn test_create_table_if_not_exists_existing() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS ife_existing (id INTEGER)").unwrap();
    let result = process_query("CREATE TABLE IF NOT EXISTS ife_existing (id INTEGER)").unwrap();
    assert!(result.contains("already exists") || !result.contains("error"));
}

#[test]
fn test_drop_table_if_exists_existing() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS ife_drop (id INTEGER)").unwrap();
    let result = process_query("DROP TABLE IF EXISTS ife_drop").unwrap();
    assert!(result.contains("dropped") || result.contains("Table"));
}

#[test]
fn test_drop_table_if_exists_not_existing() {
    setup();
    process_query("DROP TABLE IF EXISTS ife_nonexistent_xyz").unwrap_or_default();
    let result = process_query("DROP TABLE IF EXISTS ife_nonexistent_xyz").unwrap();
    assert!(result.contains("does not exist") || !result.contains("error"));
}

#[test]
fn test_create_index_if_not_exists() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS ife_idx_tbl (id INTEGER)").unwrap();
    process_query("DROP INDEX IF EXISTS ife_idx").unwrap_or_default();
    let result = process_query("CREATE INDEX IF NOT EXISTS ife_idx ON ife_idx_tbl (id)").unwrap();
    assert!(!result.to_lowercase().contains("error"));
}

#[test]
fn test_drop_index_if_exists() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS ife_idx_tbl2 (val INTEGER)").unwrap();
    process_query("CREATE INDEX IF NOT EXISTS ife_idx2 ON ife_idx_tbl2 (val)").unwrap_or_default();
    let result = process_query("DROP INDEX IF EXISTS ife_idx2").unwrap();
    assert!(!result.to_lowercase().contains("error"));
}
