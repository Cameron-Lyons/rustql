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
fn test_create_table_if_not_exists_new() {
    setup();
    let _ = execute_sql("DROP TABLE IF EXISTS ife_new");
    let result = execute_sql("CREATE TABLE IF NOT EXISTS ife_new (id INTEGER)").unwrap();
    assert_command(result, CommandTag::CreateTable, 0);
}

#[test]
fn test_create_table_if_not_exists_existing() {
    setup();
    execute_sql("CREATE TABLE IF NOT EXISTS ife_existing (id INTEGER)").unwrap();
    let result = execute_sql("CREATE TABLE IF NOT EXISTS ife_existing (id INTEGER)").unwrap();
    assert!(result.contains("already exists") || !result.contains("error"));
}

#[test]
fn test_drop_table_if_exists_existing() {
    setup();
    execute_sql("CREATE TABLE IF NOT EXISTS ife_drop (id INTEGER)").unwrap();
    let result = execute_sql("DROP TABLE IF EXISTS ife_drop").unwrap();
    assert_command(result, CommandTag::DropTable, 0);
}

#[test]
fn test_drop_table_if_exists_not_existing() {
    setup();
    let _ = execute_sql("DROP TABLE IF EXISTS ife_nonexistent_xyz");
    let result = execute_sql("DROP TABLE IF EXISTS ife_nonexistent_xyz").unwrap();
    assert!(result.contains("does not exist") || !result.contains("error"));
}

#[test]
fn test_create_index_if_not_exists() {
    setup();
    execute_sql("CREATE TABLE IF NOT EXISTS ife_idx_tbl (id INTEGER)").unwrap();
    let _ = execute_sql("DROP INDEX IF EXISTS ife_idx");
    let result = execute_sql("CREATE INDEX IF NOT EXISTS ife_idx ON ife_idx_tbl (id)").unwrap();
    assert!(!result.to_lowercase().contains("error"));
}

#[test]
fn test_drop_index_if_exists() {
    setup();
    execute_sql("CREATE TABLE IF NOT EXISTS ife_idx_tbl2 (val INTEGER)").unwrap();
    let _ = execute_sql("CREATE INDEX IF NOT EXISTS ife_idx2 ON ife_idx_tbl2 (val)");
    let result = execute_sql("DROP INDEX IF EXISTS ife_idx2").unwrap();
    assert!(!result.to_lowercase().contains("error"));
}
