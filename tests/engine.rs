use rustql::Engine;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn engine_instances_are_isolated() {
    let engine_a = Engine::new();
    let engine_b = Engine::new();

    engine_a
        .process_query("CREATE TABLE isolated (id INTEGER)")
        .unwrap();
    engine_a
        .process_query("INSERT INTO isolated VALUES (1)")
        .unwrap();

    let result = engine_b.process_query("SELECT * FROM isolated");
    assert!(result.is_err(), "{result:?}");
    assert!(
        result.unwrap_err().contains("does not exist"),
        "expected missing-table error"
    );
}

#[test]
fn transaction_state_is_per_engine() {
    let engine_a = Engine::new();
    let engine_b = Engine::new();

    engine_a.process_query("BEGIN TRANSACTION").unwrap();
    engine_b.process_query("BEGIN TRANSACTION").unwrap();

    engine_a.process_query("ROLLBACK").unwrap();
    engine_b.process_query("ROLLBACK").unwrap();
}

#[test]
fn scalar_subqueries_use_current_engine_context() {
    let engine = Engine::new();

    engine
        .process_query("CREATE TABLE sub_ctx (id INTEGER, value INTEGER)")
        .unwrap();
    engine
        .process_query("INSERT INTO sub_ctx VALUES (1, 10), (2, 20)")
        .unwrap();

    let result = engine
        .process_query(
            "SELECT CASE WHEN id = 1 THEN (SELECT MAX(value) FROM sub_ctx) ELSE 0 END AS max_value FROM sub_ctx WHERE id = 1",
        )
        .unwrap();

    assert!(result.contains("20"), "got: {result}");
}

#[test]
fn json_engine_uses_custom_path() {
    let path = unique_temp_path("json");
    fs::remove_file(&path).ok();

    let engine = Engine::builder().json_file(&path).build();
    engine
        .process_query("CREATE TABLE persisted_json (id INTEGER)")
        .unwrap();
    engine
        .process_query("INSERT INTO persisted_json VALUES (7)")
        .unwrap();

    let reloaded = Engine::builder().json_file(&path).build();
    let result = reloaded
        .process_query("SELECT * FROM persisted_json")
        .unwrap();

    assert!(result.contains("7"), "got: {result}");
    fs::remove_file(path).ok();
}

#[test]
fn btree_engine_uses_custom_path() {
    let path = unique_temp_path("dat");
    fs::remove_file(&path).ok();

    let engine = Engine::builder().btree_file(&path).build();
    engine
        .process_query("CREATE TABLE persisted_btree (id INTEGER)")
        .unwrap();
    engine
        .process_query("INSERT INTO persisted_btree VALUES (11)")
        .unwrap();

    let reloaded = Engine::builder().btree_file(&path).build();
    let result = reloaded
        .process_query("SELECT * FROM persisted_btree")
        .unwrap();

    assert!(result.contains("11"), "got: {result}");
    fs::remove_file(path).ok();
}

fn unique_temp_path(extension: &str) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "rustql-engine-{}-{}.{}",
        std::process::id(),
        timestamp,
        extension
    ))
}
