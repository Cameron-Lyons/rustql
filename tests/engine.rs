mod common;
use common::*;
use rustql::ast::Value;
use rustql::{Engine, EngineOptions, QueryResult, StorageMode};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn engine_instances_are_isolated() {
    let engine_a = open_memory_engine();
    let engine_b = open_memory_engine();
    let mut session_a = engine_a.session();
    let mut session_b = engine_b.session();

    session_a
        .execute_one("CREATE TABLE isolated (id INTEGER)")
        .unwrap();
    session_a
        .execute_one("INSERT INTO isolated VALUES (1)")
        .unwrap();

    let result = session_b.execute_one("SELECT * FROM isolated");
    assert!(result.is_err(), "{result:?}");
    assert!(
        result.unwrap_err().to_string().contains("does not exist"),
        "expected missing-table error"
    );
}

#[test]
fn transaction_state_is_per_engine() {
    let engine_a = open_memory_engine();
    let engine_b = open_memory_engine();
    let mut session_a = engine_a.session();
    let mut session_b = engine_b.session();

    session_a.execute_one("BEGIN TRANSACTION").unwrap();
    session_b.execute_one("BEGIN TRANSACTION").unwrap();

    session_a.execute_one("ROLLBACK").unwrap();
    session_b.execute_one("ROLLBACK").unwrap();
}

#[test]
fn transaction_state_is_shared_by_sessions_from_one_engine() {
    let engine = open_memory_engine();
    let mut session_a = engine.session();
    let mut session_b = engine.session();

    session_a
        .execute_one("CREATE TABLE shared_tx (id INTEGER)")
        .unwrap();
    session_a.execute_one("BEGIN TRANSACTION").unwrap();
    session_a
        .execute_one("INSERT INTO shared_tx VALUES (1)")
        .unwrap();

    let begin_result = session_b.execute_one("BEGIN TRANSACTION");
    assert!(begin_result.is_err(), "{begin_result:?}");
    assert!(
        begin_result
            .unwrap_err()
            .to_string()
            .contains("Transaction already in progress"),
        "expected shared transaction error"
    );

    session_b.execute_one("ROLLBACK").unwrap();

    let result = session_a.execute_one("SELECT * FROM shared_tx").unwrap();
    match result {
        QueryResult::Rows(rows) => assert!(rows.rows.is_empty(), "got: {rows:?}"),
        other => panic!("expected row result, got: {other:?}"),
    }
}

#[test]
fn scalar_subqueries_use_current_engine_context() {
    let engine = open_memory_engine();
    let mut session = engine.session();

    session
        .execute_one("CREATE TABLE sub_ctx (id INTEGER, value INTEGER)")
        .unwrap();
    session
        .execute_one("INSERT INTO sub_ctx VALUES (1, 10), (2, 20)")
        .unwrap();

    let result = session
        .execute_one(
            "SELECT CASE WHEN id = 1 THEN (SELECT MAX(value) FROM sub_ctx) ELSE 0 END AS max_value FROM sub_ctx WHERE id = 1",
        )
        .unwrap();

    match result {
        QueryResult::Rows(rows) => {
            assert_eq!(rows.rows, vec![vec![Value::Integer(20)]]);
        }
        other => panic!("expected row result, got: {other:?}"),
    }
}

#[test]
fn disk_engine_uses_custom_path() {
    let path = unique_temp_path("db");
    cleanup_storage_files(&path);

    let engine = open_disk_engine(&path);
    {
        let mut session = engine.session();
        session
            .execute_one("CREATE TABLE persisted_disk (id INTEGER)")
            .unwrap();
        session
            .execute_one("INSERT INTO persisted_disk VALUES (11)")
            .unwrap();
    }
    drop(engine);

    let reloaded = open_disk_engine(&path);
    let mut reloaded_session = reloaded.session();
    let result = reloaded_session
        .execute_one("SELECT * FROM persisted_disk")
        .unwrap();

    assert!(result.contains("11"), "got: {result:?}");
    cleanup_storage_files(&path);
}

fn open_memory_engine() -> Engine {
    Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap()
}

fn open_disk_engine(path: &Path) -> Engine {
    Engine::open(EngineOptions {
        storage: StorageMode::BTree {
            path: path.to_path_buf(),
        },
    })
    .unwrap()
}

fn cleanup_storage_files(path: &Path) {
    fs::remove_file(path).ok();

    let mut wal = path.as_os_str().to_os_string();
    wal.push(".wal");
    fs::remove_file(PathBuf::from(wal)).ok();
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
