mod common;
use common::*;
use rustql::{Engine, EngineOptions, StorageMode};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn test_guard() -> std::sync::MutexGuard<'static, ()> {
    TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner())
}

#[test]
fn test_persisted_state_after_reload() {
    let _guard = test_guard();
    let path = unique_temp_path("db");
    cleanup_storage_files(&path);

    let engine = open_disk_engine(&path);
    {
        let mut session = engine.session();
        session
            .execute_one("CREATE TABLE recovery_users (id INTEGER, name TEXT)")
            .unwrap();
        session
            .execute_one("INSERT INTO recovery_users VALUES (1, 'Alice')")
            .unwrap();
    }
    drop(engine);

    let reloaded = open_disk_engine(&path);
    let mut reloaded_session = reloaded.session();
    let result = reloaded_session
        .execute_one("SELECT * FROM recovery_users")
        .unwrap();

    assert!(result.contains("Alice"), "got: {result:?}");
    cleanup_storage_files(&path);
}

#[test]
fn test_rolled_back_changes_not_recovered_after_reload() {
    let _guard = test_guard();
    let path = unique_temp_path("db");
    cleanup_storage_files(&path);

    let engine = open_disk_engine(&path);
    {
        let mut session = engine.session();
        session
            .execute_one("CREATE TABLE recovery_tx (id INTEGER, name TEXT)")
            .unwrap();
        session.execute_one("BEGIN TRANSACTION").unwrap();
        session
            .execute_one("INSERT INTO recovery_tx VALUES (1, 'temp')")
            .unwrap();
        session.execute_one("ROLLBACK").unwrap();
    }
    drop(engine);

    let reloaded = open_disk_engine(&path);
    let mut reloaded_session = reloaded.session();
    let result = reloaded_session
        .execute_one("SELECT * FROM recovery_tx")
        .unwrap();

    assert!(!result.contains("temp"), "got: {result:?}");
    cleanup_storage_files(&path);
}

#[test]
fn test_partial_index_filter_persists_across_reload() {
    let _guard = test_guard();
    let path = unique_temp_path("db");
    cleanup_storage_files(&path);

    let engine = open_disk_engine(&path);
    {
        let mut session = engine.session();
        session
            .execute_one("CREATE TABLE recovery_pidx (id INTEGER, active INTEGER, val INTEGER)")
            .unwrap();
        session
            .execute_one("INSERT INTO recovery_pidx VALUES (1, 1, 10)")
            .unwrap();
        session
            .execute_one("INSERT INTO recovery_pidx VALUES (2, 0, 10)")
            .unwrap();
        session
            .execute_one("CREATE INDEX idx_recovery_active ON recovery_pidx(val) WHERE active = 1")
            .unwrap();
    }
    drop(engine);

    let reloaded = open_disk_engine(&path);
    let mut reloaded_session = reloaded.session();
    let without_filter = query_output_lines(
        &reloaded_session
            .execute_one("EXPLAIN SELECT id FROM recovery_pidx WHERE val = 10")
            .unwrap(),
    )
    .join("\n");
    assert!(
        !without_filter.contains("Index Scan using idx_recovery_active"),
        "unexpected partial-index use after reload: {without_filter}"
    );

    let with_filter = query_output_lines(
        &reloaded_session
            .execute_one("EXPLAIN SELECT id FROM recovery_pidx WHERE val = 10 AND active = 1")
            .unwrap(),
    )
    .join("\n");
    assert!(
        with_filter.contains("Index Scan using idx_recovery_active"),
        "missing partial-index use after reload: {with_filter}"
    );
    cleanup_storage_files(&path);
}

#[test]
fn json_engine_rejects_corrupt_storage_file() {
    let _guard = test_guard();
    let path = unique_temp_path("json");
    cleanup_storage_files(&path);
    fs::write(&path, "{ this is not valid json").unwrap();

    let result = Engine::open(EngineOptions {
        storage: StorageMode::Json { path: path.clone() },
    });
    let Err(error) = result else {
        panic!("expected corrupt JSON storage to fail");
    };
    assert!(
        error
            .to_string()
            .contains("Failed to parse JSON storage file"),
        "unexpected error: {error}"
    );

    cleanup_storage_files(&path);
}

#[test]
fn json_engine_rejects_empty_storage_file() {
    let _guard = test_guard();
    let path = unique_temp_path("json");
    cleanup_storage_files(&path);
    fs::write(&path, "").unwrap();

    let result = Engine::open(EngineOptions {
        storage: StorageMode::Json { path: path.clone() },
    });
    let Err(error) = result else {
        panic!("expected empty JSON storage to fail");
    };
    assert!(
        error.to_string().contains("JSON storage file") && error.to_string().contains("is empty"),
        "unexpected error: {error}"
    );

    cleanup_storage_files(&path);
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
        "rustql-recovery-{}-{}.{}",
        std::process::id(),
        timestamp,
        extension
    ))
}
