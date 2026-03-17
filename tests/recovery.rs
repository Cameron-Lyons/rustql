use rustql::{executor, process_query, reset_database};
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup() -> std::sync::MutexGuard<'static, ()> {
    let guard = TEST_MUTEX.lock().unwrap();
    reset_database();
    guard
}

#[test]
fn test_persisted_state_after_reload() {
    let _guard = setup();

    process_query("CREATE TABLE recovery_users (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO recovery_users VALUES (1, 'Alice')").unwrap();

    executor::reset_database_state();
    executor::reload_database_from_storage_for_testing();

    let result = process_query("SELECT * FROM recovery_users").unwrap();
    assert!(result.contains("Alice"), "got: {}", result);
}

#[test]
fn test_rolled_back_changes_not_recovered_after_reload() {
    let _guard = setup();

    process_query("CREATE TABLE recovery_tx (id INTEGER, name TEXT)").unwrap();
    process_query("BEGIN TRANSACTION").unwrap();
    process_query("INSERT INTO recovery_tx VALUES (1, 'temp')").unwrap();
    process_query("ROLLBACK").unwrap();

    executor::reset_database_state();
    executor::reload_database_from_storage_for_testing();

    let result = process_query("SELECT * FROM recovery_tx").unwrap();
    assert!(!result.contains("temp"), "got: {}", result);
}

#[test]
fn test_partial_index_filter_persists_across_reload() {
    let _guard = setup();

    process_query("CREATE TABLE recovery_pidx (id INTEGER, active INTEGER, val INTEGER)").unwrap();
    process_query("INSERT INTO recovery_pidx VALUES (1, 1, 10)").unwrap();
    process_query("INSERT INTO recovery_pidx VALUES (2, 0, 10)").unwrap();
    process_query("CREATE INDEX idx_recovery_active ON recovery_pidx(val) WHERE active = 1")
        .unwrap();

    executor::reset_database_state();
    executor::reload_database_from_storage_for_testing();

    let without_filter =
        process_query("EXPLAIN SELECT id FROM recovery_pidx WHERE val = 10").unwrap();
    assert!(
        !without_filter.contains("Index Scan using idx_recovery_active"),
        "unexpected partial-index use after reload: {without_filter}"
    );

    let with_filter =
        process_query("EXPLAIN SELECT id FROM recovery_pidx WHERE val = 10 AND active = 1")
            .unwrap();
    assert!(
        with_filter.contains("Index Scan using idx_recovery_active"),
        "missing partial-index use after reload: {with_filter}"
    );
}
