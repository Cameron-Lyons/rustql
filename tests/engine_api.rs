use rustql::{CommandTag, Engine, EngineOptions, QueryResult, reset_database};
use std::sync::{Mutex, OnceLock};

fn test_guard() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
}

#[test]
fn execute_select_returns_typed_rows() {
    let _guard = test_guard();
    reset_database();

    let engine = Engine::open(EngineOptions::default()).unwrap();
    let mut session = engine.session();

    session
        .execute("CREATE TABLE users (id INTEGER, name TEXT)")
        .unwrap();
    session
        .execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    let results = session
        .execute("SELECT id, name FROM users ORDER BY id")
        .unwrap();

    assert_eq!(results.len(), 1);

    match &results[0] {
        QueryResult::Rows(rows) => {
            assert_eq!(rows.columns.len(), 2);
            assert_eq!(rows.columns[0].name, "id");
            assert_eq!(rows.columns[1].name, "name");
            assert_eq!(rows.rows.len(), 2);
        }
        other => panic!("expected rows result, got: {other:?}"),
    }
}

#[test]
fn execute_update_returns_command_tag_and_affected_rows() {
    let _guard = test_guard();
    reset_database();

    let engine = Engine::open(EngineOptions::default()).unwrap();
    let mut session = engine.session();

    session
        .execute("CREATE TABLE users (id INTEGER, name TEXT)")
        .unwrap();
    session
        .execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    let results = session
        .execute("UPDATE users SET name = 'Updated' WHERE id = 1")
        .unwrap();

    assert_eq!(results.len(), 1);

    match &results[0] {
        QueryResult::Command(command) => {
            assert_eq!(command.tag, CommandTag::Update);
            assert_eq!(command.affected, 1);
        }
        other => panic!("expected command result, got: {other:?}"),
    }
}
