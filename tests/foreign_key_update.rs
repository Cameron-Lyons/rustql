mod common;
use common::*;
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup_test() -> std::sync::MutexGuard<'static, ()> {
    let guard = TEST_MUTEX.lock().unwrap();
    reset_database();
    guard
}

#[test]

fn test_on_update_cascade() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    execute_sql(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON UPDATE CASCADE)",
    )
    .unwrap();

    execute_sql("INSERT INTO parent VALUES (1, 'Alice')").unwrap();
    execute_sql("INSERT INTO child VALUES (10, 1)").unwrap();

    execute_sql("UPDATE parent SET id = 2 WHERE id = 1").unwrap();

    let result = execute_sql("SELECT parent_id FROM child").unwrap();
    assert!(result.contains("2"));
    assert!(!result.contains("\t1\t") && !result.contains("\n1\n"));
}

#[test]

fn test_on_update_set_null() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    execute_sql(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON UPDATE SET NULL)",
    )
    .unwrap();

    execute_sql("INSERT INTO parent VALUES (1, 'Alice')").unwrap();
    execute_sql("INSERT INTO child VALUES (10, 1)").unwrap();

    execute_sql("UPDATE parent SET id = 2 WHERE id = 1").unwrap();

    let result = execute_sql("SELECT parent_id FROM child").unwrap();
    assert!(result.contains("NULL"));
}

#[test]

fn test_on_update_restrict() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    execute_sql(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON UPDATE RESTRICT)",
    )
    .unwrap();

    execute_sql("INSERT INTO parent VALUES (1, 'Alice')").unwrap();
    execute_sql("INSERT INTO child VALUES (10, 1)").unwrap();

    let result = execute_sql("UPDATE parent SET id = 2 WHERE id = 1");
    assert!(result.is_err());
}
