mod common;
use common::*;
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup_test<'a>() -> std::sync::MutexGuard<'a, ()> {
    let guard = TEST_MUTEX.lock().unwrap();
    reset_database();
    guard
}

#[test]
fn test_rename_table() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE old_name (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO old_name VALUES (1, 'Alice')").unwrap();

    let result = execute_sql("ALTER TABLE old_name RENAME TO new_name");
    assert!(result.is_ok());

    let select = execute_sql("SELECT * FROM new_name").unwrap();
    assert!(select.contains("Alice"));

    let old = execute_sql("SELECT * FROM old_name");
    assert!(old.is_err());
}

#[test]
fn test_add_constraint() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE items (a INTEGER, b INTEGER, val TEXT)").unwrap();
    execute_sql("INSERT INTO items VALUES (1, 1, 'first')").unwrap();
    execute_sql("INSERT INTO items VALUES (1, 2, 'second')").unwrap();

    let result = execute_sql("ALTER TABLE items ADD CONSTRAINT uq_ab UNIQUE (a, b)");
    assert!(result.is_ok());

    let dup = execute_sql("INSERT INTO items VALUES (1, 1, 'third')");
    assert!(dup.is_err());
}

#[test]
fn test_add_constraint_validation_fails() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE items (a INTEGER, b INTEGER)").unwrap();
    execute_sql("INSERT INTO items VALUES (1, 1)").unwrap();
    execute_sql("INSERT INTO items VALUES (1, 1)").unwrap();

    let result = execute_sql("ALTER TABLE items ADD CONSTRAINT uq_ab UNIQUE (a, b)");
    assert!(result.is_err());
}

#[test]
fn test_drop_constraint() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE items (a INTEGER, b INTEGER, CONSTRAINT uq_ab UNIQUE (a, b))")
        .unwrap();
    execute_sql("INSERT INTO items VALUES (1, 1)").unwrap();

    let dup_before = execute_sql("INSERT INTO items VALUES (1, 1)");
    assert!(dup_before.is_err());

    execute_sql("ALTER TABLE items DROP CONSTRAINT uq_ab").unwrap();

    let dup_after = execute_sql("INSERT INTO items VALUES (1, 1)");
    assert!(dup_after.is_ok());
}

#[test]
fn test_drop_nonexistent_constraint() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE items (a INTEGER)").unwrap();

    let result = execute_sql("ALTER TABLE items DROP CONSTRAINT no_such");
    assert!(result.is_err());
}

#[test]
fn test_add_primary_key_constraint() {
    let _guard = setup_test();
    execute_sql("CREATE TABLE orders (order_id INTEGER, product_id INTEGER)").unwrap();
    execute_sql("INSERT INTO orders VALUES (1, 100)").unwrap();
    execute_sql("INSERT INTO orders VALUES (1, 200)").unwrap();

    execute_sql("ALTER TABLE orders ADD CONSTRAINT pk_orders PRIMARY KEY (order_id, product_id)")
        .unwrap();

    let dup = execute_sql("INSERT INTO orders VALUES (1, 100)");
    assert!(dup.is_err());
}
