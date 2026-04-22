mod common;
use common::{process_query, reset_database};
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
    process_query("CREATE TABLE old_name (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO old_name VALUES (1, 'Alice')").unwrap();

    let result = process_query("ALTER TABLE old_name RENAME TO new_name");
    assert!(result.is_ok());

    let select = process_query("SELECT * FROM new_name").unwrap();
    assert!(select.contains("Alice"));

    let old = process_query("SELECT * FROM old_name");
    assert!(old.is_err());
}

#[test]
fn test_add_constraint() {
    let _guard = setup_test();
    process_query("CREATE TABLE items (a INTEGER, b INTEGER, val TEXT)").unwrap();
    process_query("INSERT INTO items VALUES (1, 1, 'first')").unwrap();
    process_query("INSERT INTO items VALUES (1, 2, 'second')").unwrap();

    let result = process_query("ALTER TABLE items ADD CONSTRAINT uq_ab UNIQUE (a, b)");
    assert!(result.is_ok());

    let dup = process_query("INSERT INTO items VALUES (1, 1, 'third')");
    assert!(dup.is_err());
}

#[test]
fn test_add_constraint_validation_fails() {
    let _guard = setup_test();
    process_query("CREATE TABLE items (a INTEGER, b INTEGER)").unwrap();
    process_query("INSERT INTO items VALUES (1, 1)").unwrap();
    process_query("INSERT INTO items VALUES (1, 1)").unwrap();

    let result = process_query("ALTER TABLE items ADD CONSTRAINT uq_ab UNIQUE (a, b)");
    assert!(result.is_err());
}

#[test]
fn test_drop_constraint() {
    let _guard = setup_test();
    process_query("CREATE TABLE items (a INTEGER, b INTEGER, CONSTRAINT uq_ab UNIQUE (a, b))")
        .unwrap();
    process_query("INSERT INTO items VALUES (1, 1)").unwrap();

    let dup_before = process_query("INSERT INTO items VALUES (1, 1)");
    assert!(dup_before.is_err());

    process_query("ALTER TABLE items DROP CONSTRAINT uq_ab").unwrap();

    let dup_after = process_query("INSERT INTO items VALUES (1, 1)");
    assert!(dup_after.is_ok());
}

#[test]
fn test_drop_nonexistent_constraint() {
    let _guard = setup_test();
    process_query("CREATE TABLE items (a INTEGER)").unwrap();

    let result = process_query("ALTER TABLE items DROP CONSTRAINT no_such");
    assert!(result.is_err());
}

#[test]
fn test_add_primary_key_constraint() {
    let _guard = setup_test();
    process_query("CREATE TABLE orders (order_id INTEGER, product_id INTEGER)").unwrap();
    process_query("INSERT INTO orders VALUES (1, 100)").unwrap();
    process_query("INSERT INTO orders VALUES (1, 200)").unwrap();

    process_query("ALTER TABLE orders ADD CONSTRAINT pk_orders PRIMARY KEY (order_id, product_id)")
        .unwrap();

    let dup = process_query("INSERT INTO orders VALUES (1, 100)");
    assert!(dup.is_err());
}
