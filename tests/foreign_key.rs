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
fn test_foreign_key_basic() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    execute_sql(
        "CREATE TABLE orders (id INTEGER, user_id INTEGER FOREIGN KEY REFERENCES users(id))",
    )
    .unwrap();

    let result = execute_sql("INSERT INTO orders VALUES (1, 1)");
    assert!(result.is_ok(), "Should allow valid foreign key");

    let result = execute_sql("INSERT INTO orders VALUES (2, 999)");
    assert!(result.is_err(), "Should reject invalid foreign key");
    assert!(
        result
            .unwrap_err()
            .contains("Foreign key constraint violation")
    );
}

#[test]
fn test_foreign_key_with_on_delete_cascade() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    execute_sql(
        "CREATE TABLE orders (id INTEGER, user_id INTEGER FOREIGN KEY REFERENCES users(id) ON DELETE CASCADE)"
    ).unwrap();

    execute_sql("INSERT INTO orders VALUES (1, 1)").unwrap();
    execute_sql("INSERT INTO orders VALUES (2, 2)").unwrap();

    execute_sql("DELETE FROM users WHERE id = 1").unwrap();

    let result = execute_sql("SELECT * FROM orders");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(
        !output.contains("1"),
        "Order with user_id=1 should be deleted"
    );
    assert!(
        output.contains("2"),
        "Order with user_id=2 should still exist"
    );
}

#[test]
fn test_foreign_key_with_on_delete_restrict() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    execute_sql(
        "CREATE TABLE orders (id INTEGER, user_id INTEGER FOREIGN KEY REFERENCES users(id) ON DELETE RESTRICT)"
    ).unwrap();

    execute_sql("INSERT INTO orders VALUES (1, 1)").unwrap();

    let result = execute_sql("DELETE FROM users WHERE id = 1");
    assert!(result.is_err(), "Should reject delete due to RESTRICT");
    assert!(
        result
            .unwrap_err()
            .contains("Foreign key constraint violation")
    );
}

#[test]
fn test_foreign_key_with_on_delete_set_null() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    execute_sql(
        "CREATE TABLE orders (id INTEGER, user_id INTEGER FOREIGN KEY REFERENCES users(id) ON DELETE SET NULL)"
    ).unwrap();

    execute_sql("INSERT INTO orders VALUES (1, 1)").unwrap();
    execute_sql("INSERT INTO orders VALUES (2, 2)").unwrap();

    execute_sql("DELETE FROM users WHERE id = 1").unwrap();

    let result = execute_sql("SELECT * FROM orders WHERE id = 1");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("NULL"), "user_id should be set to NULL");
}

#[test]
fn test_foreign_key_update_validation() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    execute_sql(
        "CREATE TABLE orders (id INTEGER, user_id INTEGER FOREIGN KEY REFERENCES users(id))",
    )
    .unwrap();

    execute_sql("INSERT INTO orders VALUES (1, 1)").unwrap();

    let result = execute_sql("UPDATE orders SET user_id = 2 WHERE id = 1");
    assert!(result.is_ok(), "Should allow update to valid foreign key");

    let result = execute_sql("UPDATE orders SET user_id = 999 WHERE id = 1");
    assert!(
        result.is_err(),
        "Should reject update to invalid foreign key"
    );
    assert!(
        result
            .unwrap_err()
            .contains("Foreign key constraint violation")
    );
}

#[test]
fn test_foreign_key_null_allowed() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO users VALUES (1, 'Alice')").unwrap();

    execute_sql(
        "CREATE TABLE orders (id INTEGER, user_id INTEGER FOREIGN KEY REFERENCES users(id))",
    )
    .unwrap();

    let result = execute_sql("INSERT INTO orders VALUES (1, NULL)");
    assert!(result.is_ok(), "Should allow NULL foreign key");
}

#[test]
fn test_foreign_key_on_update() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO users VALUES (1, 'Alice')").unwrap();

    execute_sql(
        "CREATE TABLE orders (id INTEGER, user_id INTEGER FOREIGN KEY REFERENCES users(id) ON UPDATE CASCADE)"
    ).unwrap();

    execute_sql("INSERT INTO orders VALUES (1, 1)").unwrap();

    let result = execute_sql("SELECT * FROM orders");
    assert!(result.is_ok());
}
