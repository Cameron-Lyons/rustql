use rustql::process_query;
use rustql::reset_database;
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

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    process_query(
        "CREATE TABLE orders (id INTEGER, user_id INTEGER FOREIGN KEY REFERENCES users(id))",
    )
    .unwrap();

    let result = process_query("INSERT INTO orders VALUES (1, 1)");
    assert!(result.is_ok(), "Should allow valid foreign key");

    let result = process_query("INSERT INTO orders VALUES (2, 999)");
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

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    process_query(
        "CREATE TABLE orders (id INTEGER, user_id INTEGER FOREIGN KEY REFERENCES users(id) ON DELETE CASCADE)"
    ).unwrap();

    process_query("INSERT INTO orders VALUES (1, 1)").unwrap();
    process_query("INSERT INTO orders VALUES (2, 2)").unwrap();

    process_query("DELETE FROM users WHERE id = 1").unwrap();

    let result = process_query("SELECT * FROM orders");
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

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    process_query(
        "CREATE TABLE orders (id INTEGER, user_id INTEGER FOREIGN KEY REFERENCES users(id) ON DELETE RESTRICT)"
    ).unwrap();

    process_query("INSERT INTO orders VALUES (1, 1)").unwrap();

    let result = process_query("DELETE FROM users WHERE id = 1");
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

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    process_query(
        "CREATE TABLE orders (id INTEGER, user_id INTEGER FOREIGN KEY REFERENCES users(id) ON DELETE SET NULL)"
    ).unwrap();

    process_query("INSERT INTO orders VALUES (1, 1)").unwrap();
    process_query("INSERT INTO orders VALUES (2, 2)").unwrap();

    process_query("DELETE FROM users WHERE id = 1").unwrap();

    let result = process_query("SELECT * FROM orders WHERE id = 1");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("NULL"), "user_id should be set to NULL");
}

#[test]
fn test_foreign_key_update_validation() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    process_query(
        "CREATE TABLE orders (id INTEGER, user_id INTEGER FOREIGN KEY REFERENCES users(id))",
    )
    .unwrap();

    process_query("INSERT INTO orders VALUES (1, 1)").unwrap();

    let result = process_query("UPDATE orders SET user_id = 2 WHERE id = 1");
    assert!(result.is_ok(), "Should allow update to valid foreign key");

    let result = process_query("UPDATE orders SET user_id = 999 WHERE id = 1");
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

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO users VALUES (1, 'Alice')").unwrap();

    process_query(
        "CREATE TABLE orders (id INTEGER, user_id INTEGER FOREIGN KEY REFERENCES users(id))",
    )
    .unwrap();

    let result = process_query("INSERT INTO orders VALUES (1, NULL)");
    assert!(result.is_ok(), "Should allow NULL foreign key");
}

#[test]
fn test_foreign_key_on_update() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO users VALUES (1, 'Alice')").unwrap();

    process_query(
        "CREATE TABLE orders (id INTEGER, user_id INTEGER FOREIGN KEY REFERENCES users(id) ON UPDATE CASCADE)"
    ).unwrap();

    process_query("INSERT INTO orders VALUES (1, 1)").unwrap();

    let result = process_query("SELECT * FROM orders");
    assert!(result.is_ok());
}
