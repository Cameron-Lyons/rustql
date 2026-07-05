mod common;
use common::*;
use rustql::ast::Value;
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup_test<'a>() -> std::sync::MutexGuard<'a, ()> {
    let guard = TEST_MUTEX.lock().unwrap();
    reset_database();
    guard
}

fn setup_users_and_orders() {
    execute_script(
        "
        CREATE TABLE users (id INTEGER, name TEXT);
        CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER);
        INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');
        INSERT INTO orders VALUES (10, 1, 75), (11, 1, 25), (12, 2, 60), (13, 2, 90);
        CREATE INDEX orders_user_idx ON orders (user_id);
        ",
    )
    .unwrap();
}

#[test]
fn correlated_scalar_subquery_with_index() {
    let _guard = setup_test();
    setup_users_and_orders();

    assert_rows(
        "SELECT users.name,
                (SELECT MAX(amount) FROM orders WHERE orders.user_id = users.id)
         FROM users
         ORDER BY users.name",
        &["users.name", "<subquery>"],
        vec![
            vec![Value::Text("Alice".to_string()), Value::Integer(75)],
            vec![Value::Text("Bob".to_string()), Value::Integer(90)],
            vec![Value::Text("Carol".to_string()), Value::Null],
        ],
    );
}

#[test]
fn correlated_exists_with_index() {
    let _guard = setup_test();
    setup_users_and_orders();

    assert_rows(
        "SELECT users.name
         FROM users
         WHERE EXISTS (
             SELECT 1 FROM orders
             WHERE orders.user_id = users.id AND orders.amount > 70
         )
         ORDER BY users.name",
        &["users.name"],
        vec![
            vec![Value::Text("Alice".to_string())],
            vec![Value::Text("Bob".to_string())],
        ],
    );
}

#[test]
fn correlated_not_exists_with_index() {
    let _guard = setup_test();
    setup_users_and_orders();

    assert_rows(
        "SELECT users.name
         FROM users
         WHERE NOT EXISTS (
             SELECT 1 FROM orders WHERE orders.user_id = users.id
         )
         ORDER BY users.name",
        &["users.name"],
        vec![vec![Value::Text("Carol".to_string())]],
    );
}

#[test]
fn correlated_in_subquery_with_index() {
    let _guard = setup_test();
    setup_users_and_orders();

    assert_rows(
        "SELECT users.name
         FROM users
         WHERE 75 IN (SELECT amount FROM orders WHERE orders.user_id = users.id)
         ORDER BY users.name",
        &["users.name"],
        vec![vec![Value::Text("Alice".to_string())]],
    );
}

#[test]
fn correlated_scalar_subquery_inner_name_shadows_outer() {
    let _guard = setup_test();
    setup_users_and_orders();

    // Both tables have an `id` column; the unqualified reference inside the
    // subquery must keep resolving to the orders table.
    assert_rows(
        "SELECT users.name,
                (SELECT MAX(id) FROM orders WHERE orders.user_id = users.id)
         FROM users
         ORDER BY users.name",
        &["users.name", "<subquery>"],
        vec![
            vec![Value::Text("Alice".to_string()), Value::Integer(11)],
            vec![Value::Text("Bob".to_string()), Value::Integer(13)],
            vec![Value::Text("Carol".to_string()), Value::Null],
        ],
    );
}

#[test]
fn correlated_scalar_subquery_null_outer_value() {
    let _guard = setup_test();
    execute_script(
        "
        CREATE TABLE users (id INTEGER, name TEXT);
        CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER);
        INSERT INTO users VALUES (1, 'Alice'), (NULL, 'Nobody');
        INSERT INTO orders VALUES (10, 1, 75);
        ",
    )
    .unwrap();

    assert_rows(
        "SELECT users.name,
                (SELECT MAX(amount) FROM orders WHERE orders.user_id = users.id)
         FROM users
         ORDER BY users.name",
        &["users.name", "<subquery>"],
        vec![
            vec![Value::Text("Alice".to_string()), Value::Integer(75)],
            vec![Value::Text("Nobody".to_string()), Value::Null],
        ],
    );
}

#[test]
fn correlated_subquery_joining_outer_table_uses_fallback() {
    let _guard = setup_test();
    setup_users_and_orders();

    // The subquery joins the outer table itself, so `users.id` is both a
    // local and an outer name. The legacy scoping resolves the WHERE
    // reference to the outer row while the join ON stays local; that shape
    // must stay on the temp-table fallback path and keep its behavior.
    assert_rows(
        "SELECT users.name,
                (SELECT MAX(orders.amount)
                 FROM orders
                 JOIN users ON orders.user_id = users.id
                 WHERE orders.user_id = users.id)
         FROM users
         ORDER BY users.name",
        &["users.name", "<subquery>"],
        vec![
            vec![Value::Text("Alice".to_string()), Value::Integer(75)],
            vec![Value::Text("Bob".to_string()), Value::Integer(90)],
            vec![Value::Text("Carol".to_string()), Value::Null],
        ],
    );
}

#[test]
fn correlated_subquery_with_nested_subquery_uses_fallback() {
    let _guard = setup_test();
    setup_users_and_orders();

    // The nested EXISTS keeps this subquery on the temp-table fallback path;
    // results must match the literal-binding path's semantics.
    assert_rows(
        "SELECT users.name
         FROM users
         WHERE EXISTS (
             SELECT 1 FROM orders
             WHERE orders.user_id = users.id
               AND EXISTS (SELECT 1 FROM users u WHERE u.id = orders.user_id)
               AND orders.amount > 70
         )
         ORDER BY users.name",
        &["users.name"],
        vec![
            vec![Value::Text("Alice".to_string())],
            vec![Value::Text("Bob".to_string())],
        ],
    );
}
