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
fn lateral_top_order_with_index() {
    let _guard = setup_test();
    setup_users_and_orders();

    assert_rows(
        "SELECT users.name, recent.amount
         FROM users
         LEFT JOIN LATERAL (
             SELECT amount
             FROM orders
             WHERE orders.user_id = users.id
             ORDER BY amount DESC
             FETCH FIRST 1 ROW ONLY
         ) AS recent
         ORDER BY users.name",
        &["users.name", "recent.amount"],
        vec![
            vec![Value::Text("Alice".to_string()), Value::Integer(75)],
            vec![Value::Text("Bob".to_string()), Value::Integer(90)],
            vec![Value::Text("Carol".to_string()), Value::Null],
        ],
    );
}

#[test]
fn inner_join_lateral_drops_unmatched_outer_rows() {
    let _guard = setup_test();
    setup_users_and_orders();

    assert_rows(
        "SELECT users.name, recent.amount
         FROM users
         JOIN LATERAL (
             SELECT amount
             FROM orders
             WHERE orders.user_id = users.id
             ORDER BY amount DESC
             FETCH FIRST 1 ROW ONLY
         ) AS recent
         ORDER BY users.name",
        &["users.name", "recent.amount"],
        vec![
            vec![Value::Text("Alice".to_string()), Value::Integer(75)],
            vec![Value::Text("Bob".to_string()), Value::Integer(90)],
        ],
    );
}

#[test]
fn lateral_outer_reference_in_select_list() {
    let _guard = setup_test();
    setup_users_and_orders();

    assert_rows(
        "SELECT recent.owner, recent.amount
         FROM users
         JOIN LATERAL (
             SELECT users.name AS owner, amount
             FROM orders
             WHERE orders.user_id = users.id
             ORDER BY amount ASC
             FETCH FIRST 1 ROW ONLY
         ) AS recent
         ORDER BY recent.owner",
        &["recent.owner", "recent.amount"],
        vec![
            vec![Value::Text("Alice".to_string()), Value::Integer(25)],
            vec![Value::Text("Bob".to_string()), Value::Integer(60)],
        ],
    );
}

#[test]
fn lateral_outer_reference_in_projection_expression() {
    let _guard = setup_test();
    setup_users_and_orders();

    assert_rows(
        "SELECT users.id, boosted.total
         FROM users
         JOIN LATERAL (
             SELECT amount + users.id AS total
             FROM orders
             WHERE orders.user_id = users.id
             ORDER BY amount DESC
             FETCH FIRST 1 ROW ONLY
         ) AS boosted
         ORDER BY users.id",
        &["users.id", "boosted.total"],
        vec![
            vec![Value::Integer(1), Value::Integer(76)],
            vec![Value::Integer(2), Value::Integer(92)],
        ],
    );
}

#[test]
fn lateral_inner_column_shadows_outer_column() {
    let _guard = setup_test();
    setup_users_and_orders();

    // Both tables have an `id` column; the unqualified reference inside the
    // subquery must keep resolving to the orders table.
    assert_rows(
        "SELECT users.name, picked.id
         FROM users
         JOIN LATERAL (
             SELECT id
             FROM orders
             WHERE orders.user_id = users.id
             ORDER BY id DESC
             FETCH FIRST 1 ROW ONLY
         ) AS picked
         ORDER BY users.name",
        &["users.name", "picked.id"],
        vec![
            vec![Value::Text("Alice".to_string()), Value::Integer(11)],
            vec![Value::Text("Bob".to_string()), Value::Integer(13)],
        ],
    );
}

#[test]
fn lateral_correlated_aggregate() {
    let _guard = setup_test();
    setup_users_and_orders();

    assert_rows(
        "SELECT users.name, totals.total
         FROM users
         LEFT JOIN LATERAL (
             SELECT SUM(amount) AS total
             FROM orders
             WHERE orders.user_id = users.id
         ) AS totals
         ORDER BY users.name",
        &["users.name", "totals.total"],
        vec![
            vec![Value::Text("Alice".to_string()), Value::Float(100.0)],
            vec![Value::Text("Bob".to_string()), Value::Float(150.0)],
            vec![Value::Text("Carol".to_string()), Value::Null],
        ],
    );
}

#[test]
fn uncorrelated_lateral_repeats_for_every_outer_row() {
    let _guard = setup_test();
    setup_users_and_orders();

    assert_rows(
        "SELECT users.name, best.amount
         FROM users
         LEFT JOIN LATERAL (
             SELECT MAX(amount) AS amount
             FROM orders
         ) AS best
         ORDER BY users.name",
        &["users.name", "best.amount"],
        vec![
            vec![Value::Text("Alice".to_string()), Value::Integer(90)],
            vec![Value::Text("Bob".to_string()), Value::Integer(90)],
            vec![Value::Text("Carol".to_string()), Value::Integer(90)],
        ],
    );
}

#[test]
fn lateral_with_nested_subquery_uses_outer_scope_fallback() {
    let _guard = setup_test();
    setup_users_and_orders();

    // The nested EXISTS keeps this subquery on the temp-table fallback path;
    // results must match the literal-binding path's semantics.
    assert_rows(
        "SELECT users.name, recent.amount
         FROM users
         LEFT JOIN LATERAL (
             SELECT amount
             FROM orders
             WHERE orders.user_id = users.id
               AND EXISTS (SELECT 1 FROM users u WHERE u.id = orders.user_id)
             ORDER BY amount DESC
             FETCH FIRST 1 ROW ONLY
         ) AS recent
         ORDER BY users.name",
        &["users.name", "recent.amount"],
        vec![
            vec![Value::Text("Alice".to_string()), Value::Integer(75)],
            vec![Value::Text("Bob".to_string()), Value::Integer(90)],
            vec![Value::Text("Carol".to_string()), Value::Null],
        ],
    );
}

#[test]
fn lateral_null_outer_value_matches_nothing() {
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
        "SELECT users.name, recent.amount
         FROM users
         LEFT JOIN LATERAL (
             SELECT amount
             FROM orders
             WHERE orders.user_id = users.id
         ) AS recent
         ORDER BY users.name",
        &["users.name", "recent.amount"],
        vec![
            vec![Value::Text("Alice".to_string()), Value::Integer(75)],
            vec![Value::Text("Nobody".to_string()), Value::Null],
        ],
    );
}
