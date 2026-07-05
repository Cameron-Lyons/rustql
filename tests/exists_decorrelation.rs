mod common;
use common::*;
use rustql::ast::Value;
use std::sync::Mutex;

static GLOBAL_TEST_LOCK: Mutex<()> = Mutex::new(());

fn setup_test() -> std::sync::MutexGuard<'static, ()> {
    let guard = GLOBAL_TEST_LOCK.lock().unwrap();
    reset_database();
    guard
}

#[test]
fn test_correlated_exists_with_local_predicate() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    execute_sql("CREATE TABLE orders (user_id INTEGER, amount INTEGER)").unwrap();

    execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Cara')").unwrap();
    execute_sql("INSERT INTO orders VALUES (1, 50), (1, 900), (2, 100), (3, 950)").unwrap();

    let rows = query_rows(
        "SELECT users.name FROM users \
         WHERE EXISTS (SELECT 1 FROM orders \
                       WHERE orders.user_id = users.id AND orders.amount > 500) \
         ORDER BY users.name",
    )
    .unwrap();

    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Text("Alice".into())],
            vec![Value::Text("Cara".into())],
        ]
    );
}

#[test]
fn test_not_exists_with_null_keys() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    execute_sql("CREATE TABLE orders (user_id INTEGER, amount INTEGER)").unwrap();

    execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (NULL, 'Ghost')").unwrap();
    execute_sql("INSERT INTO orders VALUES (1, 50), (NULL, 900)").unwrap();

    // NULL keys on either side never satisfy the correlation equality, so
    // Bob (no orders) and Ghost (NULL id) both pass NOT EXISTS.
    let rows = query_rows(
        "SELECT users.name FROM users \
         WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) \
         ORDER BY users.name",
    )
    .unwrap();

    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Text("Bob".into())],
            vec![Value::Text("Ghost".into())],
        ]
    );
}

#[test]
fn test_uncorrelated_exists_folds_to_constant() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    execute_sql("CREATE TABLE flags (enabled INTEGER)").unwrap();

    execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    let rows = query_rows(
        "SELECT users.name FROM users \
         WHERE EXISTS (SELECT 1 FROM flags) \
         ORDER BY users.name",
    )
    .unwrap();
    assert!(rows.rows.is_empty());

    execute_sql("INSERT INTO flags VALUES (1)").unwrap();

    let rows = query_rows(
        "SELECT users.name FROM users \
         WHERE EXISTS (SELECT 1 FROM flags) \
         ORDER BY users.name",
    )
    .unwrap();
    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Text("Alice".into())],
            vec![Value::Text("Bob".into())],
        ]
    );
}

#[test]
fn test_correlated_exists_with_order_by_and_limit() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    execute_sql("CREATE TABLE orders (user_id INTEGER, amount INTEGER)").unwrap();

    execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Cara')").unwrap();
    execute_sql("INSERT INTO orders VALUES (1, 50), (3, 70)").unwrap();

    // ORDER BY and LIMIT cannot change EXISTS emptiness.
    let rows = query_rows(
        "SELECT users.name FROM users \
         WHERE EXISTS (SELECT amount FROM orders \
                       WHERE orders.user_id = users.id \
                       ORDER BY amount DESC LIMIT 1) \
         ORDER BY users.name",
    )
    .unwrap();

    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Text("Alice".into())],
            vec![Value::Text("Cara".into())],
        ]
    );
}

#[test]
fn test_correlated_exists_float_and_integer_keys_match() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE users (id FLOAT, name TEXT)").unwrap();
    execute_sql("CREATE TABLE orders (user_id INTEGER)").unwrap();

    execute_sql("INSERT INTO users VALUES (1.0, 'Alice'), (2.0, 'Bob'), (3.0, 'Cara')").unwrap();
    execute_sql("INSERT INTO orders VALUES (1), (3)").unwrap();

    // The evaluator compares all numerics through f64, so float outer keys
    // match integer inner keys.
    let rows = query_rows(
        "SELECT users.name FROM users \
         WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) \
         ORDER BY users.name",
    )
    .unwrap();

    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Text("Alice".into())],
            vec![Value::Text("Cara".into())],
        ]
    );
}

#[test]
fn test_correlated_exists_text_keys() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE users (email TEXT, name TEXT)").unwrap();
    execute_sql("CREATE TABLE signups (email TEXT)").unwrap();

    execute_sql(
        "INSERT INTO users VALUES ('a@x.com', 'Alice'), ('b@x.com', 'Bob'), ('c@x.com', 'Cara')",
    )
    .unwrap();
    execute_sql("INSERT INTO signups VALUES ('a@x.com'), ('c@x.com')").unwrap();

    let rows = query_rows(
        "SELECT users.name FROM users \
         WHERE EXISTS (SELECT 1 FROM signups WHERE signups.email = users.email) \
         ORDER BY users.name",
    )
    .unwrap();

    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Text("Alice".into())],
            vec![Value::Text("Cara".into())],
        ]
    );
}

#[test]
fn test_correlated_exists_single_outer_row() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    execute_sql("CREATE TABLE orders (user_id INTEGER)").unwrap();

    execute_sql("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    execute_sql("INSERT INTO orders VALUES (1), (2)").unwrap();

    // A single outer row stays on the per-row path; results must agree.
    let rows = query_rows(
        "SELECT users.name FROM users \
         WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)",
    )
    .unwrap();

    assert_eq!(rows.rows, vec![vec![Value::Text("Alice".into())]]);
}

#[test]
fn test_exists_alongside_other_predicates() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE users (id INTEGER, name TEXT, active INTEGER)").unwrap();
    execute_sql("CREATE TABLE orders (user_id INTEGER)").unwrap();

    execute_sql(
        "INSERT INTO users VALUES (1, 'Alice', 1), (2, 'Bob', 0), (3, 'Cara', 1), (4, 'Dan', 1)",
    )
    .unwrap();
    execute_sql("INSERT INTO orders VALUES (1), (2), (4)").unwrap();

    let rows = query_rows(
        "SELECT users.name FROM users \
         WHERE users.active = 1 \
           AND EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) \
           AND users.id < 4 \
         ORDER BY users.name",
    )
    .unwrap();

    assert_eq!(rows.rows, vec![vec![Value::Text("Alice".into())]]);
}

#[test]
fn test_scalar_max_subquery_missing_key_is_null() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    execute_sql("CREATE TABLE orders (user_id INTEGER, amount INTEGER)").unwrap();

    execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Cara')").unwrap();
    execute_sql("INSERT INTO orders VALUES (1, 50), (1, 900), (3, 70)").unwrap();

    let rows = query_rows(
        "SELECT users.name, \
                (SELECT MAX(amount) FROM orders WHERE orders.user_id = users.id) \
         FROM users ORDER BY users.name",
    )
    .unwrap();

    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Text("Alice".into()), Value::Integer(900)],
            vec![Value::Text("Bob".into()), Value::Null],
            vec![Value::Text("Cara".into()), Value::Integer(70)],
        ]
    );
}

#[test]
fn test_scalar_count_subquery_missing_key_is_zero() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    execute_sql("CREATE TABLE orders (user_id INTEGER, amount INTEGER)").unwrap();

    execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Cara')").unwrap();
    execute_sql("INSERT INTO orders VALUES (1, 50), (1, 900), (3, 70)").unwrap();

    // COUNT over an empty group is 0, not NULL.
    let rows = query_rows(
        "SELECT users.name, \
                (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) \
         FROM users ORDER BY users.name",
    )
    .unwrap();

    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Text("Alice".into()), Value::Integer(2)],
            vec![Value::Text("Bob".into()), Value::Integer(0)],
            vec![Value::Text("Cara".into()), Value::Integer(1)],
        ]
    );
}

#[test]
fn test_scalar_sum_subquery_with_local_predicate() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    execute_sql("CREATE TABLE orders (user_id INTEGER, amount INTEGER)").unwrap();

    execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Cara')").unwrap();
    execute_sql("INSERT INTO orders VALUES (1, 50), (1, 900), (2, 40), (3, 70), (3, 500)").unwrap();

    let rows = query_rows(
        "SELECT users.name, \
                (SELECT SUM(amount) FROM orders \
                 WHERE orders.user_id = users.id AND orders.amount > 60) \
         FROM users ORDER BY users.name",
    )
    .unwrap();

    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Text("Alice".into()), Value::Float(900.0)],
            vec![Value::Text("Bob".into()), Value::Null],
            vec![Value::Text("Cara".into()), Value::Float(570.0)],
        ]
    );
}

#[test]
fn test_uncorrelated_scalar_subquery_folds_to_constant() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    execute_sql("CREATE TABLE settings (max_limit INTEGER)").unwrap();

    execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
    execute_sql("INSERT INTO settings VALUES (250)").unwrap();

    let rows = query_rows(
        "SELECT users.name, (SELECT max_limit FROM settings) \
         FROM users ORDER BY users.name",
    )
    .unwrap();

    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Text("Alice".into()), Value::Integer(250)],
            vec![Value::Text("Bob".into()), Value::Integer(250)],
        ]
    );
}

#[test]
fn test_scalar_subquery_null_outer_key() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    execute_sql("CREATE TABLE orders (user_id INTEGER, amount INTEGER)").unwrap();

    execute_sql("INSERT INTO users VALUES (1, 'Alice'), (NULL, 'Ghost')").unwrap();
    execute_sql("INSERT INTO orders VALUES (1, 50), (NULL, 900)").unwrap();

    // A NULL outer key matches no inner rows: MAX over empty is NULL and
    // COUNT over empty is 0.
    let rows = query_rows(
        "SELECT users.name, \
                (SELECT MAX(amount) FROM orders WHERE orders.user_id = users.id), \
                (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) \
         FROM users ORDER BY users.name",
    )
    .unwrap();

    assert_eq!(
        rows.rows,
        vec![
            vec![
                Value::Text("Alice".into()),
                Value::Integer(50),
                Value::Integer(1),
            ],
            vec![Value::Text("Ghost".into()), Value::Null, Value::Integer(0)],
        ]
    );
}
