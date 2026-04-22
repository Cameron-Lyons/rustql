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
fn test_composite_primary_key_creation() {
    let _guard = setup_test();
    let result = process_query(
        "CREATE TABLE enrollment (student_id INTEGER, course_id INTEGER, grade TEXT, PRIMARY KEY (student_id, course_id))",
    );
    assert!(result.is_ok());

    process_query("INSERT INTO enrollment VALUES (1, 101, 'A')").unwrap();
    process_query("INSERT INTO enrollment VALUES (1, 102, 'B')").unwrap();
    process_query("INSERT INTO enrollment VALUES (2, 101, 'C')").unwrap();

    let result = process_query("SELECT * FROM enrollment").unwrap();
    assert!(result.contains("A"));
    assert!(result.contains("B"));
    assert!(result.contains("C"));
}

#[test]
fn test_composite_primary_key_violation() {
    let _guard = setup_test();
    process_query(
        "CREATE TABLE enrollment (student_id INTEGER, course_id INTEGER, grade TEXT, PRIMARY KEY (student_id, course_id))",
    )
    .unwrap();
    process_query("INSERT INTO enrollment VALUES (1, 101, 'A')").unwrap();

    let result = process_query("INSERT INTO enrollment VALUES (1, 101, 'B')");
    assert!(result.is_err());
}

#[test]
fn test_composite_unique_constraint() {
    let _guard = setup_test();
    process_query(
        "CREATE TABLE schedule (room TEXT, time_slot TEXT, course TEXT, UNIQUE (room, time_slot))",
    )
    .unwrap();
    process_query("INSERT INTO schedule VALUES ('101', '9AM', 'Math')").unwrap();
    process_query("INSERT INTO schedule VALUES ('101', '10AM', 'Science')").unwrap();
    process_query("INSERT INTO schedule VALUES ('102', '9AM', 'History')").unwrap();

    let result = process_query("INSERT INTO schedule VALUES ('101', '9AM', 'Art')");
    assert!(result.is_err());
}

#[test]
fn test_named_constraint() {
    let _guard = setup_test();
    let result = process_query(
        "CREATE TABLE orders (order_id INTEGER, product_id INTEGER, qty INTEGER, CONSTRAINT pk_orders PRIMARY KEY (order_id, product_id))",
    );
    assert!(result.is_ok());

    process_query("INSERT INTO orders VALUES (1, 100, 5)").unwrap();
    let dup = process_query("INSERT INTO orders VALUES (1, 100, 10)");
    assert!(dup.is_err());
}

#[test]
fn test_describe_shows_constraints() {
    let _guard = setup_test();
    process_query("CREATE TABLE test_c (a INTEGER, b INTEGER, c TEXT, PRIMARY KEY (a, b))")
        .unwrap();

    let result = process_query("DESCRIBE test_c").unwrap();
    assert!(result.contains("a"));
    assert!(result.contains("b"));
    assert!(result.contains("c"));
}
