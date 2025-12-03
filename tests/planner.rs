use rustql::executor::reset_database_state;
use rustql::*;
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup_test() -> std::sync::MutexGuard<'static, ()> {
    let guard = TEST_MUTEX.lock().unwrap();
    reset_database_state();
    guard
}

#[test]
fn test_query_planner_basic() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();

    process_query("INSERT INTO users VALUES (1, 'Alice', 25)").unwrap();
    process_query("INSERT INTO users VALUES (2, 'Bob', 30)").unwrap();
    process_query("INSERT INTO users VALUES (3, 'Charlie', 35)").unwrap();

    process_query("CREATE INDEX idx_age ON users (age)").unwrap();

    let db = executor::get_database_for_testing();

    let tokens = lexer::tokenize("SELECT * FROM users WHERE age > 25").unwrap();
    let statement = parser::parse(tokens).unwrap();

    if let ast::Statement::Select(stmt) = statement {
        let plan_result = planner::explain_query(&db, &stmt);
        assert!(plan_result.is_ok());
        let plan_str = plan_result.unwrap();

        assert!(plan_str.contains("Query Plan"));

        assert!(plan_str.contains("Index Scan") || plan_str.contains("Seq Scan"));
    }
}

#[test]
fn test_query_planner_join() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, user_id INTEGER, amount FLOAT)").unwrap();

    process_query("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    process_query("INSERT INTO orders VALUES (1, 1, 100.0)").unwrap();

    let db = executor::get_database_for_testing();

    let tokens = lexer::tokenize(
        "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id",
    )
    .unwrap();
    let statement = parser::parse(tokens).unwrap();

    if let ast::Statement::Select(stmt) = statement {
        let plan_result = planner::explain_query(&db, &stmt);
        assert!(plan_result.is_ok());
        let plan_str = plan_result.unwrap();

        assert!(
            plan_str.contains("Join")
                || plan_str.contains("Hash Join")
                || plan_str.contains("Nested Loop")
        );
    }
}

#[test]
fn test_explain_command() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();

    process_query("INSERT INTO users VALUES (1, 'Alice', 25)").unwrap();
    process_query("INSERT INTO users VALUES (2, 'Bob', 30)").unwrap();

    process_query("CREATE INDEX idx_age ON users (age)").unwrap();

    let result = process_query("EXPLAIN SELECT * FROM users WHERE age > 25");
    assert!(result.is_ok());
    let plan_str = result.unwrap();

    assert!(plan_str.contains("Query Plan"));
    assert!(plan_str.contains("users"));

    assert!(plan_str.contains("Index Scan") || plan_str.contains("Seq Scan"));
}

#[test]
fn test_explain_with_join() {
    let _guard = setup_test();

    process_query("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    process_query("CREATE TABLE orders (id INTEGER, user_id INTEGER, amount FLOAT)").unwrap();

    process_query("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    process_query("INSERT INTO orders VALUES (1, 1, 100.0)").unwrap();

    let result = process_query(
        "EXPLAIN SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id",
    );
    assert!(result.is_ok());
    let plan_str = result.unwrap();

    assert!(plan_str.contains("Query Plan"));
    assert!(
        plan_str.contains("Join")
            || plan_str.contains("Hash Join")
            || plan_str.contains("Nested Loop")
    );
}
