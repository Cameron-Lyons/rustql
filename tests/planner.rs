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

#[test]
fn test_explain_analyze_command() {
    let _guard = setup_test();

    process_query("CREATE TABLE metrics (id INTEGER, score INTEGER)").unwrap();
    process_query("INSERT INTO metrics VALUES (1, 10), (2, 20), (3, 30)").unwrap();

    let result = process_query("EXPLAIN ANALYZE SELECT * FROM metrics WHERE score >= 20");
    assert!(result.is_ok());
    let output = result.unwrap();

    assert!(output.contains("Query Plan"));
    assert!(output.contains("Planning Time"));
    assert!(output.contains("Execution Time"));
    assert!(output.contains("Actual Rows"));
}

#[test]
fn test_join_order_prefers_connected_join() {
    let _guard = setup_test();

    process_query("CREATE TABLE a (id INTEGER)").unwrap();
    process_query("CREATE TABLE b (id INTEGER, a_id INTEGER)").unwrap();
    process_query("CREATE TABLE c (id INTEGER, b_id INTEGER)").unwrap();

    process_query("INSERT INTO a VALUES (1)").unwrap();
    process_query("INSERT INTO b VALUES (10, 1)").unwrap();
    process_query("INSERT INTO c VALUES (100, 10)").unwrap();

    let result =
        process_query("EXPLAIN SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id")
            .unwrap();

    let b_pos = result.find("Seq Scan on b").unwrap();
    let c_pos = result.find("Seq Scan on c").unwrap();
    assert!(
        b_pos < c_pos,
        "expected connected join table b before c, got plan:\n{}",
        result
    );
}

#[test]
fn test_explain_analyze_reports_actual_rows() {
    let _guard = setup_test();

    process_query("CREATE TABLE cte_metrics (id INTEGER, score INTEGER)").unwrap();
    process_query("INSERT INTO cte_metrics VALUES (1, 10), (2, 30), (3, 50)").unwrap();

    let result =
        process_query("EXPLAIN ANALYZE SELECT * FROM cte_metrics WHERE score >= 30").unwrap();

    assert!(result.contains("Query Plan"), "got:\n{}", result);
    assert!(result.contains("Planning Time"), "got:\n{}", result);
    assert!(result.contains("Execution Time"), "got:\n{}", result);
    assert!(result.contains("Actual Rows: 2"), "got:\n{}", result);
}

#[test]
fn test_explain_analyze_rejects_non_select() {
    let _guard = setup_test();

    let err = process_query("EXPLAIN ANALYZE INSERT INTO nope VALUES (1)");
    assert!(err.is_err());
}

#[test]
fn test_explain_analyze_malformed_statements_fail() {
    let _guard = setup_test();

    process_query("CREATE TABLE malformed_t (id INTEGER)").unwrap();

    assert!(process_query("EXPLAIN ANALYZE").is_err());
    assert!(process_query("EXPLAIN ANALYZE SELECT FROM malformed_t").is_err());
    assert!(process_query("EXPLAIN ANALYZE WITH c AS (SELECT * FROM malformed_t) INSERT INTO malformed_t VALUES (1)").is_err());
}

#[test]
fn test_query_planner_row_estimates_are_sane() {
    let _guard = setup_test();

    process_query("CREATE TABLE sane_users (id INTEGER, age INTEGER)").unwrap();
    process_query("CREATE TABLE sane_orders (id INTEGER, user_id INTEGER)").unwrap();

    for i in 1..=20 {
        process_query(&format!(
            "INSERT INTO sane_users VALUES ({}, {})",
            i,
            20 + (i % 10)
        ))
        .unwrap();
    }
    for i in 1..=50 {
        process_query(&format!(
            "INSERT INTO sane_orders VALUES ({}, {})",
            i,
            (i % 20) + 1
        ))
        .unwrap();
    }

    let db = executor::get_database_for_testing();
    let tokens = lexer::tokenize(
        "SELECT * FROM sane_users JOIN sane_orders ON sane_users.id = sane_orders.user_id WHERE sane_users.age >= 25",
    )
    .unwrap();
    let statement = parser::parse(tokens).unwrap();
    let stmt = match statement {
        ast::Statement::Select(s) => s,
        _ => panic!("expected select"),
    };

    let planner = planner::QueryPlanner::new(&db);
    let plan = planner.plan_select(&stmt).unwrap();

    fn assert_sane(node: &planner::PlanNode) {
        match node {
            planner::PlanNode::SeqScan { rows, .. } | planner::PlanNode::IndexScan { rows, .. } => {
                assert!(*rows > 0, "scan estimated zero rows unexpectedly");
            }
            planner::PlanNode::Filter { input, rows, .. } => {
                let input_rows = match input.as_ref() {
                    planner::PlanNode::SeqScan { rows, .. }
                    | planner::PlanNode::IndexScan { rows, .. }
                    | planner::PlanNode::NestedLoopJoin { rows, .. }
                    | planner::PlanNode::HashJoin { rows, .. }
                    | planner::PlanNode::Filter { rows, .. }
                    | planner::PlanNode::Sort { rows, .. }
                    | planner::PlanNode::Limit { rows, .. }
                    | planner::PlanNode::Aggregate { rows, .. } => *rows,
                };
                assert!(
                    *rows <= input_rows,
                    "filter rows should not exceed input rows: {} > {}",
                    rows,
                    input_rows
                );
                assert_sane(input);
            }
            planner::PlanNode::NestedLoopJoin {
                left, right, rows, ..
            }
            | planner::PlanNode::HashJoin {
                left, right, rows, ..
            } => {
                let left_rows = match left.as_ref() {
                    planner::PlanNode::SeqScan { rows, .. }
                    | planner::PlanNode::IndexScan { rows, .. }
                    | planner::PlanNode::NestedLoopJoin { rows, .. }
                    | planner::PlanNode::HashJoin { rows, .. }
                    | planner::PlanNode::Filter { rows, .. }
                    | planner::PlanNode::Sort { rows, .. }
                    | planner::PlanNode::Limit { rows, .. }
                    | planner::PlanNode::Aggregate { rows, .. } => *rows,
                };
                let right_rows = match right.as_ref() {
                    planner::PlanNode::SeqScan { rows, .. }
                    | planner::PlanNode::IndexScan { rows, .. }
                    | planner::PlanNode::NestedLoopJoin { rows, .. }
                    | planner::PlanNode::HashJoin { rows, .. }
                    | planner::PlanNode::Filter { rows, .. }
                    | planner::PlanNode::Sort { rows, .. }
                    | planner::PlanNode::Limit { rows, .. }
                    | planner::PlanNode::Aggregate { rows, .. } => *rows,
                };
                assert!(
                    *rows <= left_rows.saturating_mul(right_rows),
                    "join rows should be bounded by cartesian product"
                );
                assert_sane(left);
                assert_sane(right);
            }
            planner::PlanNode::Sort { input, rows, .. } => {
                let input_rows = match input.as_ref() {
                    planner::PlanNode::SeqScan { rows, .. }
                    | planner::PlanNode::IndexScan { rows, .. }
                    | planner::PlanNode::NestedLoopJoin { rows, .. }
                    | planner::PlanNode::HashJoin { rows, .. }
                    | planner::PlanNode::Filter { rows, .. }
                    | planner::PlanNode::Sort { rows, .. }
                    | planner::PlanNode::Limit { rows, .. }
                    | planner::PlanNode::Aggregate { rows, .. } => *rows,
                };
                assert_eq!(*rows, input_rows, "sort should preserve row count");
                assert_sane(input);
            }
            planner::PlanNode::Limit {
                input, rows, limit, ..
            } => {
                assert!(
                    *rows <= *limit,
                    "limit output rows should be bounded by limit: {} > {}",
                    rows,
                    limit
                );
                assert_sane(input);
            }
            planner::PlanNode::Aggregate { input, rows, .. } => {
                let input_rows = match input.as_ref() {
                    planner::PlanNode::SeqScan { rows, .. }
                    | planner::PlanNode::IndexScan { rows, .. }
                    | planner::PlanNode::NestedLoopJoin { rows, .. }
                    | planner::PlanNode::HashJoin { rows, .. }
                    | planner::PlanNode::Filter { rows, .. }
                    | planner::PlanNode::Sort { rows, .. }
                    | planner::PlanNode::Limit { rows, .. }
                    | planner::PlanNode::Aggregate { rows, .. } => *rows,
                };
                assert!(
                    *rows <= input_rows.max(1),
                    "aggregate output rows should not exceed input rows"
                );
                assert_sane(input);
            }
        }
    }

    assert_sane(&plan);
}
