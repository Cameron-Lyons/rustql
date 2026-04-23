use rustql::ast::{Statement, Value};
use rustql::binder::{self, BoundExprKind, BoundType};
use rustql::{DataType, Engine, QueryResult, RustqlError};

#[test]
fn binder_exposes_resolved_select_metadata() {
    let engine = Engine::in_memory().unwrap();
    let mut session = engine.session();
    session
        .execute_script(
            "
            CREATE TABLE binder_users (id INTEGER, name TEXT);
            INSERT INTO binder_users VALUES (1, 'Alice');
            ",
        )
        .unwrap();

    let tokens = rustql::lexer::tokenize("SELECT name FROM binder_users WHERE id = 1").unwrap();
    let Statement::Select(select) = rustql::parser::parse(tokens).unwrap() else {
        panic!("expected SELECT");
    };
    let db = engine.snapshot_database();
    let bound = binder::bind_select(&db, &select).unwrap();

    assert_eq!(bound.output_columns[0].name, "name");
    assert_eq!(bound.output_columns[0].data_type, DataType::Text);
    let where_clause = bound.where_clause.as_ref().unwrap();
    assert_eq!(where_clause.data_type, BoundType::Known(DataType::Boolean));
    match &where_clause.kind {
        BoundExprKind::BinaryOp { left, .. } => match &left.kind {
            BoundExprKind::Column(column) => {
                assert_eq!(column.relation.as_deref(), Some("binder_users"));
                assert_eq!(column.name, "id");
                assert_eq!(column.data_type, DataType::Integer);
            }
            other => panic!("expected bound column, got {other:?}"),
        },
        other => panic!("expected bound binary expression, got {other:?}"),
    }
}

#[test]
fn ambiguous_unqualified_join_column_is_semantic_error() {
    let engine = Engine::in_memory().unwrap();
    let mut session = engine.session();
    session
        .execute_script(
            "
            CREATE TABLE binder_left (id INTEGER);
            CREATE TABLE binder_right (id INTEGER);
            ",
        )
        .unwrap();

    let error = session
        .execute_one(
            "SELECT id FROM binder_left JOIN binder_right ON binder_left.id = binder_right.id",
        )
        .unwrap_err();

    assert!(matches!(error, RustqlError::AmbiguousColumn(name) if name == "id"));
}

#[test]
fn non_boolean_where_is_type_mismatch_not_internal() {
    let engine = Engine::in_memory().unwrap();
    let mut session = engine.session();
    session
        .execute_script(
            "
            CREATE TABLE binder_predicates (id INTEGER);
            INSERT INTO binder_predicates VALUES (1);
            ",
        )
        .unwrap();

    let error = session
        .execute_one("SELECT id FROM binder_predicates WHERE id")
        .unwrap_err();

    assert!(matches!(error, RustqlError::TypeMismatch(_)));
}

#[test]
fn set_operation_width_is_checked_before_execution() {
    let engine = Engine::in_memory().unwrap();
    let mut session = engine.session();

    let error = session
        .execute_one("SELECT 1 UNION SELECT 1, 2")
        .unwrap_err();

    assert!(matches!(error, RustqlError::TypeMismatch(_)));
}

#[test]
fn scalar_subquery_width_is_checked_by_binder() {
    let engine = Engine::in_memory().unwrap();
    let mut session = engine.session();

    let error = session.execute_one("SELECT (SELECT 1, 2)").unwrap_err();

    assert!(matches!(error, RustqlError::TypeMismatch(_)));

    let ok = session.execute_one("SELECT (SELECT 1)").unwrap();
    let QueryResult::Rows(rows) = ok else {
        panic!("expected rows");
    };
    assert_eq!(rows.rows, vec![vec![Value::Integer(1)]]);
}
