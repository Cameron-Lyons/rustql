mod common;
use common::*;
use rustql::ast::*;

#[test]
fn test_insert() {
    reset_database();

    let create = Statement::CreateTable(CreateTableStatement {
        name: "users".to_string(),
        columns: vec![
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
            },
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: false,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
            },
        ],
        constraints: vec![],
        as_query: None,
        if_not_exists: false,
    });

    execute(create).unwrap();

    let insert = Statement::Insert(InsertStatement {
        table: "users".to_string(),
        columns: Some(vec!["id".into(), "name".into()]),
        values: insert_values(vec![vec![Value::Integer(1), Value::Text("Alice".into())]]),
        source_query: None,
        on_conflict: None,
        returning: None,
    });

    assert_command(execute(insert).unwrap(), CommandTag::Insert, 1);
}

#[test]
fn failed_multi_row_insert_rolls_back_prior_rows() {
    reset_database();
    execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();

    let result = execute_sql("INSERT INTO t VALUES (1), (1)");
    assert!(result.is_err());

    assert_rows("SELECT * FROM t", &["id"], vec![]);
}

#[test]
fn insert_values_accept_expressions() {
    reset_database();
    execute_sql(
        "CREATE TABLE expr_insert (
            id INTEGER,
            label TEXT,
            active BOOLEAN,
            score FLOAT
        )",
    )
    .unwrap();

    execute_sql(
        "INSERT INTO expr_insert VALUES (
            1 + 2,
            UPPER('alice'),
            10 BETWEEN 5 AND 20,
            CAST('2.5' AS FLOAT)
        )",
    )
    .unwrap();

    assert_rows(
        "SELECT id, label, active, score FROM expr_insert",
        &["id", "label", "active", "score"],
        vec![vec![
            Value::Integer(3),
            Value::Text("ALICE".to_string()),
            Value::Boolean(true),
            Value::Float(2.5),
        ]],
    );
}

#[test]
fn insert_column_list_values_accept_expressions() {
    reset_database();
    execute_sql("CREATE TABLE expr_insert_columns (id INTEGER DEFAULT 7, label TEXT, qty INTEGER)")
        .unwrap();

    execute_sql("INSERT INTO expr_insert_columns (label, qty) VALUES (LOWER('MIXED'), 2 * 3)")
        .unwrap();

    assert_rows(
        "SELECT id, label, qty FROM expr_insert_columns",
        &["id", "label", "qty"],
        vec![vec![
            Value::Integer(7),
            Value::Text("mixed".to_string()),
            Value::Integer(6),
        ]],
    );
}

#[test]
fn insert_values_accept_scalar_subqueries() {
    reset_database();
    execute_sql("CREATE TABLE insert_expr_source (value INTEGER)").unwrap();
    execute_sql("CREATE TABLE insert_expr_target (id INTEGER, label TEXT)").unwrap();
    execute_sql("INSERT INTO insert_expr_source VALUES (41)").unwrap();

    execute_sql(
        "INSERT INTO insert_expr_target VALUES (
            (SELECT value + 1 FROM insert_expr_source),
            'from_subquery'
        )",
    )
    .unwrap();

    assert_rows(
        "SELECT id, label FROM insert_expr_target",
        &["id", "label"],
        vec![vec![
            Value::Integer(42),
            Value::Text("from_subquery".to_string()),
        ]],
    );
}

#[test]
fn insert_default_values_uses_declared_defaults() {
    reset_database();
    execute_sql(
        "CREATE TABLE default_insert (
            id INTEGER DEFAULT 7,
            label TEXT DEFAULT 'new',
            missing TEXT
        )",
    )
    .unwrap();

    execute_sql("INSERT INTO default_insert DEFAULT VALUES").unwrap();

    assert_rows(
        "SELECT id, label, missing FROM default_insert",
        &["id", "label", "missing"],
        vec![vec![
            Value::Integer(7),
            Value::Text("new".to_string()),
            Value::Null,
        ]],
    );
}

#[test]
fn insert_values_accept_default_markers() {
    reset_database();
    execute_sql(
        "CREATE TABLE default_marker_insert (
            id INTEGER DEFAULT 11,
            label TEXT DEFAULT 'fallback',
            qty INTEGER
        )",
    )
    .unwrap();

    execute_sql(
        "INSERT INTO default_marker_insert VALUES
         (DEFAULT, UPPER('custom'), 2 + 3),
         (42, DEFAULT, DEFAULT)",
    )
    .unwrap();

    assert_rows(
        "SELECT id, label, qty FROM default_marker_insert ORDER BY id",
        &["id", "label", "qty"],
        vec![
            vec![
                Value::Integer(11),
                Value::Text("CUSTOM".to_string()),
                Value::Integer(5),
            ],
            vec![
                Value::Integer(42),
                Value::Text("fallback".to_string()),
                Value::Null,
            ],
        ],
    );
}

#[test]
fn insert_column_list_accepts_default_markers() {
    reset_database();
    execute_sql(
        "CREATE TABLE default_column_insert (
            id INTEGER DEFAULT 3,
            label TEXT DEFAULT 'label',
            qty INTEGER DEFAULT 9
        )",
    )
    .unwrap();

    execute_sql("INSERT INTO default_column_insert (label, qty) VALUES (DEFAULT, DEFAULT)")
        .unwrap();

    assert_rows(
        "SELECT id, label, qty FROM default_column_insert",
        &["id", "label", "qty"],
        vec![vec![
            Value::Integer(3),
            Value::Text("label".to_string()),
            Value::Integer(9),
        ]],
    );
}

#[test]
fn default_is_rejected_outside_insert_values() {
    reset_database();

    let err = execute_sql("SELECT DEFAULT").unwrap_err();
    assert!(!err.is_empty());
}

#[test]
fn failed_insert_rolls_back_inside_explicit_transaction() {
    reset_database();
    execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
    execute_sql("BEGIN TRANSACTION").unwrap();
    execute_sql("INSERT INTO t VALUES (0)").unwrap();

    let result = execute_sql("INSERT INTO t VALUES (1), (1)");
    assert!(result.is_err());

    assert_rows("SELECT * FROM t", &["id"], vec![vec![Value::Integer(0)]]);
    execute_sql("COMMIT").unwrap();
}

#[test]
fn failed_returning_rolls_back_insert() {
    reset_database();
    execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();

    let result = execute_sql("INSERT INTO t VALUES (1) RETURNING missing_column");
    assert!(result.is_err());

    assert_rows("SELECT * FROM t", &["id"], vec![]);
}
