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
        values: vec![vec![Value::Integer(1), Value::Text("Alice".into())]],
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
