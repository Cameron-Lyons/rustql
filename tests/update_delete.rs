use rustql::ast::*;
use rustql::testing::{execute, reset_database};

#[test]
fn test_update_and_delete() {
    reset_database();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "users".into(),
        columns: vec![
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
            },
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
                name: "name".into(),
                data_type: DataType::Text,
                nullable: false,
            },
        ],
        constraints: vec![],
        as_query: None,
        if_not_exists: false,
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "users".into(),
        columns: Some(vec!["id".into(), "name".into()]),
        values: vec![vec![Value::Integer(1), Value::Text("Alice".into())]],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    let update = Statement::Update(UpdateStatement {
        table: "users".into(),
        assignments: vec![Assignment {
            column: "name".into(),
            value: Expression::Value(Value::Text("Alicia".into())),
        }],
        where_clause: None,
        from: None,
        returning: None,
    });

    assert_eq!(execute(update).unwrap(), "1 row(s) updated");

    let delete = Statement::Delete(DeleteStatement {
        table: "users".into(),
        where_clause: None,
        using: None,
        returning: None,
    });

    assert_eq!(execute(delete).unwrap(), "1 row(s) deleted");
}
