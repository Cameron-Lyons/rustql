use rustql::ast::*;
use rustql::executor::{execute, reset_database_state};

#[test]
fn test_update_and_delete() {
    reset_database_state();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "users".into(),
        columns: vec![
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
            },
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                name: "name".into(),
                data_type: DataType::Text,
                nullable: false,
            },
        ],
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "users".into(),
        columns: Some(vec!["id".into(), "name".into()]),
        values: vec![vec![Value::Integer(1), Value::Text("Alice".into())]],
    }))
    .unwrap();

    let update = Statement::Update(UpdateStatement {
        table: "users".into(),
        assignments: vec![Assignment {
            column: "name".into(),
            value: Value::Text("Alicia".into()),
        }],
        where_clause: None,
    });

    assert_eq!(execute(update).unwrap(), "1 row(s) updated");

    let delete = Statement::Delete(DeleteStatement {
        table: "users".into(),
        where_clause: None,
    });

    assert_eq!(execute(delete).unwrap(), "1 row(s) deleted");
}
