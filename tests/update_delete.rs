use rustql::executor::{execute, reset_database_state};
use rustql::ast::*;

#[test]
fn test_update_and_delete() {
    reset_database_state();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "users".into(),
        columns: vec![
            ColumnDefinition { name: "id".into(), data_type: DataType::Integer },
            ColumnDefinition { name: "name".into(), data_type: DataType::Text },
        ],
    })).unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "users".into(),
        values: vec![vec![Value::Integer(1), Value::Text("Alice".into())]],
    })).unwrap();

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
