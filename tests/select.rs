use rustql::executor::{execute, reset_database_state};
use rustql::ast::*;

#[test]
fn test_select_all() {
    reset_database_state();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "users".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".into(), data_type: DataType::Integer },
            ColumnDefinition { name: "name".into(), data_type: DataType::Text },
        ],
    })).unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "users".to_string(),
        values: vec![
            vec![Value::Integer(1), Value::Text("Alice".into())],
            vec![Value::Integer(2), Value::Text("Bob".into())],
        ],
    })).unwrap();

    let select = Statement::Select(SelectStatement {
        from: "users".into(),
        columns: vec![Column::All],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    let output = execute(select).unwrap();
    assert!(output.contains("Alice"));
    assert!(output.contains("Bob"));
}
