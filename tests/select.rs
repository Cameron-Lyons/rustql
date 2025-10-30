use rustql::ast::*;
use rustql::executor::{execute, reset_database_state};

#[test]
fn test_select_all() {
    reset_database_state();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "users".to_string(),
        columns: vec![
            ColumnDefinition {
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
            },
            ColumnDefinition {
                name: "name".into(),
                data_type: DataType::Text,
                nullable: false,
            },
        ],
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "users".to_string(),
        columns: Some(vec!["id".into(), "name".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Text("Alice".into())],
            vec![Value::Integer(2), Value::Text("Bob".into())],
        ],
    }))
    .unwrap();

    let select = Statement::Select(SelectStatement {
        distinct: false,
        from: "users".into(),
        columns: vec![Column::All],
        joins: vec![],
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
