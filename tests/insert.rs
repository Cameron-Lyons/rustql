use rustql::ast::*;
use rustql::executor::{execute, reset_database_state};

#[test]
fn test_insert() {
    reset_database_state();

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
            },
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: false,
                foreign_key: None,
            },
        ],
    });

    execute(create).unwrap();

    let insert = Statement::Insert(InsertStatement {
        table: "users".to_string(),
        columns: Some(vec!["id".into(), "name".into()]),
        values: vec![vec![Value::Integer(1), Value::Text("Alice".into())]],
    });

    let result = execute(insert).unwrap();
    assert_eq!(result, "1 row(s) inserted");
}
