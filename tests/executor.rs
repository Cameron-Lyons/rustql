use rustql::ast::{ColumnDefinition, CreateTableStatement, DataType, Statement, Value};
use rustql::executor::*;

#[test]
fn test_format_value() {
    assert_eq!(format_value(&Value::Integer(42)), "42");
    assert_eq!(format_value(&Value::Null), "NULL");
}

#[test]
fn test_create_table() {
    reset_database_state();
    let stmt = Statement::CreateTable(CreateTableStatement {
        name: "users".into(),
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
    });
    assert!(execute(stmt).is_ok());
}
