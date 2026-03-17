use rustql::ast::{ColumnDefinition, CreateTableStatement, DataType, Statement, Value};
use rustql::executor::format_value;
use rustql::testing::{execute, reset_database};

#[test]
fn test_format_value() {
    assert_eq!(format_value(&Value::Integer(42)), "42");
    assert_eq!(format_value(&Value::Null), "NULL");
}

#[test]
fn test_create_table() {
    reset_database();
    let stmt = Statement::CreateTable(CreateTableStatement {
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
    });
    assert!(execute(stmt).is_ok());
}
