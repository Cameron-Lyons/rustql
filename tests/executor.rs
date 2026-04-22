mod common;
use common::{execute, reset_database};
use rustql::ast::{ColumnDefinition, CreateTableStatement, DataType, Statement, Value};

#[test]
fn test_format_value() {
    assert_eq!(Value::Integer(42).to_string(), "42");
    assert_eq!(Value::Null.to_string(), "NULL");
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
