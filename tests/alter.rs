mod common;
use common::{execute, reset_database};
use rustql::ast::*;

#[test]
fn test_alter_table_add_column() {
    reset_database();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "users".into(),
        columns: vec![ColumnDefinition {
            primary_key: false,
            unique: false,
            default_value: None,
            name: "id".into(),
            data_type: DataType::Integer,
            nullable: false,
            foreign_key: None,
            check: None,
            auto_increment: false,
            generated: None,
        }],
        constraints: vec![],
        as_query: None,
        if_not_exists: false,
    }))
    .unwrap();

    let alter = Statement::AlterTable(AlterTableStatement {
        table: "users".into(),
        operation: AlterOperation::AddColumn(ColumnDefinition {
            primary_key: false,
            unique: false,
            default_value: None,
            name: "name".into(),
            data_type: DataType::Text,
            nullable: false,
            foreign_key: None,
            check: None,
            auto_increment: false,
            generated: None,
        }),
    });

    let result = execute(alter).unwrap();
    assert!(result.contains("Column 'name' added"));
}
