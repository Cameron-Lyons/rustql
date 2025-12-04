use rustql::ast::*;
use rustql::executor::{execute, reset_database_state};

#[test]
fn test_alter_table_add_column() {
    reset_database_state();

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
        }],
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
        }),
    });

    let result = execute(alter).unwrap();
    assert!(result.contains("Column 'name' added"));
}
