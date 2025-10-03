use rustql::executor::{execute, reset_database_state};
use rustql::ast::*;

#[test]
fn test_alter_table_add_column() {
    reset_database_state();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "users".into(),
        columns: vec![ColumnDefinition {
            name: "id".into(),
            data_type: DataType::Integer,
        }],
    })).unwrap();

    let alter = Statement::AlterTable(AlterTableStatement {
        table: "users".into(),
        operation: AlterOperation::AddColumn(ColumnDefinition {
            name: "name".into(),
            data_type: DataType::Text,
        }),
    });

    let result = execute(alter).unwrap();
    assert!(result.contains("Column 'name' added"));
}
