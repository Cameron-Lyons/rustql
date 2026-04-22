mod common;
use common::{execute, reset_database};
use rustql::ast::*;

#[test]
fn test_create_and_drop_table() {
    reset_database();

    let create = Statement::CreateTable(CreateTableStatement {
        name: "users".to_string(),
        columns: vec![
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
                name: "id".to_string(),
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
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: false,
            },
        ],
        constraints: vec![],
        as_query: None,
        if_not_exists: false,
    });

    let drop = Statement::DropTable(DropTableStatement {
        name: "users".to_string(),
        if_exists: false,
    });

    assert_eq!(execute(create).unwrap(), "Table 'users' created");
    assert_eq!(execute(drop).unwrap(), "Table 'users' dropped");
}
