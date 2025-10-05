use rustql::ast::*;
use rustql::executor::{execute, reset_database_state};

#[test]
fn test_create_and_drop_table() {
    reset_database_state();

    let create = Statement::CreateTable(CreateTableStatement {
        name: "users".to_string(),
        columns: vec![
            ColumnDefinition {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
            },
            ColumnDefinition {
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: false,
            },
        ],
    });

    let drop = Statement::DropTable(DropTableStatement {
        name: "users".to_string(),
    });

    assert_eq!(execute(create).unwrap(), "Table 'users' created");
    assert_eq!(execute(drop).unwrap(), "Table 'users' dropped");
}
