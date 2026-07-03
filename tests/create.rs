mod common;
use common::*;
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

    assert_command(execute(create).unwrap(), CommandTag::CreateTable, 0);
    assert_command(execute(drop).unwrap(), CommandTag::DropTable, 0);
}

#[test]
fn create_table_rejects_duplicate_column_names() {
    reset_database();

    let err = execute_sql("CREATE TABLE duplicate_columns (id INTEGER, id TEXT)").unwrap_err();
    assert!(err.contains("Column 'id' already exists"));

    let missing = execute_sql("SELECT * FROM duplicate_columns").unwrap_err();
    assert!(missing.contains("does not exist"));
}

#[test]
fn create_table_rejects_invalid_table_constraint_columns() {
    reset_database();

    let missing =
        execute_sql("CREATE TABLE missing_constraint_column (id INTEGER, UNIQUE (missing))")
            .unwrap_err();
    assert!(missing.contains("Column 'missing' not found"));

    let duplicate =
        execute_sql("CREATE TABLE duplicate_constraint_column (id INTEGER, UNIQUE (id, id))")
            .unwrap_err();
    assert!(duplicate.contains("Constraint column 'id' specified more than once"));
}

#[test]
fn create_table_if_not_exists_keeps_existing_table_before_validation() {
    reset_database();

    execute_sql("CREATE TABLE existing_table (id INTEGER)").unwrap();
    assert_command_sql(
        "CREATE TABLE IF NOT EXISTS existing_table (id INTEGER, id TEXT)",
        CommandTag::CreateTable,
        0,
    );

    assert_rows("SELECT * FROM existing_table", &["id"], vec![]);
}
