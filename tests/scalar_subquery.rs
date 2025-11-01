use rustql::ast::*;
use rustql::executor::{execute, reset_database_state};

#[test]
fn test_scalar_subquery_basic() {
    reset_database_state();
    execute(Statement::CreateTable(CreateTableStatement {
        name: "users_scalar1".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".into(), data_type: DataType::Integer, nullable: false },
            ColumnDefinition { name: "name".into(), data_type: DataType::Text, nullable: false },
        ],
    })).unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "users_scalar1".to_string(),
        columns: Some(vec!["id".into(), "name".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Text("Alice".into())],
            vec![Value::Integer(2), Value::Text("Bob".into())],
        ],
    })).unwrap();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "orders_scalar1".to_string(),
        columns: vec![
            ColumnDefinition { name: "user_id".into(), data_type: DataType::Integer, nullable: false },
            ColumnDefinition { name: "amount".into(), data_type: DataType::Float, nullable: false },
        ],
    })).unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "orders_scalar1".to_string(),
        columns: Some(vec!["user_id".into(), "amount".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Float(150.0)],
            vec![Value::Integer(2), Value::Float(50.0)],
        ],
    })).unwrap();

    let stmt = Statement::Select(SelectStatement {
        distinct: false,
        from: "users_scalar1".into(),
        columns: vec![
            Column::Named("name".into()),
            Column::Subquery(Box::new(SelectStatement {
                distinct: false,
                columns: vec![Column::Named("amount".into())],
                from: "orders_scalar1".into(),
                joins: vec![],
                where_clause: Some(Expression::BinaryOp {
                    left: Box::new(Expression::Column("user_id".into())),
                    op: BinaryOperator::Equal,
                    right: Box::new(Expression::Column("users_scalar1.id".into())),
                }),
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
            })),
        ],
        joins: vec![],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    let output = execute(stmt).unwrap();
    assert!(output.contains("name"));
    assert!(output.contains("Alice"));
    assert!(output.contains("150"));
}

#[test]
fn test_scalar_subquery_null() {
    reset_database_state();
    execute(Statement::CreateTable(CreateTableStatement {
        name: "users_scalar2".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".into(), data_type: DataType::Integer, nullable: false },
            ColumnDefinition { name: "name".into(), data_type: DataType::Text, nullable: false },
        ],
    })).unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "users_scalar2".to_string(),
        columns: Some(vec!["id".into(), "name".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Text("Alice".into())],
        ],
    })).unwrap();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "orders_scalar2".to_string(),
        columns: vec![
            ColumnDefinition { name: "user_id".into(), data_type: DataType::Integer, nullable: false },
            ColumnDefinition { name: "amount".into(), data_type: DataType::Float, nullable: false },
        ],
    })).unwrap();

    let stmt = Statement::Select(SelectStatement {
        distinct: false,
        from: "users_scalar2".into(),
        columns: vec![
            Column::Named("name".into()),
            Column::Subquery(Box::new(SelectStatement {
                distinct: false,
                columns: vec![Column::Named("amount".into())],
                from: "orders_scalar2".into(),
                joins: vec![],
                where_clause: Some(Expression::BinaryOp {
                    left: Box::new(Expression::Column("user_id".into())),
                    op: BinaryOperator::Equal,
                    right: Box::new(Expression::Column("users_scalar2.id".into())),
                }),
                group_by: None,
                having: None,
                order_by: None,
                limit: Some(1),
                offset: None,
            })),
        ],
        joins: vec![],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    let output = execute(stmt).unwrap();
    assert!(output.contains("name"));
    assert!(output.contains("Alice"));
    assert!(output.contains("NULL"));
}

