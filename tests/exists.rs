use rustql::ast::*;
use rustql::executor::execute;

#[test]
fn test_where_exists_true() {
    execute(Statement::CreateTable(CreateTableStatement {
        name: "users_ex1".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".into(), data_type: DataType::Integer, nullable: false },
            ColumnDefinition { name: "name".into(), data_type: DataType::Text, nullable: false },
        ],
    })).unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "users_ex1".to_string(),
        columns: Some(vec!["id".into(), "name".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Text("Alice".into())],
            vec![Value::Integer(2), Value::Text("Bob".into())],
        ],
    })).unwrap();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "orders_ex1".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".into(), data_type: DataType::Integer, nullable: false },
            ColumnDefinition { name: "amount".into(), data_type: DataType::Float, nullable: false },
        ],
    })).unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "orders_ex1".to_string(),
        columns: Some(vec!["id".into(), "amount".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Float(150.0)],
            vec![Value::Integer(2), Value::Float(50.0)],
        ],
    })).unwrap();

    let sub = SelectStatement {
        distinct: false,
        columns: vec![Column::All],
        from: "orders_ex1".into(),
        joins: vec![],
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::Column("amount".into())),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expression::Value(Value::Float(100.0))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let stmt = Statement::Select(SelectStatement {
        distinct: false,
        from: "users_ex1".into(),
        columns: vec![Column::All],
        joins: vec![],
        where_clause: Some(Expression::Exists(Box::new(sub))),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    let output = execute(stmt).unwrap();
    assert!(output.contains("Alice"));
    assert!(output.contains("Bob"));
}

#[test]
fn test_where_exists_false_filters_all() {
    execute(Statement::CreateTable(CreateTableStatement {
        name: "users_ex2".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".into(), data_type: DataType::Integer, nullable: false },
            ColumnDefinition { name: "name".into(), data_type: DataType::Text, nullable: false },
        ],
    })).unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "users_ex2".to_string(),
        columns: Some(vec!["id".into(), "name".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Text("Alice".into())],
            vec![Value::Integer(2), Value::Text("Bob".into())],
        ],
    })).unwrap();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "orders_ex2".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".into(), data_type: DataType::Integer, nullable: false },
            ColumnDefinition { name: "amount".into(), data_type: DataType::Float, nullable: false },
        ],
    })).unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "orders_ex2".to_string(),
        columns: Some(vec!["id".into(), "amount".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Float(150.0)],
            vec![Value::Integer(2), Value::Float(50.0)],
        ],
    })).unwrap();

    let sub = SelectStatement {
        distinct: false,
        columns: vec![Column::All],
        from: "orders_ex2".into(),
        joins: vec![],
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::Column("amount".into())),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expression::Value(Value::Float(1000.0))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let stmt = Statement::Select(SelectStatement {
        distinct: false,
        from: "users_ex2".into(),
        columns: vec![Column::All],
        joins: vec![],
        where_clause: Some(Expression::Exists(Box::new(sub))),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    let output = execute(stmt).unwrap();
    assert!(!output.contains("Alice"));
    assert!(!output.contains("Bob"));
}


#[test]
fn test_where_not_exists_true_filters_all() {
    execute(Statement::CreateTable(CreateTableStatement {
        name: "t_users_ne1".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".into(), data_type: DataType::Integer, nullable: false },
            ColumnDefinition { name: "name".into(), data_type: DataType::Text, nullable: false },
        ],
    })).unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "t_users_ne1".to_string(),
        columns: Some(vec!["id".into(), "name".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Text("Alice".into())],
            vec![Value::Integer(2), Value::Text("Bob".into())],
        ],
    })).unwrap();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "t_orders_ne1".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".into(), data_type: DataType::Integer, nullable: false },
            ColumnDefinition { name: "amount".into(), data_type: DataType::Float, nullable: false },
        ],
    })).unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "t_orders_ne1".to_string(),
        columns: Some(vec!["id".into(), "amount".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Float(150.0)],
            vec![Value::Integer(2), Value::Float(50.0)],
        ],
    })).unwrap();

    let sub = SelectStatement {
        distinct: false,
        columns: vec![Column::All],
        from: "t_orders_ne1".into(),
        joins: vec![],
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::Column("amount".into())),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expression::Value(Value::Float(1000.0))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let stmt = Statement::Select(SelectStatement {
        distinct: false,
        from: "t_users_ne1".into(),
        columns: vec![Column::All],
        joins: vec![],
        where_clause: Some(Expression::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expression::Exists(Box::new(sub))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    let output = execute(stmt).unwrap();
    assert!(output.contains("Alice"));
    assert!(output.contains("Bob"));
}

#[test]
fn test_where_exists_correlated_true() {
    execute(Statement::CreateTable(CreateTableStatement {
        name: "users_corr1".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".into(), data_type: DataType::Integer, nullable: false },
            ColumnDefinition { name: "name".into(), data_type: DataType::Text, nullable: false },
        ],
    })).unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "users_corr1".to_string(),
        columns: Some(vec!["id".into(), "name".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Text("Alice".into())],
            vec![Value::Integer(2), Value::Text("Bob".into())],
        ],
    })).unwrap();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "orders_corr1".to_string(),
        columns: vec![
            ColumnDefinition { name: "user_id".into(), data_type: DataType::Integer, nullable: false },
            ColumnDefinition { name: "amount".into(), data_type: DataType::Float, nullable: false },
        ],
    })).unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "orders_corr1".to_string(),
        columns: Some(vec!["user_id".into(), "amount".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Float(150.0)],
            vec![Value::Integer(2), Value::Float(50.0)],
        ],
    })).unwrap();

    let sub = SelectStatement {
        distinct: false,
        columns: vec![Column::All],
        from: "orders_corr1".into(),
        joins: vec![],
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column("user_id".into())),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Column("users_corr1.id".into())),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column("amount".into())),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expression::Value(Value::Float(100.0))),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let stmt = Statement::Select(SelectStatement {
        distinct: false,
        from: "users_corr1".into(),
        columns: vec![Column::All],
        joins: vec![],
        where_clause: Some(Expression::Exists(Box::new(sub))),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    let output = execute(stmt).unwrap();
    assert!(output.contains("Alice"));
    assert!(!output.contains("Bob"));
}
