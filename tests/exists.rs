use rustql::ast::*;
use rustql::executor::execute;

#[test]
fn test_where_exists_true() {
    execute(Statement::CreateTable(CreateTableStatement {
        name: "users_ex1".to_string(),
        columns: vec![
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
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
                name: "name".into(),
                data_type: DataType::Text,
                nullable: false,
            },
        ],
        constraints: vec![],
        as_query: None,
        if_not_exists: false,
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "users_ex1".to_string(),
        columns: Some(vec!["id".into(), "name".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Text("Alice".into())],
            vec![Value::Integer(2), Value::Text("Bob".into())],
        ],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "orders_ex1".to_string(),
        columns: vec![
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
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
                name: "amount".into(),
                data_type: DataType::Float,
                nullable: false,
            },
        ],
        constraints: vec![],
        as_query: None,
        if_not_exists: false,
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "orders_ex1".to_string(),
        columns: Some(vec!["id".into(), "amount".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Float(150.0)],
            vec![Value::Integer(2), Value::Float(50.0)],
        ],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    let sub = SelectStatement {
        ctes: Vec::new(),
        from_subquery: None,
        set_op: None,
        distinct: false,
        columns: vec![Column::All],
        from: "orders_ex1".into(),
        from_alias: None,
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
        ctes: Vec::new(),
        from_subquery: None,
        set_op: None,
        distinct: false,
        from: "users_ex1".into(),
        from_alias: None,
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
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
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
                name: "name".into(),
                data_type: DataType::Text,
                nullable: false,
            },
        ],
        constraints: vec![],
        as_query: None,
        if_not_exists: false,
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "users_ex2".to_string(),
        columns: Some(vec!["id".into(), "name".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Text("Alice".into())],
            vec![Value::Integer(2), Value::Text("Bob".into())],
        ],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "orders_ex2".to_string(),
        columns: vec![
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
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
                name: "amount".into(),
                data_type: DataType::Float,
                nullable: false,
            },
        ],
        constraints: vec![],
        as_query: None,
        if_not_exists: false,
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "orders_ex2".to_string(),
        columns: Some(vec!["id".into(), "amount".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Float(150.0)],
            vec![Value::Integer(2), Value::Float(50.0)],
        ],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    let sub = SelectStatement {
        ctes: Vec::new(),
        from_subquery: None,
        set_op: None,
        distinct: false,
        columns: vec![Column::All],
        from: "orders_ex2".into(),
        from_alias: None,
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
        ctes: Vec::new(),
        from_subquery: None,
        set_op: None,
        distinct: false,
        from: "users_ex2".into(),
        from_alias: None,
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
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
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
                name: "name".into(),
                data_type: DataType::Text,
                nullable: false,
            },
        ],
        constraints: vec![],
        as_query: None,
        if_not_exists: false,
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "t_users_ne1".to_string(),
        columns: Some(vec!["id".into(), "name".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Text("Alice".into())],
            vec![Value::Integer(2), Value::Text("Bob".into())],
        ],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "t_orders_ne1".to_string(),
        columns: vec![
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
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
                name: "amount".into(),
                data_type: DataType::Float,
                nullable: false,
            },
        ],
        constraints: vec![],
        as_query: None,
        if_not_exists: false,
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "t_orders_ne1".to_string(),
        columns: Some(vec!["id".into(), "amount".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Float(150.0)],
            vec![Value::Integer(2), Value::Float(50.0)],
        ],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    let sub = SelectStatement {
        ctes: Vec::new(),
        from_subquery: None,
        set_op: None,
        distinct: false,
        columns: vec![Column::All],
        from: "t_orders_ne1".into(),
        from_alias: None,
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
        ctes: Vec::new(),
        from_subquery: None,
        set_op: None,
        distinct: false,
        from: "t_users_ne1".into(),
        from_alias: None,
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
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
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
                name: "name".into(),
                data_type: DataType::Text,
                nullable: false,
            },
        ],
        constraints: vec![],
        as_query: None,
        if_not_exists: false,
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "users_corr1".to_string(),
        columns: Some(vec!["id".into(), "name".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Text("Alice".into())],
            vec![Value::Integer(2), Value::Text("Bob".into())],
        ],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "orders_corr1".to_string(),
        columns: vec![
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                name: "user_id".into(),
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
                name: "amount".into(),
                data_type: DataType::Float,
                nullable: false,
            },
        ],
        constraints: vec![],
        as_query: None,
        if_not_exists: false,
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "orders_corr1".to_string(),
        columns: Some(vec!["user_id".into(), "amount".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Float(150.0)],
            vec![Value::Integer(2), Value::Float(50.0)],
        ],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    let sub = SelectStatement {
        ctes: Vec::new(),
        from_subquery: None,
        set_op: None,
        distinct: false,
        columns: vec![Column::All],
        from: "orders_corr1".into(),
        from_alias: None,
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
        ctes: Vec::new(),
        from_subquery: None,
        set_op: None,
        distinct: false,
        from: "users_corr1".into(),
        from_alias: None,
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

#[test]
fn test_where_not_exists_false() {
    execute(Statement::CreateTable(CreateTableStatement {
        name: "t_users_ne2".to_string(),
        columns: vec![
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
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
                name: "name".into(),
                data_type: DataType::Text,
                nullable: false,
            },
        ],
        constraints: vec![],
        as_query: None,
        if_not_exists: false,
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "t_users_ne2".to_string(),
        columns: Some(vec!["id".into(), "name".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Text("Alice".into())],
            vec![Value::Integer(2), Value::Text("Bob".into())],
        ],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "t_orders_ne2".to_string(),
        columns: vec![
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
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
                name: "amount".into(),
                data_type: DataType::Float,
                nullable: false,
            },
        ],
        constraints: vec![],
        as_query: None,
        if_not_exists: false,
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "t_orders_ne2".to_string(),
        columns: Some(vec!["id".into(), "amount".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Float(150.0)],
            vec![Value::Integer(2), Value::Float(50.0)],
        ],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    let sub = SelectStatement {
        ctes: Vec::new(),
        from_subquery: None,
        set_op: None,
        distinct: false,
        columns: vec![Column::All],
        from: "t_orders_ne2".into(),
        from_alias: None,
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
        ctes: Vec::new(),
        from_subquery: None,
        set_op: None,
        distinct: false,
        from: "t_users_ne2".into(),
        from_alias: None,
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
    assert!(!output.contains("Alice"));
    assert!(!output.contains("Bob"));
}

#[test]
fn test_where_exists_correlated_false() {
    execute(Statement::CreateTable(CreateTableStatement {
        name: "users_corr2".to_string(),
        columns: vec![
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
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
                name: "name".into(),
                data_type: DataType::Text,
                nullable: false,
            },
        ],
        constraints: vec![],
        as_query: None,
        if_not_exists: false,
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "users_corr2".to_string(),
        columns: Some(vec!["id".into(), "name".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Text("Alice".into())],
            vec![Value::Integer(2), Value::Text("Bob".into())],
        ],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "orders_corr2".to_string(),
        columns: vec![
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                name: "user_id".into(),
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
                name: "amount".into(),
                data_type: DataType::Float,
                nullable: false,
            },
        ],
        constraints: vec![],
        as_query: None,
        if_not_exists: false,
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "orders_corr2".to_string(),
        columns: Some(vec!["user_id".into(), "amount".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Float(50.0)],
            vec![Value::Integer(2), Value::Float(30.0)],
        ],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    let sub = SelectStatement {
        ctes: Vec::new(),
        from_subquery: None,
        set_op: None,
        distinct: false,
        columns: vec![Column::All],
        from: "orders_corr2".into(),
        from_alias: None,
        joins: vec![],
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column("user_id".into())),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Column("users_corr2.id".into())),
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
        ctes: Vec::new(),
        from_subquery: None,
        set_op: None,
        distinct: false,
        from: "users_corr2".into(),
        from_alias: None,
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
fn test_where_exists_with_join() {
    execute(Statement::CreateTable(CreateTableStatement {
        name: "users_join1".to_string(),
        columns: vec![
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
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
                name: "name".into(),
                data_type: DataType::Text,
                nullable: false,
            },
        ],
        constraints: vec![],
        as_query: None,
        if_not_exists: false,
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "users_join1".to_string(),
        columns: Some(vec!["id".into(), "name".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Text("Alice".into())],
            vec![Value::Integer(2), Value::Text("Bob".into())],
        ],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "orders_join1".to_string(),
        columns: vec![
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
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
                name: "user_id".into(),
                data_type: DataType::Integer,
                nullable: false,
            },
        ],
        constraints: vec![],
        as_query: None,
        if_not_exists: false,
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "orders_join1".to_string(),
        columns: Some(vec!["id".into(), "user_id".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Integer(1)],
            vec![Value::Integer(2), Value::Integer(1)],
        ],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "payments_join1".to_string(),
        columns: vec![
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
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
                name: "order_id".into(),
                data_type: DataType::Integer,
                nullable: false,
            },
        ],
        constraints: vec![],
        as_query: None,
        if_not_exists: false,
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "payments_join1".to_string(),
        columns: Some(vec!["id".into(), "order_id".into()]),
        values: vec![vec![Value::Integer(1), Value::Integer(1)]],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    let sub = SelectStatement {
        ctes: Vec::new(),
        from_subquery: None,
        set_op: None,
        distinct: false,
        columns: vec![Column::All],
        from: "orders_join1".into(),
        from_alias: None,
        joins: vec![Join {
            join_type: JoinType::Inner,
            table: "payments_join1".to_string(),
            table_alias: None,
            on: Some(Expression::BinaryOp {
                left: Box::new(Expression::Column("orders_join1.id".into())),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Column("payments_join1.order_id".into())),
            }),
        }],
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::Column("orders_join1.user_id".into())),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Column("users_join1.id".into())),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let stmt = Statement::Select(SelectStatement {
        ctes: Vec::new(),
        from_subquery: None,
        set_op: None,
        distinct: false,
        from: "users_join1".into(),
        from_alias: None,
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
