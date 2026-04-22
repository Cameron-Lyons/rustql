mod common;
use common::*;
use rustql::ast::*;
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup_test() -> std::sync::MutexGuard<'static, ()> {
    let guard = TEST_MUTEX.lock().unwrap();
    reset_database();
    guard
}

#[test]
fn test_scalar_subquery_basic() {
    let _guard = setup_test();
    execute(Statement::CreateTable(CreateTableStatement {
        name: "users_scalar1".to_string(),
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
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "users_scalar1".to_string(),
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
        name: "orders_scalar1".to_string(),
        columns: vec![
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
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
                generated: None,
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
        table: "orders_scalar1".to_string(),
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

    let stmt = Statement::Select(SelectStatement {
        ctes: Vec::new(),
        from_subquery: None,
        set_op: None,
        distinct: false,
        distinct_on: None,
        from: "users_scalar1".into(),
        from_alias: None,
        from_function: None,
        columns: vec![
            Column::Named {
                name: "name".into(),
                alias: None,
            },
            Column::Subquery(Box::new(SelectStatement {
                ctes: Vec::new(),
                from_subquery: None,
                set_op: None,
                distinct: false,
                distinct_on: None,
                columns: vec![Column::Named {
                    name: "amount".into(),
                    alias: None,
                }],
                from: "orders_scalar1".into(),
                from_alias: None,
                from_function: None,
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
                fetch: None,
                window_definitions: Vec::new(),
                from_values: None,
            })),
        ],
        joins: vec![],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        fetch: None,
        window_definitions: Vec::new(),
        from_values: None,
    });

    let output = execute(stmt).unwrap();
    assert!(output.contains("name"));
    assert!(output.contains("Alice"));
    assert!(output.contains("150"));
}

#[test]
fn test_scalar_subquery_null() {
    let _guard = setup_test();
    execute(Statement::CreateTable(CreateTableStatement {
        name: "users_scalar2".to_string(),
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
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "users_scalar2".to_string(),
        columns: Some(vec!["id".into(), "name".into()]),
        values: vec![vec![Value::Integer(1), Value::Text("Alice".into())]],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "orders_scalar2".to_string(),
        columns: vec![
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
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
                generated: None,
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

    let stmt = Statement::Select(SelectStatement {
        ctes: Vec::new(),
        from_subquery: None,
        set_op: None,
        distinct: false,
        distinct_on: None,
        from: "users_scalar2".into(),
        from_alias: None,
        from_function: None,
        columns: vec![
            Column::Named {
                name: "name".into(),
                alias: None,
            },
            Column::Subquery(Box::new(SelectStatement {
                ctes: Vec::new(),
                from_subquery: None,
                set_op: None,
                distinct: false,
                distinct_on: None,
                columns: vec![Column::Named {
                    name: "amount".into(),
                    alias: None,
                }],
                from: "orders_scalar2".into(),
                from_alias: None,
                from_function: None,
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
                fetch: None,
                window_definitions: Vec::new(),
                from_values: None,
            })),
        ],
        joins: vec![],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        fetch: None,
        window_definitions: Vec::new(),
        from_values: None,
    });

    let output = execute(stmt).unwrap();
    assert!(output.contains("name"));
    assert!(output.contains("Alice"));
    assert!(output.contains("NULL"));
}

#[test]
fn test_scalar_subquery_aggregate() {
    let _guard = setup_test();
    execute(Statement::CreateTable(CreateTableStatement {
        name: "users_agg".to_string(),
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
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "users_agg".to_string(),
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
        name: "orders_agg".to_string(),
        columns: vec![
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
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
                generated: None,
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
        table: "orders_agg".to_string(),
        columns: Some(vec!["user_id".into(), "amount".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Float(100.0)],
            vec![Value::Integer(1), Value::Float(50.0)],
            vec![Value::Integer(2), Value::Float(200.0)],
        ],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    let stmt = Statement::Select(SelectStatement {
        ctes: Vec::new(),
        from_subquery: None,
        set_op: None,
        distinct: false,
        distinct_on: None,
        from: "users_agg".into(),
        from_alias: None,
        from_function: None,
        columns: vec![
            Column::Named {
                name: "name".into(),
                alias: None,
            },
            Column::Subquery(Box::new(SelectStatement {
                ctes: Vec::new(),
                from_subquery: None,
                set_op: None,
                distinct: false,
                distinct_on: None,
                columns: vec![Column::Function(AggregateFunction {
                    function: AggregateFunctionType::Count,
                    expr: Box::new(Expression::Column("*".into())),
                    distinct: false,
                    alias: None,
                    separator: None,
                    percentile: None,
                    filter: None,
                })],
                from: "orders_agg".into(),
                from_alias: None,
                from_function: None,
                joins: vec![],
                where_clause: Some(Expression::BinaryOp {
                    left: Box::new(Expression::Column("user_id".into())),
                    op: BinaryOperator::Equal,
                    right: Box::new(Expression::Column("users_agg.id".into())),
                }),
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
                fetch: None,
                window_definitions: Vec::new(),
                from_values: None,
            })),
        ],
        joins: vec![],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        fetch: None,
        window_definitions: Vec::new(),
        from_values: None,
    });

    let output = execute(stmt).unwrap();
    assert!(output.contains("Alice"));
    assert!(output.contains("Bob"));
    assert!(output.contains("2")); // Alice has 2 orders
    assert!(output.contains("1")); // Bob has 1 order
}

#[test]
fn test_scalar_subquery_aggregate_sum() {
    let _guard = setup_test();
    execute(Statement::CreateTable(CreateTableStatement {
        name: "users_sum".to_string(),
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
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "users_sum".to_string(),
        columns: Some(vec!["id".into(), "name".into()]),
        values: vec![vec![Value::Integer(1), Value::Text("Alice".into())]],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "orders_sum".to_string(),
        columns: vec![
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
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
                generated: None,
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
        table: "orders_sum".to_string(),
        columns: Some(vec!["user_id".into(), "amount".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Float(100.0)],
            vec![Value::Integer(1), Value::Float(50.0)],
            vec![Value::Integer(1), Value::Float(25.0)],
        ],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    let stmt = Statement::Select(SelectStatement {
        ctes: Vec::new(),
        from_subquery: None,
        set_op: None,
        distinct: false,
        distinct_on: None,
        from: "users_sum".into(),
        from_alias: None,
        from_function: None,
        columns: vec![
            Column::Named {
                name: "name".into(),
                alias: None,
            },
            Column::Subquery(Box::new(SelectStatement {
                ctes: Vec::new(),
                from_subquery: None,
                set_op: None,
                distinct: false,
                distinct_on: None,
                columns: vec![Column::Function(AggregateFunction {
                    function: AggregateFunctionType::Sum,
                    expr: Box::new(Expression::Column("amount".into())),
                    distinct: false,
                    alias: None,
                    separator: None,
                    percentile: None,
                    filter: None,
                })],
                from: "orders_sum".into(),
                from_alias: None,
                from_function: None,
                joins: vec![],
                where_clause: Some(Expression::BinaryOp {
                    left: Box::new(Expression::Column("user_id".into())),
                    op: BinaryOperator::Equal,
                    right: Box::new(Expression::Column("users_sum.id".into())),
                }),
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
                fetch: None,
                window_definitions: Vec::new(),
                from_values: None,
            })),
        ],
        joins: vec![],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        fetch: None,
        window_definitions: Vec::new(),
        from_values: None,
    });

    let output = execute(stmt).unwrap();
    assert!(output.contains("Alice"));
    assert!(output.contains("175")); // 100 + 50 + 25
}

#[test]
fn test_scalar_subquery_nested() {
    let _guard = setup_test();
    execute(Statement::CreateTable(CreateTableStatement {
        name: "users_nested".to_string(),
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
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "users_nested".to_string(),
        columns: Some(vec!["id".into(), "name".into()]),
        values: vec![vec![Value::Integer(1), Value::Text("Alice".into())]],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "orders_nested".to_string(),
        columns: vec![
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
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
                generated: None,
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
        table: "orders_nested".to_string(),
        columns: Some(vec!["user_id".into(), "amount".into()]),
        values: vec![vec![Value::Integer(1), Value::Float(150.0)]],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "items_nested".to_string(),
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
                name: "order_id".into(),
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
                name: "price".into(),
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
        table: "items_nested".to_string(),
        columns: Some(vec!["id".into(), "order_id".into(), "price".into()]),
        values: vec![vec![
            Value::Integer(1),
            Value::Integer(1),
            Value::Float(50.0),
        ]],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    let stmt = Statement::Select(SelectStatement {
        ctes: Vec::new(),
        from_subquery: None,
        set_op: None,
        distinct: false,
        distinct_on: None,
        from: "users_nested".into(),
        from_alias: None,
        from_function: None,
        columns: vec![
            Column::Named {
                name: "name".into(),
                alias: None,
            },
            Column::Subquery(Box::new(SelectStatement {
                ctes: Vec::new(),
                from_subquery: None,
                set_op: None,
                distinct: false,
                distinct_on: None,
                columns: vec![Column::Subquery(Box::new(SelectStatement {
                    ctes: Vec::new(),
                    from_subquery: None,
                    set_op: None,
                    distinct: false,
                    distinct_on: None,
                    columns: vec![Column::Named {
                        name: "price".into(),
                        alias: None,
                    }],
                    from: "items_nested".into(),
                    from_alias: None,
                    from_function: None,
                    joins: vec![],
                    where_clause: Some(Expression::BinaryOp {
                        left: Box::new(Expression::Column("order_id".into())),
                        op: BinaryOperator::Equal,
                        right: Box::new(Expression::Column("user_id".into())),
                    }),
                    group_by: None,
                    having: None,
                    order_by: None,
                    limit: None,
                    offset: None,
                    fetch: None,
                    window_definitions: Vec::new(),
                    from_values: None,
                }))],
                from: "orders_nested".into(),
                from_alias: None,
                from_function: None,
                joins: vec![],
                where_clause: Some(Expression::BinaryOp {
                    left: Box::new(Expression::Column("user_id".into())),
                    op: BinaryOperator::Equal,
                    right: Box::new(Expression::Column("users_nested.id".into())),
                }),
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
                fetch: None,
                window_definitions: Vec::new(),
                from_values: None,
            })),
        ],
        joins: vec![],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        fetch: None,
        window_definitions: Vec::new(),
        from_values: None,
    });

    let output = execute(stmt).unwrap();
    assert!(output.contains("name"));
    assert!(output.contains("Alice"));
    assert!(output.contains("50")); // The nested subquery returns 50.0
}

#[test]
fn test_scalar_subquery_with_join() {
    let _guard = setup_test();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "users_join".to_string(),
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
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "users_join".to_string(),
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
        name: "orders_join".to_string(),
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
                generated: None,
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
        table: "orders_join".to_string(),
        columns: Some(vec!["id".into(), "user_id".into(), "amount".into()]),
        values: vec![
            vec![Value::Integer(10), Value::Integer(1), Value::Float(150.0)],
            vec![Value::Integer(11), Value::Integer(2), Value::Float(50.0)],
        ],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    let stmt = Statement::Select(SelectStatement {
        ctes: Vec::new(),
        from_subquery: None,
        set_op: None,
        distinct: false,
        distinct_on: None,
        from: "users_join".into(),
        from_alias: None,
        from_function: None,
        columns: vec![
            Column::Named {
                name: "name".into(),
                alias: None,
            },
            Column::Subquery(Box::new(SelectStatement {
                ctes: Vec::new(),
                from_subquery: None,
                set_op: None,
                distinct: false,
                distinct_on: None,
                columns: vec![Column::Named {
                    name: "amount".into(),
                    alias: None,
                }],
                from: "orders_join".into(),
                from_alias: None,
                from_function: None,
                joins: vec![Join {
                    join_type: JoinType::Inner,
                    table: "users_join".into(),
                    table_alias: None,
                    on: Some(Expression::BinaryOp {
                        left: Box::new(Expression::Column("orders_join.user_id".into())),
                        op: BinaryOperator::Equal,
                        right: Box::new(Expression::Column("users_join.id".into())),
                    }),
                    using_columns: None,
                    lateral: false,
                    subquery: None,
                }],
                where_clause: Some(Expression::BinaryOp {
                    left: Box::new(Expression::Column("orders_join.user_id".into())),
                    op: BinaryOperator::Equal,
                    right: Box::new(Expression::Column("users_join.id".into())),
                }),
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
                fetch: None,
                window_definitions: Vec::new(),
                from_values: None,
            })),
        ],
        joins: vec![],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        fetch: None,
        window_definitions: Vec::new(),
        from_values: None,
    });

    let output = execute(stmt).unwrap();
    assert!(output.contains("Alice"));
    assert!(output.contains("150"));
    assert!(output.contains("Bob"));
    assert!(output.contains("50"));
}

#[test]
fn test_scalar_subquery_with_join_and_aggregate() {
    let _guard = setup_test();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "users_join_agg".to_string(),
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
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "users_join_agg".to_string(),
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
        name: "orders_join_agg".to_string(),
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
                generated: None,
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
        table: "orders_join_agg".to_string(),
        columns: Some(vec!["id".into(), "user_id".into(), "amount".into()]),
        values: vec![
            vec![Value::Integer(10), Value::Integer(1), Value::Float(100.0)],
            vec![Value::Integer(11), Value::Integer(1), Value::Float(50.0)],
            vec![Value::Integer(12), Value::Integer(2), Value::Float(200.0)],
        ],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "products_join_agg".to_string(),
        columns: vec![
            ColumnDefinition {
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
                name: "order_id".into(),
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
                name: "price".into(),
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
        table: "products_join_agg".to_string(),
        columns: Some(vec!["order_id".into(), "price".into()]),
        values: vec![
            vec![Value::Integer(10), Value::Float(10.0)],
            vec![Value::Integer(11), Value::Float(20.0)],
            vec![Value::Integer(12), Value::Float(30.0)],
        ],
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    let stmt = Statement::Select(SelectStatement {
        ctes: Vec::new(),
        from_subquery: None,
        set_op: None,
        distinct: false,
        distinct_on: None,
        from: "users_join_agg".into(),
        from_alias: None,
        from_function: None,
        columns: vec![
            Column::Named {
                name: "name".into(),
                alias: None,
            },
            Column::Subquery(Box::new(SelectStatement {
                ctes: Vec::new(),
                from_subquery: None,
                set_op: None,
                distinct: false,
                distinct_on: None,
                columns: vec![Column::Function(AggregateFunction {
                    function: AggregateFunctionType::Sum,
                    expr: Box::new(Expression::Column("products_join_agg.price".into())),
                    distinct: false,
                    alias: None,
                    separator: None,
                    percentile: None,
                    filter: None,
                })],
                from: "orders_join_agg".into(),
                from_alias: None,
                from_function: None,
                joins: vec![Join {
                    join_type: JoinType::Inner,
                    table: "products_join_agg".into(),
                    table_alias: None,
                    on: Some(Expression::BinaryOp {
                        left: Box::new(Expression::Column("orders_join_agg.id".into())),
                        op: BinaryOperator::Equal,
                        right: Box::new(Expression::Column("products_join_agg.order_id".into())),
                    }),
                    using_columns: None,
                    lateral: false,
                    subquery: None,
                }],
                where_clause: Some(Expression::BinaryOp {
                    left: Box::new(Expression::Column("orders_join_agg.user_id".into())),
                    op: BinaryOperator::Equal,
                    right: Box::new(Expression::Column("users_join_agg.id".into())),
                }),
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
                fetch: None,
                window_definitions: Vec::new(),
                from_values: None,
            })),
        ],
        joins: vec![],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        fetch: None,
        window_definitions: Vec::new(),
        from_values: None,
    });

    let output = execute(stmt).unwrap();
    assert!(output.contains("Alice"));
    assert!(output.contains("30")); // 10 + 20 = 30 (sum of products for Alice's orders)
    assert!(output.contains("Bob"));
    assert!(output.contains("30")); // 30 (sum of products for Bob's orders)
}
