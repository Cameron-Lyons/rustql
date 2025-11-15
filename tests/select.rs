use rustql::ast::*;
use rustql::executor::{execute, reset_database_state};

#[test]
fn test_select_all() {
    reset_database_state();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "users".to_string(),
        columns: vec![
            ColumnDefinition {
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
            },
            ColumnDefinition {
                name: "name".into(),
                data_type: DataType::Text,
                nullable: false,
            },
        ],
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "users".to_string(),
        columns: Some(vec!["id".into(), "name".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Text("Alice".into())],
            vec![Value::Integer(2), Value::Text("Bob".into())],
        ],
    }))
    .unwrap();

    let select = Statement::Select(SelectStatement {
        distinct: false,
        from: "users".into(),
        columns: vec![Column::All],
        joins: vec![],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    let output = execute(select).unwrap();
    assert!(output.contains("Alice"));
    assert!(output.contains("Bob"));
}

#[test]
fn test_count_distinct_values() {
    reset_database_state();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "cities".to_string(),
        columns: vec![
            ColumnDefinition {
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
            },
            ColumnDefinition {
                name: "city".into(),
                data_type: DataType::Text,
                nullable: false,
            },
        ],
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "cities".into(),
        columns: Some(vec!["id".into(), "city".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Text("Paris".into())],
            vec![Value::Integer(2), Value::Text("Paris".into())],
            vec![Value::Integer(3), Value::Text("London".into())],
        ],
    }))
    .unwrap();

    let stmt = Statement::Select(SelectStatement {
        distinct: false,
        from: "cities".into(),
        columns: vec![Column::Function(AggregateFunction {
            function: AggregateFunctionType::Count,
            expr: Box::new(Expression::Column("city".into())),
            distinct: true,
            alias: Some("unique_cities".into()),
        })],
        joins: vec![],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    let output = execute(stmt).unwrap();
    assert!(output.contains("2"));
}

#[test]
fn test_sum_distinct_values() {
    reset_database_state();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "purchases".to_string(),
        columns: vec![
            ColumnDefinition {
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
            },
            ColumnDefinition {
                name: "amount".into(),
                data_type: DataType::Float,
                nullable: false,
            },
        ],
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "purchases".into(),
        columns: Some(vec!["id".into(), "amount".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Float(10.0)],
            vec![Value::Integer(2), Value::Float(10.0)],
            vec![Value::Integer(3), Value::Float(20.0)],
            vec![Value::Integer(4), Value::Null],
        ],
    }))
    .unwrap();

    let stmt = Statement::Select(SelectStatement {
        distinct: false,
        from: "purchases".into(),
        columns: vec![Column::Function(AggregateFunction {
            function: AggregateFunctionType::Sum,
            expr: Box::new(Expression::Column("amount".into())),
            distinct: true,
            alias: Some("distinct_sum".into()),
        })],
        joins: vec![],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    let output = execute(stmt).unwrap();
    assert!(output.contains("30"));
}
