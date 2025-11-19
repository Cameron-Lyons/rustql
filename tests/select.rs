use rustql::ast::*;
use rustql::executor::{execute, reset_database_state};
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup_test() -> std::sync::MutexGuard<'static, ()> {
    let guard = TEST_MUTEX.lock().unwrap();
    reset_database_state();
    guard
}

#[test]
fn test_select_all() {
    let _guard = setup_test();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "users".to_string(),
        columns: vec![
            ColumnDefinition {
                foreign_key: None,
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
            },
            ColumnDefinition {
                foreign_key: None,
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
    let _guard = setup_test();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "cities".to_string(),
        columns: vec![
            ColumnDefinition {
                foreign_key: None,
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
            },
            ColumnDefinition {
                foreign_key: None,
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
    let _guard = setup_test();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "purchases".to_string(),
        columns: vec![
            ColumnDefinition {
                foreign_key: None,
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
            },
            ColumnDefinition {
                foreign_key: None,
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

#[test]
fn test_min_distinct_values() {
    let _guard = setup_test();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "scores".to_string(),
        columns: vec![
            ColumnDefinition {
                foreign_key: None,
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
            },
            ColumnDefinition {
                foreign_key: None,
                name: "score".into(),
                data_type: DataType::Integer,
                nullable: false,
            },
        ],
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "scores".into(),
        columns: Some(vec!["id".into(), "score".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Integer(100)],
            vec![Value::Integer(2), Value::Integer(100)],
            vec![Value::Integer(3), Value::Integer(50)],
            vec![Value::Integer(4), Value::Integer(75)],
            vec![Value::Integer(5), Value::Null],
        ],
    }))
    .unwrap();

    let stmt = Statement::Select(SelectStatement {
        distinct: false,
        from: "scores".into(),
        columns: vec![Column::Function(AggregateFunction {
            function: AggregateFunctionType::Min,
            expr: Box::new(Expression::Column("score".into())),
            distinct: true,
            alias: Some("min_distinct_score".into()),
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
    // MIN(DISTINCT score) should be 50 (ignoring duplicates of 100 and 75)
    assert!(output.contains("50"));
}

#[test]
fn test_max_distinct_values() {
    let _guard = setup_test();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "prices".to_string(),
        columns: vec![
            ColumnDefinition {
                foreign_key: None,
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
            },
            ColumnDefinition {
                foreign_key: None,
                name: "price".into(),
                data_type: DataType::Float,
                nullable: false,
            },
        ],
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "prices".into(),
        columns: Some(vec!["id".into(), "price".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Float(25.5)],
            vec![Value::Integer(2), Value::Float(25.5)],
            vec![Value::Integer(3), Value::Float(50.0)],
            vec![Value::Integer(4), Value::Float(30.0)],
            vec![Value::Integer(5), Value::Null],
        ],
    }))
    .unwrap();

    let stmt = Statement::Select(SelectStatement {
        distinct: false,
        from: "prices".into(),
        columns: vec![Column::Function(AggregateFunction {
            function: AggregateFunctionType::Max,
            expr: Box::new(Expression::Column("price".into())),
            distinct: true,
            alias: Some("max_distinct_price".into()),
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
    // MAX(DISTINCT price) should be 50.0 (ignoring duplicates of 25.5 and 30.0)
    assert!(output.contains("50"));
}

#[test]
fn test_avg_distinct_values() {
    let _guard = setup_test();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "grades".to_string(),
        columns: vec![
            ColumnDefinition {
                foreign_key: None,
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
            },
            ColumnDefinition {
                foreign_key: None,
                name: "grade".into(),
                data_type: DataType::Float,
                nullable: false,
            },
        ],
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "grades".to_string(),
        columns: Some(vec!["id".into(), "grade".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Float(85.0)],
            vec![Value::Integer(2), Value::Float(85.0)],
            vec![Value::Integer(3), Value::Float(90.0)],
            vec![Value::Integer(4), Value::Float(95.0)],
            vec![Value::Integer(5), Value::Null],
        ],
    }))
    .unwrap();

    let stmt = Statement::Select(SelectStatement {
        distinct: false,
        from: "grades".into(),
        columns: vec![Column::Function(AggregateFunction {
            function: AggregateFunctionType::Avg,
            expr: Box::new(Expression::Column("grade".into())),
            distinct: true,
            alias: Some("avg_distinct_grade".into()),
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
    // AVG(DISTINCT grade) should be (85.0 + 90.0 + 95.0) / 3 = 90.0 (ignoring duplicate 85.0 and NULL)
    assert!(output.contains("90"));
}
