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
fn test_select_all() {
    let _guard = setup_test();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "users".to_string(),
        columns: vec![
            ColumnDefinition {
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
            },
            ColumnDefinition {
                name: "name".into(),
                data_type: DataType::Text,
                nullable: false,
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
            },
        ],
        constraints: vec![],
        as_query: None,
        if_not_exists: false,
    }))
    .unwrap();

    execute(Statement::Insert(InsertStatement {
        table: "users".to_string(),
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

    let select = Statement::Select(SelectStatement {
        ctes: Vec::new(),
        from_subquery: None,
        set_op: None,
        distinct: false,
        distinct_on: None,
        from: "users".into(),
        from_alias: None,
        from_function: None,
        columns: vec![Column::All],
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
                name: "city".into(),
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
        table: "cities".into(),
        columns: Some(vec!["id".into(), "city".into()]),
        values: vec![
            vec![Value::Integer(1), Value::Text("Paris".into())],
            vec![Value::Integer(2), Value::Text("Paris".into())],
            vec![Value::Integer(3), Value::Text("London".into())],
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
        from: "cities".into(),
        from_alias: None,
        from_function: None,
        columns: vec![Column::Function(AggregateFunction {
            function: AggregateFunctionType::Count,
            expr: Box::new(Expression::Column("city".into())),
            distinct: true,
            alias: Some("unique_cities".into()),
            separator: None,
            percentile: None,
            filter: None,
        })],
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
    assert!(output.contains("2"));
}

#[test]
fn test_sum_distinct_values() {
    let _guard = setup_test();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "purchases".to_string(),
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
                name: "amount".into(),
                data_type: DataType::Float,
                nullable: true,
            },
        ],
        constraints: vec![],
        as_query: None,
        if_not_exists: false,
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
        from: "purchases".into(),
        from_alias: None,
        from_function: None,
        columns: vec![Column::Function(AggregateFunction {
            function: AggregateFunctionType::Sum,
            expr: Box::new(Expression::Column("amount".into())),
            distinct: true,
            alias: Some("distinct_sum".into()),
            separator: None,
            percentile: None,
            filter: None,
        })],
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
    assert!(output.contains("30"));
}

#[test]
fn test_min_distinct_values() {
    let _guard = setup_test();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "scores".to_string(),
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
                name: "score".into(),
                data_type: DataType::Integer,
                nullable: true,
            },
        ],
        constraints: vec![],
        as_query: None,
        if_not_exists: false,
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
        from: "scores".into(),
        from_alias: None,
        from_function: None,
        columns: vec![Column::Function(AggregateFunction {
            function: AggregateFunctionType::Min,
            expr: Box::new(Expression::Column("score".into())),
            distinct: true,
            alias: Some("min_distinct_score".into()),
            separator: None,
            percentile: None,
            filter: None,
        })],
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

    assert!(output.contains("50"));
}

#[test]
fn test_max_distinct_values() {
    let _guard = setup_test();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "prices".to_string(),
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
                name: "price".into(),
                data_type: DataType::Float,
                nullable: true,
            },
        ],
        constraints: vec![],
        as_query: None,
        if_not_exists: false,
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
        from: "prices".into(),
        from_alias: None,
        from_function: None,
        columns: vec![Column::Function(AggregateFunction {
            function: AggregateFunctionType::Max,
            expr: Box::new(Expression::Column("price".into())),
            distinct: true,
            alias: Some("max_distinct_price".into()),
            separator: None,
            percentile: None,
            filter: None,
        })],
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

    assert!(output.contains("50"));
}

#[test]
fn test_avg_distinct_values() {
    let _guard = setup_test();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "grades".to_string(),
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
                name: "grade".into(),
                data_type: DataType::Float,
                nullable: true,
            },
        ],
        constraints: vec![],
        as_query: None,
        if_not_exists: false,
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
        from: "grades".into(),
        from_alias: None,
        from_function: None,
        columns: vec![Column::Function(AggregateFunction {
            function: AggregateFunctionType::Avg,
            expr: Box::new(Expression::Column("grade".into())),
            distinct: true,
            alias: Some("avg_distinct_grade".into()),
            separator: None,
            percentile: None,
            filter: None,
        })],
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

    assert!(output.contains("90"));
}
