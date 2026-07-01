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
fn test_update_and_delete() {
    let _guard = setup_test();

    execute(Statement::CreateTable(CreateTableStatement {
        name: "users".into(),
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
        table: "users".into(),
        columns: Some(vec!["id".into(), "name".into()]),
        values: insert_values(vec![vec![Value::Integer(1), Value::Text("Alice".into())]]),
        source_query: None,
        on_conflict: None,
        returning: None,
    }))
    .unwrap();

    let update = Statement::Update(UpdateStatement {
        table: "users".into(),
        assignments: vec![Assignment {
            column: "name".into(),
            value: Expression::Value(Value::Text("Alicia".into())),
        }],
        where_clause: None,
        from: None,
        returning: None,
    });

    assert_command(execute(update).unwrap(), CommandTag::Update, 1);

    let delete = Statement::Delete(DeleteStatement {
        table: "users".into(),
        where_clause: None,
        using: None,
        returning: None,
    });

    assert_command(execute(delete).unwrap(), CommandTag::Delete, 1);
}

#[test]
fn test_indexed_not_predicate_update_and_delete() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE upd_not (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO upd_not VALUES (1, 'one'), (2, 'two'), (3, 'three')").unwrap();
    execute_sql("CREATE INDEX idx_upd_not_id ON upd_not (id)").unwrap();

    assert_command_sql(
        "UPDATE upd_not SET name = 'updated' WHERE NOT (id = 1)",
        CommandTag::Update,
        2,
    );

    let updated = query_rows("SELECT id, name FROM upd_not ORDER BY id").unwrap();
    updated.assert_columns(&["id", "name"]);
    assert_eq!(
        updated.rows,
        vec![
            vec![Value::Integer(1), Value::Text("one".into())],
            vec![Value::Integer(2), Value::Text("updated".into())],
            vec![Value::Integer(3), Value::Text("updated".into())],
        ]
    );

    execute_sql("CREATE TABLE del_not (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO del_not VALUES (1, 'one'), (2, 'two'), (3, 'three')").unwrap();
    execute_sql("CREATE INDEX idx_del_not_id ON del_not (id)").unwrap();

    assert_command_sql(
        "DELETE FROM del_not WHERE NOT (id = 1)",
        CommandTag::Delete,
        2,
    );

    let remaining = query_rows("SELECT id, name FROM del_not ORDER BY id").unwrap();
    remaining.assert_columns(&["id", "name"]);
    assert_eq!(
        remaining.rows,
        vec![vec![Value::Integer(1), Value::Text("one".into())]]
    );
}

#[test]
fn test_update_set_default() {
    let _guard = setup_test();

    execute_sql(
        "CREATE TABLE update_defaults (
            id INTEGER,
            label TEXT DEFAULT 'fallback',
            qty INTEGER DEFAULT 9,
            note TEXT
        )",
    )
    .unwrap();
    execute_sql("INSERT INTO update_defaults VALUES (1, 'custom', 3, 'keep')").unwrap();

    assert_command_sql(
        "UPDATE update_defaults SET label = DEFAULT, qty = DEFAULT, note = DEFAULT WHERE id = 1",
        CommandTag::Update,
        1,
    );

    assert_rows(
        "SELECT label, qty, note FROM update_defaults WHERE id = 1",
        &["label", "qty", "note"],
        vec![vec![
            Value::Text("fallback".to_string()),
            Value::Integer(9),
            Value::Null,
        ]],
    );
}

#[test]
fn test_update_from_uses_source_rows() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE upd_from_target (id INTEGER, name TEXT, qty INTEGER)").unwrap();
    execute_sql("CREATE TABLE upd_from_source (id INTEGER, suffix TEXT, new_qty INTEGER)").unwrap();
    execute_sql("INSERT INTO upd_from_target VALUES (1, 'one', 1), (2, 'two', 2), (3, 'three', 3)")
        .unwrap();
    execute_sql("INSERT INTO upd_from_source VALUES (1, 'hot', 10), (3, 'cold', 30)").unwrap();

    assert_command_sql(
        "UPDATE upd_from_target
         SET name = name || '-' || suffix, qty = upd_from_source.new_qty
         FROM upd_from_source
         WHERE upd_from_target.id = upd_from_source.id",
        CommandTag::Update,
        2,
    );

    assert_rows(
        "SELECT id, name, qty FROM upd_from_target ORDER BY id",
        &["id", "name", "qty"],
        vec![
            vec![
                Value::Integer(1),
                Value::Text("one-hot".to_string()),
                Value::Integer(10),
            ],
            vec![
                Value::Integer(2),
                Value::Text("two".to_string()),
                Value::Integer(2),
            ],
            vec![
                Value::Integer(3),
                Value::Text("three-cold".to_string()),
                Value::Integer(30),
            ],
        ],
    );
}

#[test]
fn test_update_from_accepts_source_alias_and_returning_target() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE upd_from_alias_target (id INTEGER, qty INTEGER)").unwrap();
    execute_sql("CREATE TABLE upd_from_alias_source (target_id INTEGER, delta INTEGER)").unwrap();
    execute_sql("INSERT INTO upd_from_alias_target VALUES (1, 5), (2, 7)").unwrap();
    execute_sql("INSERT INTO upd_from_alias_source VALUES (1, 4), (2, 8)").unwrap();

    let rows = query_rows(
        "UPDATE upd_from_alias_target
         SET qty = qty + src.delta
         FROM upd_from_alias_source AS src
         WHERE upd_from_alias_target.id = src.target_id
         RETURNING id, qty",
    )
    .unwrap();

    rows.assert_columns(&["id", "qty"]);
    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Integer(1), Value::Integer(9)],
            vec![Value::Integer(2), Value::Integer(15)],
        ]
    );
}

#[test]
fn test_update_from_without_source_match_updates_no_rows() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE upd_from_nomatch_target (id INTEGER, qty INTEGER)").unwrap();
    execute_sql("CREATE TABLE upd_from_nomatch_source (id INTEGER, qty INTEGER)").unwrap();
    execute_sql("INSERT INTO upd_from_nomatch_target VALUES (1, 5), (2, 7)").unwrap();
    execute_sql("INSERT INTO upd_from_nomatch_source VALUES (3, 100)").unwrap();

    assert_command_sql(
        "UPDATE upd_from_nomatch_target
         SET qty = upd_from_nomatch_source.qty
         FROM upd_from_nomatch_source
         WHERE upd_from_nomatch_target.id = upd_from_nomatch_source.id",
        CommandTag::Update,
        0,
    );

    assert_rows(
        "SELECT id, qty FROM upd_from_nomatch_target ORDER BY id",
        &["id", "qty"],
        vec![
            vec![Value::Integer(1), Value::Integer(5)],
            vec![Value::Integer(2), Value::Integer(7)],
        ],
    );
}

#[test]
fn test_update_from_join_uses_joined_source_rows() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE upd_from_join_target (id INTEGER, qty INTEGER)").unwrap();
    execute_sql("CREATE TABLE upd_from_join_source (id INTEGER, qty INTEGER)").unwrap();
    execute_sql("CREATE TABLE upd_from_join_adjust (id INTEGER, delta INTEGER)").unwrap();
    execute_sql("INSERT INTO upd_from_join_target VALUES (1, 0), (2, 0), (3, 0)").unwrap();
    execute_sql("INSERT INTO upd_from_join_source VALUES (1, 10), (2, 20), (3, 30)").unwrap();
    execute_sql("INSERT INTO upd_from_join_adjust VALUES (1, 5), (3, 7)").unwrap();

    assert_command_sql(
        "UPDATE upd_from_join_target
         SET qty = src.qty + adj.delta
         FROM upd_from_join_source AS src
         JOIN upd_from_join_adjust AS adj ON src.id = adj.id
         WHERE upd_from_join_target.id = src.id",
        CommandTag::Update,
        2,
    );

    assert_rows(
        "SELECT id, qty FROM upd_from_join_target ORDER BY id",
        &["id", "qty"],
        vec![
            vec![Value::Integer(1), Value::Integer(15)],
            vec![Value::Integer(2), Value::Integer(0)],
            vec![Value::Integer(3), Value::Integer(37)],
        ],
    );
}

#[test]
fn test_update_from_left_join_null_extends_source_rows() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE upd_from_left_target (id INTEGER, label TEXT)").unwrap();
    execute_sql("CREATE TABLE upd_from_left_source (id INTEGER)").unwrap();
    execute_sql("CREATE TABLE upd_from_left_flags (id INTEGER, marker TEXT)").unwrap();
    execute_sql("INSERT INTO upd_from_left_target VALUES (1, 'old'), (2, 'old')").unwrap();
    execute_sql("INSERT INTO upd_from_left_source VALUES (1), (2)").unwrap();
    execute_sql("INSERT INTO upd_from_left_flags VALUES (1, 'hit')").unwrap();

    assert_command_sql(
        "UPDATE upd_from_left_target
         SET label = COALESCE(flag.marker, 'missing')
         FROM upd_from_left_source AS src
         LEFT JOIN upd_from_left_flags AS flag ON src.id = flag.id
         WHERE upd_from_left_target.id = src.id",
        CommandTag::Update,
        2,
    );

    assert_rows(
        "SELECT id, label FROM upd_from_left_target ORDER BY id",
        &["id", "label"],
        vec![
            vec![Value::Integer(1), Value::Text("hit".to_string())],
            vec![Value::Integer(2), Value::Text("missing".to_string())],
        ],
    );
}
