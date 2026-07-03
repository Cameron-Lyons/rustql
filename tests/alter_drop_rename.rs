mod common;
use common::*;
use rustql::ast::Value;
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup_test() -> std::sync::MutexGuard<'static, ()> {
    let guard = TEST_MUTEX.lock().unwrap();
    reset_database();
    guard
}

#[test]
fn test_alter_drop_column_basic() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT, age INTEGER)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'Alice', 25)").unwrap();

    execute_sql("ALTER TABLE t DROP COLUMN age").unwrap();

    let result = execute_sql("SELECT * FROM t").unwrap();
    assert!(result.contains("id"));
    assert!(result.contains("name"));
    assert!(!result.contains("age"));
    assert!(result.contains("Alice"));
    assert!(!result.contains("25"));
}

#[test]
fn test_alter_drop_column_data_preserved() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (a INTEGER, b TEXT, c INTEGER)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'hello', 100)").unwrap();
    execute_sql("INSERT INTO t VALUES (2, 'world', 200)").unwrap();

    execute_sql("ALTER TABLE t DROP COLUMN b").unwrap();

    let result = execute_sql("SELECT * FROM t").unwrap();
    assert!(result.contains("1"));
    assert!(result.contains("100"));
    assert!(result.contains("2"));
    assert!(result.contains("200"));
    assert!(!result.contains("hello"));
    assert!(!result.contains("world"));
}

#[test]
fn test_alter_drop_column_select_dropped_errors() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT, age INTEGER)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'Alice', 25)").unwrap();

    execute_sql("ALTER TABLE t DROP COLUMN age").unwrap();

    let result = execute_sql("SELECT age FROM t");
    assert!(result.is_err());
}

#[test]
fn test_alter_drop_column_nonexistent() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();

    let result = execute_sql("ALTER TABLE t DROP COLUMN nonexistent");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("does not exist"));
}

#[test]
fn test_alter_rename_column_basic() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();

    execute_sql("ALTER TABLE t RENAME COLUMN name TO username").unwrap();

    let result = execute_sql("SELECT username FROM t").unwrap();
    assert!(result.contains("Alice"));
}

#[test]
fn test_alter_rename_column_old_name_fails() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();

    execute_sql("ALTER TABLE t RENAME COLUMN name TO username").unwrap();

    let result = execute_sql("SELECT name FROM t");
    assert!(result.is_err());
}

#[test]
fn test_alter_rename_column_data_preserved() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    execute_sql("ALTER TABLE t RENAME COLUMN name TO username").unwrap();

    let result = execute_sql("SELECT * FROM t").unwrap();
    assert!(result.contains("username"));
    assert!(!result.contains("\tname"));
    assert!(result.contains("Alice"));
    assert!(result.contains("Bob"));
}

#[test]
fn test_alter_rename_to_existing_name_errors() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();

    let result = execute_sql("ALTER TABLE t RENAME COLUMN name TO id");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("already exists"));
}

#[test]
fn test_alter_rename_nonexistent_column_errors() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();

    let result = execute_sql("ALTER TABLE t RENAME COLUMN nonexistent TO something");
    assert!(result.is_err());
}

#[test]
fn test_alter_rename_column_updates_index_metadata() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE rename_idx (id INTEGER, name TEXT, age INTEGER)").unwrap();
    execute_sql("INSERT INTO rename_idx VALUES (1, 'Alice', 25)").unwrap();
    execute_sql("CREATE INDEX idx_rename_name ON rename_idx (name)").unwrap();
    execute_sql("CREATE INDEX idx_rename_name_age ON rename_idx (name, age)").unwrap();

    execute_sql("ALTER TABLE rename_idx RENAME COLUMN name TO username").unwrap();
    execute_sql("INSERT INTO rename_idx VALUES (2, 'Bob', 30)").unwrap();

    let single_plan =
        execute_sql("EXPLAIN SELECT id FROM rename_idx WHERE username = 'Bob'").unwrap();
    assert!(
        single_plan.contains("Index Scan using idx_rename_name"),
        "{single_plan:?}"
    );

    let composite_plan =
        execute_sql("EXPLAIN SELECT id FROM rename_idx WHERE username = 'Bob' AND age = 30")
            .unwrap();
    assert!(
        composite_plan.contains("Index Scan using idx_rename_name_age"),
        "{composite_plan:?}"
    );

    assert_rows(
        "SELECT id FROM rename_idx WHERE username = 'Bob' AND age = 30",
        &["id"],
        vec![vec![Value::Integer(2)]],
    );
}

#[test]
fn test_alter_rename_column_updates_partial_index_filter() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE rename_partial_idx (id INTEGER, name TEXT, age INTEGER)").unwrap();
    execute_sql("INSERT INTO rename_partial_idx VALUES (1, 'Alice', 25)").unwrap();
    execute_sql(
        "CREATE INDEX idx_rename_partial_age ON rename_partial_idx (age) WHERE name = 'Alice'",
    )
    .unwrap();

    execute_sql("ALTER TABLE rename_partial_idx RENAME COLUMN name TO username").unwrap();
    execute_sql("INSERT INTO rename_partial_idx VALUES (2, 'Bob', 30)").unwrap();

    let plan = execute_sql(
        "EXPLAIN SELECT id FROM rename_partial_idx WHERE age = 25 AND username = 'Alice'",
    )
    .unwrap();
    assert!(
        plan.contains("Index Scan using idx_rename_partial_age"),
        "{plan:?}"
    );

    assert_rows(
        "SELECT id FROM rename_partial_idx WHERE age = 25 AND username = 'Alice'",
        &["id"],
        vec![vec![Value::Integer(1)]],
    );
}

#[test]
fn test_alter_rename_column_updates_table_constraints() {
    let _guard = setup_test();

    execute_sql(
        "CREATE TABLE rename_constraint (a INTEGER, b INTEGER, CONSTRAINT uq_ab UNIQUE (a, b))",
    )
    .unwrap();
    execute_sql("INSERT INTO rename_constraint VALUES (1, 1)").unwrap();

    execute_sql("ALTER TABLE rename_constraint RENAME COLUMN a TO x").unwrap();

    let duplicate = execute_sql("INSERT INTO rename_constraint VALUES (1, 1)");
    assert!(duplicate.is_err(), "{duplicate:?}");
    assert!(duplicate.unwrap_err().contains("UNIQUE constraint"));
}

#[test]
fn test_alter_rename_column_updates_foreign_key_references() {
    let _guard = setup_test();

    execute_sql("CREATE TABLE rename_fk_parent (id INTEGER, label TEXT)").unwrap();
    execute_sql("INSERT INTO rename_fk_parent VALUES (1, 'parent')").unwrap();
    execute_sql(
        "CREATE TABLE rename_fk_child (
            id INTEGER,
            parent_id INTEGER FOREIGN KEY REFERENCES rename_fk_parent(id)
        )",
    )
    .unwrap();

    execute_sql("ALTER TABLE rename_fk_parent RENAME COLUMN id TO parent_key").unwrap();

    let valid = execute_sql("INSERT INTO rename_fk_child VALUES (1, 1)");
    assert!(valid.is_ok(), "{valid:?}");

    let invalid = execute_sql("INSERT INTO rename_fk_child VALUES (2, 999)");
    assert!(invalid.is_err(), "{invalid:?}");
    assert!(invalid.unwrap_err().contains("Foreign key constraint"));
}

#[test]
fn test_alter_rename_column_rolls_back_schema_references() {
    let _guard = setup_test();

    execute_sql(
        "CREATE TABLE rollback_rename (
            id INTEGER,
            name TEXT,
            CONSTRAINT uq_rollback_name UNIQUE (name)
        )",
    )
    .unwrap();
    execute_sql("INSERT INTO rollback_rename VALUES (1, 'Alice')").unwrap();
    execute_sql("CREATE INDEX idx_rollback_name ON rollback_rename (name)").unwrap();

    execute_sql("BEGIN TRANSACTION").unwrap();
    execute_sql("ALTER TABLE rollback_rename RENAME COLUMN name TO username").unwrap();
    execute_sql("ROLLBACK").unwrap();

    let duplicate = execute_sql("INSERT INTO rollback_rename VALUES (2, 'Alice')");
    assert!(duplicate.is_err(), "{duplicate:?}");

    execute_sql("INSERT INTO rollback_rename VALUES (3, 'Bob')").unwrap();
    let plan = execute_sql("EXPLAIN SELECT id FROM rollback_rename WHERE name = 'Bob'").unwrap();
    assert!(
        plan.contains("Index Scan using idx_rollback_name"),
        "{plan:?}"
    );
}
