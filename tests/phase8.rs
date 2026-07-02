mod common;
use common::*;
use rustql::ast::Value;
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup() -> std::sync::MutexGuard<'static, ()> {
    let guard = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
    reset_database();
    guard
}

#[test]
fn test_merge_when_matched_update() {
    let _g = setup();
    execute_sql("CREATE TABLE mt1 (tid INTEGER, name TEXT, val INTEGER)").unwrap();
    execute_sql("INSERT INTO mt1 VALUES (1, 'Alice', 10)").unwrap();
    execute_sql("INSERT INTO mt1 VALUES (2, 'Bob', 20)").unwrap();

    execute_sql("CREATE TABLE ms1 (sid INTEGER, new_val INTEGER)").unwrap();
    execute_sql("INSERT INTO ms1 VALUES (1, 100)").unwrap();
    execute_sql("INSERT INTO ms1 VALUES (2, 200)").unwrap();

    let result = execute_sql(
        "MERGE INTO mt1 USING ms1 ON tid = sid \
         WHEN MATCHED THEN UPDATE SET val = new_val",
    )
    .unwrap();
    assert!(
        result.contains("2"),
        "Expected 2 rows affected, got: {:?}",
        result
    );

    let result = execute_sql("SELECT tid, val FROM mt1 ORDER BY tid").unwrap();
    assert!(
        result.contains("100"),
        "Expected val=100, got: {:?}",
        result
    );
    assert!(
        result.contains("200"),
        "Expected val=200, got: {:?}",
        result
    );
}

#[test]
fn test_merge_when_matched_update_set_default() {
    let _g = setup();
    execute_sql("CREATE TABLE merge_default (id INTEGER, label TEXT DEFAULT 'fallback', qty INTEGER DEFAULT 5)").unwrap();
    execute_sql("INSERT INTO merge_default VALUES (1, 'custom', 99)").unwrap();
    execute_sql("CREATE TABLE merge_default_source (id INTEGER)").unwrap();
    execute_sql("INSERT INTO merge_default_source VALUES (1)").unwrap();

    execute_sql(
        "MERGE INTO merge_default USING merge_default_source ON merge_default.id = merge_default_source.id \
         WHEN MATCHED THEN UPDATE SET label = DEFAULT, qty = DEFAULT",
    )
    .unwrap();

    let result = execute_sql("SELECT label, qty FROM merge_default WHERE id = 1").unwrap();
    assert!(result.contains("fallback"), "got: {:?}", result);
    assert!(result.contains("5"), "got: {:?}", result);
    assert!(!result.contains("custom"), "got: {:?}", result);
    assert!(!result.contains("99"), "got: {:?}", result);
}

#[test]
fn test_merge_when_not_matched_insert() {
    let _g = setup();
    execute_sql("CREATE TABLE mt2 (tid INTEGER, name TEXT)").unwrap();
    execute_sql("INSERT INTO mt2 VALUES (1, 'Alice')").unwrap();

    execute_sql("CREATE TABLE ms2 (sid INTEGER, sname TEXT)").unwrap();
    execute_sql("INSERT INTO ms2 VALUES (1, 'Alice_new')").unwrap();
    execute_sql("INSERT INTO ms2 VALUES (2, 'Bob')").unwrap();

    execute_sql(
        "MERGE INTO mt2 USING ms2 ON tid = sid \
         WHEN NOT MATCHED THEN INSERT (tid, name) VALUES (sid, sname)",
    )
    .unwrap();

    let result = execute_sql("SELECT tid, name FROM mt2 ORDER BY tid").unwrap();
    assert!(result.contains("Alice"), "got: {:?}", result);
    assert!(
        result.contains("Bob"),
        "Expected Bob inserted, got: {:?}",
        result
    );

    let lines: Vec<String> = result.lines().collect();
    let data_lines: Vec<String> = lines
        .iter()
        .skip(2)
        .filter(|l| !l.is_empty())
        .cloned()
        .collect();
    assert_eq!(data_lines.len(), 2, "Expected 2 rows, got: {:?}", result);
}

#[test]
fn test_merge_when_not_matched_insert_uses_default_markers() {
    let _g = setup();
    execute_sql(
        "CREATE TABLE merge_insert_defaults (
            id INTEGER,
            label TEXT DEFAULT 'fallback',
            qty INTEGER DEFAULT 5,
            note TEXT
        )",
    )
    .unwrap();
    execute_sql("CREATE TABLE merge_insert_default_source (id INTEGER, note TEXT)").unwrap();
    execute_sql("INSERT INTO merge_insert_default_source VALUES (1, 'source-note')").unwrap();

    execute_sql(
        "MERGE INTO merge_insert_defaults USING merge_insert_default_source \
         ON merge_insert_defaults.id = merge_insert_default_source.id \
         WHEN NOT MATCHED THEN INSERT (id, label, qty, note) \
         VALUES (merge_insert_default_source.id, DEFAULT, DEFAULT, merge_insert_default_source.note)",
    )
    .unwrap();

    assert_rows(
        "SELECT id, label, qty, note FROM merge_insert_defaults",
        &["id", "label", "qty", "note"],
        vec![vec![
            Value::Integer(1),
            Value::Text("fallback".to_string()),
            Value::Integer(5),
            Value::Text("source-note".to_string()),
        ]],
    );
}

#[test]
fn test_merge_when_not_matched_insert_full_row_defaults() {
    let _g = setup();
    execute_sql(
        "CREATE TABLE merge_insert_full_defaults (
            id INTEGER,
            label TEXT DEFAULT 'fallback',
            qty INTEGER DEFAULT 5
        )",
    )
    .unwrap();
    execute_sql("CREATE TABLE merge_insert_full_source (id INTEGER)").unwrap();
    execute_sql("INSERT INTO merge_insert_full_source VALUES (2)").unwrap();

    execute_sql(
        "MERGE INTO merge_insert_full_defaults USING merge_insert_full_source \
         ON merge_insert_full_defaults.id = merge_insert_full_source.id \
         WHEN NOT MATCHED THEN INSERT VALUES (merge_insert_full_source.id, DEFAULT, DEFAULT)",
    )
    .unwrap();

    assert_rows(
        "SELECT id, label, qty FROM merge_insert_full_defaults",
        &["id", "label", "qty"],
        vec![vec![
            Value::Integer(2),
            Value::Text("fallback".to_string()),
            Value::Integer(5),
        ]],
    );
}

#[test]
fn test_merge_update_recomputes_generated_columns_and_indexes() {
    let _g = setup();
    execute_sql(
        "CREATE TABLE merge_generated_idx (
            id INTEGER,
            price INTEGER,
            qty INTEGER,
            total INTEGER GENERATED ALWAYS AS (price * qty)
        )",
    )
    .unwrap();
    execute_sql("INSERT INTO merge_generated_idx (id, price, qty) VALUES (1, 2, 3)").unwrap();
    execute_sql("CREATE INDEX idx_merge_generated_total ON merge_generated_idx (total)").unwrap();
    execute_sql("CREATE TABLE merge_generated_source (id INTEGER, price INTEGER, qty INTEGER)")
        .unwrap();
    execute_sql("INSERT INTO merge_generated_source VALUES (1, 5, 4)").unwrap();

    assert_command_sql(
        "MERGE INTO merge_generated_idx USING merge_generated_source
         ON merge_generated_idx.id = merge_generated_source.id
         WHEN MATCHED THEN UPDATE SET price = merge_generated_source.price, qty = merge_generated_source.qty",
        CommandTag::Merge,
        1,
    );

    assert_rows(
        "SELECT id, total FROM merge_generated_idx WHERE total = 20",
        &["id", "total"],
        vec![vec![Value::Integer(1), Value::Integer(20)]],
    );
    assert_rows(
        "SELECT id, total FROM merge_generated_idx WHERE total = 6",
        &["id", "total"],
        Vec::new(),
    );
}

#[test]
fn test_merge_insert_applies_auto_increment_generated_columns_and_indexes() {
    let _g = setup();
    execute_sql(
        "CREATE TABLE merge_insert_runtime (
            id INTEGER AUTO_INCREMENT,
            price INTEGER,
            qty INTEGER,
            total INTEGER GENERATED ALWAYS AS (price * qty)
        )",
    )
    .unwrap();
    execute_sql("CREATE INDEX idx_merge_insert_runtime_total ON merge_insert_runtime (total)")
        .unwrap();
    execute_sql("CREATE TABLE merge_insert_runtime_source (price INTEGER, qty INTEGER)").unwrap();
    execute_sql("INSERT INTO merge_insert_runtime_source VALUES (3, 4)").unwrap();

    assert_command_sql(
        "MERGE INTO merge_insert_runtime USING merge_insert_runtime_source
         ON merge_insert_runtime.price = merge_insert_runtime_source.price
         WHEN NOT MATCHED THEN INSERT (price, qty)
         VALUES (merge_insert_runtime_source.price, merge_insert_runtime_source.qty)",
        CommandTag::Merge,
        1,
    );

    assert_rows(
        "SELECT id, total FROM merge_insert_runtime WHERE total = 12",
        &["id", "total"],
        vec![vec![Value::Integer(1), Value::Integer(12)]],
    );
}

#[test]
fn test_merge_insert_validates_unique_constraints() {
    let _g = setup();
    execute_sql(
        "CREATE TABLE merge_unique_target (
            id INTEGER PRIMARY KEY,
            code TEXT UNIQUE,
            amount INTEGER CHECK (amount > 0)
        )",
    )
    .unwrap();
    execute_sql("INSERT INTO merge_unique_target VALUES (1, 'dup', 1)").unwrap();
    execute_sql("CREATE TABLE merge_unique_source (id INTEGER, code TEXT, amount INTEGER)")
        .unwrap();
    execute_sql("INSERT INTO merge_unique_source VALUES (2, 'dup', 5)").unwrap();

    let err = execute_sql(
        "MERGE INTO merge_unique_target USING merge_unique_source
         ON merge_unique_target.id = merge_unique_source.id
         WHEN NOT MATCHED THEN INSERT (id, code, amount)
         VALUES (merge_unique_source.id, merge_unique_source.code, merge_unique_source.amount)",
    )
    .unwrap_err();

    assert!(err.contains("Unique"), "unexpected error: {err}");
    assert_rows(
        "SELECT id, code, amount FROM merge_unique_target",
        &["id", "code", "amount"],
        vec![vec![
            Value::Integer(1),
            Value::Text("dup".to_string()),
            Value::Integer(1),
        ]],
    );
}

#[test]
fn test_merge_update_validates_check_constraints() {
    let _g = setup();
    execute_sql("CREATE TABLE merge_check_target (id INTEGER, amount INTEGER CHECK (amount > 0))")
        .unwrap();
    execute_sql("INSERT INTO merge_check_target VALUES (1, 5)").unwrap();
    execute_sql("CREATE TABLE merge_check_source (id INTEGER, amount INTEGER)").unwrap();
    execute_sql("INSERT INTO merge_check_source VALUES (1, -10)").unwrap();

    let err = execute_sql(
        "MERGE INTO merge_check_target USING merge_check_source
         ON merge_check_target.id = merge_check_source.id
         WHEN MATCHED THEN UPDATE SET amount = merge_check_source.amount",
    )
    .unwrap_err();

    assert!(err.contains("CHECK"), "unexpected error: {err}");
    assert_rows(
        "SELECT id, amount FROM merge_check_target",
        &["id", "amount"],
        vec![vec![Value::Integer(1), Value::Integer(5)]],
    );
}

#[test]
fn test_merge_applies_foreign_key_update_and_delete_actions() {
    let _g = setup();
    execute_sql("CREATE TABLE merge_fk_parent (id INTEGER PRIMARY KEY)").unwrap();
    execute_sql(
        "CREATE TABLE merge_fk_update_child (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER REFERENCES merge_fk_parent(id) ON UPDATE CASCADE
        )",
    )
    .unwrap();
    execute_sql(
        "CREATE TABLE merge_fk_delete_child (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER REFERENCES merge_fk_parent(id) ON DELETE CASCADE
        )",
    )
    .unwrap();
    execute_sql("INSERT INTO merge_fk_parent VALUES (1), (3)").unwrap();
    execute_sql("INSERT INTO merge_fk_update_child VALUES (10, 1)").unwrap();
    execute_sql("INSERT INTO merge_fk_delete_child VALUES (20, 3)").unwrap();
    execute_sql("CREATE TABLE merge_fk_source (old_id INTEGER, new_id INTEGER)").unwrap();
    execute_sql("INSERT INTO merge_fk_source VALUES (1, 2), (3, 3)").unwrap();

    assert_command_sql(
        "MERGE INTO merge_fk_parent USING merge_fk_source
         ON merge_fk_parent.id = merge_fk_source.old_id
         WHEN MATCHED AND merge_fk_source.old_id = 1 THEN UPDATE SET id = merge_fk_source.new_id
         WHEN MATCHED AND merge_fk_source.old_id = 3 THEN DELETE",
        CommandTag::Merge,
        2,
    );

    assert_rows(
        "SELECT parent_id FROM merge_fk_update_child",
        &["parent_id"],
        vec![vec![Value::Integer(2)]],
    );
    assert_rows(
        "SELECT id, parent_id FROM merge_fk_delete_child",
        &["id", "parent_id"],
        Vec::new(),
    );
}

#[test]
fn test_merge_when_matched_delete() {
    let _g = setup();
    execute_sql("CREATE TABLE mt3 (tid INTEGER, val INTEGER)").unwrap();
    execute_sql("INSERT INTO mt3 VALUES (1, 10)").unwrap();
    execute_sql("INSERT INTO mt3 VALUES (2, 20)").unwrap();
    execute_sql("INSERT INTO mt3 VALUES (3, 30)").unwrap();

    execute_sql("CREATE TABLE ms3 (sid INTEGER)").unwrap();
    execute_sql("INSERT INTO ms3 VALUES (1)").unwrap();
    execute_sql("INSERT INTO ms3 VALUES (3)").unwrap();

    execute_sql(
        "MERGE INTO mt3 USING ms3 ON tid = sid \
         WHEN MATCHED THEN DELETE",
    )
    .unwrap();

    let result = execute_sql("SELECT tid FROM mt3").unwrap();
    assert!(
        result.contains("2"),
        "Expected only tid=2 to remain, got: {:?}",
        result
    );
}

#[test]
fn test_merge_combined_matched_and_not_matched() {
    let _g = setup();
    execute_sql("CREATE TABLE mt4 (tid INTEGER, name TEXT, score INTEGER)").unwrap();
    execute_sql("INSERT INTO mt4 VALUES (1, 'Alice', 50)").unwrap();
    execute_sql("INSERT INTO mt4 VALUES (2, 'Bob', 60)").unwrap();

    execute_sql("CREATE TABLE ms4 (sid INTEGER, sname TEXT, new_score INTEGER)").unwrap();
    execute_sql("INSERT INTO ms4 VALUES (2, 'Bob', 90)").unwrap();
    execute_sql("INSERT INTO ms4 VALUES (3, 'Carol', 80)").unwrap();

    execute_sql(
        "MERGE INTO mt4 USING ms4 ON tid = sid \
         WHEN MATCHED THEN UPDATE SET score = new_score \
         WHEN NOT MATCHED THEN INSERT (tid, name, score) VALUES (sid, sname, new_score)",
    )
    .unwrap();

    let result = execute_sql("SELECT tid, name, score FROM mt4 ORDER BY tid").unwrap();
    assert!(result.contains("Alice"), "got: {:?}", result);
    assert!(
        result.contains("90"),
        "Bob's score should be updated to 90, got: {:?}",
        result
    );
    assert!(
        result.contains("Carol"),
        "Carol should be inserted, got: {:?}",
        result
    );
    assert!(
        result.contains("80"),
        "Carol's score should be 80, got: {:?}",
        result
    );
}

#[test]
fn test_merge_with_subquery_source() {
    let _g = setup();
    execute_sql("CREATE TABLE mt5 (tid INTEGER, val INTEGER)").unwrap();
    execute_sql("INSERT INTO mt5 VALUES (1, 10)").unwrap();

    execute_sql("CREATE TABLE ms5_raw (sid INTEGER, new_val INTEGER)").unwrap();
    execute_sql("INSERT INTO ms5_raw VALUES (1, 99)").unwrap();
    execute_sql("INSERT INTO ms5_raw VALUES (2, 88)").unwrap();

    execute_sql(
        "MERGE INTO mt5 USING (SELECT sid, new_val FROM ms5_raw) AS src ON tid = sid \
         WHEN MATCHED THEN UPDATE SET val = new_val \
         WHEN NOT MATCHED THEN INSERT (tid, val) VALUES (sid, new_val)",
    )
    .unwrap();

    let result = execute_sql("SELECT tid, val FROM mt5 ORDER BY tid").unwrap();
    assert!(
        result.contains("99"),
        "tid=1 should be updated to 99, got: {:?}",
        result
    );
    assert!(
        result.contains("88"),
        "tid=2 should be inserted with val=88, got: {:?}",
        result
    );
}

#[test]
fn test_merge_conditional_when() {
    let _g = setup();
    execute_sql("CREATE TABLE mt6 (tid INTEGER, status TEXT, val INTEGER)").unwrap();
    execute_sql("INSERT INTO mt6 VALUES (1, 'active', 10)").unwrap();
    execute_sql("INSERT INTO mt6 VALUES (2, 'inactive', 20)").unwrap();

    execute_sql("CREATE TABLE ms6 (sid INTEGER, new_val INTEGER)").unwrap();
    execute_sql("INSERT INTO ms6 VALUES (1, 100)").unwrap();
    execute_sql("INSERT INTO ms6 VALUES (2, 200)").unwrap();

    execute_sql(
        "MERGE INTO mt6 USING ms6 ON tid = sid \
         WHEN MATCHED AND status = 'active' THEN UPDATE SET val = new_val",
    )
    .unwrap();

    let result = execute_sql("SELECT tid, val FROM mt6 ORDER BY tid").unwrap();
    assert!(
        result.contains("100"),
        "Active row should be updated, got: {:?}",
        result
    );
    assert!(
        result.contains("20"),
        "Inactive row should remain 20, got: {:?}",
        result
    );
}

#[test]
fn test_merge_empty_source() {
    let _g = setup();
    execute_sql("CREATE TABLE mt7 (tid INTEGER, val INTEGER)").unwrap();
    execute_sql("INSERT INTO mt7 VALUES (1, 10)").unwrap();

    execute_sql("CREATE TABLE ms7 (sid INTEGER, new_val INTEGER)").unwrap();

    let result = execute_sql(
        "MERGE INTO mt7 USING ms7 ON tid = sid \
         WHEN MATCHED THEN UPDATE SET val = new_val",
    )
    .unwrap();
    assert!(
        result.contains("0"),
        "Expected 0 rows affected, got: {:?}",
        result
    );

    let result = execute_sql("SELECT val FROM mt7").unwrap();
    assert!(
        result.contains("10"),
        "Value should be unchanged, got: {:?}",
        result
    );
}

#[test]
fn test_generated_column_insert() {
    let _g = setup();
    execute_sql(
        "CREATE TABLE gen_test (id INTEGER, price INTEGER, qty INTEGER, total INTEGER GENERATED ALWAYS AS (price * qty))",
    )
    .unwrap();

    execute_sql("INSERT INTO gen_test (id, price, qty) VALUES (1, 10, 5)").unwrap();
    execute_sql("INSERT INTO gen_test (id, price, qty) VALUES (2, 20, 3)").unwrap();

    let result = execute_sql("SELECT id, total FROM gen_test ORDER BY id").unwrap();
    assert!(result.contains("50"), "Expected 10*5=50, got: {:?}", result);
    assert!(result.contains("60"), "Expected 20*3=60, got: {:?}", result);
}

#[test]
fn test_generated_column_update() {
    let _g = setup();
    execute_sql(
        "CREATE TABLE gen_upd (id INTEGER, a INTEGER, b INTEGER, sum_ab INTEGER GENERATED ALWAYS AS (a + b))",
    )
    .unwrap();

    execute_sql("INSERT INTO gen_upd (id, a, b) VALUES (1, 10, 20)").unwrap();
    let result = execute_sql("SELECT sum_ab FROM gen_upd WHERE id = 1").unwrap();
    assert!(
        result.contains("30"),
        "Expected 10+20=30, got: {:?}",
        result
    );

    execute_sql("UPDATE gen_upd SET a = 50 WHERE id = 1").unwrap();
    let result = execute_sql("SELECT sum_ab FROM gen_upd WHERE id = 1").unwrap();
    assert!(
        result.contains("70"),
        "Expected 50+20=70 after update, got: {:?}",
        result
    );
}

#[test]
fn test_generated_column_stored() {
    let _g = setup();
    execute_sql(
        "CREATE TABLE gen_stored (id INTEGER, val INTEGER, doubled INTEGER GENERATED ALWAYS AS (val * 2) STORED)",
    )
    .unwrap();

    execute_sql("INSERT INTO gen_stored (id, val) VALUES (1, 7)").unwrap();
    let result = execute_sql("SELECT doubled FROM gen_stored WHERE id = 1").unwrap();
    assert!(result.contains("14"), "Expected 7*2=14, got: {:?}", result);
}

#[test]
fn test_generated_column_with_column_ref() {
    let _g = setup();
    execute_sql(
        "CREATE TABLE gen_ref (id INTEGER, first_name TEXT, last_name TEXT, display TEXT GENERATED ALWAYS AS (first_name))",
    )
    .unwrap();

    execute_sql("INSERT INTO gen_ref (id, first_name, last_name) VALUES (1, 'John', 'Doe')")
        .unwrap();
    let result = execute_sql("SELECT display FROM gen_ref WHERE id = 1").unwrap();
    assert!(
        result.contains("John"),
        "Expected generated column to contain first_name, got: {:?}",
        result
    );
}

#[test]
fn test_window_definition_basic() {
    let _g = setup();
    execute_sql("CREATE TABLE win_def (id INTEGER, dept TEXT, salary INTEGER)").unwrap();
    execute_sql("INSERT INTO win_def VALUES (1, 'eng', 100)").unwrap();
    execute_sql("INSERT INTO win_def VALUES (2, 'eng', 200)").unwrap();
    execute_sql("INSERT INTO win_def VALUES (3, 'sales', 150)").unwrap();
    execute_sql("INSERT INTO win_def VALUES (4, 'sales', 250)").unwrap();

    let result = execute_sql(
        "SELECT id, dept, ROW_NUMBER() OVER w FROM win_def \
         WINDOW w AS (PARTITION BY dept ORDER BY salary)",
    )
    .unwrap();
    assert!(result.contains("1"), "got: {:?}", result);
    assert!(result.contains("2"), "got: {:?}", result);
}

#[test]
fn test_from_values_basic() {
    let _g = setup();
    let result =
        execute_sql("SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS t(id, name)").unwrap();
    assert!(result.contains("Alice"), "got: {:?}", result);
    assert!(result.contains("Bob"), "got: {:?}", result);

    let lines: Vec<String> = result.lines().collect();
    let data_lines: Vec<String> = lines
        .iter()
        .skip(2)
        .filter(|l| !l.is_empty())
        .cloned()
        .collect();
    assert_eq!(data_lines.len(), 2, "Expected 2 rows, got: {:?}", result);
}

#[test]
fn test_from_values_with_where() {
    let _g = setup();
    let result = execute_sql(
        "SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')) AS t(id, name) WHERE id > 1",
    )
    .unwrap();
    assert!(
        !result.contains("Alice"),
        "Alice should be filtered out, got: {:?}",
        result
    );
    assert!(result.contains("Bob"), "got: {:?}", result);
    assert!(result.contains("Carol"), "got: {:?}", result);
}

#[test]
fn test_from_values_with_order() {
    let _g = setup();
    let result = execute_sql(
        "SELECT * FROM (VALUES (3, 'Carol'), (1, 'Alice'), (2, 'Bob')) AS t(id, name) ORDER BY id",
    )
    .unwrap();
    let lines: Vec<String> = result.lines().collect();
    let data_lines: Vec<String> = lines
        .iter()
        .skip(2)
        .filter(|l| !l.is_empty())
        .cloned()
        .collect();
    assert_eq!(data_lines.len(), 3, "Expected 3 rows, got: {:?}", result);
    assert!(
        data_lines[0].contains("Alice"),
        "First row should be Alice, got: {:?}",
        result
    );
}

#[test]
fn test_from_values_single_column() {
    let _g = setup();
    let result = execute_sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(num)").unwrap();
    assert!(result.contains("1"), "got: {:?}", result);
    assert!(result.contains("2"), "got: {:?}", result);
    assert!(result.contains("3"), "got: {:?}", result);
}

#[test]
fn test_partial_index() {
    let _g = setup();
    execute_sql("CREATE TABLE pidx_test (id INTEGER, status TEXT, val INTEGER)").unwrap();
    execute_sql("INSERT INTO pidx_test VALUES (1, 'active', 10)").unwrap();
    execute_sql("INSERT INTO pidx_test VALUES (2, 'inactive', 20)").unwrap();
    execute_sql("INSERT INTO pidx_test VALUES (3, 'active', 30)").unwrap();

    let result = execute_sql("CREATE INDEX idx_active ON pidx_test(val) WHERE status = 'active'");
    assert!(
        result.is_ok(),
        "Partial index creation should succeed, got: {:?}",
        result
    );

    let result =
        execute_sql("SELECT id FROM pidx_test WHERE status = 'active' ORDER BY id").unwrap();
    assert!(result.contains("1"), "got: {:?}", result);
    assert!(result.contains("3"), "got: {:?}", result);
}

#[test]
fn test_partial_index_if_not_exists() {
    let _g = setup();
    execute_sql("CREATE TABLE pidx2 (id INTEGER, active INTEGER, val INTEGER)").unwrap();
    execute_sql("INSERT INTO pidx2 VALUES (1, 1, 10)").unwrap();
    execute_sql("INSERT INTO pidx2 VALUES (2, 0, 20)").unwrap();

    execute_sql("CREATE INDEX IF NOT EXISTS idx_active2 ON pidx2(val) WHERE active = 1").unwrap();
    let result =
        execute_sql("CREATE INDEX IF NOT EXISTS idx_active2 ON pidx2(val) WHERE active = 1");
    assert!(
        result.is_ok(),
        "IF NOT EXISTS should not error, got: {:?}",
        result
    );
}

#[test]
fn test_partial_index_not_used_without_matching_filter() {
    let _g = setup();
    execute_sql("CREATE TABLE pidx_guard (id INTEGER, active INTEGER, val INTEGER)").unwrap();
    execute_sql("INSERT INTO pidx_guard VALUES (1, 1, 10)").unwrap();
    execute_sql("INSERT INTO pidx_guard VALUES (2, 0, 10)").unwrap();
    execute_sql("CREATE INDEX idx_guard ON pidx_guard(val) WHERE active = 1").unwrap();

    let explain = execute_sql("EXPLAIN SELECT id FROM pidx_guard WHERE val = 10").unwrap();
    assert!(
        !explain.contains("Index Scan using idx_guard"),
        "unexpected partial-index use: {explain:?}"
    );

    let result = execute_sql("SELECT id FROM pidx_guard WHERE val = 10 ORDER BY id").unwrap();
    assert!(result.contains("1"), "got: {result:?}");
    assert!(result.contains("2"), "got: {result:?}");
}

#[test]
fn test_partial_index_numeric_filter_implication_uses_comparison_semantics() {
    let _g = setup();
    execute_sql("CREATE TABLE pidx_numeric (id INTEGER, active INTEGER, val INTEGER)").unwrap();
    execute_sql("INSERT INTO pidx_numeric VALUES (1, 1, 30)").unwrap();
    execute_sql("INSERT INTO pidx_numeric VALUES (2, 0, 30)").unwrap();
    execute_sql("CREATE INDEX idx_numeric_active ON pidx_numeric(val) WHERE active = 1.0").unwrap();

    let explain =
        execute_sql("EXPLAIN SELECT id FROM pidx_numeric WHERE val = 30 AND active = 1").unwrap();
    assert!(
        explain.contains("Index Scan using idx_numeric_active"),
        "expected numeric-equivalent partial-index use: {explain:?}"
    );

    let result =
        execute_sql("SELECT id FROM pidx_numeric WHERE val = 30 AND active = 1 ORDER BY id")
            .unwrap();
    assert!(result.contains("1"), "got: {result:?}");
    assert!(!result.contains("2"), "got: {result:?}");
}

#[test]
fn test_partial_index_tracks_insert_and_update_membership() {
    let _g = setup();
    execute_sql("CREATE TABLE pidx_membership (id INTEGER, active INTEGER, val INTEGER)").unwrap();
    execute_sql("CREATE INDEX idx_membership ON pidx_membership(val) WHERE active = 1").unwrap();

    execute_sql("INSERT INTO pidx_membership VALUES (1, 1, 30)").unwrap();
    execute_sql("INSERT INTO pidx_membership VALUES (2, 0, 30)").unwrap();

    let active_only =
        execute_sql("SELECT id FROM pidx_membership WHERE val = 30 AND active = 1 ORDER BY id")
            .unwrap();
    assert!(active_only.contains("1"), "got: {active_only:?}");
    assert!(!active_only.contains("2"), "got: {active_only:?}");

    execute_sql("UPDATE pidx_membership SET active = 1 WHERE id = 2").unwrap();
    let promoted =
        execute_sql("SELECT id FROM pidx_membership WHERE val = 30 AND active = 1 ORDER BY id")
            .unwrap();
    assert!(promoted.contains("1"), "got: {promoted:?}");
    assert!(promoted.contains("2"), "got: {promoted:?}");

    execute_sql("UPDATE pidx_membership SET active = 0 WHERE id = 1").unwrap();
    let demoted =
        execute_sql("SELECT id FROM pidx_membership WHERE val = 30 AND active = 1 ORDER BY id")
            .unwrap();
    assert!(!demoted.contains("1"), "got: {demoted:?}");
    assert!(demoted.contains("2"), "got: {demoted:?}");
}

#[test]
fn test_scalar_pi() {
    let _g = setup();
    let result = execute_sql("SELECT PI() AS val").unwrap();
    assert!(
        result.contains("3.14"),
        "Expected pi ~3.14, got: {:?}",
        result
    );
}

#[test]
fn test_scalar_trunc_default() {
    let _g = setup();
    let result = execute_sql("SELECT TRUNC(3.789) AS val").unwrap();
    assert!(result.contains("3"), "Expected 3, got: {:?}", result);
    assert!(
        !result.contains("3.7"),
        "Should be truncated to integer, got: {:?}",
        result
    );
}

#[test]
fn test_scalar_trunc_with_precision() {
    let _g = setup();
    let result = execute_sql("SELECT TRUNC(3.789, 2) AS val").unwrap();
    assert!(result.contains("3.78"), "Expected 3.78, got: {:?}", result);
}

#[test]
fn test_scalar_trunc_integer() {
    let _g = setup();
    let result = execute_sql("SELECT TRUNC(12345, -2) AS val").unwrap();
    assert!(
        result.contains("12300"),
        "Expected 12300, got: {:?}",
        result
    );
}

#[test]
fn test_scalar_log10() {
    let _g = setup();
    let result = execute_sql("SELECT LOG10(100) AS val").unwrap();
    assert!(
        result.contains("2"),
        "Expected log10(100)=2, got: {:?}",
        result
    );
}

#[test]
fn test_scalar_log10_float() {
    let _g = setup();
    let result = execute_sql("SELECT LOG10(1000.0) AS val").unwrap();
    assert!(
        result.contains("3"),
        "Expected log10(1000)=3, got: {:?}",
        result
    );
}

#[test]
fn test_scalar_log2() {
    let _g = setup();
    let result = execute_sql("SELECT LOG2(8) AS val").unwrap();
    assert!(
        result.contains("3"),
        "Expected log2(8)=3, got: {:?}",
        result
    );
}

#[test]
fn test_scalar_cbrt() {
    let _g = setup();
    let result = execute_sql("SELECT CBRT(27) AS val").unwrap();
    assert!(
        result.contains("3"),
        "Expected cbrt(27)=3, got: {:?}",
        result
    );
}

#[test]
fn test_scalar_cbrt_negative() {
    let _g = setup();
    let result = execute_sql("SELECT CBRT(-8) AS val").unwrap();
    assert!(
        result.contains("-2"),
        "Expected cbrt(-8)=-2, got: {:?}",
        result
    );
}

#[test]
fn test_scalar_gcd() {
    let _g = setup();
    let result = execute_sql("SELECT GCD(12, 8) AS val").unwrap();
    assert!(
        result.contains("4"),
        "Expected gcd(12,8)=4, got: {:?}",
        result
    );
}

#[test]
fn test_scalar_gcd_zero() {
    let _g = setup();
    let result = execute_sql("SELECT GCD(0, 5) AS val").unwrap();
    assert!(
        result.contains("5"),
        "Expected gcd(0,5)=5, got: {:?}",
        result
    );
}

#[test]
fn test_scalar_gcd_minimum_integer_with_unit() {
    let _g = setup();
    let result = execute_sql("SELECT GCD(-9223372036854775808, 1) AS val").unwrap();
    assert!(
        result.contains("1"),
        "Expected gcd(i64::MIN,1)=1, got: {:?}",
        result
    );
}

#[test]
fn test_scalar_gcd_rejects_unrepresentable_result() {
    let _g = setup();
    let error = execute_sql("SELECT GCD(-9223372036854775808, 0) AS val").unwrap_err();
    assert!(error.contains("GCD result is outside the i64 range"));
}

#[test]
fn test_scalar_lcm() {
    let _g = setup();
    let result = execute_sql("SELECT LCM(4, 6) AS val").unwrap();
    assert!(
        result.contains("12"),
        "Expected lcm(4,6)=12, got: {:?}",
        result
    );
}

#[test]
fn test_scalar_lcm_zero() {
    let _g = setup();
    let result = execute_sql("SELECT LCM(0, 0) AS val").unwrap();
    assert!(
        result.contains("0"),
        "Expected lcm(0,0)=0, got: {:?}",
        result
    );
}

#[test]
fn test_scalar_lcm_rejects_unrepresentable_result() {
    let _g = setup();
    let error = execute_sql("SELECT LCM(9223372036854775807, 2) AS val").unwrap_err();
    assert!(error.contains("LCM result is outside the i64 range"));
}

#[test]
fn test_scalar_initcap() {
    let _g = setup();
    let result = execute_sql("SELECT INITCAP('hello world') AS val").unwrap();
    assert!(
        result.contains("Hello World"),
        "Expected 'Hello World', got: {:?}",
        result
    );
}

#[test]
fn test_scalar_initcap_mixed() {
    let _g = setup();
    let result = execute_sql("SELECT INITCAP('hELLO wORLD') AS val").unwrap();
    assert!(
        result.contains("Hello World"),
        "Expected 'Hello World', got: {:?}",
        result
    );
}

#[test]
fn test_scalar_initcap_with_punctuation() {
    let _g = setup();
    let result = execute_sql("SELECT INITCAP('one-two three') AS val").unwrap();
    assert!(
        result.contains("One-Two Three"),
        "Expected capitalize after punctuation, got: {:?}",
        result
    );
}

#[test]
fn test_scalar_split_part() {
    let _g = setup();
    let result = execute_sql("SELECT SPLIT_PART('one,two,three', ',', 2) AS val").unwrap();
    assert!(result.contains("two"), "Expected 'two', got: {:?}", result);
}

#[test]
fn test_scalar_split_part_first() {
    let _g = setup();
    let result = execute_sql("SELECT SPLIT_PART('a.b.c', '.', 1) AS val").unwrap();
    assert!(result.contains("a"), "Expected 'a', got: {:?}", result);
}

#[test]
fn test_scalar_split_part_last() {
    let _g = setup();
    let result = execute_sql("SELECT SPLIT_PART('a.b.c', '.', 3) AS val").unwrap();
    assert!(result.contains("c"), "Expected 'c', got: {:?}", result);
}

#[test]
fn test_scalar_split_part_out_of_bounds() {
    let _g = setup();
    let result = execute_sql("SELECT SPLIT_PART('a,b', ',', 5) AS val").unwrap();
    let lines: Vec<String> = result.lines().collect();
    let data_lines: Vec<String> = lines
        .iter()
        .skip(2)
        .filter(|l| !l.is_empty())
        .cloned()
        .collect();
    assert!(
        data_lines.len() <= 1,
        "Expected at most 1 data row, got: {:?}",
        result
    );
}

#[test]
fn test_scalar_translate() {
    let _g = setup();
    let result = execute_sql("SELECT TRANSLATE('hello', 'helo', 'HELO') AS val").unwrap();
    assert!(
        result.contains("HELLO"),
        "Expected 'HELLO', got: {:?}",
        result
    );
}

#[test]
fn test_scalar_translate_remove() {
    let _g = setup();
    let result = execute_sql("SELECT TRANSLATE('abcdef', 'ace', 'AC') AS val").unwrap();
    assert!(
        result.contains("AbCdf"),
        "Expected 'AbCdf' (e removed), got: {:?}",
        result
    );
}

#[test]
fn test_scalar_regexp_match() {
    let _g = setup();
    let result = execute_sql("SELECT REGEXP_MATCH('hello world 123', '\\\\d+') AS val").unwrap();
    assert!(result.contains("123"), "Expected '123', got: {:?}", result);
}

#[test]
fn test_scalar_regexp_match_no_match() {
    let _g = setup();
    let result = execute_sql("SELECT REGEXP_MATCH('hello', '\\\\d+') AS val").unwrap();
    assert!(
        result.contains("NULL"),
        "Expected NULL for no match, got: {:?}",
        result
    );
}

#[test]
fn test_scalar_regexp_replace() {
    let _g = setup();
    let result =
        execute_sql("SELECT REGEXP_REPLACE('hello 123 world 456', '\\\\d+', 'NUM') AS val")
            .unwrap();
    assert!(
        result.contains("hello NUM world NUM"),
        "Expected all digits replaced, got: {:?}",
        result
    );
}

#[test]
fn test_scalar_regexp_replace_partial() {
    let _g = setup();
    let result =
        execute_sql("SELECT REGEXP_REPLACE('foo123bar456', '[0-9]+', '#') AS val").unwrap();
    assert!(
        result.contains("foo#bar#"),
        "Expected digits replaced, got: {:?}",
        result
    );
}

#[test]
fn test_scalar_null_handling() {
    let _g = setup();
    let r1 = execute_sql("SELECT PI() AS val").unwrap();
    assert!(r1.contains("3.14"), "PI should work, got: {:?}", r1);

    let r2 = execute_sql("SELECT TRUNC(NULL) AS val").unwrap();
    assert!(
        r2.contains("NULL"),
        "TRUNC(NULL) should be NULL, got: {:?}",
        r2
    );

    let r3 = execute_sql("SELECT LOG10(NULL) AS val").unwrap();
    assert!(
        r3.contains("NULL"),
        "LOG10(NULL) should be NULL, got: {:?}",
        r3
    );

    let r4 = execute_sql("SELECT CBRT(NULL) AS val").unwrap();
    assert!(
        r4.contains("NULL"),
        "CBRT(NULL) should be NULL, got: {:?}",
        r4
    );

    let r5 = execute_sql("SELECT INITCAP(NULL) AS val").unwrap();
    assert!(
        r5.contains("NULL"),
        "INITCAP(NULL) should be NULL, got: {:?}",
        r5
    );
}

#[test]
fn test_scalar_functions_in_where() {
    let _g = setup();
    execute_sql("CREATE TABLE fn_where (id INTEGER, name TEXT, val FLOAT)").unwrap();
    execute_sql("INSERT INTO fn_where VALUES (1, 'hello world', 3.789)").unwrap();
    execute_sql("INSERT INTO fn_where VALUES (2, 'foo bar', 7.123)").unwrap();

    let result = execute_sql("SELECT id FROM fn_where WHERE TRUNC(val) = 3 ORDER BY id").unwrap();
    assert!(result.contains("1"), "got: {:?}", result);
    assert!(!result.contains("2"), "got: {:?}", result);
}

#[test]
fn test_do_block() {
    let _g = setup();
    let result = execute_sql(
        "DO BEGIN \
         CREATE TABLE do_test (id INTEGER, name TEXT); \
         INSERT INTO do_test VALUES (1, 'Alice'); \
         INSERT INTO do_test VALUES (2, 'Bob'); \
         END",
    )
    .unwrap();
    assert!(
        !result.is_empty(),
        "DO block should return output, got: {:?}",
        result
    );

    let result = execute_sql("SELECT * FROM do_test ORDER BY id").unwrap();
    assert!(result.contains("Alice"), "got: {:?}", result);
    assert!(result.contains("Bob"), "got: {:?}", result);
}

#[test]
fn test_merge_qualified_columns_same_names() {
    let _g = setup();
    execute_sql("CREATE TABLE target8 (id INTEGER, name TEXT, val INTEGER)").unwrap();
    execute_sql("INSERT INTO target8 VALUES (1, 'Alice', 10)").unwrap();
    execute_sql("INSERT INTO target8 VALUES (2, 'Bob', 20)").unwrap();

    execute_sql("CREATE TABLE source8 (id INTEGER, val INTEGER)").unwrap();
    execute_sql("INSERT INTO source8 VALUES (1, 100)").unwrap();
    execute_sql("INSERT INTO source8 VALUES (2, 200)").unwrap();
    execute_sql("INSERT INTO source8 VALUES (3, 300)").unwrap();

    execute_sql(
        "MERGE INTO target8 USING source8 ON target8.id = source8.id \
         WHEN MATCHED THEN UPDATE SET val = source8.val \
         WHEN NOT MATCHED THEN INSERT (id, name, val) VALUES (source8.id, 'New', source8.val)",
    )
    .unwrap();

    let result = execute_sql("SELECT id, name, val FROM target8 ORDER BY id").unwrap();
    assert!(
        result.contains("100"),
        "id=1 val should be 100, got: {:?}",
        result
    );
    assert!(
        result.contains("200"),
        "id=2 val should be 200, got: {:?}",
        result
    );
    assert!(
        result.contains("New"),
        "id=3 should be inserted, got: {:?}",
        result
    );
    assert!(
        result.contains("300"),
        "id=3 val should be 300, got: {:?}",
        result
    );
}
