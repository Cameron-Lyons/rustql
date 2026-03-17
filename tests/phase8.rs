use rustql::testing::{process_query, reset_database};
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
    process_query("CREATE TABLE mt1 (tid INTEGER, name TEXT, val INTEGER)").unwrap();
    process_query("INSERT INTO mt1 VALUES (1, 'Alice', 10)").unwrap();
    process_query("INSERT INTO mt1 VALUES (2, 'Bob', 20)").unwrap();

    process_query("CREATE TABLE ms1 (sid INTEGER, new_val INTEGER)").unwrap();
    process_query("INSERT INTO ms1 VALUES (1, 100)").unwrap();
    process_query("INSERT INTO ms1 VALUES (2, 200)").unwrap();

    let result = process_query(
        "MERGE INTO mt1 USING ms1 ON tid = sid \
         WHEN MATCHED THEN UPDATE SET val = new_val",
    )
    .unwrap();
    assert!(
        result.contains("2"),
        "Expected 2 rows affected, got: {}",
        result
    );

    let result = process_query("SELECT tid, val FROM mt1 ORDER BY tid").unwrap();
    assert!(result.contains("100"), "Expected val=100, got: {}", result);
    assert!(result.contains("200"), "Expected val=200, got: {}", result);
}

#[test]
fn test_merge_when_not_matched_insert() {
    let _g = setup();
    process_query("CREATE TABLE mt2 (tid INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO mt2 VALUES (1, 'Alice')").unwrap();

    process_query("CREATE TABLE ms2 (sid INTEGER, sname TEXT)").unwrap();
    process_query("INSERT INTO ms2 VALUES (1, 'Alice_new')").unwrap();
    process_query("INSERT INTO ms2 VALUES (2, 'Bob')").unwrap();

    process_query(
        "MERGE INTO mt2 USING ms2 ON tid = sid \
         WHEN NOT MATCHED THEN INSERT (tid, name) VALUES (sid, sname)",
    )
    .unwrap();

    let result = process_query("SELECT tid, name FROM mt2 ORDER BY tid").unwrap();
    assert!(result.contains("Alice"), "got: {}", result);
    assert!(
        result.contains("Bob"),
        "Expected Bob inserted, got: {}",
        result
    );

    let lines: Vec<&str> = result.lines().collect();
    let data_lines: Vec<&str> = lines
        .iter()
        .skip(2)
        .filter(|l| !l.is_empty())
        .copied()
        .collect();
    assert_eq!(data_lines.len(), 2, "Expected 2 rows, got: {}", result);
}

#[test]
fn test_merge_when_matched_delete() {
    let _g = setup();
    process_query("CREATE TABLE mt3 (tid INTEGER, val INTEGER)").unwrap();
    process_query("INSERT INTO mt3 VALUES (1, 10)").unwrap();
    process_query("INSERT INTO mt3 VALUES (2, 20)").unwrap();
    process_query("INSERT INTO mt3 VALUES (3, 30)").unwrap();

    process_query("CREATE TABLE ms3 (sid INTEGER)").unwrap();
    process_query("INSERT INTO ms3 VALUES (1)").unwrap();
    process_query("INSERT INTO ms3 VALUES (3)").unwrap();

    process_query(
        "MERGE INTO mt3 USING ms3 ON tid = sid \
         WHEN MATCHED THEN DELETE",
    )
    .unwrap();

    let result = process_query("SELECT tid FROM mt3").unwrap();
    assert!(
        result.contains("2"),
        "Expected only tid=2 to remain, got: {}",
        result
    );
}

#[test]
fn test_merge_combined_matched_and_not_matched() {
    let _g = setup();
    process_query("CREATE TABLE mt4 (tid INTEGER, name TEXT, score INTEGER)").unwrap();
    process_query("INSERT INTO mt4 VALUES (1, 'Alice', 50)").unwrap();
    process_query("INSERT INTO mt4 VALUES (2, 'Bob', 60)").unwrap();

    process_query("CREATE TABLE ms4 (sid INTEGER, sname TEXT, new_score INTEGER)").unwrap();
    process_query("INSERT INTO ms4 VALUES (2, 'Bob', 90)").unwrap();
    process_query("INSERT INTO ms4 VALUES (3, 'Carol', 80)").unwrap();

    process_query(
        "MERGE INTO mt4 USING ms4 ON tid = sid \
         WHEN MATCHED THEN UPDATE SET score = new_score \
         WHEN NOT MATCHED THEN INSERT (tid, name, score) VALUES (sid, sname, new_score)",
    )
    .unwrap();

    let result = process_query("SELECT tid, name, score FROM mt4 ORDER BY tid").unwrap();
    assert!(result.contains("Alice"), "got: {}", result);
    assert!(
        result.contains("90"),
        "Bob's score should be updated to 90, got: {}",
        result
    );
    assert!(
        result.contains("Carol"),
        "Carol should be inserted, got: {}",
        result
    );
    assert!(
        result.contains("80"),
        "Carol's score should be 80, got: {}",
        result
    );
}

#[test]
fn test_merge_with_subquery_source() {
    let _g = setup();
    process_query("CREATE TABLE mt5 (tid INTEGER, val INTEGER)").unwrap();
    process_query("INSERT INTO mt5 VALUES (1, 10)").unwrap();

    process_query("CREATE TABLE ms5_raw (sid INTEGER, new_val INTEGER)").unwrap();
    process_query("INSERT INTO ms5_raw VALUES (1, 99)").unwrap();
    process_query("INSERT INTO ms5_raw VALUES (2, 88)").unwrap();

    process_query(
        "MERGE INTO mt5 USING (SELECT sid, new_val FROM ms5_raw) AS src ON tid = sid \
         WHEN MATCHED THEN UPDATE SET val = new_val \
         WHEN NOT MATCHED THEN INSERT (tid, val) VALUES (sid, new_val)",
    )
    .unwrap();

    let result = process_query("SELECT tid, val FROM mt5 ORDER BY tid").unwrap();
    assert!(
        result.contains("99"),
        "tid=1 should be updated to 99, got: {}",
        result
    );
    assert!(
        result.contains("88"),
        "tid=2 should be inserted with val=88, got: {}",
        result
    );
}

#[test]
fn test_merge_conditional_when() {
    let _g = setup();
    process_query("CREATE TABLE mt6 (tid INTEGER, status TEXT, val INTEGER)").unwrap();
    process_query("INSERT INTO mt6 VALUES (1, 'active', 10)").unwrap();
    process_query("INSERT INTO mt6 VALUES (2, 'inactive', 20)").unwrap();

    process_query("CREATE TABLE ms6 (sid INTEGER, new_val INTEGER)").unwrap();
    process_query("INSERT INTO ms6 VALUES (1, 100)").unwrap();
    process_query("INSERT INTO ms6 VALUES (2, 200)").unwrap();

    process_query(
        "MERGE INTO mt6 USING ms6 ON tid = sid \
         WHEN MATCHED AND status = 'active' THEN UPDATE SET val = new_val",
    )
    .unwrap();

    let result = process_query("SELECT tid, val FROM mt6 ORDER BY tid").unwrap();
    assert!(
        result.contains("100"),
        "Active row should be updated, got: {}",
        result
    );
    assert!(
        result.contains("20"),
        "Inactive row should remain 20, got: {}",
        result
    );
}

#[test]
fn test_merge_empty_source() {
    let _g = setup();
    process_query("CREATE TABLE mt7 (tid INTEGER, val INTEGER)").unwrap();
    process_query("INSERT INTO mt7 VALUES (1, 10)").unwrap();

    process_query("CREATE TABLE ms7 (sid INTEGER, new_val INTEGER)").unwrap();

    let result = process_query(
        "MERGE INTO mt7 USING ms7 ON tid = sid \
         WHEN MATCHED THEN UPDATE SET val = new_val",
    )
    .unwrap();
    assert!(
        result.contains("0"),
        "Expected 0 rows affected, got: {}",
        result
    );

    let result = process_query("SELECT val FROM mt7").unwrap();
    assert!(
        result.contains("10"),
        "Value should be unchanged, got: {}",
        result
    );
}

#[test]
fn test_generated_column_insert() {
    let _g = setup();
    process_query(
        "CREATE TABLE gen_test (id INTEGER, price INTEGER, qty INTEGER, total INTEGER GENERATED ALWAYS AS (price * qty))",
    )
    .unwrap();

    process_query("INSERT INTO gen_test (id, price, qty) VALUES (1, 10, 5)").unwrap();
    process_query("INSERT INTO gen_test (id, price, qty) VALUES (2, 20, 3)").unwrap();

    let result = process_query("SELECT id, total FROM gen_test ORDER BY id").unwrap();
    assert!(result.contains("50"), "Expected 10*5=50, got: {}", result);
    assert!(result.contains("60"), "Expected 20*3=60, got: {}", result);
}

#[test]
fn test_generated_column_update() {
    let _g = setup();
    process_query(
        "CREATE TABLE gen_upd (id INTEGER, a INTEGER, b INTEGER, sum_ab INTEGER GENERATED ALWAYS AS (a + b))",
    )
    .unwrap();

    process_query("INSERT INTO gen_upd (id, a, b) VALUES (1, 10, 20)").unwrap();
    let result = process_query("SELECT sum_ab FROM gen_upd WHERE id = 1").unwrap();
    assert!(result.contains("30"), "Expected 10+20=30, got: {}", result);

    process_query("UPDATE gen_upd SET a = 50 WHERE id = 1").unwrap();
    let result = process_query("SELECT sum_ab FROM gen_upd WHERE id = 1").unwrap();
    assert!(
        result.contains("70"),
        "Expected 50+20=70 after update, got: {}",
        result
    );
}

#[test]
fn test_generated_column_stored() {
    let _g = setup();
    process_query(
        "CREATE TABLE gen_stored (id INTEGER, val INTEGER, doubled INTEGER GENERATED ALWAYS AS (val * 2) STORED)",
    )
    .unwrap();

    process_query("INSERT INTO gen_stored (id, val) VALUES (1, 7)").unwrap();
    let result = process_query("SELECT doubled FROM gen_stored WHERE id = 1").unwrap();
    assert!(result.contains("14"), "Expected 7*2=14, got: {}", result);
}

#[test]
fn test_generated_column_with_column_ref() {
    let _g = setup();
    process_query(
        "CREATE TABLE gen_ref (id INTEGER, first_name TEXT, last_name TEXT, display TEXT GENERATED ALWAYS AS (first_name))",
    )
    .unwrap();

    process_query("INSERT INTO gen_ref (id, first_name, last_name) VALUES (1, 'John', 'Doe')")
        .unwrap();
    let result = process_query("SELECT display FROM gen_ref WHERE id = 1").unwrap();
    assert!(
        result.contains("John"),
        "Expected generated column to contain first_name, got: {}",
        result
    );
}

#[test]
fn test_window_definition_basic() {
    let _g = setup();
    process_query("CREATE TABLE win_def (id INTEGER, dept TEXT, salary INTEGER)").unwrap();
    process_query("INSERT INTO win_def VALUES (1, 'eng', 100)").unwrap();
    process_query("INSERT INTO win_def VALUES (2, 'eng', 200)").unwrap();
    process_query("INSERT INTO win_def VALUES (3, 'sales', 150)").unwrap();
    process_query("INSERT INTO win_def VALUES (4, 'sales', 250)").unwrap();

    let result = process_query(
        "SELECT id, dept, ROW_NUMBER() OVER w FROM win_def \
         WINDOW w AS (PARTITION BY dept ORDER BY salary)",
    )
    .unwrap();
    assert!(result.contains("1"), "got: {}", result);
    assert!(result.contains("2"), "got: {}", result);
}

#[test]
fn test_from_values_basic() {
    let _g = setup();
    let result =
        process_query("SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS t(id, name)").unwrap();
    assert!(result.contains("Alice"), "got: {}", result);
    assert!(result.contains("Bob"), "got: {}", result);

    let lines: Vec<&str> = result.lines().collect();
    let data_lines: Vec<&str> = lines
        .iter()
        .skip(2)
        .filter(|l| !l.is_empty())
        .copied()
        .collect();
    assert_eq!(data_lines.len(), 2, "Expected 2 rows, got: {}", result);
}

#[test]
fn test_from_values_with_where() {
    let _g = setup();
    let result = process_query(
        "SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')) AS t(id, name) WHERE id > 1",
    )
    .unwrap();
    assert!(
        !result.contains("Alice"),
        "Alice should be filtered out, got: {}",
        result
    );
    assert!(result.contains("Bob"), "got: {}", result);
    assert!(result.contains("Carol"), "got: {}", result);
}

#[test]
fn test_from_values_with_order() {
    let _g = setup();
    let result = process_query(
        "SELECT * FROM (VALUES (3, 'Carol'), (1, 'Alice'), (2, 'Bob')) AS t(id, name) ORDER BY id",
    )
    .unwrap();
    let lines: Vec<&str> = result.lines().collect();
    let data_lines: Vec<&str> = lines
        .iter()
        .skip(2)
        .filter(|l| !l.is_empty())
        .copied()
        .collect();
    assert_eq!(data_lines.len(), 3, "Expected 3 rows, got: {}", result);
    assert!(
        data_lines[0].contains("Alice"),
        "First row should be Alice, got: {}",
        result
    );
}

#[test]
fn test_from_values_single_column() {
    let _g = setup();
    let result = process_query("SELECT * FROM (VALUES (1), (2), (3)) AS t(num)").unwrap();
    assert!(result.contains("1"), "got: {}", result);
    assert!(result.contains("2"), "got: {}", result);
    assert!(result.contains("3"), "got: {}", result);
}

#[test]
fn test_partial_index() {
    let _g = setup();
    process_query("CREATE TABLE pidx_test (id INTEGER, status TEXT, val INTEGER)").unwrap();
    process_query("INSERT INTO pidx_test VALUES (1, 'active', 10)").unwrap();
    process_query("INSERT INTO pidx_test VALUES (2, 'inactive', 20)").unwrap();
    process_query("INSERT INTO pidx_test VALUES (3, 'active', 30)").unwrap();

    let result = process_query("CREATE INDEX idx_active ON pidx_test(val) WHERE status = 'active'");
    assert!(
        result.is_ok(),
        "Partial index creation should succeed, got: {:?}",
        result
    );

    let result =
        process_query("SELECT id FROM pidx_test WHERE status = 'active' ORDER BY id").unwrap();
    assert!(result.contains("1"), "got: {}", result);
    assert!(result.contains("3"), "got: {}", result);
}

#[test]
fn test_partial_index_if_not_exists() {
    let _g = setup();
    process_query("CREATE TABLE pidx2 (id INTEGER, active INTEGER, val INTEGER)").unwrap();
    process_query("INSERT INTO pidx2 VALUES (1, 1, 10)").unwrap();
    process_query("INSERT INTO pidx2 VALUES (2, 0, 20)").unwrap();

    process_query("CREATE INDEX IF NOT EXISTS idx_active2 ON pidx2(val) WHERE active = 1").unwrap();
    let result =
        process_query("CREATE INDEX IF NOT EXISTS idx_active2 ON pidx2(val) WHERE active = 1");
    assert!(
        result.is_ok(),
        "IF NOT EXISTS should not error, got: {:?}",
        result
    );
}

#[test]
fn test_partial_index_not_used_without_matching_filter() {
    let _g = setup();
    process_query("CREATE TABLE pidx_guard (id INTEGER, active INTEGER, val INTEGER)").unwrap();
    process_query("INSERT INTO pidx_guard VALUES (1, 1, 10)").unwrap();
    process_query("INSERT INTO pidx_guard VALUES (2, 0, 10)").unwrap();
    process_query("CREATE INDEX idx_guard ON pidx_guard(val) WHERE active = 1").unwrap();

    let explain = process_query("EXPLAIN SELECT id FROM pidx_guard WHERE val = 10").unwrap();
    assert!(
        !explain.contains("Index Scan using idx_guard"),
        "unexpected partial-index use: {explain}"
    );

    let result = process_query("SELECT id FROM pidx_guard WHERE val = 10 ORDER BY id").unwrap();
    assert!(result.contains("1"), "got: {result}");
    assert!(result.contains("2"), "got: {result}");
}

#[test]
fn test_partial_index_tracks_insert_and_update_membership() {
    let _g = setup();
    process_query("CREATE TABLE pidx_membership (id INTEGER, active INTEGER, val INTEGER)")
        .unwrap();
    process_query("CREATE INDEX idx_membership ON pidx_membership(val) WHERE active = 1").unwrap();

    process_query("INSERT INTO pidx_membership VALUES (1, 1, 30)").unwrap();
    process_query("INSERT INTO pidx_membership VALUES (2, 0, 30)").unwrap();

    let active_only =
        process_query("SELECT id FROM pidx_membership WHERE val = 30 AND active = 1 ORDER BY id")
            .unwrap();
    assert!(active_only.contains("1"), "got: {active_only}");
    assert!(!active_only.contains("2"), "got: {active_only}");

    process_query("UPDATE pidx_membership SET active = 1 WHERE id = 2").unwrap();
    let promoted =
        process_query("SELECT id FROM pidx_membership WHERE val = 30 AND active = 1 ORDER BY id")
            .unwrap();
    assert!(promoted.contains("1"), "got: {promoted}");
    assert!(promoted.contains("2"), "got: {promoted}");

    process_query("UPDATE pidx_membership SET active = 0 WHERE id = 1").unwrap();
    let demoted =
        process_query("SELECT id FROM pidx_membership WHERE val = 30 AND active = 1 ORDER BY id")
            .unwrap();
    assert!(!demoted.contains("1"), "got: {demoted}");
    assert!(demoted.contains("2"), "got: {demoted}");
}

#[test]
fn test_scalar_pi() {
    let _g = setup();
    let result = process_query("SELECT PI() AS val").unwrap();
    assert!(
        result.contains("3.14"),
        "Expected pi ~3.14, got: {}",
        result
    );
}

#[test]
fn test_scalar_trunc_default() {
    let _g = setup();
    let result = process_query("SELECT TRUNC(3.789) AS val").unwrap();
    assert!(result.contains("3"), "Expected 3, got: {}", result);
    assert!(
        !result.contains("3.7"),
        "Should be truncated to integer, got: {}",
        result
    );
}

#[test]
fn test_scalar_trunc_with_precision() {
    let _g = setup();
    let result = process_query("SELECT TRUNC(3.789, 2) AS val").unwrap();
    assert!(result.contains("3.78"), "Expected 3.78, got: {}", result);
}

#[test]
fn test_scalar_trunc_integer() {
    let _g = setup();
    let result = process_query("SELECT TRUNC(12345, -2) AS val").unwrap();
    assert!(result.contains("12300"), "Expected 12300, got: {}", result);
}

#[test]
fn test_scalar_log10() {
    let _g = setup();
    let result = process_query("SELECT LOG10(100) AS val").unwrap();
    assert!(
        result.contains("2"),
        "Expected log10(100)=2, got: {}",
        result
    );
}

#[test]
fn test_scalar_log10_float() {
    let _g = setup();
    let result = process_query("SELECT LOG10(1000.0) AS val").unwrap();
    assert!(
        result.contains("3"),
        "Expected log10(1000)=3, got: {}",
        result
    );
}

#[test]
fn test_scalar_log2() {
    let _g = setup();
    let result = process_query("SELECT LOG2(8) AS val").unwrap();
    assert!(result.contains("3"), "Expected log2(8)=3, got: {}", result);
}

#[test]
fn test_scalar_cbrt() {
    let _g = setup();
    let result = process_query("SELECT CBRT(27) AS val").unwrap();
    assert!(result.contains("3"), "Expected cbrt(27)=3, got: {}", result);
}

#[test]
fn test_scalar_cbrt_negative() {
    let _g = setup();
    let result = process_query("SELECT CBRT(-8) AS val").unwrap();
    assert!(
        result.contains("-2"),
        "Expected cbrt(-8)=-2, got: {}",
        result
    );
}

#[test]
fn test_scalar_gcd() {
    let _g = setup();
    let result = process_query("SELECT GCD(12, 8) AS val").unwrap();
    assert!(
        result.contains("4"),
        "Expected gcd(12,8)=4, got: {}",
        result
    );
}

#[test]
fn test_scalar_gcd_zero() {
    let _g = setup();
    let result = process_query("SELECT GCD(0, 5) AS val").unwrap();
    assert!(result.contains("5"), "Expected gcd(0,5)=5, got: {}", result);
}

#[test]
fn test_scalar_lcm() {
    let _g = setup();
    let result = process_query("SELECT LCM(4, 6) AS val").unwrap();
    assert!(
        result.contains("12"),
        "Expected lcm(4,6)=12, got: {}",
        result
    );
}

#[test]
fn test_scalar_lcm_zero() {
    let _g = setup();
    let result = process_query("SELECT LCM(0, 0) AS val").unwrap();
    assert!(result.contains("0"), "Expected lcm(0,0)=0, got: {}", result);
}

#[test]
fn test_scalar_initcap() {
    let _g = setup();
    let result = process_query("SELECT INITCAP('hello world') AS val").unwrap();
    assert!(
        result.contains("Hello World"),
        "Expected 'Hello World', got: {}",
        result
    );
}

#[test]
fn test_scalar_initcap_mixed() {
    let _g = setup();
    let result = process_query("SELECT INITCAP('hELLO wORLD') AS val").unwrap();
    assert!(
        result.contains("Hello World"),
        "Expected 'Hello World', got: {}",
        result
    );
}

#[test]
fn test_scalar_initcap_with_punctuation() {
    let _g = setup();
    let result = process_query("SELECT INITCAP('one-two three') AS val").unwrap();
    assert!(
        result.contains("One-Two Three"),
        "Expected capitalize after punctuation, got: {}",
        result
    );
}

#[test]
fn test_scalar_split_part() {
    let _g = setup();
    let result = process_query("SELECT SPLIT_PART('one,two,three', ',', 2) AS val").unwrap();
    assert!(result.contains("two"), "Expected 'two', got: {}", result);
}

#[test]
fn test_scalar_split_part_first() {
    let _g = setup();
    let result = process_query("SELECT SPLIT_PART('a.b.c', '.', 1) AS val").unwrap();
    assert!(result.contains("a"), "Expected 'a', got: {}", result);
}

#[test]
fn test_scalar_split_part_last() {
    let _g = setup();
    let result = process_query("SELECT SPLIT_PART('a.b.c', '.', 3) AS val").unwrap();
    assert!(result.contains("c"), "Expected 'c', got: {}", result);
}

#[test]
fn test_scalar_split_part_out_of_bounds() {
    let _g = setup();
    let result = process_query("SELECT SPLIT_PART('a,b', ',', 5) AS val").unwrap();
    let lines: Vec<&str> = result.lines().collect();
    let data_lines: Vec<&str> = lines
        .iter()
        .skip(2)
        .filter(|l| !l.is_empty())
        .copied()
        .collect();
    assert!(
        data_lines.len() <= 1,
        "Expected at most 1 data row, got: {}",
        result
    );
}

#[test]
fn test_scalar_translate() {
    let _g = setup();
    let result = process_query("SELECT TRANSLATE('hello', 'helo', 'HELO') AS val").unwrap();
    assert!(
        result.contains("HELLO"),
        "Expected 'HELLO', got: {}",
        result
    );
}

#[test]
fn test_scalar_translate_remove() {
    let _g = setup();
    let result = process_query("SELECT TRANSLATE('abcdef', 'ace', 'AC') AS val").unwrap();
    assert!(
        result.contains("AbCdf"),
        "Expected 'AbCdf' (e removed), got: {}",
        result
    );
}

#[test]
fn test_scalar_regexp_match() {
    let _g = setup();
    let result = process_query("SELECT REGEXP_MATCH('hello world 123', '\\\\d+') AS val").unwrap();
    assert!(result.contains("123"), "Expected '123', got: {}", result);
}

#[test]
fn test_scalar_regexp_match_no_match() {
    let _g = setup();
    let result = process_query("SELECT REGEXP_MATCH('hello', '\\\\d+') AS val").unwrap();
    assert!(
        result.contains("NULL"),
        "Expected NULL for no match, got: {}",
        result
    );
}

#[test]
fn test_scalar_regexp_replace() {
    let _g = setup();
    let result =
        process_query("SELECT REGEXP_REPLACE('hello 123 world 456', '\\\\d+', 'NUM') AS val")
            .unwrap();
    assert!(
        result.contains("hello NUM world NUM"),
        "Expected all digits replaced, got: {}",
        result
    );
}

#[test]
fn test_scalar_regexp_replace_partial() {
    let _g = setup();
    let result =
        process_query("SELECT REGEXP_REPLACE('foo123bar456', '[0-9]+', '#') AS val").unwrap();
    assert!(
        result.contains("foo#bar#"),
        "Expected digits replaced, got: {}",
        result
    );
}

#[test]
fn test_scalar_null_handling() {
    let _g = setup();
    let r1 = process_query("SELECT PI() AS val").unwrap();
    assert!(r1.contains("3.14"), "PI should work, got: {}", r1);

    let r2 = process_query("SELECT TRUNC(NULL) AS val").unwrap();
    assert!(
        r2.contains("NULL"),
        "TRUNC(NULL) should be NULL, got: {}",
        r2
    );

    let r3 = process_query("SELECT LOG10(NULL) AS val").unwrap();
    assert!(
        r3.contains("NULL"),
        "LOG10(NULL) should be NULL, got: {}",
        r3
    );

    let r4 = process_query("SELECT CBRT(NULL) AS val").unwrap();
    assert!(
        r4.contains("NULL"),
        "CBRT(NULL) should be NULL, got: {}",
        r4
    );

    let r5 = process_query("SELECT INITCAP(NULL) AS val").unwrap();
    assert!(
        r5.contains("NULL"),
        "INITCAP(NULL) should be NULL, got: {}",
        r5
    );
}

#[test]
fn test_scalar_functions_in_where() {
    let _g = setup();
    process_query("CREATE TABLE fn_where (id INTEGER, name TEXT, val FLOAT)").unwrap();
    process_query("INSERT INTO fn_where VALUES (1, 'hello world', 3.789)").unwrap();
    process_query("INSERT INTO fn_where VALUES (2, 'foo bar', 7.123)").unwrap();

    let result = process_query("SELECT id FROM fn_where WHERE TRUNC(val) = 3 ORDER BY id").unwrap();
    assert!(result.contains("1"), "got: {}", result);
    assert!(!result.contains("2"), "got: {}", result);
}

#[test]
fn test_do_block() {
    let _g = setup();
    let result = process_query(
        "DO BEGIN \
         CREATE TABLE do_test (id INTEGER, name TEXT); \
         INSERT INTO do_test VALUES (1, 'Alice'); \
         INSERT INTO do_test VALUES (2, 'Bob'); \
         END",
    )
    .unwrap();
    assert!(
        !result.is_empty(),
        "DO block should return output, got: {}",
        result
    );

    let result = process_query("SELECT * FROM do_test ORDER BY id").unwrap();
    assert!(result.contains("Alice"), "got: {}", result);
    assert!(result.contains("Bob"), "got: {}", result);
}

#[test]
fn test_merge_qualified_columns_same_names() {
    let _g = setup();
    process_query("CREATE TABLE target8 (id INTEGER, name TEXT, val INTEGER)").unwrap();
    process_query("INSERT INTO target8 VALUES (1, 'Alice', 10)").unwrap();
    process_query("INSERT INTO target8 VALUES (2, 'Bob', 20)").unwrap();

    process_query("CREATE TABLE source8 (id INTEGER, val INTEGER)").unwrap();
    process_query("INSERT INTO source8 VALUES (1, 100)").unwrap();
    process_query("INSERT INTO source8 VALUES (2, 200)").unwrap();
    process_query("INSERT INTO source8 VALUES (3, 300)").unwrap();

    process_query(
        "MERGE INTO target8 USING source8 ON target8.id = source8.id \
         WHEN MATCHED THEN UPDATE SET val = source8.val \
         WHEN NOT MATCHED THEN INSERT (id, name, val) VALUES (source8.id, 'New', source8.val)",
    )
    .unwrap();

    let result = process_query("SELECT id, name, val FROM target8 ORDER BY id").unwrap();
    assert!(
        result.contains("100"),
        "id=1 val should be 100, got: {}",
        result
    );
    assert!(
        result.contains("200"),
        "id=2 val should be 200, got: {}",
        result
    );
    assert!(
        result.contains("New"),
        "id=3 should be inserted, got: {}",
        result
    );
    assert!(
        result.contains("300"),
        "id=3 val should be 300, got: {}",
        result
    );
}
