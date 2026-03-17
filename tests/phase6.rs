use rustql::testing::{process_query, reset_database};
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup() -> std::sync::MutexGuard<'static, ()> {
    let guard = TEST_MUTEX.lock().unwrap();
    reset_database();
    guard
}

#[test]
fn test_ltrim() {
    let _g = setup();
    let result = process_query("SELECT LTRIM('  hello  ') AS val").unwrap();
    assert!(result.contains("hello  "), "got: {}", result);
}

#[test]
fn test_rtrim() {
    let _g = setup();
    let result = process_query("SELECT RTRIM('  hello  ') AS val").unwrap();
    assert!(result.contains("  hello"), "got: {}", result);
    assert!(
        !result.contains("  hello  "),
        "should be trimmed, got: {}",
        result
    );
}

#[test]
fn test_ascii() {
    let _g = setup();
    let result = process_query("SELECT ASCII('A') AS val").unwrap();
    assert!(result.contains("65"), "got: {}", result);
}

#[test]
fn test_ascii_empty() {
    let _g = setup();
    let result = process_query("SELECT ASCII('') AS val").unwrap();
    assert!(result.contains("NULL"), "got: {}", result);
}

#[test]
fn test_chr() {
    let _g = setup();
    let result = process_query("SELECT CHR(65) AS val").unwrap();
    assert!(result.contains('A'), "got: {}", result);
}

#[test]
fn test_sin() {
    let _g = setup();
    let result = process_query("SELECT SIN(0) AS val").unwrap();
    assert!(result.contains('0'), "got: {}", result);
}

#[test]
fn test_cos() {
    let _g = setup();
    let result = process_query("SELECT COS(0) AS val").unwrap();
    assert!(result.contains('1'), "got: {}", result);
}

#[test]
fn test_tan() {
    let _g = setup();
    let result = process_query("SELECT TAN(0) AS val").unwrap();
    assert!(result.contains('0'), "got: {}", result);
}

#[test]
fn test_asin_acos_atan() {
    let _g = setup();
    let result = process_query("SELECT ASIN(0) AS a, ACOS(1) AS b, ATAN(0) AS c").unwrap();
    assert!(result.contains('0'), "got: {}", result);
}

#[test]
fn test_atan2() {
    let _g = setup();
    let result = process_query("SELECT ATAN2(1.0, 1.0) AS val").unwrap();
    assert!(result.contains("0.78"), "expected ~pi/4, got: {}", result);
}

#[test]
fn test_random() {
    let _g = setup();
    let result = process_query("SELECT RANDOM() AS val").unwrap();
    assert!(
        result.contains("0."),
        "expected float in [0,1), got: {}",
        result
    );
}

#[test]
fn test_degrees_radians() {
    let _g = setup();
    let result = process_query("SELECT DEGREES(3.141592653589793) AS deg").unwrap();
    assert!(result.contains("180"), "got: {}", result);
    let result2 = process_query("SELECT RADIANS(180) AS rad").unwrap();
    assert!(result2.contains("3.14"), "got: {}", result2);
}

#[test]
fn test_quarter() {
    let _g = setup();
    process_query("CREATE TABLE qtr_test (id INTEGER, dt DATE)").unwrap();
    process_query("INSERT INTO qtr_test (id, dt) VALUES (1, '2024-01-15')").unwrap();
    process_query("INSERT INTO qtr_test (id, dt) VALUES (2, '2024-04-15')").unwrap();
    process_query("INSERT INTO qtr_test (id, dt) VALUES (3, '2024-07-15')").unwrap();
    process_query("INSERT INTO qtr_test (id, dt) VALUES (4, '2024-10-15')").unwrap();
    let result = process_query("SELECT id, QUARTER(dt) AS q FROM qtr_test ORDER BY id").unwrap();
    assert!(result.contains("1\t1"), "row 1 q=1, got: {}", result);
    assert!(result.contains("2\t2"), "row 2 q=2, got: {}", result);
    assert!(result.contains("3\t3"), "row 3 q=3, got: {}", result);
    assert!(result.contains("4\t4"), "row 4 q=4, got: {}", result);
}

#[test]
fn test_week() {
    let _g = setup();
    let result = process_query("SELECT WEEK('2024-01-01') AS w").unwrap();
    assert!(result.contains('1'), "got: {}", result);
    let result2 = process_query("SELECT WEEK('2024-01-08') AS w").unwrap();
    assert!(result2.contains('2'), "got: {}", result2);
}

#[test]
fn test_dayofweek() {
    let _g = setup();
    let result = process_query("SELECT DAYOFWEEK('2024-01-01') AS dow").unwrap();
    let lines: Vec<&str> = result.lines().collect();
    let val_line = lines.last().unwrap();
    let val: i64 = val_line.trim().trim_matches('|').trim().parse().unwrap();
    assert!((1..=7).contains(&val), "got: {}", val);
}

#[test]
fn test_extract_quarter_week_dow() {
    let _g = setup();
    let result = process_query("SELECT EXTRACT(QUARTER FROM '2024-07-15') AS q").unwrap();
    assert!(result.contains('3'), "got: {}", result);
    let result2 = process_query("SELECT EXTRACT(WEEK FROM '2024-01-08') AS w").unwrap();
    assert!(result2.contains('2'), "got: {}", result2);
    let result3 = process_query("SELECT EXTRACT(DAYOFWEEK FROM '2024-01-01') AS dow").unwrap();
    let lines: Vec<&str> = result3.lines().collect();
    let val_line = lines.last().unwrap();
    let val: i64 = val_line.trim().trim_matches('|').trim().parse().unwrap();
    assert!((1..=7).contains(&val), "got: {}", val);
}

#[test]
fn test_inner_join_using() {
    let _g = setup();
    process_query("CREATE TABLE jusing_a (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO jusing_a (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
        .unwrap();
    process_query("CREATE TABLE jusing_b (id INTEGER, score INTEGER)").unwrap();
    process_query("INSERT INTO jusing_b (id, score) VALUES (1, 90), (2, 85)").unwrap();
    let result = process_query(
        "SELECT jusing_a.name, jusing_b.score FROM jusing_a JOIN jusing_b USING (id) ORDER BY jusing_a.name",
    )
    .unwrap();
    assert!(result.contains("Alice"), "got: {}", result);
    assert!(result.contains("90"), "got: {}", result);
    assert!(result.contains("Bob"), "got: {}", result);
    assert!(result.contains("85"), "got: {}", result);
    assert!(
        !result.contains("Carol"),
        "Carol should not appear, got: {}",
        result
    );
}

#[test]
fn test_left_join_using() {
    let _g = setup();
    process_query("CREATE TABLE ljusing_a (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO ljusing_a (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
        .unwrap();
    process_query("CREATE TABLE ljusing_b (id INTEGER, score INTEGER)").unwrap();
    process_query("INSERT INTO ljusing_b (id, score) VALUES (1, 90), (2, 85)").unwrap();
    let result = process_query(
        "SELECT ljusing_a.name, ljusing_b.score FROM ljusing_a LEFT JOIN ljusing_b USING (id) ORDER BY ljusing_a.name",
    )
    .unwrap();
    assert!(result.contains("Alice"), "got: {}", result);
    assert!(result.contains("Bob"), "got: {}", result);
    assert!(
        result.contains("Carol"),
        "Carol should appear with NULL, got: {}",
        result
    );
    assert!(result.contains("NULL"), "got: {}", result);
}

#[test]
fn test_join_using_multi_column() {
    let _g = setup();
    process_query("CREATE TABLE mjusing_a (x INTEGER, y INTEGER, val TEXT)").unwrap();
    process_query(
        "INSERT INTO mjusing_a (x, y, val) VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')",
    )
    .unwrap();
    process_query("CREATE TABLE mjusing_b (x INTEGER, y INTEGER, info TEXT)").unwrap();
    process_query("INSERT INTO mjusing_b (x, y, info) VALUES (1, 10, 'foo'), (2, 20, 'bar')")
        .unwrap();
    let result = process_query(
        "SELECT mjusing_a.val, mjusing_b.info FROM mjusing_a JOIN mjusing_b USING (x, y) ORDER BY mjusing_a.val",
    )
    .unwrap();
    assert!(result.contains("foo"), "got: {}", result);
    assert!(result.contains("bar"), "got: {}", result);
    let lines: Vec<&str> = result.lines().collect();
    assert_eq!(lines.len() - 2, 2, "expected 2 data rows, got: {}", result);
}

#[test]
fn test_any_equal() {
    let _g = setup();
    process_query("CREATE TABLE any_vals (v INTEGER)").unwrap();
    process_query("INSERT INTO any_vals (v) VALUES (10), (20), (30)").unwrap();
    process_query("CREATE TABLE any_test (id INTEGER, val INTEGER)").unwrap();
    process_query("INSERT INTO any_test (id, val) VALUES (1, 10), (2, 25), (3, 30)").unwrap();
    let result = process_query(
        "SELECT id FROM any_test WHERE val = ANY (SELECT v FROM any_vals) ORDER BY id",
    )
    .unwrap();
    assert!(result.contains('1'), "got: {}", result);
    assert!(result.contains('3'), "got: {}", result);
    let lines: Vec<&str> = result.lines().collect();
    assert_eq!(lines.len() - 2, 2, "expected 2 rows, got: {}", result);
}

#[test]
fn test_any_less_than() {
    let _g = setup();
    process_query("CREATE TABLE anylt_vals (v INTEGER)").unwrap();
    process_query("INSERT INTO anylt_vals (v) VALUES (5), (15), (25)").unwrap();
    process_query("CREATE TABLE anylt_test (id INTEGER, val INTEGER)").unwrap();
    process_query("INSERT INTO anylt_test (id, val) VALUES (1, 3), (2, 10), (3, 30)").unwrap();
    let result = process_query(
        "SELECT id FROM anylt_test WHERE val < ANY (SELECT v FROM anylt_vals) ORDER BY id",
    )
    .unwrap();
    assert!(result.contains('1'), "got: {}", result);
    assert!(result.contains('2'), "got: {}", result);
    let lines: Vec<&str> = result.lines().collect();
    assert_eq!(lines.len() - 2, 2, "expected 2 rows, got: {}", result);
}

#[test]
fn test_all_greater() {
    let _g = setup();
    process_query("CREATE TABLE allgt_vals (v INTEGER)").unwrap();
    process_query("INSERT INTO allgt_vals (v) VALUES (5), (10), (15)").unwrap();
    process_query("CREATE TABLE allgt_test (id INTEGER, val INTEGER)").unwrap();
    process_query("INSERT INTO allgt_test (id, val) VALUES (1, 20), (2, 10), (3, 3)").unwrap();
    let result = process_query(
        "SELECT id FROM allgt_test WHERE val > ALL (SELECT v FROM allgt_vals) ORDER BY id",
    )
    .unwrap();
    let lines: Vec<&str> = result.lines().collect();
    assert_eq!(lines.len() - 2, 1, "expected 1 row, got: {}", result);
    assert!(result.contains('1'), "got: {}", result);
}

#[test]
fn test_all_empty_subquery() {
    let _g = setup();
    process_query("CREATE TABLE allempty_vals (v INTEGER)").unwrap();
    process_query("CREATE TABLE allempty_test (id INTEGER, val INTEGER)").unwrap();
    process_query("INSERT INTO allempty_test (id, val) VALUES (1, 5), (2, 10)").unwrap();
    let result = process_query(
        "SELECT id FROM allempty_test WHERE val > ALL (SELECT v FROM allempty_vals) ORDER BY id",
    )
    .unwrap();
    let lines: Vec<&str> = result.lines().collect();
    assert_eq!(
        lines.len() - 2,
        2,
        "ALL with empty subquery should return all rows, got: {}",
        result
    );
}

#[test]
fn test_null_handling_new_functions() {
    let _g = setup();
    let result = process_query("SELECT LTRIM(NULL) AS a").unwrap();
    assert!(result.contains("NULL"), "got: {}", result);
    let result2 = process_query("SELECT SIN(NULL) AS b").unwrap();
    assert!(result2.contains("NULL"), "got: {}", result2);
    let result3 = process_query("SELECT QUARTER(NULL) AS c").unwrap();
    assert!(result3.contains("NULL"), "got: {}", result3);
    let result4 = process_query("SELECT CHR(NULL) AS d").unwrap();
    assert!(result4.contains("NULL"), "got: {}", result4);
}

#[test]
fn test_scalar_in_where() {
    let _g = setup();
    process_query("CREATE TABLE swhere (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO swhere (id, name) VALUES (1, '  hello'), (2, 'world  ')").unwrap();
    let result =
        process_query("SELECT id FROM swhere WHERE LTRIM(name) = 'hello' ORDER BY id").unwrap();
    let lines: Vec<&str> = result.lines().collect();
    assert_eq!(lines.len() - 2, 1, "expected 1 row, got: {}", result);
    assert!(lines[2].contains('1'), "got: {}", result);
}

#[test]
fn test_some_alias_for_any() {
    let _g = setup();
    process_query("CREATE TABLE some_vals (v INTEGER)").unwrap();
    process_query("INSERT INTO some_vals (v) VALUES (10), (20)").unwrap();
    process_query("CREATE TABLE some_test (id INTEGER, val INTEGER)").unwrap();
    process_query("INSERT INTO some_test (id, val) VALUES (1, 10), (2, 30)").unwrap();
    let result = process_query(
        "SELECT id FROM some_test WHERE val = SOME (SELECT v FROM some_vals) ORDER BY id",
    )
    .unwrap();
    let lines: Vec<&str> = result.lines().collect();
    assert_eq!(lines.len() - 2, 1, "got: {}", result);
}

#[test]
fn test_rand_alias() {
    let _g = setup();
    let result = process_query("SELECT RAND() AS val").unwrap();
    assert!(result.contains("0."), "expected float, got: {}", result);
}
