mod common;
use common::*;
use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

#[test]
fn test_filter_clause_count() {
    let _lock = TEST_MUTEX.lock().unwrap();
    reset_database();

    execute_sql("CREATE TABLE filter_test (id INTEGER, category TEXT, amount INTEGER)").unwrap();
    execute_sql("INSERT INTO filter_test VALUES (1, 'A', 10)").unwrap();
    execute_sql("INSERT INTO filter_test VALUES (2, 'B', 20)").unwrap();
    execute_sql("INSERT INTO filter_test VALUES (3, 'A', 30)").unwrap();
    execute_sql("INSERT INTO filter_test VALUES (4, 'B', 40)").unwrap();
    execute_sql("INSERT INTO filter_test VALUES (5, 'A', 50)").unwrap();

    let result =
        execute_sql("SELECT COUNT(*) FILTER (WHERE category = 'A') AS count_a FROM filter_test")
            .unwrap();
    assert!(
        result.contains("3"),
        "Expected count_a=3, got: {:?}",
        result
    );
}

#[test]
fn test_filter_clause_sum() {
    let _lock = TEST_MUTEX.lock().unwrap();
    reset_database();

    execute_sql("CREATE TABLE filter_sum (id INTEGER, category TEXT, amount INTEGER)").unwrap();
    execute_sql("INSERT INTO filter_sum VALUES (1, 'A', 10)").unwrap();
    execute_sql("INSERT INTO filter_sum VALUES (2, 'B', 20)").unwrap();
    execute_sql("INSERT INTO filter_sum VALUES (3, 'A', 30)").unwrap();

    let result =
        execute_sql("SELECT SUM(amount) FILTER (WHERE category = 'A') AS sum_a FROM filter_sum")
            .unwrap();
    assert!(
        result.contains("40"),
        "Expected sum_a=40, got: {:?}",
        result
    );
}

#[test]
fn test_filter_clause_with_group_by() {
    let _lock = TEST_MUTEX.lock().unwrap();
    reset_database();

    execute_sql("CREATE TABLE filter_group (id INTEGER, dept TEXT, status TEXT, salary INTEGER)")
        .unwrap();
    execute_sql("INSERT INTO filter_group VALUES (1, 'eng', 'active', 100)").unwrap();
    execute_sql("INSERT INTO filter_group VALUES (2, 'eng', 'inactive', 200)").unwrap();
    execute_sql("INSERT INTO filter_group VALUES (3, 'eng', 'active', 150)").unwrap();
    execute_sql("INSERT INTO filter_group VALUES (4, 'sales', 'active', 120)").unwrap();
    execute_sql("INSERT INTO filter_group VALUES (5, 'sales', 'inactive', 80)").unwrap();

    let result = execute_sql("SELECT dept, COUNT(*) FILTER (WHERE status = 'active') AS active_count FROM filter_group GROUP BY dept ORDER BY dept").unwrap();
    assert!(
        result.contains("eng") && result.contains("2"),
        "Expected eng with active_count=2, got: {:?}",
        result
    );
    assert!(
        result.contains("sales") && result.contains("1"),
        "Expected sales with active_count=1, got: {:?}",
        result
    );
}

#[test]
fn test_is_distinct_from() {
    let _lock = TEST_MUTEX.lock().unwrap();
    reset_database();

    execute_sql("CREATE TABLE dist_test (id INTEGER, val INTEGER)").unwrap();
    execute_sql("INSERT INTO dist_test VALUES (1, 10)").unwrap();
    execute_sql("INSERT INTO dist_test VALUES (2, NULL)").unwrap();
    execute_sql("INSERT INTO dist_test VALUES (3, 10)").unwrap();
    execute_sql("INSERT INTO dist_test VALUES (4, NULL)").unwrap();

    let result =
        execute_sql("SELECT id FROM dist_test WHERE val IS DISTINCT FROM 10 ORDER BY id").unwrap();
    assert!(result.contains("2"), "Expected id=2, got: {:?}", result);
    assert!(result.contains("4"), "Expected id=4, got: {:?}", result);
    assert!(
        !result.contains("1\t") && !result.contains("1\n"),
        "Should not contain id=1"
    );
}

#[test]
fn test_is_not_distinct_from() {
    let _lock = TEST_MUTEX.lock().unwrap();
    reset_database();

    execute_sql("CREATE TABLE ndist_test (id INTEGER, val INTEGER)").unwrap();
    execute_sql("INSERT INTO ndist_test VALUES (1, NULL)").unwrap();
    execute_sql("INSERT INTO ndist_test VALUES (2, 10)").unwrap();
    execute_sql("INSERT INTO ndist_test VALUES (3, NULL)").unwrap();

    let result =
        execute_sql("SELECT id FROM ndist_test WHERE val IS NOT DISTINCT FROM NULL ORDER BY id")
            .unwrap();
    assert!(result.contains("1"), "Expected id=1, got: {:?}", result);
    assert!(result.contains("3"), "Expected id=3, got: {:?}", result);
}

#[test]
fn test_double_colon_cast() {
    let _lock = TEST_MUTEX.lock().unwrap();
    reset_database();

    execute_sql("CREATE TABLE cast_test (id INTEGER, val TEXT)").unwrap();
    execute_sql("INSERT INTO cast_test VALUES (1, '42')").unwrap();

    let result = execute_sql("SELECT val::integer AS num FROM cast_test").unwrap();
    assert!(result.contains("42"), "Expected 42, got: {:?}", result);
}

#[test]
fn test_double_colon_cast_float() {
    let _lock = TEST_MUTEX.lock().unwrap();
    reset_database();

    execute_sql("CREATE TABLE cast_f (id INTEGER, val TEXT)").unwrap();
    execute_sql("INSERT INTO cast_f VALUES (1, '3.14')").unwrap();

    let result = execute_sql("SELECT val::float AS num FROM cast_f").unwrap();
    assert!(result.contains("3.14"), "Expected 3.14, got: {:?}", result);
}

#[test]
fn test_fetch_first_rows_only() {
    let _lock = TEST_MUTEX.lock().unwrap();
    reset_database();

    execute_sql("CREATE TABLE fetch_test (id INTEGER, val INTEGER)").unwrap();
    execute_sql("INSERT INTO fetch_test VALUES (1, 10)").unwrap();
    execute_sql("INSERT INTO fetch_test VALUES (2, 20)").unwrap();
    execute_sql("INSERT INTO fetch_test VALUES (3, 30)").unwrap();
    execute_sql("INSERT INTO fetch_test VALUES (4, 40)").unwrap();
    execute_sql("INSERT INTO fetch_test VALUES (5, 50)").unwrap();

    let result =
        execute_sql("SELECT id FROM fetch_test ORDER BY id FETCH FIRST 3 ROWS ONLY").unwrap();
    let lines: Vec<String> = result.lines().collect();
    let data_lines: Vec<String> = lines
        .iter()
        .skip(2)
        .filter(|l| !l.is_empty())
        .cloned()
        .collect();
    assert_eq!(data_lines.len(), 3, "Expected 3 rows, got: {:?}", result);
}

#[test]
fn test_fetch_with_ties() {
    let _lock = TEST_MUTEX.lock().unwrap();
    reset_database();

    execute_sql("CREATE TABLE fetch_ties (id INTEGER, score INTEGER)").unwrap();
    execute_sql("INSERT INTO fetch_ties VALUES (1, 100)").unwrap();
    execute_sql("INSERT INTO fetch_ties VALUES (2, 90)").unwrap();
    execute_sql("INSERT INTO fetch_ties VALUES (3, 90)").unwrap();
    execute_sql("INSERT INTO fetch_ties VALUES (4, 80)").unwrap();
    execute_sql("INSERT INTO fetch_ties VALUES (5, 70)").unwrap();

    let result = execute_sql(
        "SELECT id, score FROM fetch_ties ORDER BY score DESC FETCH FIRST 2 ROWS WITH TIES",
    )
    .unwrap();
    let lines: Vec<String> = result.lines().collect();
    let data_lines: Vec<String> = lines
        .iter()
        .skip(2)
        .filter(|l| !l.is_empty())
        .cloned()
        .collect();
    assert_eq!(
        data_lines.len(),
        3,
        "Expected 3 rows (2 + 1 tied), got: {:?}",
        result
    );
}

#[test]
fn test_generate_series_basic() {
    let _lock = TEST_MUTEX.lock().unwrap();
    reset_database();

    let result = execute_sql("SELECT * FROM GENERATE_SERIES(1, 5)").unwrap();
    assert!(result.contains("1"), "Expected 1, got: {:?}", result);
    assert!(result.contains("5"), "Expected 5, got: {:?}", result);
    let lines: Vec<String> = result.lines().collect();
    let data_lines: Vec<String> = lines
        .iter()
        .skip(2)
        .filter(|l| !l.is_empty())
        .cloned()
        .collect();
    assert_eq!(data_lines.len(), 5, "Expected 5 rows, got: {:?}", result);
}

#[test]
fn test_generate_series_with_step() {
    let _lock = TEST_MUTEX.lock().unwrap();
    reset_database();

    let result = execute_sql("SELECT * FROM GENERATE_SERIES(0, 10, 2)").unwrap();
    let lines: Vec<String> = result.lines().collect();
    let data_lines: Vec<String> = lines
        .iter()
        .skip(2)
        .filter(|l| !l.is_empty())
        .cloned()
        .collect();
    assert_eq!(
        data_lines.len(),
        6,
        "Expected 6 rows (0,2,4,6,8,10), got: {:?}",
        result
    );
}

#[test]
fn test_generate_series_negative_step() {
    let _lock = TEST_MUTEX.lock().unwrap();
    reset_database();

    let result = execute_sql("SELECT * FROM GENERATE_SERIES(5, 1, -1)").unwrap();
    let lines: Vec<String> = result.lines().collect();
    let data_lines: Vec<String> = lines
        .iter()
        .skip(2)
        .filter(|l| !l.is_empty())
        .cloned()
        .collect();
    assert_eq!(
        data_lines.len(),
        5,
        "Expected 5 rows (5,4,3,2,1), got: {:?}",
        result
    );
}

#[test]
fn test_rollup() {
    let _lock = TEST_MUTEX.lock().unwrap();
    reset_database();

    execute_sql("CREATE TABLE rollup_test (region TEXT, product TEXT, sales INTEGER)").unwrap();
    execute_sql("INSERT INTO rollup_test VALUES ('East', 'Widget', 100)").unwrap();
    execute_sql("INSERT INTO rollup_test VALUES ('East', 'Gadget', 150)").unwrap();
    execute_sql("INSERT INTO rollup_test VALUES ('West', 'Widget', 200)").unwrap();
    execute_sql("INSERT INTO rollup_test VALUES ('West', 'Gadget', 250)").unwrap();

    let result = execute_sql(
        "SELECT region, product, SUM(sales) AS total FROM rollup_test GROUP BY ROLLUP(region, product)",
    )
    .unwrap();

    let lines: Vec<String> = result.lines().collect();
    let data_lines: Vec<String> = lines
        .iter()
        .skip(2)
        .filter(|l| !l.is_empty())
        .cloned()
        .collect();
    assert!(
        data_lines.len() >= 7,
        "ROLLUP(a,b) should produce (a,b), (a), () sets = at least 7 rows, got: {:?}",
        result
    );
    assert!(
        result.contains("700"),
        "Grand total should be 700, got: {:?}",
        result
    );
}

#[test]
fn test_cube() {
    let _lock = TEST_MUTEX.lock().unwrap();
    reset_database();

    execute_sql("CREATE TABLE cube_test (a TEXT, b TEXT, val INTEGER)").unwrap();
    execute_sql("INSERT INTO cube_test VALUES ('x', '1', 10)").unwrap();
    execute_sql("INSERT INTO cube_test VALUES ('x', '2', 20)").unwrap();
    execute_sql("INSERT INTO cube_test VALUES ('y', '1', 30)").unwrap();
    execute_sql("INSERT INTO cube_test VALUES ('y', '2', 40)").unwrap();

    let result =
        execute_sql("SELECT a, b, SUM(val) AS total FROM cube_test GROUP BY CUBE(a, b)").unwrap();

    let lines: Vec<String> = result.lines().collect();
    let data_lines: Vec<String> = lines
        .iter()
        .skip(2)
        .filter(|l| !l.is_empty())
        .cloned()
        .collect();
    assert!(
        data_lines.len() >= 9,
        "CUBE(a,b) should produce (a,b), (a), (b), () sets = at least 9 rows, got: {:?}",
        result
    );
    assert!(
        result.contains("100"),
        "Grand total should be 100, got: {:?}",
        result
    );
}

#[test]
fn test_grouping_sets() {
    let _lock = TEST_MUTEX.lock().unwrap();
    reset_database();

    execute_sql("CREATE TABLE gs_test (dept TEXT, role TEXT, salary INTEGER)").unwrap();
    execute_sql("INSERT INTO gs_test VALUES ('eng', 'dev', 100)").unwrap();
    execute_sql("INSERT INTO gs_test VALUES ('eng', 'qa', 90)").unwrap();
    execute_sql("INSERT INTO gs_test VALUES ('sales', 'rep', 80)").unwrap();

    let result = execute_sql("SELECT dept, role, SUM(salary) AS total FROM gs_test GROUP BY GROUPING SETS ((dept), (role))").unwrap();

    let lines: Vec<String> = result.lines().collect();
    let data_lines: Vec<String> = lines
        .iter()
        .skip(2)
        .filter(|l| !l.is_empty())
        .cloned()
        .collect();
    assert!(
        data_lines.len() >= 5,
        "GROUPING SETS ((dept), (role)) with 2 depts + 3 roles = 5 rows, got: {:?}",
        result
    );
}

#[test]
fn test_distinct_on() {
    let _lock = TEST_MUTEX.lock().unwrap();
    reset_database();

    execute_sql("CREATE TABLE don_test (category TEXT, item TEXT, price INTEGER)").unwrap();
    execute_sql("INSERT INTO don_test VALUES ('fruit', 'apple', 1)").unwrap();
    execute_sql("INSERT INTO don_test VALUES ('fruit', 'banana', 2)").unwrap();
    execute_sql("INSERT INTO don_test VALUES ('veggie', 'carrot', 3)").unwrap();
    execute_sql("INSERT INTO don_test VALUES ('veggie', 'broccoli', 4)").unwrap();

    let result = execute_sql(
        "SELECT DISTINCT ON (category) category, item, price FROM don_test ORDER BY category, price",
    )
    .unwrap();
    let lines: Vec<String> = result.lines().collect();
    let data_lines: Vec<String> = lines
        .iter()
        .skip(2)
        .filter(|l| !l.is_empty())
        .cloned()
        .collect();
    assert_eq!(
        data_lines.len(),
        2,
        "Expected 2 rows (one per category), got: {:?}",
        result
    );
    assert!(
        result.contains("apple"),
        "Expected cheapest fruit 'apple', got: {:?}",
        result
    );
    assert!(
        result.contains("carrot"),
        "Expected cheapest veggie 'carrot', got: {:?}",
        result
    );
}

#[test]
fn test_is_distinct_from_in_where() {
    let _lock = TEST_MUTEX.lock().unwrap();
    reset_database();

    execute_sql("CREATE TABLE idf_test (id INTEGER, a INTEGER, b INTEGER)").unwrap();
    execute_sql("INSERT INTO idf_test VALUES (1, 10, 10)").unwrap();
    execute_sql("INSERT INTO idf_test VALUES (2, 10, 20)").unwrap();
    execute_sql("INSERT INTO idf_test VALUES (3, NULL, NULL)").unwrap();
    execute_sql("INSERT INTO idf_test VALUES (4, NULL, 10)").unwrap();

    let result =
        execute_sql("SELECT id FROM idf_test WHERE a IS NOT DISTINCT FROM b ORDER BY id").unwrap();
    assert!(
        result.contains("1"),
        "Expected id=1 (10=10), got: {:?}",
        result
    );
    assert!(
        result.contains("3"),
        "Expected id=3 (NULL=NULL), got: {:?}",
        result
    );
}

#[test]
fn test_generate_series_with_where() {
    let _lock = TEST_MUTEX.lock().unwrap();
    reset_database();

    let result =
        execute_sql("SELECT * FROM GENERATE_SERIES(1, 10) WHERE generate_series > 7").unwrap();
    let lines: Vec<String> = result.lines().collect();
    let data_lines: Vec<String> = lines
        .iter()
        .skip(2)
        .filter(|l| !l.is_empty())
        .cloned()
        .collect();
    assert_eq!(
        data_lines.len(),
        3,
        "Expected 3 rows (8,9,10), got: {:?}",
        result
    );
}

#[test]
fn test_fetch_next() {
    let _lock = TEST_MUTEX.lock().unwrap();
    reset_database();

    execute_sql("CREATE TABLE fetch_next (id INTEGER)").unwrap();
    execute_sql("INSERT INTO fetch_next VALUES (1)").unwrap();
    execute_sql("INSERT INTO fetch_next VALUES (2)").unwrap();
    execute_sql("INSERT INTO fetch_next VALUES (3)").unwrap();

    let result =
        execute_sql("SELECT id FROM fetch_next ORDER BY id FETCH NEXT 2 ROWS ONLY").unwrap();
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
fn test_double_colon_cast_in_where() {
    let _lock = TEST_MUTEX.lock().unwrap();
    reset_database();

    execute_sql("CREATE TABLE cast_w (id INTEGER, val TEXT)").unwrap();
    execute_sql("INSERT INTO cast_w VALUES (1, '100')").unwrap();
    execute_sql("INSERT INTO cast_w VALUES (2, '200')").unwrap();
    execute_sql("INSERT INTO cast_w VALUES (3, '50')").unwrap();

    let result = execute_sql("SELECT id FROM cast_w WHERE val::integer > 75 ORDER BY id").unwrap();
    assert!(result.contains("1"), "Expected id=1, got: {:?}", result);
    assert!(result.contains("2"), "Expected id=2, got: {:?}", result);
    assert!(
        !result.contains("3"),
        "Should not contain id=3, got: {:?}",
        result
    );
}
