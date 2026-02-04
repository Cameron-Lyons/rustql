use rustql::{process_query, reset_database};
use std::sync::Mutex;

static GLOBAL_TEST_LOCK: Mutex<()> = Mutex::new(());

fn setup_employees() {
    reset_database();
    process_query(
        "CREATE TABLE employees (id INTEGER, name TEXT, dept TEXT, salary INTEGER, active INTEGER)",
    )
    .unwrap();
    process_query("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 90000, 1)").unwrap();
    process_query("INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 85000, 1)").unwrap();
    process_query("INSERT INTO employees VALUES (3, 'Charlie', 'Sales', 70000, 1)").unwrap();
    process_query("INSERT INTO employees VALUES (4, 'Diana', 'Sales', 75000, 0)").unwrap();
    process_query("INSERT INTO employees VALUES (5, 'Eve', 'HR', 65000, 1)").unwrap();
    process_query("INSERT INTO employees VALUES (6, 'Frank', 'HR', 65000, 0)").unwrap();
    process_query("INSERT INTO employees VALUES (7, 'Grace', 'Engineering', 95000, 1)").unwrap();
}

#[test]
fn test_group_concat_basic() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_database();
    process_query("CREATE TABLE gc_test (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO gc_test VALUES (1, 'Alice')").unwrap();
    process_query("INSERT INTO gc_test VALUES (2, 'Bob')").unwrap();
    process_query("INSERT INTO gc_test VALUES (3, 'Charlie')").unwrap();
    let result = process_query("SELECT GROUP_CONCAT(name) AS names FROM gc_test").unwrap();
    assert!(result.contains("Alice,Bob,Charlie"));
}

#[test]
fn test_group_concat_with_separator() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_database();
    process_query("CREATE TABLE gc_sep (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO gc_sep VALUES (1, 'Alice')").unwrap();
    process_query("INSERT INTO gc_sep VALUES (2, 'Bob')").unwrap();
    process_query("INSERT INTO gc_sep VALUES (3, 'Charlie')").unwrap();
    let result =
        process_query("SELECT GROUP_CONCAT(name SEPARATOR '; ') AS names FROM gc_sep").unwrap();
    assert!(result.contains("Alice; Bob; Charlie"));
}

#[test]
fn test_string_agg_alias() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_database();
    process_query("CREATE TABLE sa_test (id INTEGER, name TEXT)").unwrap();
    process_query("INSERT INTO sa_test VALUES (1, 'Alice')").unwrap();
    process_query("INSERT INTO sa_test VALUES (2, 'Bob')").unwrap();
    let result = process_query("SELECT STRING_AGG(name, ' | ') AS names FROM sa_test").unwrap();
    assert!(result.contains("Alice | Bob"));
}

#[test]
fn test_group_concat_with_group_by() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    setup_employees();
    let result = process_query(
        "SELECT dept, GROUP_CONCAT(name) AS members FROM employees GROUP BY dept ORDER BY dept",
    )
    .unwrap();
    assert!(result.contains("Engineering"));
    assert!(result.contains("HR"));
    assert!(result.contains("Sales"));
}

#[test]
fn test_bool_and_all_true() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_database();
    process_query("CREATE TABLE ba_test (id INTEGER, active INTEGER)").unwrap();
    process_query("INSERT INTO ba_test VALUES (1, 1)").unwrap();
    process_query("INSERT INTO ba_test VALUES (2, 1)").unwrap();
    process_query("INSERT INTO ba_test VALUES (3, 1)").unwrap();
    let result = process_query("SELECT BOOL_AND(active) AS all_active FROM ba_test").unwrap();
    assert!(result.contains("true"));
}

#[test]
fn test_bool_and_not_all_true() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_database();
    process_query("CREATE TABLE ba_test2 (id INTEGER, active INTEGER)").unwrap();
    process_query("INSERT INTO ba_test2 VALUES (1, 1)").unwrap();
    process_query("INSERT INTO ba_test2 VALUES (2, 0)").unwrap();
    process_query("INSERT INTO ba_test2 VALUES (3, 1)").unwrap();
    let result = process_query("SELECT BOOL_AND(active) AS all_active FROM ba_test2").unwrap();
    assert!(result.contains("false"));
}

#[test]
fn test_every_is_bool_and() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_database();
    process_query("CREATE TABLE ev_test (id INTEGER, val INTEGER)").unwrap();
    process_query("INSERT INTO ev_test VALUES (1, 1)").unwrap();
    process_query("INSERT INTO ev_test VALUES (2, 1)").unwrap();
    let result = process_query("SELECT EVERY(val) AS all_true FROM ev_test").unwrap();
    assert!(result.contains("true"));
}

#[test]
fn test_bool_or_some_true() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_database();
    process_query("CREATE TABLE bo_test (id INTEGER, val INTEGER)").unwrap();
    process_query("INSERT INTO bo_test VALUES (1, 0)").unwrap();
    process_query("INSERT INTO bo_test VALUES (2, 1)").unwrap();
    process_query("INSERT INTO bo_test VALUES (3, 0)").unwrap();
    let result = process_query("SELECT BOOL_OR(val) AS any_true FROM bo_test").unwrap();
    assert!(result.contains("true"));
}

#[test]
fn test_bool_or_none_true() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_database();
    process_query("CREATE TABLE bo_test2 (id INTEGER, val INTEGER)").unwrap();
    process_query("INSERT INTO bo_test2 VALUES (1, 0)").unwrap();
    process_query("INSERT INTO bo_test2 VALUES (2, 0)").unwrap();
    let result = process_query("SELECT BOOL_OR(val) AS any_true FROM bo_test2").unwrap();
    assert!(result.contains("false"));
}

#[test]
fn test_median_odd_count() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_database();
    process_query("CREATE TABLE med_odd (val INTEGER)").unwrap();
    process_query("INSERT INTO med_odd VALUES (1)").unwrap();
    process_query("INSERT INTO med_odd VALUES (3)").unwrap();
    process_query("INSERT INTO med_odd VALUES (5)").unwrap();
    process_query("INSERT INTO med_odd VALUES (7)").unwrap();
    process_query("INSERT INTO med_odd VALUES (9)").unwrap();
    let result = process_query("SELECT MEDIAN(val) AS med FROM med_odd").unwrap();
    assert!(result.contains("5"));
}

#[test]
fn test_median_even_count() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_database();
    process_query("CREATE TABLE med_even (val INTEGER)").unwrap();
    process_query("INSERT INTO med_even VALUES (1)").unwrap();
    process_query("INSERT INTO med_even VALUES (3)").unwrap();
    process_query("INSERT INTO med_even VALUES (5)").unwrap();
    process_query("INSERT INTO med_even VALUES (7)").unwrap();
    let result = process_query("SELECT MEDIAN(val) AS med FROM med_even").unwrap();
    assert!(result.contains("4"));
}

#[test]
fn test_mode() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_database();
    process_query("CREATE TABLE mode_test (val INTEGER)").unwrap();
    process_query("INSERT INTO mode_test VALUES (1)").unwrap();
    process_query("INSERT INTO mode_test VALUES (2)").unwrap();
    process_query("INSERT INTO mode_test VALUES (2)").unwrap();
    process_query("INSERT INTO mode_test VALUES (3)").unwrap();
    process_query("INSERT INTO mode_test VALUES (3)").unwrap();
    process_query("INSERT INTO mode_test VALUES (3)").unwrap();
    let result = process_query("SELECT MODE(val) AS modal FROM mode_test").unwrap();
    assert!(result.contains("3"));
}

#[test]
fn test_percentile_cont_simple() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_database();
    process_query("CREATE TABLE pct_test (val INTEGER)").unwrap();
    process_query("INSERT INTO pct_test VALUES (10)").unwrap();
    process_query("INSERT INTO pct_test VALUES (20)").unwrap();
    process_query("INSERT INTO pct_test VALUES (30)").unwrap();
    process_query("INSERT INTO pct_test VALUES (40)").unwrap();
    let result = process_query("SELECT PERCENTILE_CONT(0.5, val) AS p50 FROM pct_test").unwrap();
    assert!(result.contains("25"));
}

#[test]
fn test_percentile_cont_within_group() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_database();
    process_query("CREATE TABLE pct_wg (val INTEGER)").unwrap();
    process_query("INSERT INTO pct_wg VALUES (10)").unwrap();
    process_query("INSERT INTO pct_wg VALUES (20)").unwrap();
    process_query("INSERT INTO pct_wg VALUES (30)").unwrap();
    process_query("INSERT INTO pct_wg VALUES (40)").unwrap();
    let result = process_query(
        "SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY val) AS p25 FROM pct_wg",
    )
    .unwrap();
    assert!(result.contains("17.5"));
}

#[test]
fn test_percentile_disc() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_database();
    process_query("CREATE TABLE pcd_test (val INTEGER)").unwrap();
    process_query("INSERT INTO pcd_test VALUES (10)").unwrap();
    process_query("INSERT INTO pcd_test VALUES (20)").unwrap();
    process_query("INSERT INTO pcd_test VALUES (30)").unwrap();
    process_query("INSERT INTO pcd_test VALUES (40)").unwrap();
    let result = process_query("SELECT PERCENTILE_DISC(0.5, val) AS p50 FROM pcd_test").unwrap();
    assert!(result.contains("20"));
}

#[test]
fn test_bool_and_with_group_by() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    setup_employees();
    let result = process_query(
        "SELECT dept, BOOL_AND(active) AS all_active FROM employees GROUP BY dept ORDER BY dept",
    )
    .unwrap();
    assert!(result.contains("Engineering"));
    assert!(result.contains("true"));
}

#[test]
fn test_median_with_group_by() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    setup_employees();
    let result = process_query(
        "SELECT dept, MEDIAN(salary) AS med_sal FROM employees GROUP BY dept ORDER BY dept",
    )
    .unwrap();
    assert!(result.contains("Engineering"));
    assert!(result.contains("90000"));
}

#[test]
fn test_mode_with_group_by() {
    let _lock = GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    setup_employees();
    let result = process_query(
        "SELECT dept, MODE(salary) AS modal_sal FROM employees GROUP BY dept ORDER BY dept",
    )
    .unwrap();
    assert!(result.contains("HR"));
    assert!(result.contains("65000"));
}
