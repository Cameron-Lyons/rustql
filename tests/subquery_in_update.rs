use rustql::executor::reset_database_state;
use rustql::process_query;
use std::sync::Once;

static INIT: Once = Once::new();

fn setup() {
    INIT.call_once(|| {
        reset_database_state();
    });
}

#[test]
fn test_update_with_scalar_subquery() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS squ_prices1 (id INTEGER, price INTEGER)").unwrap();
    process_query("CREATE TABLE IF NOT EXISTS squ_products1 (id INTEGER, max_price INTEGER)")
        .unwrap();
    process_query("DELETE FROM squ_prices1").unwrap_or_default();
    process_query("DELETE FROM squ_products1").unwrap_or_default();
    process_query("INSERT INTO squ_prices1 VALUES (1, 100), (2, 200), (3, 150)").unwrap();
    process_query("INSERT INTO squ_products1 VALUES (1, 0)").unwrap();

    let result =
        process_query("UPDATE squ_products1 SET max_price = (SELECT MAX(price) FROM squ_prices1)")
            .unwrap();
    assert!(result.contains("1 row(s) updated"));

    let check = process_query("SELECT max_price FROM squ_products1").unwrap();
    assert!(check.contains("200"));
}

#[test]
fn test_update_with_subquery_where() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS squ_inventory2 (id INTEGER, qty INTEGER)").unwrap();
    process_query("CREATE TABLE IF NOT EXISTS squ_low_stock2 (id INTEGER)").unwrap();
    process_query("DELETE FROM squ_inventory2").unwrap_or_default();
    process_query("DELETE FROM squ_low_stock2").unwrap_or_default();
    process_query("INSERT INTO squ_inventory2 VALUES (1, 5), (2, 10), (3, 2)").unwrap();
    process_query("INSERT INTO squ_low_stock2 VALUES (1), (3)").unwrap();

    process_query(
        "UPDATE squ_inventory2 SET qty = qty + 10 WHERE id IN (SELECT id FROM squ_low_stock2)",
    )
    .unwrap();

    let result = process_query("SELECT id, qty FROM squ_inventory2 ORDER BY id").unwrap();
    assert!(result.contains("15"));
    assert!(result.contains("12"));
    assert!(result.contains("10"));
}

#[test]
fn test_update_subquery_returns_null() {
    setup();
    process_query("CREATE TABLE IF NOT EXISTS squ_target_null3 (id INTEGER, val INTEGER)").unwrap();
    process_query("CREATE TABLE IF NOT EXISTS squ_empty_source3 (val INTEGER)").unwrap();
    process_query("DELETE FROM squ_target_null3").unwrap_or_default();
    process_query("DELETE FROM squ_empty_source3").unwrap_or_default();
    process_query("INSERT INTO squ_target_null3 VALUES (1, 100)").unwrap();

    process_query("UPDATE squ_target_null3 SET val = (SELECT val FROM squ_empty_source3 LIMIT 1)")
        .unwrap();

    let result = process_query("SELECT val FROM squ_target_null3").unwrap();
    assert!(result.contains("NULL") || result.contains("null"));
}
