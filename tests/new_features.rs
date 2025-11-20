use rustql::process_query;
use rustql::reset_database;

#[test]
fn test_insert_with_column_names() {
    reset_database();

    process_query("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();

    let result = process_query("INSERT INTO users (name, age, id) VALUES ('Alice', 25, 1)");
    assert!(result.is_ok(), "Insert with column names should succeed");
    assert_eq!(result.unwrap(), "1 row(s) inserted");

    let result = process_query("SELECT id, name, age FROM users");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("1"));
    assert!(output.contains("Alice"));
    assert!(output.contains("25"));
}

#[test]
fn test_arithmetic_expressions_in_select() {
    reset_database();

    process_query("CREATE TABLE products (id INTEGER, price FLOAT, quantity INTEGER)").unwrap();
    process_query("INSERT INTO products VALUES (1, 10.5, 3), (2, 20.0, 2)").unwrap();

    let result = process_query("SELECT id, price * quantity AS total FROM products");
    match &result {
        Ok(output) => {
            println!("Success! Output: {}", output);
            assert!(output.contains("total"));
            assert!(output.contains("31.5")); // 10.5 * 3
        }
        Err(e) => {
            panic!("Arithmetic expression failed with error: {}", e);
        }
    }

    let result = process_query("SELECT id, price + quantity AS sum FROM products");
    match &result {
        Ok(output) => println!("Addition works! Output: {}", output),
        Err(e) => panic!("Addition failed: {}", e),
    }
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("13.5")); // 10.5 + 3
    assert!(output.contains("22")); // 20.0 + 2

    let result = process_query("SELECT id, price - quantity AS diff FROM products");
    assert!(result.is_ok());

    let result = process_query("SELECT id, price / quantity AS unit_price FROM products");
    assert!(result.is_ok());
}
