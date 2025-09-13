#!/bin/bash

echo "RustQL Test Suite"
echo "=================="
echo ""

# Test CREATE TABLE
echo "Test 1: CREATE TABLE"
echo "CREATE TABLE users (id INT, name TEXT, age INT, salary FLOAT)" | cargo run --quiet
echo ""

# Test INSERT
echo "Test 2: INSERT VALUES"
echo "INSERT INTO users VALUES (1, 'Alice', 25, 50000.0)" | cargo run --quiet
echo "INSERT INTO users VALUES (2, 'Bob', 30, 60000.0)" | cargo run --quiet
echo "INSERT INTO users VALUES (3, 'Charlie', 35, 75000.0)" | cargo run --quiet
echo "INSERT INTO users VALUES (4, 'Diana', 28, 55000.0)" | cargo run --quiet
echo "INSERT INTO users VALUES (5, 'Eve', 32, 65000.0)" | cargo run --quiet
echo ""

# Test SELECT *
echo "Test 3: SELECT * FROM users"
echo "SELECT * FROM users" | cargo run --quiet
echo ""

# Test SELECT with specific columns
echo "Test 4: SELECT name, age FROM users"
echo "SELECT name, age FROM users" | cargo run --quiet
echo ""

# Test WHERE clause
echo "Test 5: SELECT with WHERE clause"
echo "SELECT * FROM users WHERE age > 30" | cargo run --quiet
echo ""

# Test ORDER BY
echo "Test 6: SELECT with ORDER BY (ascending)"
echo "SELECT * FROM users ORDER BY age" | cargo run --quiet
echo ""

echo "Test 7: SELECT with ORDER BY (descending)"
echo "SELECT * FROM users ORDER BY salary DESC" | cargo run --quiet
echo ""

# Test LIMIT
echo "Test 8: SELECT with LIMIT"
echo "SELECT * FROM users LIMIT 3" | cargo run --quiet
echo ""

# Test ORDER BY with LIMIT
echo "Test 9: SELECT with ORDER BY and LIMIT"
echo "SELECT * FROM users ORDER BY salary DESC LIMIT 2" | cargo run --quiet
echo ""

# Test OFFSET
echo "Test 10: SELECT with LIMIT and OFFSET"
echo "SELECT * FROM users ORDER BY id LIMIT 2 OFFSET 2" | cargo run --quiet
echo ""

# Test UPDATE
echo "Test 11: UPDATE"
echo "UPDATE users SET salary = 70000.0 WHERE name = 'Bob'" | cargo run --quiet
echo "SELECT * FROM users WHERE name = 'Bob'" | cargo run --quiet
echo ""

# Test DELETE
echo "Test 12: DELETE"
echo "DELETE FROM users WHERE age < 30" | cargo run --quiet
echo "SELECT * FROM users" | cargo run --quiet
echo ""

# Test compound WHERE conditions
echo "Test 13: Compound WHERE with AND"
echo "SELECT * FROM users WHERE age > 30 AND salary > 60000" | cargo run --quiet
echo ""

# Test DROP TABLE
echo "Test 14: DROP TABLE"
echo "DROP TABLE users" | cargo run --quiet
echo ""

# Test error handling
echo "Test 15: Error handling - SELECT from non-existent table"
echo "SELECT * FROM nonexistent" | cargo run --quiet
echo ""

echo "==================="
echo "Test Suite Complete"
echo "==================="

