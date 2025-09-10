#!/bin/bash

echo "Testing RustQL - SQL Engine in Rust"
echo "===================================="
echo ""

# Test CREATE TABLE
echo "CREATE TABLE users (id INT, name TEXT, age INT)" | cargo run --quiet 2>/dev/null | head -n 5

echo ""

# Test INSERT
echo -e "CREATE TABLE users (id INT, name TEXT, age INT)\nINSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)" | cargo run --quiet 2>/dev/null | tail -n 3

echo ""

# Test SELECT
echo -e "CREATE TABLE users (id INT, name TEXT, age INT)\nINSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)\nSELECT * FROM users" | cargo run --quiet 2>/dev/null | tail -n 6

echo ""

# Test SELECT with WHERE
echo -e "CREATE TABLE users (id INT, name TEXT, age INT)\nINSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)\nSELECT name, age FROM users WHERE age > 25" | cargo run --quiet 2>/dev/null | tail -n 5

echo ""

# Test UPDATE
echo -e "CREATE TABLE users (id INT, name TEXT, age INT)\nINSERT INTO users VALUES (1, 'Alice', 30)\nUPDATE users SET age = 31 WHERE name = 'Alice'\nSELECT * FROM users" | cargo run --quiet 2>/dev/null | tail -n 4

echo ""

# Test DELETE
echo -e "CREATE TABLE users (id INT, name TEXT, age INT)\nINSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)\nDELETE FROM users WHERE age < 30\nSELECT * FROM users" | cargo run --quiet 2>/dev/null | tail -n 4