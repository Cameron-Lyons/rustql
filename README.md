# RustQL

A lightweight SQL database engine written in Rust. RustQL is an educational implementation of a SQL database with support for DDL, DML, and basic query features.

## Features

### ✅ Implemented Features

- **Data Definition Language (DDL)**
  - `CREATE TABLE` - Create tables with column definitions
  - `DROP TABLE` - Delete tables
  - `ALTER TABLE` - Add, drop, and rename columns
  - **Foreign Key Constraints** - Enforce referential integrity with ON DELETE and ON UPDATE actions

- **Data Manipulation Language (DML)**
  - `INSERT` - Insert single or multiple rows
  - `UPDATE` - Update rows with WHERE clause
  - `DELETE` - Delete rows with WHERE clause
  - `SELECT` - Query data with various clauses

- **SELECT Features**
  - Column selection (SELECT *)
  - **SELECT DISTINCT**: `SELECT DISTINCT column FROM table` - Remove duplicate rows
  - WHERE clause with comparison operators (=, !=, <, <=, >, >=)
  - Logical operators (AND, OR, NOT)
  - **IN operator**: `WHERE column IN (value1, value2, ...)`
  - **LIKE operator**: `WHERE column LIKE 'pattern'` (supports % and _ wildcards)
  - **BETWEEN operator**: `WHERE column BETWEEN value1 AND value2`
  - **IS NULL / IS NOT NULL**: `WHERE column IS NULL` or `WHERE column IS NOT NULL`
  - ORDER BY (ASC/DESC)
  - LIMIT and OFFSET
  - Aggregate functions: COUNT, SUM, AVG, MIN, MAX (with DISTINCT support: `COUNT(DISTINCT column)`)
  - GROUP BY with HAVING clause
  - **JOIN operations** (INNER, LEFT, RIGHT, FULL) - All join types now supported!
  - **Subqueries**: 
    - `IN (SELECT single_column FROM table [WHERE ...])` - Subquery in WHERE clause
    - `WHERE EXISTS (SELECT ...)` - EXISTS subquery (supports correlated subqueries)
    - `WHERE NOT EXISTS (SELECT ...)` - NOT EXISTS subquery
    - `SELECT ..., (SELECT ...) FROM ...` - Scalar subqueries in SELECT clause (correlated subqueries supported)

- **Data Types**
  - INTEGER
  - FLOAT
  - TEXT
  - BOOLEAN
  - DATE
  - TIME
  - DATETIME

- **Storage**
  - Persistent JSON-based storage
  - Automatic database state management

## Installation

```bash
# Clone the repository
git clone https://github.com/your-username/rustql.git
cd rustql

# Build the project
cargo build --release

# Run the interactive SQL shell
./target/release/rustql

# Or run tests
cargo test
```

## Usage

### Interactive SQL Shell

```bash
cargo run

# Or run the release binary
./target/release/rustql
```

### From Command Line

```bash
echo "SELECT * FROM users" | ./target/release/rustql
```

## Examples

### Creating Tables

```sql
CREATE TABLE users (
    id INTEGER,
    name TEXT,
    age INTEGER,
    email TEXT
);

CREATE TABLE orders (
    id INTEGER,
    user_id INTEGER,
    product TEXT,
    amount FLOAT
);
```

### Inserting Data

```sql
INSERT INTO users VALUES (1, 'Alice', 25, 'alice@example.com');
INSERT INTO users VALUES (2, 'Bob', 30, 'bob@example.com'), (3, 'Charlie', 35, 'charlie@example.com');
```

### Querying Data

```sql
-- Simple select
SELECT * FROM users;
SELECT name, email FROM users;

-- With WHERE clause
SELECT * FROM users WHERE age > 30;
SELECT * FROM users WHERE age > 25 AND age < 35;
SELECT * FROM users WHERE name = 'Alice' OR name = 'Bob';

-- With IN operator
SELECT * FROM users WHERE age IN (25, 30, 35);

-- With LIKE operator
SELECT * FROM users WHERE email LIKE '%@gmail.com';
SELECT * FROM products WHERE name LIKE 'Laptop%';

-- With BETWEEN operator
SELECT * FROM employees WHERE salary BETWEEN 50000 AND 100000;
SELECT * FROM products WHERE price BETWEEN 10.99 AND 29.99;

-- With NOT (inverts conditions)
SELECT * FROM users WHERE NOT (age IN (25, 30));
SELECT * FROM products WHERE NOT (name LIKE '%old%');
SELECT * FROM employees WHERE NOT (salary BETWEEN 40000 AND 60000);

-- With IS NULL / IS NOT NULL
SELECT * FROM users WHERE email IS NULL;
SELECT * FROM products WHERE description IS NOT NULL;
SELECT * FROM employees WHERE name IS NULL AND salary IS NOT NULL;

-- Ordering
SELECT * FROM users ORDER BY age ASC;
SELECT * FROM users ORDER BY name DESC;

-- Limit and Offset
SELECT * FROM users LIMIT 5;
SELECT * FROM users OFFSET 2 LIMIT 3;

-- SELECT DISTINCT
SELECT DISTINCT age FROM users;
SELECT DISTINCT name, email FROM users;
```

### Aggregate Functions

```sql
SELECT COUNT(*) FROM users;
SELECT AVG(age) FROM users;
SELECT MIN(age) FROM users;
SELECT MAX(age) FROM users;
SELECT SUM(amount) FROM orders;

-- With DISTINCT
SELECT COUNT(DISTINCT age) FROM users;
SELECT SUM(DISTINCT amount) FROM orders;
SELECT AVG(DISTINCT score) FROM grades;
SELECT MIN(DISTINCT price) FROM products;
SELECT MAX(DISTINCT salary) FROM employees;
```

### GROUP BY

```sql
-- Group by department
SELECT department, COUNT(*) FROM employees GROUP BY department;

-- With HAVING
SELECT department, AVG(salary) FROM employees 
GROUP BY department 
HAVING AVG(salary) > 60000;
```

### JOIN Operations

```sql
-- INNER JOIN
SELECT users.name, orders.product 
FROM users 
JOIN orders ON users.id = orders.user_id;

-- LEFT JOIN
SELECT users.name, orders.product 
FROM users 
LEFT JOIN orders ON users.id = orders.user_id;

-- With WHERE clause
SELECT users.name, orders.amount 
FROM users 
JOIN orders ON users.id = orders.user_id 
WHERE orders.amount > 100;
```

### Subqueries

```sql
-- EXISTS subquery
SELECT * FROM users 
WHERE EXISTS (SELECT * FROM orders WHERE orders.user_id = users.id);

-- NOT EXISTS subquery
SELECT * FROM users 
WHERE NOT EXISTS (SELECT * FROM orders WHERE orders.user_id = users.id);

-- Correlated EXISTS subquery
SELECT * FROM users 
WHERE EXISTS (
    SELECT * FROM orders 
    WHERE orders.user_id = users.id 
    AND orders.amount > 100
);

-- IN with subquery
SELECT * FROM users 
WHERE id IN (SELECT user_id FROM orders WHERE amount > 100);

-- Scalar subquery in SELECT clause
SELECT name, (SELECT amount FROM orders WHERE orders.user_id = users.id) AS order_amount
FROM users;
```

### Updating Data

```sql
UPDATE users SET age = 26 WHERE name = 'Alice';
UPDATE users SET email = 'newemail@example.com' WHERE id = 1;
```

### Deleting Data

```sql
DELETE FROM users WHERE age < 18;
DELETE FROM users WHERE name = 'Charlie';
```

### Foreign Key Constraints

```sql
CREATE TABLE orders (
    id INTEGER,
    user_id INTEGER FOREIGN KEY REFERENCES users(id)
);

CREATE TABLE order_items (
    id INTEGER,
    order_id INTEGER FOREIGN KEY REFERENCES orders(id) ON DELETE CASCADE
);

CREATE TABLE payments (
    id INTEGER,
    order_id INTEGER FOREIGN KEY REFERENCES orders(id) ON DELETE RESTRICT
);

CREATE TABLE comments (
    id INTEGER,
    post_id INTEGER FOREIGN KEY REFERENCES posts(id) ON DELETE SET NULL
);

CREATE TABLE order_history (
    id INTEGER,
    order_id INTEGER FOREIGN KEY REFERENCES orders(id) ON UPDATE CASCADE
);
```

### Altering Tables

```sql
ALTER TABLE users ADD COLUMN city TEXT;

ALTER TABLE users RENAME COLUMN email TO email_address;

ALTER TABLE users DROP COLUMN city;
```

### Dropping Tables

```sql
DROP TABLE users;
DROP TABLE orders;
```

## Architecture

The project is organized into several key modules:

- **`lexer.rs`** - Tokenizes SQL input into tokens
- **`parser.rs`** - Parses tokens into an Abstract Syntax Tree (AST)
- **`ast.rs`** - Defines the AST structures for all SQL statements
- **`executor.rs`** - Executes parsed statements and returns results
- **`database.rs`** - Handles database persistence and management
- **`main.rs`** - Interactive REPL interface
- **`lib.rs`** - Library entry point

## Testing

The project includes comprehensive tests in the `tests/` directory:

```bash
cargo test

cargo test integration_tests
cargo test alter
cargo test select
```

## Limitations

- No indexes for performance optimization
- Limited to single-file JSON storage
- No concurrent access support
- No transactions or rollback capabilities

## Future Enhancements

Possible improvements for the project:

- [ ] Index implementation for better performance
- [ ] Transaction support with rollback
- [ ] B-tree or LSM-tree storage engine
- [ ] Concurrency control

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is open source and available under the MIT License.

