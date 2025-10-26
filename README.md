# RustQL

A lightweight SQL database engine written in Rust. RustQL is an educational implementation of a SQL database with support for DDL, DML, and basic query features.

## Features

### ✅ Implemented Features

- **Data Definition Language (DDL)**
  - `CREATE TABLE` - Create tables with column definitions
  - `DROP TABLE` - Delete tables
  - `ALTER TABLE` - Add, drop, and rename columns

- **Data Manipulation Language (DML)**
  - `INSERT` - Insert single or multiple rows
  - `UPDATE` - Update rows with WHERE clause
  - `DELETE` - Delete rows with WHERE clause
  - `SELECT` - Query data with various clauses

- **SELECT Features**
  - Column selection (SELECT *)
  - WHERE clause with comparison operators (=, !=, <, <=, >, >=)
  - Logical operators (AND, OR, NOT)
  - **IN operator**: `WHERE column IN (value1, value2, ...)`
  - **LIKE operator**: `WHERE column LIKE 'pattern'` (supports % and _ wildcards)
  - **BETWEEN operator**: `WHERE column BETWEEN value1 AND value2`
  - ORDER BY (ASC/DESC)
  - LIMIT and OFFSET
  - Aggregate functions: COUNT, SUM, AVG, MIN, MAX
  - GROUP BY with HAVING clause
  - **JOIN operations** (INNER, LEFT, RIGHT, FULL)

- **Data Types**
  - INTEGER
  - FLOAT
  - TEXT
  - BOOLEAN

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

-- Ordering
SELECT * FROM users ORDER BY age ASC;
SELECT * FROM users ORDER BY name DESC;

-- Limit and Offset
SELECT * FROM users LIMIT 5;
SELECT * FROM users OFFSET 2 LIMIT 3;
```

### Aggregate Functions

```sql
SELECT COUNT(*) FROM users;
SELECT AVG(age) FROM users;
SELECT MIN(age) FROM users;
SELECT MAX(age) FROM users;
SELECT SUM(amount) FROM orders;
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

### Altering Tables

```sql
-- Add a column
ALTER TABLE users ADD COLUMN city TEXT;

-- Rename a column
ALTER TABLE users RENAME COLUMN email TO email_address;

-- Drop a column
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
# Run all tests
cargo test

# Run tests for specific modules
cargo test integration_tests
cargo test alter
cargo test select
```

## Limitations

- Currently supports only simple equality joins
- No support for subqueries
- No indexes for performance optimization
- No foreign key constraints
- Limited to single-file JSON storage
- No concurrent access support
- No transactions or rollback capabilities

## Future Enhancements

Possible improvements for the project:

- [ ] Full JOIN support with complex conditions
- [ ] Subquery support
- [ ] Index implementation for better performance
- [ ] Foreign key constraints
- [ ] Transaction support with rollback
- [ ] B-tree or LSM-tree storage engine
- [ ] Concurrency control
- [ ] LIKE, IN, BETWEEN operators
- [ ] Date/time types
- [ ] NULL handling improvements

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is open source and available under the MIT License.

