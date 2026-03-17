# RustQL

A SQL database engine written in Rust with an interactive REPL. RustQL supports a broad subset of SQL including DDL, DML, joins, subqueries, aggregates, transactions, indexing, and cost-based query optimization, backed by pluggable storage engines.

## Features

**DDL**
- `CREATE TABLE` / `DROP TABLE`
- `ALTER TABLE` &mdash; `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`
- `CREATE INDEX` / `DROP INDEX`

**DML**
- `SELECT`, `INSERT`, `UPDATE`, `DELETE`

**Queries**
- `INNER` / `LEFT` / `RIGHT` / `FULL` JOIN
- Scalar subqueries, `IN` subqueries, `EXISTS` / `NOT EXISTS`
- Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` (with `DISTINCT`)
- `GROUP BY` / `HAVING`
- `ORDER BY` (ASC / DESC)
- `LIMIT` / `OFFSET`
- `UNION` / `UNION ALL`
- `DISTINCT`

**WHERE operators**
- Comparison: `=`, `<>`, `<`, `>`, `<=`, `>=`
- Logical: `AND`, `OR`, `NOT`
- `IN`, `LIKE`, `BETWEEN`, `IS NULL` / `IS NOT NULL`

**Arithmetic**
- `+`, `-`, `*`, `/`

**Constraints**
- `PRIMARY KEY`, `UNIQUE`, `NOT NULL`, `DEFAULT`
- `FOREIGN KEY` with `ON DELETE` actions (`RESTRICT`, `CASCADE`, `SET NULL`, `NO ACTION`)

**Transactions**
- `BEGIN` / `COMMIT` / `ROLLBACK` with write-ahead log (WAL)

**Other**
- `EXPLAIN` &mdash; display query execution plan
- `EXPLAIN ANALYZE` &mdash; plan + measured execution metrics
- `DESCRIBE` *table* &mdash; show table schema
- `SHOW TABLES`
- Backtick-quoted identifiers
- Cost-based query planner with index scan and predicate pushdown

## Quick start

```sh
cargo build --release
cargo run --release
```

An interactive REPL session:

```
rustql> CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE);
CREATE TABLE

rustql> INSERT INTO users VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com');
INSERT 2

rustql> SELECT * FROM users WHERE name LIKE 'A%';
id	name	email
----------------------------------------
1	Alice	alice@example.com

rustql> exit
```

You can also pipe queries through stdin:

```sh
echo "SHOW TABLES" | cargo run --release
```

For embedding, open an engine and execute SQL through a session:

```rust
use rustql::{Engine, EngineOptions, StorageMode};

let engine = Engine::open(EngineOptions {
    storage: StorageMode::Memory,
})
    .unwrap();
let mut session = engine.session();

session.execute_one("CREATE TABLE users (id INTEGER)").unwrap();
```

## Storage modes

| Mode | Description | Default |
|------|-------------|---------|
| `StorageMode::Memory` | In-memory engine state with no persistence | no |
| `StorageMode::Disk { path }` | B-tree-backed on-disk storage with WAL recovery | `rustql.db` |

Use `EngineOptions::default()` to open the default disk-backed engine at `rustql.db`.

## Project structure

| File | Purpose |
|------|---------|
| `main.rs` | Interactive REPL and result renderer |
| `lib.rs` | Library entry point exporting the typed engine/session API |
| `engine.rs` | `Engine`, `Session`, and typed result types |
| `lexer.rs` | Tokenizer |
| `parser.rs` | Recursive-descent SQL parser |
| `ast.rs` | Abstract syntax tree types |
| `database.rs` | Core `Database`, `Table`, and `Index` structures |
| `executor.rs` | Statement executor (DDL, DML, constraints, foreign keys) |
| `planner.rs` | Cost-based query planner |
| `plan_executor.rs` | Executes optimized query plans |
| `storage.rs` | Disk storage engine, file format, and recovery journal |
| `testing.rs` | Hidden compatibility helpers used by the test suite |
| `wal.rs` | Write-ahead log for transaction rollback |
| `error.rs` | Error types |

## Roadmap

- [v1 Breaking Roadmap](docs/v1-breaking-roadmap.md)

## Testing

Run the full test suite:

```sh
cargo test
```

Run SQL logic regression corpus:

```sh
cargo test sql_logic_corpus
```

Run microbenchmarks:

```sh
cargo bench --bench engine
```

## Commit checks

Enable the repo-managed git hooks so every commit runs formatting and lints:

```sh
git config core.hooksPath .githooks
```

The pre-commit hook enforces:
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
