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
Table created: users

rustql> INSERT INTO users VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com');
Inserted 2 row(s)

rustql> SELECT * FROM users WHERE name LIKE 'A%';
id | name | email
---+------+-----------------
1  | Alice | alice@example.com

rustql> exit
```

You can also pipe queries through stdin:

```sh
echo "SHOW TABLES" | cargo run --release
```

For explicit backend selection from the CLI:

```sh
cargo run --release -- --storage memory
cargo run --release -- --storage json --json-path /tmp/rustql.json
cargo run --release -- --storage btree --btree-path /tmp/rustql.dat
```

For embedding, build an engine instance directly:

```rust
use rustql::Engine;

let engine = Engine::builder()
    .json_file("/tmp/rustql.json")
    .build();

engine
    .process_query("CREATE TABLE users (id INTEGER)")
    .unwrap();
```

## Storage backends

| Backend | File | Activation |
|---------|------|------------|
| JSON (default) | `rustql_data.json` | default |
| B-tree | `rustql_btree.dat` | `RUSTQL_STORAGE=btree` |

The **JSON** backend serializes the entire database to a single JSON file on every write. It is human-readable and simple to debug.

The **B-tree** backend stores data in a page-based binary format (4 KB pages) with an LRU page cache, making it more efficient for larger datasets.

```sh
RUSTQL_STORAGE=btree cargo run --release
```

Override the default file locations with:

```sh
RUSTQL_JSON_PATH=/tmp/rustql.json cargo test
RUSTQL_BTREE_PATH=/tmp/rustql.dat RUSTQL_STORAGE=btree cargo run --release
```

## Project structure

| File | Purpose |
|------|---------|
| `main.rs` | Interactive REPL |
| `lib.rs` | Library entry point; tokenize &rarr; parse &rarr; execute pipeline |
| `lexer.rs` | Tokenizer |
| `parser.rs` | Recursive-descent SQL parser |
| `ast.rs` | Abstract syntax tree types |
| `database.rs` | Core `Database`, `Table`, and `Index` structures |
| `executor.rs` | Statement executor (DDL, DML, constraints, foreign keys) |
| `planner.rs` | Cost-based query planner |
| `plan_executor.rs` | Executes optimized query plans |
| `storage.rs` | Pluggable storage engines (JSON and B-tree) |
| `wal.rs` | Write-ahead log for transaction rollback |
| `error.rs` | Error types |

## Testing

Run the full test suite with the default JSON backend:

```sh
cargo test
```

Run tests against the B-tree backend:

```sh
RUSTQL_STORAGE=btree cargo test
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
