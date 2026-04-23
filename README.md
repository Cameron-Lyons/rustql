# RustQL

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

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
- `BEGIN` / `COMMIT` / `ROLLBACK`; B-tree storage adds durable commit recovery

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
use rustql::Engine;

let engine = Engine::in_memory().unwrap();
let mut session = engine.session();

session.execute_one("CREATE TABLE users (id INTEGER)").unwrap();
let results = session.execute("SELECT * FROM users").unwrap();
```

`Engine` is the connection boundary. A `Session` is a lightweight borrow of an
engine, so transactions and savepoints are shared by all sessions created from
the same engine. Open a separate `Engine` when you need an independent
transaction context.

## Storage modes

| Mode | Description | Default | Storage guarantee |
|------|-------------|---------|-------------------|
| `StorageMode::Memory` | In-memory engine state | no | No persistence. |
| `StorageMode::Json { path }` | Human-readable snapshot storage for debugging, tests, and demos | `rustql_data.json` | Raw JSON database snapshot with atomic whole-file replacement. It has no format version, no durable transaction journal, and no recovery path for interrupted transactions. |
| `StorageMode::BTree { path }` | Page-formatted B-tree snapshot storage | no | Versioned file format with atomic whole-file snapshot replacement. The versioned `.wal` journal records prepared snapshot redo frames for committed transaction recovery, not incremental per-mutation replay. |
| `StorageMode::Disk { path }` | Deprecated alias for B-tree storage | no | Same guarantee as `StorageMode::BTree`. |

Use `EngineOptions::default()` to open the compatibility default JSON-backed engine at `rustql_data.json`.
Use `StorageMode::BTree` or set `RUSTQL_STORAGE=btree` for the durable CLI store at `rustql_btree.dat`.
Set `RUSTQL_STORAGE_PATH=/path/to/file` to override the selected storage file.

`StorageMode::BTree` has a page-based file layout for compatibility and
recovery, but saves are snapshot writes: RustQL serializes the current
in-memory database into a fresh B-tree page image, syncs it, and swaps it into
place. It should not be treated as an incremental storage engine for large
write-heavy datasets yet.

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
| `executor/` | Statement executor (DDL, DML, constraints, foreign keys) |
| `planner.rs` | Cost-based query planner |
| `plan_executor.rs` | Executes optimized query plans |
| `storage.rs` | JSON and B-tree storage engines, file format, and recovery journal |
| `tests/common/` | Test harness helpers and compatibility result renderer |
| `wal.rs` | Write-ahead log for transaction rollback |
| `error.rs` | Error types |

## Roadmap

- [v1 Status Matrix](docs/v1-breaking-roadmap.md)

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

## License

MIT. See [LICENSE](LICENSE).
