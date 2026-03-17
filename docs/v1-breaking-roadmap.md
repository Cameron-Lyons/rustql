# RustQL v1 Breaking Roadmap

This document defines a concrete migration from the current v0 architecture to a deliberately breaking v1.

## Goals

- Replace string-oriented APIs with typed query results.
- Remove global mutable singletons to support multiple independent engines/sessions.
- Make planner and executor a single pipeline used by both `SELECT` and `EXPLAIN`.
- Move to a durable transaction and storage model with explicit format versioning.
- Tighten SQL/type semantics where current behavior is ambiguous.

## Non-goals

- Full PostgreSQL compatibility in v1.
- Distributed storage or network protocol support.
- Performance tuning before correctness and API clarity.

## Target v1 Architecture

### 1) Engine + Session API

```rust
pub struct Engine { /* storage, catalog, config */ }
pub struct Session<'e> { /* txn state, temp objects, settings */ }

pub enum QueryResult {
    Rows { columns: Vec<ColumnMeta>, rows: Vec<Row> },
    Command { tag: CommandTag, affected: u64 },
    Explain { plan: PlanTree },
}

impl Engine {
    pub fn open(opts: EngineOptions) -> Result<Self, RustqlError>;
    pub fn session(&self) -> Session<'_>;
}

impl Session<'_> {
    pub fn execute(&mut self, sql: &str) -> Result<Vec<QueryResult>, RustqlError>;
}
```

Key break:
- Remove `process_query(&str) -> Result<String, String>`.

### 2) Typed execution boundary

- All internal execution paths return typed row batches.
- Text formatting is done only in CLI adapters.
- Eliminate parse-from-string roundtrips in `INSERT ... SELECT`, aggregate, and grouping paths.

### 3) Transaction and storage model

- WAL is persisted to disk, replayed on startup, and checkpointed.
- Savepoints only valid inside explicit transactions.
- Storage format has required version header + migration path.

### 4) Data model updates

- Replace row-position identity (`usize`) with stable row IDs.
- Index entries point to row IDs, not vector positions.
- Type comparison and ordering reject undefined cross-type comparisons unless explicit casts exist.

## Milestones

## M0: Branch Cut + Safety Rails (1 week)

Deliverables:
- Create `v1` feature branch and mark `main` as v0 maintenance-only.
- Add architecture decision records in `docs/adr/` for API, storage, txn model.
- Add test harness scaffolding for API-level and storage-level tests.

Acceptance criteria:
- ADRs approved.
- CI has separate v0 and v1 jobs.

## M1: New Public API Surface (1 week)

Deliverables:
- Introduce `Engine`, `Session`, `QueryResult`, `CommandTag`, `ColumnMeta`.
- New error type boundary returns `RustqlError` directly (no `String` conversion).
- CLI rewritten to call `Session::execute` and render `QueryResult`.

Breaking changes:
- Remove old `process_query` export from default API.

Acceptance criteria:
- REPL and stdin modes still work.
- Existing SQL smoke tests pass through new API adapter.

## M2: Remove Global Singletons (1 week)

Deliverables:
- Delete global `DATABASE`, `TRANSACTION_WAL`, and storage singleton usage.
- Thread `&Engine`/`&mut Session` through parser/planner/executor paths.
- Make tests instantiate isolated engines with temporary storage roots.

Breaking changes:
- No implicit global DB state in library mode.

Acceptance criteria:
- Tests can run in parallel with isolated sessions.
- Two independent engines in one process do not share state.

## M3: Typed Executor Refactor (2 weeks)

Deliverables:
- Convert select/dml/ddl paths to return typed results only.
- Remove all internal text parsing (`parse_value_from_string` loops used as internal glue).
- Introduce unified output structs for rows and schema metadata.

Breaking changes:
- Internal modules no longer expose text formatting utilities as primary interfaces.

Acceptance criteria:
- `INSERT ... SELECT`, grouped queries, aggregates, and set ops have zero text roundtrip paths.
- Result correctness parity with v0 test corpus.

## M4: Unified Planner/Executor Pipeline (2 weeks)

Deliverables:
- `SELECT` execution goes through planner -> physical plan -> plan executor.
- `EXPLAIN` prints the same plan nodes used for execution.
- Remove duplicate execution logic from legacy select paths.

Breaking changes:
- Plan shape and explain text can change.

Acceptance criteria:
- No direct bypass of planner for standard `SELECT` paths.
- Plan-based tests cover scans, joins, filters, aggregation, limits, sorts.

## M5: Row IDs + Index Redesign (2 weeks)

Deliverables:
- Add row ID allocation per table.
- Change index payload from `Vec<usize>` to `Vec<RowId>`.
- Rework DML, FK checks, and join code to operate on stable IDs.

Breaking changes:
- On-disk index representation incompatible with v0.

Acceptance criteria:
- Deletes/updates no longer cause index pointer drift.
- WAL rollback works correctly after mixed DML sequences.

## M6: Durable WAL + Transaction Semantics (2 weeks)

Deliverables:
- Write WAL records to disk before applying committed changes.
- Crash recovery applies/reverts incomplete transactions deterministically.
- Savepoint semantics aligned to explicit transaction scope.

Breaking changes:
- `SAVEPOINT` outside transaction returns error.
- Transaction edge-case behavior may differ from v0.

Acceptance criteria:
- Recovery tests pass: crash-before-commit, crash-after-commit, nested savepoints.
- No data loss in power-failure simulation tests.

## M7: Storage Format v2 + Migration Tooling (2 weeks)

Deliverables:
- Define storage header with `magic + major/minor`.
- Implement online/offline migration command:
  - `rustql migrate --from v0 --to v1 --input <path> --output <path> --backup`.
- Keep JSON debug export/import utilities for diagnostics.

Breaking changes:
- v1 does not read raw v0 files directly in normal startup path.

Acceptance criteria:
- Migration tool handles JSON and B-tree v0 inputs.
- Migrated DB passes validation checks and golden query suite.

## M8: Type Semantics Tightening + Final Cleanup (1-2 weeks)

Deliverables:
- Enforce explicit coercion rules for comparisons/sorts.
- Date/time types validated and normalized on write.
- Remove deprecated v0 compatibility shims.

Breaking changes:
- Queries that relied on permissive mixed-type behavior now fail with clear errors.

Acceptance criteria:
- Type behavior documented with examples.
- Compatibility mode removed or disabled by default at GA.

## Migration Shim Strategy

Shims exist only to reduce adoption pain during v1 pre-GA and early GA.

## Shim A: Legacy API adapter

Provide `rustql::legacy` module (or separate `rustql-compat` crate):

```rust
pub fn process_query(sql: &str) -> Result<String, String>
```

Implementation:
- Internally creates/uses a process-local default `Engine` + `Session`.
- Executes SQL using v1 typed API.
- Formats typed results into v0-style text table output.

Removal window:
- Available through v1.2.
- Removed in v1.3.

## Shim B: Legacy output formatter

- Keep `legacy::format_query_result(&QueryResult) -> String` for clients that still consume text.
- Encourage direct typed consumption via examples and docs.

Removal window:
- Deprecated in v1.1.
- Removed in v1.3.

## Shim C: SQL compatibility mode

Session setting:
- `SET compatibility_mode = 'v0';`

Scope:
- Limited to known semantic differences only (for example mixed-type ordering fallback).
- Explicit warning emitted in command result metadata.

Removal window:
- Off by default in v1.0.
- Removed in v1.2.

## Shim D: Storage migration bridge

- `rustql migrate` utility performs one-time conversion and validation.
- Startup error in v1 when opening unknown/old format includes exact migration command.

Removal window:
- Keep indefinitely for at least one major cycle.

## Release Timeline

- v1.0.0-alpha1: M1-M3 complete, legacy API shim included.
- v1.0.0-beta1: M4-M6 complete, migration tool available.
- v1.0.0-rc1: M7 complete, compatibility mode off by default.
- v1.0.0: M8 complete, docs + migration guides finalized.
- v1.2.0: remove compatibility mode.
- v1.3.0: remove legacy API/output shims.

## Workstreams and Ownership

- API/runtime: `src/lib.rs`, `src/main.rs`, executor boundary.
- Planner/executor unification: `src/planner.rs`, `src/plan_executor.rs`, `src/executor/select.rs`.
- Storage/WAL: `src/storage.rs`, `src/wal.rs`, `src/database.rs`.
- Migration tooling: new `src/bin/rustql-migrate.rs` + validation checks.
- Test migration: `tests/` split into `tests/v0_compat/` and `tests/v1/`.

## Test and Quality Gates

Required gates per milestone:

- Unit tests for all new public API types and error paths.
- Differential tests comparing v0/v1 behavior where parity is required.
- Crash/recovery tests for WAL durability milestones.
- Storage format fuzz tests for header parsing and corruption handling.
- Performance baseline checks on representative read/write workloads.

## Known Risks and Mitigations

- Risk: Refactor scope too broad.
  - Mitigation: enforce milestone boundaries; no cross-milestone spillover without sign-off.
- Risk: Compatibility shim becomes permanent.
  - Mitigation: time-box removals in milestone and release criteria.
- Risk: Query regressions during planner unification.
  - Mitigation: run full existing suite + add plan-shape golden tests.
- Risk: Migration tool trust gap.
  - Mitigation: checksums, dry-run mode, and post-migration validator.

## Definition of Done (v1 GA)

- New API is the only default public API.
- No global singleton state in runtime path.
- Planner drives all query execution paths.
- Durable WAL recovery demonstrated in automated crash tests.
- Storage v2 fully versioned and migration documented.
- Compatibility shims are optional and clearly deprecated.
