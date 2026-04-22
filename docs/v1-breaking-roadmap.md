# RustQL v1 Current Status Matrix

Last audited: 2026-04-22.

This document replaces the old milestone roadmap with the current state of the
v1 work in this repository. It records what is implemented now, where the
runtime still has compatibility or fallback paths, and what remains open.

## Status Key

- Done: implemented in the current runtime path.
- Partial: implemented for some paths, with known cleanup or coverage gaps.
- Open: not present in the current tree.
- Deferred: not part of current v1 scope unless explicitly reopened.

## Goals

- Replace string-oriented APIs with typed query results.
- Remove global mutable singletons so multiple engines and sessions can run in
  one process.
- Make planner and executor a single pipeline used by both `SELECT` and
  `EXPLAIN`.
- Move to a durable transaction and storage model with explicit format
  versioning.
- Tighten SQL/type semantics where current behavior is ambiguous.

## Non-goals

- Full PostgreSQL compatibility.
- Distributed storage or a network protocol.
- Performance tuning before correctness and API clarity.

## Current Status

| Area | Status | Current implementation | Remaining work |
|------|--------|------------------------|----------------|
| Public engine/session API | Done | `src/engine.rs` defines `EngineOptions`, `Engine`, `Session`, `QueryResult`, `CommandTag`, `ColumnMeta`, and `RowBatch`; `src/lib.rs` exports the typed API. The CLI opens an engine and renders typed results at the edge. | Keep README/API examples aligned with the typed API. |
| Legacy `process_query` API | Done | No `process_query` function is exported from `src/` or kept in test helpers; `tests/common/mod.rs` uses the typed engine API and renders only at assertion boundaries. | Add a separate public compatibility module only if there is a supported migration need. |
| Global runtime state | Done | `Engine` owns an `ExecutionContext` with per-engine `Database`, `WalState`, and optional storage. Tests cover isolated engines and transaction state. | None known. |
| Typed execution boundary | Done | Public execution returns typed `QueryResult` values, and CLI/test renderers convert to text outside the API boundary. `SELECT` results are converted to `RowBatch`, and old internal text parsing/aggregate table-formatting helpers have been removed. | None known. |
| Planner/executor pipeline | Done | `SELECT`, `EXPLAIN`, and `EXPLAIN ANALYZE` use planner-backed `PlanNode` execution through `PlanExecutor`; the old select fallback gate has been removed. | None known. |
| Row IDs and index storage | Done | `src/database.rs` has `RowId`, per-table `row_ids`, and `next_row_id`. Regular and composite indexes store `Vec<RowId>`, and DML, WAL rollback, index maintenance, and storage normalization use stable row IDs. | Keep new table/index work on row IDs; do not persist vector positions as row identity. |
| Transactions and WAL | Partial | `WalState` supports rollback and savepoints. B-tree storage writes a versioned `.wal` transaction journal for pending and committed states and recovers committed journals on load. JSON storage is intentionally debug/demo snapshot storage, not a durable transactional backend. | The B-tree journal is commit-recovery support, not a general replay log of every mutation. |
| Storage format versioning | Done | B-tree files use magic/version headers and currently read legacy version 2 plus the current version 3. The B-tree journal also has a magic/version header. JSON storage remains raw JSON by design because it is not a durable storage format. | Add a JSON envelope only if JSON is promoted beyond debug/demo snapshot storage. |
| Migration tooling | Open | There is no `src/bin` migration tool and no `rustql migrate` command. | Add a converter and validator only if v0 files need one-way migration into the current storage format. |
| Type semantics | Partial | Mixed nonnumeric comparisons return type mismatch errors; integer/float comparisons are intentionally numeric-compatible; date/time casts exist. | Normalize date/time values on write, tighten casts that still accept arbitrary text, and document sort/coercion rules. |
| Compatibility mode and shims | Deferred | No runtime compatibility mode, public legacy module, or hidden `Database::load/save` env shim exists. | Reopen only if users need a supported transition window. |
| Test and CI gates | Partial | GitHub Actions runs MSRV check, fmt, clippy, build, tests, and a benchmark smoke profile. Tests cover API, planner, recovery, typed rows, and row IDs. | There are no separate v0/v1 jobs, ADRs, storage fuzz tests, or migration validation suite. |

## Current Priorities

| Priority | Work | Reason |
|----------|------|--------|
| 1 | Keep planner-backed SELECT execution as the only runtime path. | New SELECT features should be implemented in the planner and `PlanExecutor` instead of adding side execution paths. |
| 2 | Add migration/export tooling only if v0 users need it. | JSON is now documented as debug/demo snapshot storage; durable storage guarantees belong to B-tree. |
| 3 | Keep stale internal compatibility helpers out. | The typed execution boundary is now cleaner; new compatibility helpers should be added only with an explicit migration need. |
| 4 | Document and test type coercion rules. | Mixed-type comparisons are stricter now, but cast and date/time behavior still needs a crisp contract. |
