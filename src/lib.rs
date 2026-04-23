//! Stable public API for embedding RustQL.
//!
//! The default public surface is the engine, session, query result/value types,
//! and errors. Parser, AST, catalog, lexer, and planner internals are available
//! only with the `testing-api` feature.

#[cfg(feature = "testing-api")]
#[doc(hidden)]
pub mod ast;
#[cfg(not(feature = "testing-api"))]
#[allow(dead_code)]
pub(crate) mod ast;

#[cfg(feature = "testing-api")]
#[doc(hidden)]
pub mod database;
#[cfg(not(feature = "testing-api"))]
#[allow(dead_code)]
pub(crate) mod database;

#[cfg(feature = "testing-api")]
#[doc(hidden)]
pub mod binder;
#[cfg(not(feature = "testing-api"))]
#[allow(dead_code)]
pub(crate) mod binder;

pub mod engine;
pub mod error;
mod executor;

#[cfg(feature = "testing-api")]
#[doc(hidden)]
pub mod lexer;
#[cfg(not(feature = "testing-api"))]
#[allow(dead_code)]
pub(crate) mod lexer;

#[cfg(feature = "testing-api")]
#[doc(hidden)]
pub mod parser;
#[cfg(not(feature = "testing-api"))]
#[allow(dead_code)]
pub(crate) mod parser;

#[allow(dead_code)]
mod plan_executor;

#[cfg(feature = "testing-api")]
#[doc(hidden)]
pub mod planner;
#[cfg(not(feature = "testing-api"))]
#[allow(dead_code)]
pub(crate) mod planner;

#[allow(dead_code)]
mod storage;
#[allow(dead_code)]
mod wal;

pub use ast::{DataType, Value};
pub use engine::{
    ColumnMeta, CommandResult, CommandTag, Engine, EngineOptions, ExplainAnalyzeResult, PlanTree,
    QueryResult, Row, RowBatch, Session, StorageMode,
};
pub use error::{ConstraintKind, Result, RustqlError};

#[cfg(feature = "testing-api")]
#[doc(hidden)]
pub use database::Database;
