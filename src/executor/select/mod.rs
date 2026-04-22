use crate::ast::*;
use crate::database::{Database, DatabaseCatalog, Table};
use crate::engine::QueryResult;
use crate::error::RustqlError;
use crate::plan_executor::PlanExecutor;
use crate::planner;
use std::collections::HashMap;

use super::expr::evaluate_value_expression;
use super::{ExecutionContext, SelectResult, get_database_read, get_database_write, rows_result};

mod core;
mod materialize;
mod window;

pub use core::execute_select;
pub(crate) use core::{execute_select_internal, explain_select};

use core::execute_select_in_db;
use materialize::*;
use window::resolve_window_definitions;
