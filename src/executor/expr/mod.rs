use crate::ast::*;
use crate::database::DatabaseCatalog;
use crate::error::RustqlError;
use std::cmp::Ordering;

mod cast;
mod compare;
mod date;
mod functions;
mod predicate;
mod value;

pub(crate) use cast::coerce_value_for_type;
pub use compare::{
    apply_arithmetic, compare_values, compare_values_for_sort, compare_values_same_type,
    format_value,
};
pub use predicate::evaluate_expression;
pub use value::{evaluate_value_expression, evaluate_value_expression_with_db};
