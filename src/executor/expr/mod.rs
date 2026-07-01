use crate::ast::*;
use crate::database::DatabaseCatalog;
use crate::error::RustqlError;
use std::cmp::Ordering;

mod cast;
mod compare;
mod date;
mod functions;
mod predicate;
mod row_identity;
mod value;

pub(crate) use cast::coerce_value_for_type;
pub use compare::{
    apply_arithmetic, compare_order_values, compare_values, compare_values_for_sort,
    compare_values_same_type, format_value,
};
pub use predicate::{evaluate_expression, evaluate_predicate_value};
pub(crate) use row_identity::{
    SqlRowMultiset, SqlRowSet, row_has_finite_numeric_value, rows_equal_for_sql_identity,
    values_equal_for_sql_identity,
};
pub use value::{evaluate_value_expression, evaluate_value_expression_with_db};
