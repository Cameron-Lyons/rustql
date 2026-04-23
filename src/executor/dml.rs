use crate::ast::*;
use crate::database::Database;
use crate::engine::{CommandTag, QueryResult};
use crate::error::{ConstraintKind, RustqlError};
use crate::wal::WalEntry;
use std::collections::HashSet;

mod constraints;
mod delete;
mod generated;
mod insert;
mod merge;
mod update;

pub(crate) use delete::execute_delete;
pub(crate) use insert::execute_insert;
pub(crate) use merge::execute_merge;
pub(crate) use update::execute_update;

use constraints::{
    find_conflict_row, handle_foreign_keys_for_delete, handle_foreign_keys_for_update,
    validate_check_constraints, validate_foreign_keys_for_insert, validate_foreign_keys_for_update,
    validate_not_null_constraints, validate_primary_keys_for_insert,
    validate_table_constraints_for_insert, validate_unique_constraints_for_insert,
};
use generated::{
    coerce_row_to_column_types, evaluate_generated_columns, evaluate_generated_columns_update,
};

use super::expr::{
    coerce_value_for_type, evaluate_expression, evaluate_value_expression,
    evaluate_value_expression_with_db,
};
use super::{
    ExecutionContext, SelectResult, command_result, ddl, get_database_read, get_database_write,
    record_wal_entry, rows_result, save_if_not_in_transaction, select,
};

fn format_returning(
    returning: &[Column],
    columns: &[ColumnDefinition],
    rows: &[Vec<Value>],
) -> Result<QueryResult, RustqlError> {
    let mut headers: Vec<String> = Vec::new();
    for col in returning {
        match col {
            Column::All => {
                for c in columns {
                    headers.push(c.name.clone());
                }
            }
            Column::Named { name, alias } => {
                headers.push(alias.clone().unwrap_or_else(|| name.clone()));
            }
            Column::Expression { alias, .. } => {
                headers.push(alias.clone().unwrap_or_else(|| "?column?".to_string()));
            }
            _ => {
                headers.push("?column?".to_string());
            }
        }
    }

    let mut projected_rows = Vec::new();

    for row in rows {
        let mut projected: Vec<Value> = Vec::new();
        for col in returning {
            match col {
                Column::All => {
                    for v in row {
                        projected.push(v.clone());
                    }
                }
                Column::Named { name, .. } => {
                    let col_name = if name.contains('.') {
                        name.split('.').next_back().unwrap_or(name)
                    } else {
                        name.as_str()
                    };
                    let idx = columns
                        .iter()
                        .position(|c| c.name == col_name)
                        .ok_or_else(|| RustqlError::ColumnNotFound(name.clone()))?;
                    projected.push(row[idx].clone());
                }
                Column::Expression { expr, .. } => {
                    let val = evaluate_value_expression(expr, columns, row)?;
                    projected.push(val);
                }
                _ => {
                    projected.push(Value::Null);
                }
            }
        }
        projected_rows.push(projected);
    }

    Ok(rows_result(SelectResult {
        headers,
        rows: projected_rows,
    }))
}
