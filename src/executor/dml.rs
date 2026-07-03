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
    evaluate_value_expression_with_db, values_equal_for_sql_identity,
};
use super::{
    ExecutionContext, SelectResult, command_result, ddl, get_database_read, get_database_write,
    record_wal_entry, rows_result, save_if_not_in_transaction, select,
};

fn column_default_value(column: &ColumnDefinition) -> Value {
    column.default_value.clone().unwrap_or(Value::Null)
}

fn evaluate_assignment_value(
    expr: &Expression,
    column: &ColumnDefinition,
    columns: &[ColumnDefinition],
    row: &[Value],
    db: &dyn crate::database::DatabaseCatalog,
) -> Result<Value, RustqlError> {
    match expr {
        Expression::Default => Ok(column_default_value(column)),
        _ => evaluate_value_expression_with_db(expr, columns, row, Some(db)),
    }
}

fn evaluate_merge_insert_value(
    expr: &Expression,
    column: &ColumnDefinition,
    columns: &[ColumnDefinition],
    row: &[Value],
) -> Result<Value, RustqlError> {
    match expr {
        Expression::Default => Ok(column_default_value(column)),
        _ => evaluate_value_expression(expr, columns, row),
    }
}

#[derive(Clone)]
struct DmlJoinedSource {
    columns: Vec<ColumnDefinition>,
    rows: Vec<Vec<Value>>,
}

fn build_joined_dml_source(
    db: &Database,
    table: &str,
    alias: Option<&str>,
    joins: &[Join],
    context: &str,
) -> Result<DmlJoinedSource, RustqlError> {
    let base_table = db
        .tables
        .get(table)
        .ok_or_else(|| RustqlError::TableNotFound(table.to_string()))?;
    let base_label = alias.unwrap_or(table);
    let mut source = DmlJoinedSource {
        columns: qualify_columns(&base_table.columns, base_label),
        rows: base_table.rows.clone(),
    };

    for join in joins {
        source = apply_joined_dml_source_join(db, source, join, context)?;
    }

    Ok(source)
}

fn apply_joined_dml_source_join(
    db: &Database,
    left: DmlJoinedSource,
    join: &Join,
    context: &str,
) -> Result<DmlJoinedSource, RustqlError> {
    if !matches!(
        join.join_type,
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
    ) {
        return Err(RustqlError::TypeMismatch(format!(
            "{} joins support INNER, LEFT, RIGHT, and FULL joins",
            context
        )));
    }
    if join.subquery.is_some() || join.using_columns.is_some() || join.lateral {
        return Err(RustqlError::TypeMismatch(format!(
            "{} joins only support table sources with ON conditions",
            context
        )));
    }
    let on_expr = join.on.as_ref().ok_or_else(|| {
        RustqlError::TypeMismatch(format!("{} JOIN requires an ON condition", context))
    })?;
    let right_table = db
        .tables
        .get(&join.table)
        .ok_or_else(|| RustqlError::TableNotFound(join.table.clone()))?;
    let right_label = join.table_alias.as_deref().unwrap_or(&join.table);
    let right_columns = qualify_columns(&right_table.columns, right_label);
    let right_rows = right_table.rows.clone();

    let mut joined_columns = left.columns.clone();
    joined_columns.extend(right_columns.clone());

    let mut joined_rows = Vec::new();
    let mut matched_right = vec![false; right_rows.len()];
    for left_row in &left.rows {
        let mut has_match = false;
        for (right_idx, right_row) in right_rows.iter().enumerate() {
            let mut joined_row = left_row.clone();
            joined_row.extend(right_row.clone());

            if evaluate_expression(Some(db), on_expr, &joined_columns, &joined_row)? {
                joined_rows.push(joined_row);
                has_match = true;
                matched_right[right_idx] = true;
            }
        }

        if matches!(join.join_type, JoinType::Left | JoinType::Full) && !has_match {
            let mut joined_row = left_row.clone();
            joined_row.extend(std::iter::repeat_n(Value::Null, right_columns.len()));
            joined_rows.push(joined_row);
        }
    }

    if matches!(join.join_type, JoinType::Right | JoinType::Full) {
        for (right_idx, right_row) in right_rows.iter().enumerate() {
            if !matched_right[right_idx] {
                let mut joined_row = vec![Value::Null; left.columns.len()];
                joined_row.extend(right_row.clone());
                joined_rows.push(joined_row);
            }
        }
    }

    Ok(DmlJoinedSource {
        columns: joined_columns,
        rows: joined_rows,
    })
}

fn qualify_columns(columns: &[ColumnDefinition], relation: &str) -> Vec<ColumnDefinition> {
    columns
        .iter()
        .map(|column| {
            let mut column = column.clone();
            column.name = format!("{}.{}", relation, column.name);
            column
        })
        .collect()
}

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
